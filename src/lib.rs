//! A thread pool for running synchronous I/O in asynchronous applications.
//!
//! This crate is low-level, and is meant to be used as a building block for higher level
//! asynchronous runtimes.
#![warn(missing_debug_implementations, missing_docs)]

use std::borrow::Cow;
use std::collections::VecDeque;
use std::mem;
use std::panic;
use std::sync::Arc;
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// A thread pool.
///
/// This can be cheaply cloned to create more handles to the same thread pool, so there is no need
/// to wrap it in an [`Arc`] or similar type.
///
/// When dropped, the destructor won't block but the thread pool itself will continue to run and
/// process tasks. You can call [`ThreadPool::wait_all_complete`] to wait for all the active tasks
/// to complete.
#[derive(Debug, Clone)]
pub struct ThreadPool {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// The mutable part of the shared state.
    locked: Mutex<Locked>,

    /// The condvar that pool threads wait on for work to come in or pruning to start. This is
    /// associated with the above mutex.
    thread_condvar: Condvar,

    /// The condvar that is notified when all work is complete. This is also associated with the
    /// above mutex.
    all_complete: Condvar,

    /// The name of spawned threads.
    thread_name: Cow<'static, str>,

    /// The stack size of spawned threads.
    thread_stack_size: Option<usize>,

    /// The duration a thread waits without any work to be performed before exiting.
    idle_timeout: Duration,
}

#[derive(Debug)]
struct Locked {
    /// The queue of functions that need to be run.
    work: VecDeque<RawFunction>,

    /// The number of threads that need to be pruned.
    to_prune: usize,

    /// The number of threads currently waiting for new work to come in.
    sleeping_threads: usize,

    /// The number of threads currently doing work.
    workers: usize,

    /// The number of threads that can be spawned.
    spawnable: usize,

    /// Miri doesn't support threads that outlive `main`, so we make sure to wait on them in that
    /// case.
    #[cfg(miri)]
    join_handles: Vec<thread::JoinHandle<()>>,
}

impl Inner {
    /// The main loop run by each worker thread.
    fn thread_loop(&self) {
        let mut locked = self.locked.lock().unwrap();

        loop {
            if let Some(f) = locked.work.pop_front() {
                // There is a function to run, so run it.
                locked.workers += 1;
                drop(locked);

                // Catching unwinds is done inside the function itself.
                unsafe { (f.run)(f.data) };

                locked = self.locked.lock().unwrap();
                locked.workers -= 1;
                if locked.workers == 0 && locked.work.is_empty() {
                    self.all_complete.notify_all();
                }
            } else if locked.to_prune > 0 {
                locked.to_prune -= 1;
                break;
            } else {
                // There are no functions to run; wait with a timeout.
                locked.sleeping_threads += 1;

                let timed_out = if cfg!(miri) {
                    locked = self.thread_condvar.wait(locked).unwrap();
                    false
                } else {
                    let (new_locked, wait_res) = self
                        .thread_condvar
                        .wait_timeout(locked, self.idle_timeout)
                        .unwrap();
                    locked = new_locked;
                    wait_res.timed_out()
                };

                locked.sleeping_threads -= 1;

                if timed_out {
                    break;
                }
            }
        }

        locked.spawnable += 1;
    }

    /// Start a new worker thread.
    fn start_thread(self: Arc<Self>) {
        #[cfg(miri)]
        let self_2 = Arc::clone(&self);

        let mut builder = thread::Builder::new();
        // https://github.com/rust-lang/miri/issues/1717
        if cfg!(not(miri)) {
            builder = builder.name(self.thread_name.clone().into_owned());
        }
        if let Some(stack_size) = self.thread_stack_size {
            builder = builder.stack_size(stack_size);
        }
        let handle = builder
            .spawn(move || self.thread_loop())
            .expect("failed to spawn worker thread");

        #[cfg(miri)]
        {
            self_2.locked.lock().unwrap().join_handles.push(handle);
        }
        #[cfg(not(miri))]
        drop(handle);
    }
}

impl ThreadPool {
    /// Construct a new thread pool using the default configuration. See [`Builder`] for how to
    /// customize it further.
    #[must_use]
    pub fn new() -> Self {
        Builder::new().build()
    }

    /// Create a [`Builder`] for customizing a thread pool.
    #[must_use]
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Raw API for spawning a function on the thread pool.
    ///
    /// This will call the given function pointer with the provided `data` pointer on the blocking
    /// thread pool at some point in the future.
    ///
    /// # Safety
    ///
    /// When called with the provided data pointer from another thread, the `run` function must not
    /// cause UB or panic.
    pub unsafe fn spawn_raw<T>(&self, data: *const T, run: unsafe fn(*const T)) {
        let raw = RawFunction {
            data: data.cast(),
            run: mem::transmute::<unsafe fn(*const T), unsafe fn(*const ())>(run),
        };

        let mut locked = self.inner.locked.lock().unwrap();
        locked.work.push_back(raw);

        if locked.sleeping_threads == 0 {
            if let Some(new_spawnable) = locked.spawnable.checked_sub(1) {
                locked.spawnable = new_spawnable;
                drop(locked);
                self.inner.clone().start_thread();
            }
        } else {
            self.inner.thread_condvar.notify_one();
        }
    }

    /// Spawn a boxed closure on the thread pool.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let pool = blocking_pool::ThreadPool::new();
    /// pool.spawn_boxed(Box::new(|| println!("Hello world")));
    /// ```
    pub fn spawn_boxed<F: FnOnce() + Send + 'static>(&self, f: Box<F>) {
        unsafe {
            self.spawn_raw(Box::into_raw(f), |f| {
                struct AbortOnDrop;
                impl Drop for AbortOnDrop {
                    fn drop(&mut self) {
                        std::process::abort();
                    }
                }
                // Make sure that panic payloads panicking on drop don't get propagated.
                let guard = AbortOnDrop;

                let f = Box::from_raw(f as *mut F);
                let _ = panic::catch_unwind(panic::AssertUnwindSafe(f));

                mem::forget(guard);
            })
        };
    }

    /// Wait for all the running and queued tasks in the thread pool to complete.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::{Duration, Instant};
    ///
    /// let pool = blocking_pool::ThreadPool::new();
    ///
    /// pool.spawn_boxed(Box::new(|| std::thread::sleep(Duration::from_secs(3))));
    ///
    /// let start = Instant::now();
    /// pool.wait_all_complete();
    /// assert!(start.elapsed().as_secs() >= 3);
    /// ```
    pub fn wait_all_complete(&self) {
        let locked = self.inner.locked.lock().unwrap();
        let locked = self
            .inner
            .all_complete
            .wait_while(locked, |locked| {
                locked.workers != 0 || !locked.work.is_empty()
            })
            .unwrap();
        drop(locked);
    }

    /// Prune unused threads. These threads would time out and exit eventually of their own accord
    /// if they are not receiving any work, but this will force them to exit early.
    ///
    /// # Examples
    ///
    /// ```
    /// let pool = blocking_pool::ThreadPool::new();
    ///
    /// // This will start up a thread that will linger around even after the task is finished.
    /// pool.spawn_boxed(Box::new(|| {}));
    ///
    /// // This will forcibly kill that thread.
    /// pool.prune();
    /// ```
    pub fn prune(&self) {
        let mut locked = self.inner.locked.lock().unwrap();
        locked.to_prune = locked.sleeping_threads;
        self.inner.thread_condvar.notify_all();
    }

    /// Joins all threads when Miri is enabled. This should be called at the end of code that needs
    /// to pass Miri to avoid having threads that last longer than the main one, which Miri doesn't
    /// support.
    #[cfg(test)]
    fn miri_shutdown(&self) {
        #[cfg(miri)]
        {
            let mut locked = self.inner.locked.lock().unwrap();

            locked.to_prune = locked.sleeping_threads + locked.workers;
            self.inner.thread_condvar.notify_all();

            let join_handles = std::mem::take(&mut locked.join_handles);

            drop(locked);

            for handle in join_handles {
                handle.join().unwrap();
            }
        }
    }
}

impl Default for ThreadPool {
    fn default() -> Self {
        Self::new()
    }
}

/// A builder for a [thread pool](ThreadPool).
///
/// # Examples
///
/// ```
/// use blocking_pool::ThreadPool;
///
/// let pool = ThreadPool::builder()
///     .max_threads(128)
///     .thread_name("my-app-worker-thread")
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct Builder {
    max_threads: usize,
    thread_name: Cow<'static, str>,
    thread_stack_size: Option<usize>,
    idle_timeout: Duration,
}

impl Builder {
    /// Construct a new builder with its default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            max_threads: 512,
            thread_name: Cow::Borrowed("blocking-worker"),
            thread_stack_size: None,
            idle_timeout: Duration::from_secs(10),
        }
    }

    /// Set the maximum number of threads that can exist at a time in the thread pool. If all the
    /// threads are currently working and new work comes in, it will have to wait for a thread to
    /// be free in order to start.
    ///
    /// The default value is 512, but this may change in the future.
    #[must_use]
    pub fn max_threads(mut self, max_threads: usize) -> Self {
        self.max_threads = max_threads;
        self
    }

    /// Set the name that the spawned threads have.
    ///
    /// The default value is `blocking-worker`, but this may change in the future.
    #[must_use]
    pub fn thread_name<N: Into<Cow<'static, str>>>(mut self, name: N) -> Self {
        self.thread_name = name.into();
        self
    }

    /// Set the stack size of each spawned worker thread.
    ///
    /// If unset, currently it will use the `RUST_MIN_STACK` environment variable or default to 2
    /// MiB if that is not present, but this behaviour may change in the future.
    #[must_use]
    pub fn thread_stack_size(mut self, stack_size: usize) -> Self {
        self.thread_stack_size = Some(stack_size);
        self
    }

    /// Set the duration a worker thread waits for without any work to perform before exiting.
    ///
    /// The default value is 10 seconds, but this may change in the future.
    #[must_use]
    pub fn idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Consume this builder and return the newly created thread pool.
    #[must_use]
    pub fn build(self) -> ThreadPool {
        ThreadPool {
            inner: Arc::new(Inner {
                locked: Mutex::new(Locked {
                    work: VecDeque::new(),
                    to_prune: 0,
                    sleeping_threads: 0,
                    workers: 0,
                    spawnable: self.max_threads,
                    #[cfg(miri)]
                    join_handles: Vec::new(),
                }),
                thread_condvar: Condvar::new(),
                all_complete: Condvar::new(),
                thread_name: self.thread_name,
                thread_stack_size: self.thread_stack_size,
                idle_timeout: self.idle_timeout,
            }),
        }
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct RawFunction {
    data: *const (),
    run: unsafe fn(*const ()),
}
unsafe impl Send for RawFunction {}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    #[test]
    fn more_work_than_threads() {
        let thread_pool = Builder::new().max_threads(2).build();

        let value: AtomicUsize = AtomicUsize::new(0);
        let value: &'static AtomicUsize = unsafe { &*(&value as *const AtomicUsize) };

        thread_pool.spawn_boxed(Box::new(move || {
            assert_eq!(value.load(SeqCst), 0);
            wait();
            assert!(value.fetch_add(1, SeqCst) < 2);
        }));
        thread_pool.spawn_boxed(Box::new(move || {
            assert_eq!(value.load(SeqCst), 0);
            wait();
            assert!(value.fetch_add(1, SeqCst) < 2);
        }));
        thread_pool.spawn_boxed(Box::new(move || {
            assert!(matches!(value.load(SeqCst), 1 | 2));
            wait();
            assert_eq!(value.load(SeqCst), 2);
        }));

        thread_pool.wait_all_complete();

        assert_eq!(value.load(SeqCst), 2);

        thread_pool.miri_shutdown();
    }
}

/// Wait a duration of time.
#[cfg(test)]
fn wait() {
    if cfg!(miri) {
        for _ in 0..3_000 {
            thread::yield_now();
        }
    } else {
        thread::sleep(Duration::from_secs(1));
    }
}
