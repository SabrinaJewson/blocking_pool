//! A thread pool for running synchronous I/O in asynchronous applications.
//!
//! In asynchronous code, blocking the thread - that is calling some function which takes a long
//! time to return - is a very bad idea. It will prevent all the other asynchronous tasks from
//! running, and can cause all sorts of undesirable behaviour. However, sometimes blocking calls
//! are needed; for example many libraries are not built with `async`, but you might want to use
//! them in an `async` context. The solution is to have a thread pool where blocking code can be
//! offloaded to, so that it doesn't block the main asynchronous threads.
//!
//! In comparison with [`blocking`](https://docs.rs/blocking), another crate that provides similar
//! functionality, this crate uses a local thread pool instead of a global one. This allows for
//! multiple thread pools to be created, and each one can be configured, allowing you to fine-tune
//! your application for maximum speed. Also, this crate has support for [spawning blocking
//! functions that borrow from the outer scope][spawn_child], which is not possible in
//! `blocking`.
//!
//! # Examples
//!
//! Call [`std::fs::read_to_string`][std read_to_string] from asynchronous code:
//!
//! ```no_run
//! use blocking_pool::ThreadPool;
//!
//! # completion::completion_async! {
//! let pool = ThreadPool::new();
//! let filename = "file.txt";
//! let contents = pool.spawn_child(|| std::fs::read_to_string(&filename)).await?;
//! println!("The contents of {} is: {}", filename, contents);
//! # Ok::<(), std::io::Error>(())
//! # };
//! ```
//!
//!
//! # Tasks and Children
//!
//! [Thread pools][ThreadPool] support two methods of running functions: tasks and children,
//! spawned via [`spawn_task`][spawn_task] and [`spawn_child`][spawn_child] respectively. The most
//! important difference is that tasks are required to live for `'static`, whereas children can have
//! any lifetime, allowing them to borrow from the outer scope. The trade-off is that children
//! cannot be detached to run independently of the outer scope; once you start one, you must see it
//! to completion straight after.
//!
//! There are also a few smaller differences between the two:
//! - Tasks are spawned immediately, whereas children require the returned [`Child`][Child] to be
//! polled before it is started.
//! - [`JoinHandle`][JoinHandle] will catch panics and return an [`Err`][Err] if your function
//! panicked. [`Child`][Child] simply propagates them.
//! - [`JoinHandle`][JoinHandle] implements both [`Future`][Future] and
//! [`CompletionFuture`][CompletionFuture], whereas [`Child`][Child] only implements
//! [`CompletionFuture`][CompletionFuture].
//!
//! If you need to detach the function so that it runs in the background, use a task - otherwise,
//! use a child.
//!
//! [ThreadPool]: https://docs.rs/blocking_pool/*/blocking_pool/struct.ThreadPool.html
//! [spawn_child]: https://docs.rs/blocking_pool/*/blocking_pool/struct.ThreadPool.html#method.spawn_child
//! [Child]: https://docs.rs/blocking_pool/*/blocking_pool/struct.Child.html
//! [spawn_task]: https://docs.rs/blocking_pool/*/blocking_pool/struct.ThreadPool.html#method.spawn_task
//! [JoinHandle]: https://docs.rs/blocking_pool/*/blocking_pool/struct.JoinHandle.html
//!
//! [CompletionFuture]: https://docs.rs/completion-core/0.2/completion_core/trait.CompletionFuture.html
//!
//! [std read_to_string]: https://doc.rust-lang.org/stable/std/fs/fn.read_to_string.html
//! [Err]: https://doc.rust-lang.org/stable/core/result/enum.Result.html#variant.Err
//! [Future]: https://doc.rust-lang.org/stable/core/future/trait.Future.html
#![warn(missing_debug_implementations, missing_docs)]
#![cfg_attr(miri, allow(non_fmt_panic))]

use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::Duration;

// TSan doesn't support fences; to avoid false positives perform a load instead.
#[cfg(tsan)]
macro_rules! acquire_fence {
    ($x:expr) => {
        $x.load(::std::sync::atomic::Ordering::Acquire);
    };
}
#[cfg(not(tsan))]
macro_rules! acquire_fence {
    ($x:expr) => {
        ::std::sync::atomic::fence(::std::sync::atomic::Ordering::Acquire);
    };
}

mod task;
pub use task::JoinHandle;

mod child;
pub use child::Child;

/// A thread pool.
///
/// This can be cheaply cloned to create more handles to the same thread pool, so there is never any
/// need to wrap it in an [`Arc`] or similar type.
///
/// When dropped, the destructor won't block but the thread pool itself will continue to run and
/// process tasks.
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
                (f.run)(f.data);

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

    fn spawn_raw_by_ref(self: &Arc<Self>, raw: RawFunction) {
        let mut locked = self.locked.lock().unwrap();

        locked.work.push_back(raw);

        if locked.sleeping_threads == 0 {
            if let Some(new_spawnable) = locked.spawnable.checked_sub(1) {
                locked.spawnable = new_spawnable;
                drop(locked);
                Arc::clone(self).start_thread();
            }
        } else {
            self.thread_condvar.notify_one();
        }
    }

    fn spawn_raw(self: Arc<Self>, raw: RawFunction) {
        let mut locked = self.locked.lock().unwrap();

        locked.work.push_back(raw);

        if locked.sleeping_threads == 0 {
            if let Some(new_spawnable) = locked.spawnable.checked_sub(1) {
                locked.spawnable = new_spawnable;
                drop(locked);
                self.start_thread();
            }
        } else {
            self.thread_condvar.notify_one();
        }
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

    /// Spawn a task on this thread pool.
    ///
    /// See [Tasks and Children](index.html#tasks-and-children) for more information on the
    /// difference between this and [`spawn_child`](Self::spawn_child).
    ///
    /// # Examples
    ///
    /// Use [`std::fs::write`] in asynchronous code:
    ///
    /// ```no_run
    /// let pool = blocking_pool::ThreadPool::new();
    /// pool.spawn_task(|| std::fs::write("foo.txt", "Lorem ipsum"));
    /// ```
    pub fn spawn_task<O, F>(&self, f: F) -> JoinHandle<O>
    where
        F: FnOnce() -> O + Send + 'static,
        O: Send + 'static,
    {
        JoinHandle::new(f, &self.inner)
    }

    /// Spawn a child on this thread pool.
    ///
    /// See [Tasks and Children](index.html#tasks-and-children) for more information on the
    /// difference between this and [`spawn_task`](Self::spawn_task).
    ///
    /// # Examples
    ///
    /// Use [`std::fs::write`] in asynchronous code:
    ///
    /// ```no_run
    /// # completion::completion_async! {
    /// let pool = blocking_pool::ThreadPool::new();
    /// let data = "Lorem ipsum".to_owned();
    /// pool.spawn_child(|| std::fs::write("foo.txt", &data)).await?;
    /// # Ok::<(), std::io::Error>(())
    /// # };
    /// ```
    pub fn spawn_child<'a, O, F>(&self, f: F) -> Child<'a, O>
    where
        F: FnOnce() -> O + Send + 'a,
        O: Send,
    {
        Child::new(f, Arc::clone(&self.inner))
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
    /// pool.spawn_task(|| std::thread::sleep(Duration::from_secs(3)));
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
    /// # completion::future::block_on(completion::completion_async! {
    /// let pool = blocking_pool::ThreadPool::new();
    ///
    /// // This will start up a thread that will linger around even after the task is finished.
    /// pool.spawn_child(|| {}).await;
    ///
    /// // This will forcibly kill that thread.
    /// pool.prune();
    /// # });
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
    run: fn(*const ()),
}
unsafe impl Send for RawFunction {}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    use completion::future;

    #[test]
    fn more_work_than_threads() {
        let thread_pool = Builder::new().max_threads(2).build();

        let value: AtomicUsize = AtomicUsize::new(0);

        future::block_on(future::zip((
            thread_pool.spawn_child(|| {
                assert_eq!(value.load(SeqCst), 0);
                wait();
                assert!(value.fetch_add(1, SeqCst) < 2);
            }),
            thread_pool.spawn_child(|| {
                assert_eq!(value.load(SeqCst), 0);
                wait();
                assert!(value.fetch_add(1, SeqCst) < 2);
            }),
            thread_pool.spawn_child(|| {
                assert!(matches!(value.load(SeqCst), 1 | 2));
                wait();
                assert_eq!(value.load(SeqCst), 2);
            }),
        )));

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
