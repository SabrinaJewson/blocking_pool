//! Spawning tasks.

use std::cell::UnsafeCell;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::mem::{self, ManuallyDrop};
use std::panic;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{self, AtomicU32};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::thread;

use completion_core::CompletionFuture;

use crate::RawFunction;

/// The heap state shared between handles and their tasks.
struct Shared<F, O> {
    /// The state of the task; see the `state_bits` module.
    state: AtomicU32,

    /// A reference to the thread pool. Only accessed by the handle, `Some` when the function hasn't
    /// been started yet.
    pool: UnsafeCell<Option<Arc<crate::Inner>>>,

    /// The function or its output.
    data: UnsafeCell<FnData<F, O>>,

    /// Waker woken when the task completes.
    waker: UnsafeCell<Option<Waker>>,
}

/// Bits set in `Shared::state`.
mod state_bits {
    /// Bit set if the handle holds a reference to the `Shared`.
    pub(super) const HANDLE_REF: u32 = 0b00001;

    /// Bit set if the task holds a reference to `Shared`.
    pub(super) const TASK_REF: u32 = 0b00010;

    /// Bit set when the task completes.
    pub(super) const COMPLETE: u32 = 0b00100;

    /// Bit set if the `JoinHandle` is dropped.
    pub(super) const DROPPED_HANDLE: u32 = 0b01000;

    /// Bit set when either the task or the handle is currently using the `waker`.
    pub(super) const WAKER_LOCKED: u32 = 0b10000;
}

union FnData<F, O> {
    function: ManuallyDrop<F>,
    output: ManuallyDrop<thread::Result<O>>,
}

impl<F: FnOnce() -> O + Send, O: Send> Shared<F, O> {
    /// Drop the handle's reference to the `Shared`.
    unsafe fn drop_handle_ref(self_ptr: *const Self) {
        // Release is necessary to ensure that we don't use `Self` after marking it as freeable.
        let old_state = (*self_ptr)
            .state
            .fetch_and(!state_bits::HANDLE_REF, atomic::Ordering::Release);

        if old_state & state_bits::TASK_REF == 0 {
            // Make sure the other owner is done with `Self` so we can free it.
            acquire_fence!((*self_ptr).state);
            Box::from_raw(self_ptr as *mut Self);
        }
    }

    /// Drop the blocking task's reference to the `Shared`.
    unsafe fn drop_task_ref(self_ptr: *const Self) {
        // Release is necessary to ensure that we don't use `Self` after marking it as freeable.
        let old_state = (*self_ptr)
            .state
            .fetch_and(!state_bits::TASK_REF, atomic::Ordering::Release);

        if old_state & state_bits::HANDLE_REF == 0 {
            // Make sure the other owner is done with `Self` so we can free it.
            acquire_fence!((*self_ptr).state);
            Box::from_raw(self_ptr as *mut Self);
        }
    }

    /// Poll the blocking task, starting it if it hasn't started already.
    ///
    /// This is called by `Task::poll`.
    unsafe fn poll(self_ptr: *const Self, cx: &mut Context<'_>) -> Poll<thread::Result<O>> {
        let this = &*self_ptr;
        if let Some(pool) = (*this.pool.get()).take() {
            // We don't need to protect against panics yet since the function hasn't been started.
            *this.waker.get() = Some(cx.waker().clone());

            pool.spawn_raw(RawFunction {
                data: self_ptr.cast(),
                run: |ptr| Self::run(ptr.cast()),
            });

            Poll::Pending
        } else {
            Self::poll_started(self_ptr, cx)
        }
    }

    /// Poll the blocking task, cancelling it if it hasn't started already.
    ///
    /// This is called by `Task::poll_cancel`.
    unsafe fn poll_cancel(self_ptr: *const Self, cx: &mut Context<'_>) -> Poll<thread::Result<()>> {
        let this = &*self_ptr;
        if (*this.pool.get()).is_some() {
            Self::drop_handle_not_started(self_ptr);
            Poll::Ready(Ok(()))
        } else {
            Self::poll_started(self_ptr, cx).map(|res| res.map(drop))
        }
    }

    /// Drop the blocking task handle. Must only be called when the task hasn't started yet.
    ///
    /// This is called by `Task::drop`.
    unsafe fn drop_handle_not_started(self_ptr: *const Self) {
        let mut this = Box::from_raw(self_ptr as *mut Self);
        ManuallyDrop::drop(&mut this.data.get_mut().function);
    }

    /// Start the blocking task if it hasn't been started already.
    ///
    /// This is called by `Task::detach`.
    unsafe fn start(self_ptr: *const Self) {
        let this = &*self_ptr;
        if let Some(pool) = (*this.pool.get()).take() {
            pool.spawn_raw(RawFunction {
                data: self_ptr.cast(),
                run: |ptr| Self::run(ptr.cast()),
            });
        }
    }

    /// Poll the blocking task, assuming it to have started.
    ///
    /// This is called by `JoinHandle::poll`.
    unsafe fn poll_started(self_ptr: *const Self, cx: &mut Context<'_>) -> Poll<thread::Result<O>> {
        let this = &*self_ptr;

        // Read the function state and attempt to lock the waker.
        //
        // Acquire ensures that if we read the task's output or store our waker, it happens-after
        // the load.
        let old_state = this
            .state
            .fetch_or(state_bits::WAKER_LOCKED, atomic::Ordering::Acquire);

        let complete = if old_state & state_bits::COMPLETE != 0 {
            true
        } else {
            // Register this task for wakeup.

            // The function shouldn't have locked the waker yet.
            debug_assert_eq!(old_state & state_bits::WAKER_LOCKED, 0);

            // Store our new waker. `ignore_unwind` is used because we don't want the `Task` or
            // `JoinHandle` to panic before the task is finished.
            ignore_unwind(panic::AssertUnwindSafe(|| {
                *this.waker.get() = Some(cx.waker().clone());
            }));

            // Release the waker lock.
            //
            // Release ensures the above write to the waker happens-before the spawned task can read
            // from it.
            // Acquire ensures that if we read the task's output, it happens-after the load.
            this.state
                .fetch_and(!state_bits::WAKER_LOCKED, atomic::Ordering::AcqRel)
                & state_bits::COMPLETE
                != 0
        };

        if complete {
            let output = ManuallyDrop::take(&mut (*this.data.get()).output);
            Self::drop_handle_ref(self_ptr);
            Poll::Ready(output)
        } else {
            Poll::Pending
        }
    }

    /// Drop the blocking task handle after the task has started.
    ///
    /// This is called by `JoinHandle::drop`.
    unsafe fn drop_handle_started(self_ptr: *const Self) {
        let this = &*self_ptr;

        // Store that this `JoinHandle` has been dropped.
        //
        // Acquire ensures that if we read the function's output, it happens-after the load.
        let old_state = this
            .state
            .fetch_or(state_bits::DROPPED_HANDLE, atomic::Ordering::Acquire);

        let _output = (old_state & state_bits::COMPLETE != 0).then(|| {
            // Make sure to take the function's output to avoid leaks.
            ManuallyDrop::take(&mut (*this.data.get()).output)
        });

        Self::drop_handle_ref(self_ptr);
    }

    /// Run the task itself.
    unsafe fn run(self_ptr: *const Self) {
        let this = &*self_ptr;

        // SAFETY: The handle will not access the data until we set its state to finished.
        let data = &mut *this.data.get();

        // Take the function and run it.
        let function = ManuallyDrop::take(&mut data.function);
        let output = panic::catch_unwind(panic::AssertUnwindSafe(function));

        // Store the output of the function.
        data.output = ManuallyDrop::new(output);

        // Store that we are finished and attempt to lock the waker.
        //
        // Release ensures that the above storing of `data` happens-before the handle can read it.
        let old_state = this.state.fetch_or(
            state_bits::COMPLETE | state_bits::WAKER_LOCKED,
            atomic::Ordering::Release,
        );

        if old_state & state_bits::DROPPED_HANDLE != 0 {
            // The handle has been dropped; we must drop the output ourselves to avoid a
            // memory leak.
            ignore_unwind(panic::AssertUnwindSafe(|| {
                ManuallyDrop::drop(&mut data.output)
            }));
        } else if old_state & state_bits::WAKER_LOCKED == 0 {
            // The handle has not been dropped, and we have acquired the waker lock.
            //
            // If we didn't acquire the waker lock, the handle is currently registering a new
            // waker and will see that we have completed after finishing that, so we don't need to
            // do anything.

            acquire_fence!((*self_ptr).state);

            if let Some(waker) = (*this.waker.get()).take() {
                ignore_unwind(|| waker.wake());
            }
            // We don't need to release the waker lock, because the waker is not going to used
            // anymore.
        }

        Self::drop_task_ref(self_ptr);
    }
}

struct VTable<O> {
    poll: unsafe fn(*const (), &mut Context<'_>) -> Poll<thread::Result<O>>,
    poll_cancel: unsafe fn(*const (), &mut Context<'_>) -> Poll<thread::Result<()>>,
    drop_not_started: unsafe fn(*const ()),

    start: unsafe fn(*const ()),
    poll_started: unsafe fn(*const (), &mut Context<'_>) -> Poll<thread::Result<O>>,
    drop_started: unsafe fn(*const ()),
}

impl<F: FnOnce() -> O + Send, O: Send> Shared<F, O> {
    const VTABLE: VTable<O> = VTable {
        poll: |ptr, cx| unsafe { Self::poll(ptr.cast(), cx) },
        poll_cancel: |ptr, cx| unsafe { Self::poll_cancel(ptr.cast(), cx) },
        drop_not_started: |ptr| unsafe { Self::drop_handle_not_started(ptr.cast()) },
        start: |ptr| unsafe { Self::start(ptr.cast()) },
        poll_started: |ptr, cx| unsafe { Self::poll_started(ptr.cast(), cx) },
        drop_started: |ptr| unsafe { Self::drop_handle_started(ptr.cast()) },
    };
}

/// Special value that the pointer field in `Task` and `JoinHandle` can be to indicate that the
/// task has finished.
///
/// This cannot overlap with the pointer to `Shared`, as the pointer to `Shared` requires an
/// alignment of at least 4.
const TASK_COMPLETE: NonNull<()> = unsafe { NonNull::new_unchecked(1 as *mut ()) };

/// A handle to a spawned blocking task, created by [`spawn`](crate::ThreadPool::spawn).
///
/// This can be awaited on to wait for the task to complete, or [`detach()`](Self::detach) can be
/// called to run the task in the background.
#[must_use = "Tasks will not run unless polled. To run it in the background, use `Task::detach`"]
pub struct Task<'a, O: Send> {
    /// Type-erased pointer to a `Shared`, or `TASK_COMPLETE` if the task is finished.
    ptr: NonNull<()>,
    /// This is a `&'static VTable<O>`, but without the lifetime bound on `O`.
    vtable: NonNull<VTable<O>>,
    _covariant: PhantomData<&'a ()>,
}

unsafe impl<O: Send> Send for Task<'_, O> {}
unsafe impl<O: Send> Sync for Task<'_, O> {}
impl<O: Send> Unpin for Task<'_, O> {}

impl<'a, O: Send> Task<'a, O> {
    pub(crate) fn new<F: FnOnce() -> O + Send + 'a>(f: F, pool: Arc<crate::Inner>) -> Self {
        let shared: Shared<F, O> = Shared {
            state: AtomicU32::new(state_bits::HANDLE_REF | state_bits::TASK_REF),
            pool: UnsafeCell::new(Some(pool)),
            data: UnsafeCell::new(FnData {
                function: ManuallyDrop::new(f),
            }),
            waker: UnsafeCell::new(None),
        };
        Self {
            ptr: NonNull::from(Box::leak(Box::new(shared))).cast(),
            vtable: NonNull::from(&<Shared<F, O>>::VTABLE),
            _covariant: PhantomData,
        }
    }

    unsafe fn poll_with<T>(
        &mut self,
        cx: &mut Context<'_>,
        poll: unsafe fn(*const (), &mut Context<'_>) -> Poll<thread::Result<T>>,
    ) -> Poll<T> {
        if self.ptr == TASK_COMPLETE {
            panic!("Polled blocking task after completion");
        }
        (poll)(self.ptr.as_ptr(), cx).map(|output| {
            self.ptr = TASK_COMPLETE;
            output.unwrap_or_else(|panic| panic::resume_unwind(panic))
        })
    }
}

impl<O: Send> CompletionFuture for Task<'_, O> {
    type Output = O;

    unsafe fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let poll = self.vtable.as_ref().poll;
        self.poll_with(cx, poll)
    }
    unsafe fn poll_cancel(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let poll_cancel = self.vtable.as_ref().poll_cancel;
        self.poll_with(cx, poll_cancel)
    }
}

impl<O: Send> Debug for Task<'_, O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad("Task")
    }
}

impl<O: Send> Drop for Task<'_, O> {
    fn drop(&mut self) {
        if self.ptr != TASK_COMPLETE {
            unsafe { (self.vtable.as_ref().drop_not_started)(self.ptr.as_ptr()) };
        }
    }
}

impl<O: Send + 'static> Task<'static, O> {
    /// Detach the task to keep it running in the background without polling this future.
    ///
    /// This method is only available on `'static` tasks because the task may outlive the scope of
    /// its handle.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # completion::completion_async! {
    /// let pool = blocking_pool::ThreadPool::new();
    /// pool.spawn(|| std::fs::write("foo.txt", "Lorem ipsum").unwrap()).detach();
    /// # };
    /// ```
    pub fn detach(self) -> JoinHandle<O> {
        if self.ptr != TASK_COMPLETE {
            unsafe { (self.vtable.as_ref().start)(self.ptr.as_ptr()) };
        }
        let handle = JoinHandle {
            ptr: self.ptr,
            vtable: unsafe { &*self.vtable.as_ptr() },
        };
        mem::forget(self);
        handle
    }
}

/// A handle to a detached blocking task, created by [`Task::detach`].
///
/// This can be awaited on to wait for the task to complete. Unlike [`Task`], it catches panics
/// that occur in the task, so it outputs a [`thread::Result`]`<T>`.
pub struct JoinHandle<O: Send + 'static> {
    /// Type-erased pointer to a `Shared`, or `TASK_COMPLETE` if the task is complete.
    ptr: NonNull<()>,
    vtable: &'static VTable<O>,
}

unsafe impl<O: Send + 'static> Send for JoinHandle<O> {}
unsafe impl<O: Send + 'static> Sync for JoinHandle<O> {}
impl<O: Send + 'static> Unpin for JoinHandle<O> {}

impl<O: Send + 'static> Future for JoinHandle<O> {
    type Output = thread::Result<O>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.ptr == TASK_COMPLETE {
            panic!("Polled blocking `JoinHandle` after completion");
        }

        unsafe { (self.vtable.poll_started)(self.ptr.as_ptr(), cx) }.map(|output| {
            self.ptr = TASK_COMPLETE;
            output
        })
    }
}

impl<O: Send + 'static> CompletionFuture for JoinHandle<O> {
    type Output = thread::Result<O>;

    unsafe fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Future::poll(self, cx)
    }
    unsafe fn poll_cancel(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}

impl<O: Send + 'static> Debug for JoinHandle<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad("JoinHandle")
    }
}

impl<O: Send + 'static> Drop for JoinHandle<O> {
    fn drop(&mut self) {
        if self.ptr != TASK_COMPLETE {
            unsafe { (self.vtable.drop_started)(self.ptr.as_ptr()) };
        }
    }
}

/// Run a closure, ignoring any panics that occur.
fn ignore_unwind<F: FnOnce() + panic::UnwindSafe>(f: F) {
    struct AbortOnDrop;
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            std::process::abort();
        }
    }
    // If the panic payload itself panics on drop, we simply abort the process.
    let guard = AbortOnDrop;
    drop(panic::catch_unwind(f));
    mem::forget(guard);
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::panic::panic_any;
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

    use completion::future::{self, CompletionFutureExt};

    use crate::{wait, ThreadPool};

    #[test]
    fn no_run() {
        let pool = ThreadPool::new();
        let data = Box::new(16);
        drop(pool.spawn(move || {
            drop(data);
            std::process::abort()
        }));
    }

    #[test]
    fn wait_task() {
        let pool = ThreadPool::new();

        let task = pool.spawn(|| {
            wait();
            Box::new(16)
        });
        assert_eq!(future::block_on(task), Box::new(16));

        pool.miri_shutdown()
    }

    #[test]
    fn wait_join_handle() {
        let pool = ThreadPool::new();

        let handle = pool
            .spawn(|| {
                wait();
                Box::new(16)
            })
            .detach();
        assert_eq!(future::block_on(handle).unwrap(), Box::new(16));

        pool.miri_shutdown();
    }

    #[test]
    fn cancellation() {
        let pool = ThreadPool::new();

        let mut val = false;

        let task = pool.spawn(|| {
            wait();
            val = true;
            Box::new(12)
        });
        assert_eq!(future::block_on(task.now_or_never()), None);
        assert!(val);

        pool.miri_shutdown();
    }

    #[test]
    fn dropped_join_handle() {
        static MARKER: AtomicBool = AtomicBool::new(false);

        let pool = ThreadPool::new();

        MARKER.store(false, SeqCst);

        pool.spawn(|| {
            wait();
            assert!(!MARKER.swap(true, SeqCst));
            Box::new(16)
        })
        .detach();
        pool.wait_all_complete();
        assert!(MARKER.load(SeqCst));

        pool.miri_shutdown();
    }

    #[test]
    fn panic_task() {
        let pool = ThreadPool::new();

        let task = pool.spawn(|| {
            wait();
            panic_any(5_i16);
        });
        let payload =
            panic::catch_unwind(panic::AssertUnwindSafe(|| future::block_on(task))).unwrap_err();
        assert_eq!(*payload.downcast::<i16>().unwrap(), 5);

        pool.miri_shutdown();
    }

    #[test]
    fn panic_detached() {
        let pool = ThreadPool::new();

        let handle = pool.spawn(|| panic_any(5_i32)).detach();
        let payload = future::block_on(handle).unwrap_err();
        assert_eq!(*payload.downcast::<i32>().unwrap(), 5);

        pool.miri_shutdown();
    }

    #[test]
    fn panic_in_detached_destructor() {
        struct DropPanic;
        impl Drop for DropPanic {
            fn drop(&mut self) {
                panic_any(5_i32);
            }
        }

        let pool = ThreadPool::new();
        pool.spawn(|| {
            wait();
            DropPanic
        })
        .detach();
        pool.wait_all_complete();

        pool.miri_shutdown();
    }
}
