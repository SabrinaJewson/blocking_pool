//! Spawning tasks.

use std::cell::UnsafeCell;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::mem::ManuallyDrop;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{self, AtomicU32};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;

use atomic_waker::AtomicWaker;
use completion_core::CompletionFuture;

use crate::RawFunction;

/// The heap state shared between static join handles and the tasks.
struct Shared<F, O> {
    /// The state of the task.
    ///
    /// - `0`: The task is currently running, and the join handle has not been dropped.
    /// - `1`: The task is currently running, and the join handle has been dropped.
    /// - `2`: The task has finished.
    state: AtomicU32,

    /// The function and output.
    data: UnsafeCell<FnData<F, O>>,

    /// Waker woken when the task completes.
    waker: AtomicWaker,
}

union FnData<F, O> {
    function: ManuallyDrop<F>,
    output: ManuallyDrop<thread::Result<O>>,
}

/// A handle to a [spawned task](crate::ThreadPool::spawn_task).
///
/// This can be awaited on to wait for the task to complete, or it can be dropped to detach the
/// task and have it run to completion on its own.
pub struct JoinHandle<O: Send + 'static> {
    /// Type-erased pointer to a `Shared`.
    ptr: *const (),
    poll: unsafe fn(*const (), &mut Context<'_>) -> Poll<thread::Result<O>>,
    drop: unsafe fn(*const ()),
}

unsafe impl<O: Send + 'static> Send for JoinHandle<O> {}
unsafe impl<O: Send + Sync + 'static> Sync for JoinHandle<O> {}

impl<O: Send + 'static> JoinHandle<O> {
    pub(crate) fn new<F: FnOnce() -> O + Send + 'static>(f: F, pool: &Arc<crate::Inner>) -> Self {
        let shared: Shared<F, O> = Shared {
            state: AtomicU32::new(0),
            data: UnsafeCell::new(FnData {
                function: ManuallyDrop::new(f),
            }),
            waker: AtomicWaker::new(),
        };
        // Heap-allocate and erase the shared state's type.
        let ptr: *const () = Box::into_raw(Box::new(shared)).cast();

        let this = Self {
            ptr,
            poll: |ptr, cx| {
                let shared_ptr: *const Shared<F, O> = ptr.cast();
                let shared = unsafe { &*shared_ptr };

                let mut registered = false;

                loop {
                    // Query whether the function has completed.
                    //
                    // Acquire is necessary so that potential memory frees are ordered after this swap.
                    match shared.state.load(atomic::Ordering::Acquire) {
                        // The function has not completed yet and we are registered for wake up.
                        0 if registered => break Poll::Pending,

                        // The function has not completed yet; register us for wake up and try the
                        // above check again, to make sure that the task hasn't completed in
                        // between us checking and us registering the waker.
                        0 => {
                            shared.waker.register(cx.waker());
                            registered = true;
                        }

                        // The function has completed; take its output and free the shared state.
                        2 => {
                            let output =
                                unsafe { ManuallyDrop::take(&mut (*shared.data.get()).output) };
                            unsafe { Box::from_raw(shared_ptr as *mut Shared<F, O>) };
                            break Poll::Ready(output);
                        }
                        _ => unreachable!(),
                    }
                }
            },
            drop: |ptr| {
                let shared_ptr: *const Shared<F, O> = ptr.cast();
                let shared = unsafe { &*shared_ptr };

                // Store that this join handle has been dropped.
                //
                // - Acquire is necessary so that any potential memory frees are ordered after
                // loading this value.
                // - Release is necessary so that any accesses to `data` are ordered before the
                // swap.
                match shared.state.swap(1, atomic::Ordering::AcqRel) {
                    // The function has't completed yet.
                    0 => {}
                    // The function has completed.
                    2 => unsafe {
                        // Make sure to drop its output to avoid leaks. Panics are fine, they will
                        // just propagate to the caller.
                        ManuallyDrop::drop(&mut (*shared.data.get()).output);
                        // Free the shared state.
                        Box::from_raw(shared_ptr as *mut Shared<F, O>);
                    },
                    _ => unreachable!(),
                }
            },
        };
        pool.spawn_raw_by_ref(RawFunction {
            data: ptr as *mut (),
            run: |ptr| {
                let shared_ptr: *const Shared<F, O> = ptr.cast();
                let shared = unsafe { &*shared_ptr };

                // SAFETY: The join handle will not access the data until we set its state
                // to finished.
                let data = unsafe { &mut *shared.data.get() };

                // Take the function and run it.
                let function = unsafe { ManuallyDrop::take(&mut data.function) };
                let output = catch_unwind(AssertUnwindSafe(function));

                // Store the output of the function.
                *data = FnData {
                    output: ManuallyDrop::new(output),
                };

                // Store that we are finished.
                //
                // - Acquire is necessary so that any potential memory frees are ordered after
                // loading this value.
                // - Release is necessary so that the above storing of `data` is ordered before
                // the swap.
                match shared.state.swap(2, atomic::Ordering::AcqRel) {
                    // The join handle is waiting; notify it.
                    0 => {
                        let waker = shared.waker.take();
                        if let Some(waker) = waker {
                            waker.wake();
                        }
                    }

                    // The join handle has been dropped.
                    1 => unsafe {
                        // Make sure to drop the output to avoid leaks.
                        let _ =
                            catch_unwind(AssertUnwindSafe(|| ManuallyDrop::drop(&mut data.output)));
                        // Free the shared state.
                        Box::from_raw(shared_ptr as *mut Shared<F, O>);
                    },

                    _ => unreachable!(),
                }
            },
        });

        this
    }
}

impl<O: Send + 'static> Future for JoinHandle<O> {
    type Output = thread::Result<O>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.ptr.is_null() {
            panic!("Polled `JoinHandle` after completion");
        }

        match unsafe { (self.poll)(self.ptr, cx) } {
            Poll::Ready(output) => {
                self.ptr = ptr::null();
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
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

impl<O: Send + 'static> Drop for JoinHandle<O> {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                (self.drop)(self.ptr);
            }
        }
    }
}

impl<O: Debug + Send + 'static> Debug for JoinHandle<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad("JoinHandle")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

    use completion::future;

    use crate::{wait, ThreadPool};

    #[test]
    fn wait_join_handle() {
        let pool = ThreadPool::new();

        let handle = pool.spawn_task(|| {
            wait();
            Box::new(16)
        });
        assert_eq!(future::block_on(handle).unwrap(), Box::new(16));

        pool.miri_shutdown();
    }

    #[test]
    fn dropped_join_handle() {
        static MARKER: AtomicBool = AtomicBool::new(false);

        let pool = ThreadPool::new();

        MARKER.store(false, SeqCst);

        pool.spawn_task(|| {
            wait();
            assert!(!MARKER.swap(true, SeqCst));
            Box::new(16)
        });
        pool.wait_all_complete();
        assert!(MARKER.load(SeqCst));

        pool.miri_shutdown();
    }

    #[test]
    fn panic() {
        let pool = ThreadPool::new();

        let handle = pool.spawn_task(|| panic!(5_i32));
        let payload = future::block_on(handle).unwrap_err();
        assert_eq!(*payload.downcast::<i32>().unwrap(), 5);

        pool.miri_shutdown();
    }

    #[test]
    fn panic_in_destructor() {
        struct DropPanic;
        impl Drop for DropPanic {
            fn drop(&mut self) {
                panic!(5_i32);
            }
        }

        let pool = ThreadPool::new();
        pool.spawn_task(|| {
            wait();
            DropPanic
        });
        pool.wait_all_complete();

        pool.miri_shutdown();
    }
}
