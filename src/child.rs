//! Spawning children.

use std::cell::UnsafeCell;
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomPinned;
use std::mem::ManuallyDrop;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{self, AtomicU32};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;

use aliasable::boxed::AliasableBox;
use atomic_waker::AtomicWaker;
use completion_core::CompletionFuture;
use pin_project_lite::pin_project;

use crate::RawFunction;

pin_project! {
    /// A [spawned child](crate::ThreadPool::spawn_child).
    ///
    /// This must be awaited on so that the child will complete.
    #[must_use = "Futures do nothing unless polled"]
    pub struct Child<O, F> {
        // A reference to the thread pool. Only `Some` if the child has not started running yet.
        pool: Option<Arc<crate::Inner>>,
        shared: AliasableBox<Shared<O, F>>,
        // We want to support unboxing `Shared` in the future.
        #[pin]
        _pinned: PhantomPinned,
    }
}

/// Shared state between the `Child` and the child itself.
struct Shared<O, F> {
    /// The state of the task. This is zero if it has not completed, 1 if it has completed and 2 if
    /// it has completed and the output has been taken.
    state: AtomicU32,

    /// The function and output.
    data: UnsafeCell<FnData<O, F>>,

    /// Waker woken when the task completes.
    waker: AtomicWaker,
}

unsafe impl<O: Sync, F: Sync> Sync for Shared<O, F> {}

union FnData<O, F> {
    function: ManuallyDrop<F>,
    output: ManuallyDrop<thread::Result<O>>,
}

impl<O, F> Child<O, F>
where
    F: FnOnce() -> O + Send,
    O: Send,
{
    pub(crate) fn new(f: F, pool: Arc<crate::Inner>) -> Self {
        let shared = AliasableBox::from_unique(Box::new(Shared {
            state: AtomicU32::new(0),
            data: UnsafeCell::new(FnData {
                function: ManuallyDrop::new(f),
            }),
            waker: AtomicWaker::new(),
        }));

        Self {
            pool: Some(pool),
            shared,
            _pinned: PhantomPinned,
        }
    }
}

impl<O, F> CompletionFuture for Child<O, F>
where
    F: FnOnce() -> O + Send,
    O: Send,
{
    type Output = O;

    unsafe fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // Spawn the function on the thread pool if we haven't yet.
        if let Some(pool) = this.pool.take() {
            pool.spawn_raw(RawFunction {
                data: &**this.shared as *const Shared<O, F> as *mut (),
                run: |ptr| {
                    let shared: &Shared<O, F> = &*ptr.cast();

                    // SAFETY: The `Child` will not access the data until we set its state to finished.
                    let data = &mut *shared.data.get();

                    // Take the function and run it.
                    let function = ManuallyDrop::take(&mut data.function);
                    let output = catch_unwind(AssertUnwindSafe(function));

                    // Store the output of the function.
                    *data = FnData {
                        output: ManuallyDrop::new(output),
                    };

                    // Store that we are finished.
                    //
                    // Release is necessary so that the above storing of `data` is ordered before
                    // the swap.
                    shared.state.store(1, atomic::Ordering::Release);

                    // Notify the `Child` that we are done.
                    shared.waker.wake();
                },
            });
        }

        let mut registered = false;

        loop {
            // Query whether the function has completed.
            //
            // Acquire is necessary so that the potential reading of data is ordered after this load.
            match this.shared.state.load(atomic::Ordering::Acquire) {
                // The function has not completed yet and we are registered for wake up.
                0 if registered => break Poll::Pending,

                // The function has not completed yet; register us for wake up and try the
                // above check again, to make sure that the task hasn't completed in
                // between us checking and us registering the waker.
                0 => {
                    this.shared.waker.register(cx.waker());
                    registered = true;
                }

                // The function has completed, take its output.
                1 => {
                    // No specific ordering is required as there are no threads using the value
                    // anymore.
                    this.shared.state.store(2, atomic::Ordering::Relaxed);

                    let res = ManuallyDrop::take(&mut (*this.shared.data.get()).output);
                    break Poll::Ready(res.unwrap_or_else(|e| resume_unwind(e)));
                }

                // Polled after completion.
                2 => panic!("polled `Child` after completion"),
                _ => unreachable!(),
            }
        }
    }
    unsafe fn poll_cancel(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.pool.is_some() {
            Poll::Ready(())
        } else {
            // This type must not be dropped before the closure has finished running.
            self.poll(cx).map(drop)
        }
    }
}

impl<O: Debug, F: Debug> Debug for Child<O, F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad("Child")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::panic::panic_any;

    use completion::future;

    use crate::{wait, ThreadPool};

    #[test]
    fn no_run() {
        let pool = ThreadPool::new();
        drop(pool.spawn_child(|| std::process::abort()));
    }

    #[test]
    fn waiting() {
        let pool = ThreadPool::new();

        let child = pool.spawn_child(|| {
            wait();
            Box::new(16)
        });
        assert_eq!(future::block_on(child), Box::new(16));

        pool.miri_shutdown();
    }

    #[test]
    fn panic() {
        let pool = ThreadPool::new();

        let child = pool.spawn_child(|| {
            wait();
            panic_any(5_i16);
        });
        let payload = catch_unwind(AssertUnwindSafe(|| future::block_on(child))).unwrap_err();
        assert_eq!(*payload.downcast::<i16>().unwrap(), 5);

        pool.miri_shutdown();
    }
}
