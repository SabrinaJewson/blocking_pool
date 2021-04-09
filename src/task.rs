//! Spawning tasks.

use std::cell::UnsafeCell;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::mem::ManuallyDrop;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{self, AtomicU32};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;

use atomic_waker::AtomicWaker;
use completion_core::CompletionFuture;

use crate::RawFunction;

/// The heap state shared between static join handles and the tasks.
struct Shared<F, O> {
    /// The state of the task; see the `state_bits` module.
    state: AtomicU32,

    /// The function and output.
    data: UnsafeCell<FnData<F, O>>,

    /// Waker woken when the task completes.
    waker: AtomicWaker,
}

/// Bits set in `Shared::state`.
mod state_bits {
    /// Bit set if the `JoinHandle` holds a reference to the `Shared`.
    pub(super) const HANDLE_REF: u32 = 0b0001;

    /// Bit set if the function holds a reference to `Shared`.
    pub(super) const FUNCTION_REF: u32 = 0b0010;

    /// Bit set when the function completes.
    pub(super) const COMPLETE: u32 = 0b0100;

    /// Bit set when the join handle is dropped.
    pub(super) const DROPPED_HANDLE: u32 = 0b1000;
}

union FnData<F, O> {
    function: ManuallyDrop<F>,
    output: ManuallyDrop<thread::Result<O>>,
}

impl<F: FnOnce() -> O + Send + 'static, O: Send + 'static> Shared<F, O> {
    /// Drop the `JoinHandle`'s reference to the `Shared`.
    unsafe fn drop_handle_ref(self_ptr: *const Self) {
        // Release is necessary to ensure that we don't use `Self` after marking it as freeable.
        let old_state = (*self_ptr)
            .state
            .fetch_and(!state_bits::HANDLE_REF, atomic::Ordering::Release);

        if old_state & state_bits::FUNCTION_REF == 0 {
            // Make sure the other owner is done with `Self` so we can free it.
            acquire_fence!((*self_ptr).state);
            Box::from_raw(self_ptr as *mut Self);
        }
    }

    /// Drop the function's reference to the `Shared`.
    unsafe fn drop_function_ref(self_ptr: *const Self) {
        // Release is necessary to ensure that we don't use `Self` after marking it as freeable.
        let old_state = (*self_ptr)
            .state
            .fetch_and(!state_bits::FUNCTION_REF, atomic::Ordering::Release);

        if old_state & state_bits::HANDLE_REF == 0 {
            // Make sure the other owner is done with `Self` so we can free it.
            acquire_fence!((*self_ptr).state);
            Box::from_raw(self_ptr as *mut Self);
        }
    }

    unsafe fn poll(self_ptr: *const Self, cx: &mut Context<'_>) -> Poll<thread::Result<O>> {
        let this = &*self_ptr;

        // Acquire is necessary so that if we read the function's output, it occurs after
        // the load.
        let complete = if this.state.load(atomic::Ordering::Acquire) & state_bits::COMPLETE != 0 {
            true
        } else {
            this.waker.register(cx.waker());
            this.state.load(atomic::Ordering::Acquire) & state_bits::COMPLETE != 0
        };

        if complete {
            let output = ManuallyDrop::take(&mut (*this.data.get()).output);
            Self::drop_handle_ref(self_ptr);
            Poll::Ready(output)
        } else {
            Poll::Pending
        }
    }

    unsafe fn drop_handle(self_ptr: *const Self) {
        let this = &*self_ptr;

        // Store that this join handle has been dropped.
        //
        // Acquire is necessary so that if we read the function's output, it occurs after
        // the load.
        let old_state = this
            .state
            .fetch_or(state_bits::DROPPED_HANDLE, atomic::Ordering::Acquire);

        let _output = (old_state & state_bits::COMPLETE != 0).then(|| {
            // Make sure to take the function's output to avoid leaks.
            ManuallyDrop::take(&mut (*this.data.get()).output)
        });

        Self::drop_handle_ref(self_ptr);
    }

    unsafe fn run(self_ptr: *const Self) {
        let this = &*self_ptr;

        // SAFETY: The join handle will not access the data until we set its state
        // to finished.
        let data = &mut *this.data.get();

        // Take the function and run it.
        let function = ManuallyDrop::take(&mut data.function);
        let output = catch_unwind(AssertUnwindSafe(function));

        // Store the output of the function.
        data.output = ManuallyDrop::new(output);

        // Store that we are finished.
        //
        // Release is necessary so that the above storing of `data` is ordered before the
        // `JoinHandle` can read it.
        let old_state = this
            .state
            .fetch_or(state_bits::COMPLETE, atomic::Ordering::Release);

        if old_state & state_bits::DROPPED_HANDLE != 0 {
            // The handle has been dropped; we must drop the output ourselves to avoid a
            // memory leak.
            let _ = catch_unwind(AssertUnwindSafe(|| ManuallyDrop::drop(&mut data.output)));
        } else {
            // The handle still exists; wake it.
            this.waker.wake();
        }

        Self::drop_function_ref(self_ptr);
    }
}

/// A handle to a [spawned task](crate::ThreadPool::spawn_task).
///
/// This can be awaited on to wait for the task to complete, or it can be dropped to detach the
/// task and have it run to completion on its own.
pub struct JoinHandle<O: Send + 'static> {
    /// Type-erased pointer to a `Shared`.
    ptr: NonNull<()>,
    poll: unsafe fn(*const (), &mut Context<'_>) -> Poll<thread::Result<O>>,
    drop: unsafe fn(*const ()),
}

unsafe impl<O: Send + 'static> Send for JoinHandle<O> {}
unsafe impl<O: Send + 'static> Sync for JoinHandle<O> {}

impl<O: Send + 'static> JoinHandle<O> {
    pub(crate) fn new<F: FnOnce() -> O + Send + 'static>(f: F, pool: &Arc<crate::Inner>) -> Self {
        let shared: Shared<F, O> = Shared {
            state: AtomicU32::new(state_bits::HANDLE_REF | state_bits::FUNCTION_REF),
            data: UnsafeCell::new(FnData {
                function: ManuallyDrop::new(f),
            }),
            waker: AtomicWaker::new(),
        };
        // Heap-allocate and erase the shared state's type.
        let ptr: *const () = Box::into_raw(Box::new(shared)).cast();

        let this = Self {
            ptr: unsafe { NonNull::new_unchecked(ptr as *mut ()) },
            poll: |ptr, cx| unsafe { <Shared<F, O>>::poll(ptr.cast(), cx) },
            drop: |ptr| unsafe { <Shared<F, O>>::drop_handle(ptr.cast()) },
        };
        pool.spawn_raw_by_ref(RawFunction {
            data: ptr as *mut (),
            run: |ptr| unsafe { <Shared<F, O>>::run(ptr.cast()) },
        });

        this
    }
}

impl<O: Send + 'static> Future for JoinHandle<O> {
    type Output = thread::Result<O>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.ptr.as_ptr() as usize == 1 {
            panic!("Polled `JoinHandle` after completion");
        }

        unsafe { (self.poll)(self.ptr.as_ptr(), cx) }.map(|output| {
            self.ptr = unsafe { NonNull::new_unchecked(1 as *mut ()) };
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

impl<O: Send + 'static> Drop for JoinHandle<O> {
    fn drop(&mut self) {
        if self.ptr.as_ptr() as usize != 1 {
            unsafe { (self.drop)(self.ptr.as_ptr()) };
        }
    }
}

impl<O: Send + 'static> Debug for JoinHandle<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad("JoinHandle")
    }
}

#[cfg(test)]
mod tests {
    use std::panic::panic_any;
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

        let handle = pool.spawn_task(|| panic_any(5_i32));
        let payload = future::block_on(handle).unwrap_err();
        assert_eq!(*payload.downcast::<i32>().unwrap(), 5);

        pool.miri_shutdown();
    }

    #[test]
    fn panic_in_destructor() {
        struct DropPanic;
        impl Drop for DropPanic {
            fn drop(&mut self) {
                panic_any(5_i32);
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
