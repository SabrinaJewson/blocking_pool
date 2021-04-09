//! Spawning children.

use std::cell::UnsafeCell;
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{self, AtomicU32};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;

use atomic_waker::AtomicWaker;
use completion_core::CompletionFuture;

use crate::RawFunction;

/// Shared state between the `Child` and the child itself.
struct Shared<O, F> {
    /// The state of the task; see the `state_bits` module.
    state: AtomicU32,

    /// A reference to the thread pool. Only accessed by the `Child`, `Some` when the function
    /// hasn't been started yet.
    pool: UnsafeCell<Option<Arc<crate::Inner>>>,

    /// The function and output.
    data: UnsafeCell<FnData<O, F>>,

    /// Waker woken when the task completes.
    waker: AtomicWaker,
}

/// Bits set in `Shared::state`.
mod state_bits {
    /// Bit set if the `Child` holds a reference to the `Shared`.
    pub(super) const CHILD_REF: u32 = 0b0001;

    /// Bit set if the function holds a reference to `Shared`.
    pub(super) const FUNCTION_REF: u32 = 0b0010;

    /// Bit set when the function completes.
    pub(super) const COMPLETE: u32 = 0b1000;
}

union FnData<F, O> {
    function: ManuallyDrop<F>,
    output: ManuallyDrop<thread::Result<O>>,
}

impl<F: FnOnce() -> O + Send, O: Send> Shared<F, O> {
    const VTABLE: VTable<O> = VTable {
        poll: |ptr, cx| unsafe { Self::poll(ptr.cast(), cx) },
        poll_cancel: |ptr, cx| unsafe { Self::poll_cancel(ptr.cast(), cx) },
        drop: |ptr| unsafe {
            <Box<Self>>::from_raw(ptr as *mut _);
        },
    };

    /// Drop the `Child`'s reference to the `Shared`.
    unsafe fn drop_child_ref(self_ptr: *const Self) {
        // Release is necessary to ensure that we don't use `Self` after marking it as freeable.
        let old_state = (*self_ptr)
            .state
            .fetch_and(!state_bits::CHILD_REF, atomic::Ordering::Release);

        if old_state & state_bits::FUNCTION_REF == 0 {
            // Make sure the other owner is done with `Self` so we can free it.
            atomic::fence(atomic::Ordering::Acquire);
            Box::from_raw(self_ptr as *mut Self);
        }
    }

    /// Drop the function's reference to the `Shared`.
    unsafe fn drop_function_ref(self_ptr: *const Self) {
        // Release is necessary to ensure that we don't use `Self` after marking it as freeable.
        let old_state = (*self_ptr)
            .state
            .fetch_and(!state_bits::FUNCTION_REF, atomic::Ordering::Release);

        if old_state & state_bits::CHILD_REF == 0 {
            // Make sure the other owner is done with `Self` so we can free it.
            atomic::fence(atomic::Ordering::Acquire);
            Box::from_raw(self_ptr as *mut Self);
        }
    }

    unsafe fn poll(self_ptr: *const Self, cx: &mut Context<'_>) -> Poll<thread::Result<O>> {
        let this = &*self_ptr;

        // Start the function if it hasn't been started yet.
        if let Some(pool) = (*this.pool.get()).take() {
            this.waker.register(cx.waker());
            pool.spawn_raw(RawFunction {
                data: self_ptr.cast(),
                run: |ptr| Self::run(ptr.cast()),
            });
            Poll::Pending
        } else {
            Self::poll_inner(self_ptr, cx)
        }
    }

    unsafe fn poll_cancel(self_ptr: *const Self, cx: &mut Context<'_>) -> Poll<thread::Result<()>> {
        let this = &*self_ptr;

        if (*this.pool.get()).is_some() {
            Self::drop_child_ref(self_ptr);
            Poll::Ready(Ok(()))
        } else {
            Self::poll_inner(self_ptr, cx).map(|res| res.map(drop))
        }
    }

    unsafe fn poll_inner(self_ptr: *const Self, cx: &mut Context<'_>) -> Poll<thread::Result<O>> {
        let this = &*self_ptr;

        // Acquire makes sure that if we read the function's output, it occurs after the load.
        let complete = if this.state.load(atomic::Ordering::Acquire) & state_bits::COMPLETE != 0 {
            true
        } else {
            this.waker.register(cx.waker());
            this.state.load(atomic::Ordering::Acquire) & state_bits::COMPLETE != 0
        };

        if complete {
            let output = ManuallyDrop::take(&mut (*this.data.get()).output);
            Self::drop_child_ref(self_ptr);
            Poll::Ready(output)
        } else {
            Poll::Pending
        }
    }

    unsafe fn run(self_ptr: *const Self) {
        let this = &*self_ptr;

        // SAFETY: `poll` will not access the data until we set its state to finished.
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
        this.state
            .fetch_or(state_bits::COMPLETE, atomic::Ordering::Release);

        // Wake the `Child`.
        this.waker.wake();

        Self::drop_function_ref(self_ptr);
    }
}

/// A [spawned child](crate::ThreadPool::spawn_child).
///
/// This must be awaited on so that the child will complete.
#[must_use = "Futures do nothing unless polled"]
pub struct Child<'a, O: Send> {
    /// Type-erased pointer to a `Shared`.
    ptr: NonNull<()>,
    /// This is a `&'static`, but that would enforce lifetime bounds on `O`.
    vtable: NonNull<VTable<O>>,
    _lifetime: PhantomData<&'a ()>,
}

unsafe impl<O: Send> Send for Child<'_, O> {}
unsafe impl<O: Send> Sync for Child<'_, O> {}

struct VTable<O> {
    poll: unsafe fn(*const (), &mut Context<'_>) -> Poll<thread::Result<O>>,
    poll_cancel: unsafe fn(*const (), &mut Context<'_>) -> Poll<thread::Result<()>>,
    drop: unsafe fn(*const ()),
}

impl<'a, O: Send> Child<'a, O> {
    pub(crate) fn new<F: FnOnce() -> O + Send + 'a>(f: F, pool: Arc<crate::Inner>) -> Self {
        let shared: Shared<F, O> = Shared {
            state: AtomicU32::new(state_bits::CHILD_REF | state_bits::FUNCTION_REF),
            pool: UnsafeCell::new(Some(pool)),
            data: UnsafeCell::new(FnData {
                function: ManuallyDrop::new(f),
            }),
            waker: AtomicWaker::new(),
        };
        // Heap-allocate and erase the shared state's type.
        let ptr: *const () = Box::into_raw(Box::new(shared)).cast();
        let ptr: NonNull<()> = unsafe { NonNull::new_unchecked(ptr as *mut ()) };

        Self {
            ptr,
            vtable: NonNull::from(&<Shared<F, O>>::VTABLE),
            _lifetime: PhantomData,
        }
    }
    unsafe fn poll_with<T>(
        &mut self,
        cx: &mut Context<'_>,
        poll: unsafe fn(*const (), &mut Context<'_>) -> Poll<thread::Result<T>>,
    ) -> Poll<T> {
        if self.ptr.as_ptr() as usize == 1 {
            panic!("Polled `Child` after completion");
        }
        (poll)(self.ptr.as_ptr(), cx).map(|output| {
            self.ptr = NonNull::new_unchecked(1 as *mut ());
            output.unwrap_or_else(|panic| resume_unwind(panic))
        })
    }
}

impl<O: Send> CompletionFuture for Child<'_, O> {
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

impl<O: Send> Debug for Child<'_, O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad("Child")
    }
}

impl<O: Send> Drop for Child<'_, O> {
    fn drop(&mut self) {
        if self.ptr.as_ptr() as usize != 1 {
            unsafe { (self.vtable.as_ref().drop)(self.ptr.as_ptr()) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::panic::panic_any;

    use completion::future::{self, CompletionFutureExt};

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

    #[test]
    fn cancellation() {
        let pool = ThreadPool::new();

        let mut val = false;

        let child = pool.spawn_child(|| {
            wait();
            val = true;
            Box::new(12)
        });
        assert_eq!(future::block_on(child.now_or_never()), None);
        assert!(val);

        pool.miri_shutdown();
    }
}
