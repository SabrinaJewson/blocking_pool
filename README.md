# blocking_pool

A thread pool for running synchronous I/O in asynchronous applications.

In asynchronous code, blocking the thread - that is calling some function which takes a long
time to return - is a very bad idea. It will prevent all the other asynchronous tasks from
running, and can cause all sorts of undesirable behaviour. However, sometimes blocking calls
are needed; for example many libraries are not built with `async`, but you might want to use
them in an `async` context. The solution is to have a thread pool where blocking code can be
offloaded to, so that it doesn't block the main asynchronous threads.

In comparison with [`blocking`](https://docs.rs/blocking), another crate that provides similar
functionality, this crate uses a local thread pool instead of a global one. This allows for
multiple thread pools to be created, and each one can be configured, allowing you to fine-tune
your application for maximum speed. Also, this crate has support for [spawning blocking
functions that borrow from the outer scope](ThreadPool::spawn_child), which is not possible in
`blocking`.

## Examples

Call [`std::fs::read_to_string`] from asynchronous code:

```rust
use blocking_pool::ThreadPool;

let pool = ThreadPool::new();
let filename = "file.txt";
let contents = pool.spawn_child(|| std::fs::read_to_string(&filename)).await?;
println!("The contents of {} is: {}", filename, contents);
```


## Tasks and Children

[Thread pools](ThreadPool) support two methods of running functions: tasks and children,
spawned via [`spawn_task`](ThreadPool::spawn_task) and [`spawn_child`](ThreadPool::spawn_child)
respectively. The most important difference is that tasks are required to live for `'static`,
whereas children can have any lifetime, allowing them to borrow from the outer scope. The
trade-off is that children cannot be detached to run independently of the outer scope; once you
start one, you must see it to completion straight after.

There are also a few smaller differences between the two:
- Tasks are spawned immediately, whereas children require the returned [`Child`] to be polled
before it is started.
- [`JoinHandle`] will catch panics and return an [`Err`] if your function panicked. [`Child`]
simply propagates them.
- [`JoinHandle`] implements both [`Future`](core::future::Future) and [`CompletionFuture`],
whereas [`Child`] only implements [`CompletionFuture`].
- [`JoinHandle`] is [`Unpin`], whereas [`Child`] is `!`[`Unpin`]. This can make [`Child`]
slightly harder to use.
- [`Child`] has the type of the function being run as a generic parameter, whereas
[`JoinHandle`] only has the output type of the function. This makes it difficult to store
[`Child`] in structs, whereas [`JoinHandle`] can be stored easily.
- [`JoinHandle`] has a mandatory heap allocation, whereas [`Child`] can be theoretically
implemented without any heap allocations at all. Currently it still requires one due to
temporary [limitations in Rust](https://github.com/rust-lang/rust/issues/63818).
- Children are slightly faster than tasks due to less synchronization overhead needed.

If you need to detach the function so that it runs in the background, use a task - otherwise,
use a child.

[`CompletionFuture`]: completion_core::CompletionFuture

License: MIT OR Apache-2.0
