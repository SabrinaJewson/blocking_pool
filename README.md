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
functions that borrow from the outer scope][spawn_child], which is not possible in
`blocking`.

## Examples

Call [`std::fs::read_to_string`][std read_to_string] from asynchronous code:

```rust
use blocking_pool::ThreadPool;

let pool = ThreadPool::new();
let filename = "file.txt";
let contents = pool.spawn_child(|| std::fs::read_to_string(&filename)).await?;
println!("The contents of {} is: {}", filename, contents);
```


## Tasks and Children

[Thread pools][ThreadPool] support two methods of running functions: tasks and children,
spawned via [`spawn_task`][spawn_task] and [`spawn_child`][spawn_child] respectively. The most
important difference is that tasks are required to live for `'static`, whereas children can have
any lifetime, allowing them to borrow from the outer scope. The trade-off is that children
cannot be detached to run independently of the outer scope; once you start one, you must see it
to completion straight after.

There are also a few smaller differences between the two:
- Tasks are spawned immediately, whereas children require the returned [`Child`][Child] to be
polled before it is started.
- [`JoinHandle`][JoinHandle] will catch panics and return an [`Err`][Err] if your function
panicked. [`Child`][Child] simply propagates them.
- [`JoinHandle`][JoinHandle] implements both [`Future`][Future] and
[`CompletionFuture`][CompletionFuture], whereas [`Child`][Child] only implements
[`CompletionFuture`][CompletionFuture].

If you need to detach the function so that it runs in the background, use a task - otherwise,
use a child.

[ThreadPool]: https://docs.rs/blocking_pool/*/blocking_pool/struct.ThreadPool.html
[spawn_child]: https://docs.rs/blocking_pool/*/blocking_pool/struct.ThreadPool.html#method.spawn_child
[Child]: https://docs.rs/blocking_pool/*/blocking_pool/struct.Child.html
[spawn_task]: https://docs.rs/blocking_pool/*/blocking_pool/struct.ThreadPool.html#method.spawn_task
[JoinHandle]: https://docs.rs/blocking_pool/*/blocking_pool/struct.JoinHandle.html

[CompletionFuture]: https://docs.rs/completion-core/0.2/completion_core/trait.CompletionFuture.html

[std read_to_string]: https://doc.rust-lang.org/stable/std/fs/fn.read_to_string.html
[Err]: https://doc.rust-lang.org/stable/core/result/enum.Result.html#variant.Err
[Future]: https://doc.rust-lang.org/stable/core/future/trait.Future.html

License: MIT OR Apache-2.0
