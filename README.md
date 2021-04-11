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
your application for maximum speed. Also, this crate has support for spawning blocking
functions that borrow from the outer scope, which is not possible in `blocking`.

## Examples

Call [`std::fs::read_to_string`][std read_to_string] from asynchronous code:

```rust
use blocking_pool::ThreadPool;

let pool = ThreadPool::new();
let filename = "file.txt";
let contents = pool.spawn(|| std::fs::read_to_string(&filename)).await?;
println!("The contents of {} is: {}", filename, contents);
```

[std read_to_string]: https://doc.rust-lang.org/stable/std/fs/fn.read_to_string.html

License: MIT OR Apache-2.0
