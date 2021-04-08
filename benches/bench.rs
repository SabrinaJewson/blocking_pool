use std::thread;
use std::time::Duration;

use completion::future;
use criterion::Criterion;

use blocking_pool::ThreadPool;

fn main() {
    let mut c = Criterion::default().configure_from_args();

    let pool = ThreadPool::new();

    // Make sure a thread exists on the thread pool to begin with, to give more consistent results.
    pool.spawn_task(|| {});

    c.bench_function("noop task", |b| {
        b.iter(|| future::block_on(pool.spawn_task(|| {})));
    });
    c.bench_function("noop child", |b| {
        b.iter(|| future::block_on(pool.spawn_child(|| {})));
    });

    let sleep = || thread::sleep(Duration::from_micros(1));
    c.bench_function("sleeping task", |b| {
        b.iter(|| future::block_on(pool.spawn_task(sleep)));
    });
    c.bench_function("sleeping child", |b| {
        b.iter(|| future::block_on(pool.spawn_child(sleep)));
    });
}
