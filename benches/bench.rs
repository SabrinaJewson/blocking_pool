use std::thread;
use std::time::Duration;

use criterion::Criterion;

use blocking_pool::ThreadPool;

fn main() {
    let mut c = Criterion::default().configure_from_args();

    let pool = ThreadPool::new();

    // Make sure a thread exists on the thread pool to begin with, to give more consistent results.
    pool.spawn_boxed(Box::new(|| {}));

    c.bench_function("noop", |b| {
        b.iter(|| {
            pool.spawn_boxed(Box::new(|| {}));
            pool.wait_all_complete();
        });
    });

    let sleep = || thread::sleep(Duration::from_micros(1));
    c.bench_function("sleeping task", |b| {
        b.iter(|| {
            pool.spawn_boxed(Box::new(sleep));
            pool.wait_all_complete();
        });
    });
}
