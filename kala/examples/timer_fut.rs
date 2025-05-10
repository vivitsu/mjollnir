use kala::{executor::Executor, sleep};
use std::time::{Duration, Instant};

fn main() {
    let mut runtime = Executor::new();
    let t1 = runtime.spawn(async {
        println!("starting timer 1");
        sleep(Duration::from_millis(1000)).await;
        println!("timer 1 done!");
    });

    let t2 = runtime.spawn(async {
        println!("starting timer 2");
        sleep(Duration::from_millis(1000)).await;
        println!("timer 2 done!");
    });

    let now = Instant::now();
    runtime.block_on(async move {
        t1.await;
        t2.await;
    });

    println!(
        "Ran both timers for 1 second each, but total runtime was {}ms",
        now.elapsed().as_millis()
    );
}
