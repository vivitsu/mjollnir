use std::{
    pin::Pin, task::{Context, Poll}, thread, time::{Duration, Instant}
};

use kala::executor::Executor;

struct Timer {
    deadline: Instant,
    started: bool,
}

impl Timer {
    fn new(duration: Duration) -> Self {
        let deadline = match Instant::now().checked_add(duration) {
            Some(deadline) => deadline,
            _ => Instant::now() + Duration::from_secs(1),
        };

        Self {
            deadline,
            started: false,
        }
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = Instant::now();
        if now >= self.deadline {
            Poll::Ready(())
        } else {
            if !self.started {
                let waker = cx.waker().clone();
                let deadline = self.deadline;

                thread::spawn(move || {
                    if deadline > now {
                        thread::sleep(deadline - now);
                    }
                    waker.wake();
                });

                self.started = true;
            }
            Poll::Pending
        }
    }
}

fn sleep(duration: Duration) -> Timer {
    Timer::new(duration)
}

fn main() {
    let mut runtime = Executor::new().expect("runtime");
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
