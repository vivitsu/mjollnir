use std::{pin::Pin, sync::Arc, task::{Context, Poll}, thread, time::{Duration, Instant}};

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
    let runtime = Arc::new(Executor::new());
    let now = Instant::now();
    let rt = runtime.clone();
    runtime.block_on(async move {
        rt.spawn(async {
            println!("starting timer 1");
            sleep(Duration::from_millis(1000)).await;
            println!("timer 1 done!")
        });
        
        rt.spawn(async {
            println!("starting timer 2");
            sleep(Duration::from_millis(1000)).await;
            println!("timer 2 done!")
        });
    });
    println!("Ran both timers for 1 second each, but total runtime was {}ms", now.elapsed().as_millis());
}