use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use crate::{reactor::REACTOR, timer_entry::TimerEntry};

pub struct Timer {
    deadline: Instant,
    registered: bool,
}

impl Timer {
    fn new(duration: Duration) -> Self {
        let deadline = match Instant::now().checked_add(duration) {
            Some(deadline) => deadline,
            _ => Instant::now() + Duration::from_secs(1),
        };

        Self {
            deadline,
            registered: false,
        }
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = Instant::now();
        if now >= self.deadline {
            Poll::Ready(())
        } else if !self.registered {
            let waker = cx.waker().clone();
            let event = TimerEntry {
                deadline: self.deadline,
                waker,
            };
            REACTOR.with_borrow(|reactor| {
                reactor.register_timer(event);
            });
            self.registered = true;
            Poll::Pending
        } else {
            Poll::Pending
        }
    }
}

pub fn sleep(duration: Duration) -> Timer {
    Timer::new(duration)
}
