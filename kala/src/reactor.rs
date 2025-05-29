use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::os::fd::RawFd;
use std::task::Waker;
use std::time::{Duration, Instant};
use std::{io::Result, sync::Arc};

use mio::{Events, Interest, Poll as MioPoll, Token, Waker as MioWaker};
use slab::Slab;

use crate::timer_entry::TimerEntry;

fn slab_index(token: Token) -> usize {
    token.0 - 1
}

struct IoSource {
    waker: Option<Waker>,
    interests: Interest,
}

const REACTOR_TOKEN: Token = Token(0);

thread_local! {
    pub(crate) static REACTOR: RefCell<Reactor> = RefCell::new(Reactor::new().expect("reactor"));
}

pub(crate) struct Reactor {
    poll: MioPoll,
    events: Events,
    waker: Arc<MioWaker>,
    timers: RefCell<BinaryHeap<Reverse<TimerEntry>>>,
    sources: RefCell<Slab<IoSource>>,
    next_token: RefCell<usize>,
}

impl Reactor {
    fn new() -> Result<Self> {
        let poll = MioPoll::new()?;
        let events = Events::with_capacity(1024);
        let waker = Arc::new(MioWaker::new(poll.registry(), REACTOR_TOKEN)?);
        let timers = RefCell::new(BinaryHeap::new());
        let sources = RefCell::new(Slab::with_capacity(1024));
        let next_token = RefCell::new(0);
        Ok(Self {
            poll,
            events,
            waker,
            timers,
            sources,
            next_token,
        })
    }

    pub(crate) fn register_io(&self, fd: RawFd, interest: Interest) -> Token {
        let mut sources = self.sources.borrow_mut();
        let mut next_token = self.next_token.borrow_mut();

        *next_token += 1;
        let token = Token(*next_token);

        token
    }

    pub(crate) fn register_timer(&self, timer: TimerEntry) {
        self.timers.borrow_mut().push(Reverse(timer));
        let _ = self.waker.wake();
    }

    pub(crate) fn waker(&self) -> Arc<MioWaker> {
        self.waker.clone()
    }

    pub(crate) fn run_blocking(&mut self) {
        let timeout = self.next_timeout();
        self.poll.poll(&mut self.events, timeout).unwrap();

        for event in self.events.iter() {
            match event.token() {
                REACTOR_TOKEN => {
                    // Nothing to do. Pass control back to the runtime/executor
                }
                _token => {
                    todo!()
                }
            }
        }

        self.wake_expired_timers();
    }

    fn wake_expired_timers(&self) {
        let now = Instant::now();
        let mut timers = self.timers.borrow_mut();
        while timers.peek().is_some_and(|Reverse(e)| e.deadline <= now) {
            let Reverse(entry) = timers.pop().unwrap();
            entry.waker.wake();
        }
    }

    fn next_timeout(&self) -> Option<Duration> {
        self.timers
            .borrow()
            .peek()
            .map(|Reverse(e)| e.deadline.saturating_duration_since(Instant::now()))
    }
}
