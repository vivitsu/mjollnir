use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::os::fd::RawFd;
use std::task::Waker;
use std::time::{Duration, Instant};
use std::{io::Result, sync::Arc};

use mio::unix::SourceFd;
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
}

impl Reactor {
    fn new() -> Result<Self> {
        let poll = MioPoll::new()?;
        let events = Events::with_capacity(1024);
        let waker = Arc::new(MioWaker::new(poll.registry(), REACTOR_TOKEN)?);
        let timers = RefCell::new(BinaryHeap::new());
        let sources = RefCell::new(Slab::with_capacity(1024));
        Ok(Self {
            poll,
            events,
            waker,
            timers,
            sources,
        })
    }

    pub(crate) fn register_io(&self, fd: RawFd, interests: Interest) -> Result<Token> {
        let mut sources = self.sources.borrow_mut();
        let mut source = SourceFd(&fd);
        let entry = sources.vacant_entry();
        let slab_index = entry.key() + 1; // Token(0) is reserved for the reactor
        let token = Token(slab_index);
        self.poll
            .registry()
            .register(&mut source, token, interests)?;

        entry.insert(IoSource {
            waker: None,
            interests,
        });

        Ok(token)
    }

    pub(crate) fn register_io_waker(&self, token: Token, waker: Waker) {
        let mut sources = self.sources.borrow_mut();
        if let Some(source) = sources.get_mut(slab_index(token)) {
            source.waker = Some(waker);
        }
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
                token => {
                    let slab_index = slab_index(token);
                    let mut sources = self.sources.borrow_mut();
                    if let Some(source) = sources.get_mut(slab_index) {
                        if let Some(waker) = source.waker.take() {
                            waker.wake()
                        }
                    }
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
