use std::{io::Result, sync::Arc};
use std::task::Waker;

use mio::{Events, Poll as MioPoll, Token, Waker as MioWaker};
use slab::Slab;

const REACTOR_TOKEN: Token = Token(0);

pub(crate) struct Reactor {
    poll: MioPoll,
    events: Events,
    waker: Arc<MioWaker>,
    _registrations: Slab<Waker>,
}

impl Reactor {
    pub(crate) fn new() -> Result<Self> {
        let poll = MioPoll::new()?;
        let events = Events::with_capacity(1024);
        let waker = Arc::new(MioWaker::new(poll.registry(), REACTOR_TOKEN)?);
        let _registrations = Slab::new();
        Ok(Self {
            poll,
            events,
            waker,
            _registrations,
        })
    }
    
    pub(crate) fn waker(&self) -> Arc<MioWaker> {
        self.waker.clone()
    }

    pub(crate) fn run_blocking(&mut self) {
        self.poll.poll(&mut self.events, None).unwrap();

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
    }
}
