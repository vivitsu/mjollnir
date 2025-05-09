use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{
    join_handle::{JoinHandle, Shared},
    queue::ConcurrentQueue,
    task::Task,
};

use mio::{Events, Poll as MioPoll, Token, Waker as MioWaker};

const WAKER_TOKEN: Token = Token(usize::MAX);

pub struct Executor {
    poll: RefCell<MioPoll>,
    events: RefCell<Events>,
    waker: Arc<MioWaker>,
    queue: ConcurrentQueue<Arc<Task>>,
    active: AtomicUsize,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new().expect("new executor")
    }
}

impl Executor {
    pub fn new() -> std::io::Result<Self> {
        let poll = RefCell::new(MioPoll::new()?);
        let events = RefCell::new(Events::with_capacity(1024));
        let queue = ConcurrentQueue::new();
        let waker = Arc::new(MioWaker::new(poll.borrow().registry(), WAKER_TOKEN)?);
        Ok(Self {
            poll,
            events,
            waker,
            queue,
            active: AtomicUsize::new(0),
        })
    }

    pub fn block_on<F, T>(self: Arc<Self>, future: F) -> T
    where
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let shared = Shared::new();
        let shared_clone = shared.clone();

        let wrapper = async move {
            let out = future.await;
            shared_clone.complete(out);
        };

        self.spawn_task(wrapper);

        // Drive all tasks to completion
        self.run();

        shared.take_result()
    }

    pub fn spawn<F, T>(&self, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let shared = Shared::new();
        let shared_clone = shared.clone();

        let wrapper = async move {
            let out = future.await;
            shared_clone.complete(out);
        };

        self.spawn_task(wrapper);

        JoinHandle { shared }
    }

    pub fn run(self: Arc<Self>) {
        while self.active.load(Ordering::SeqCst) > 0 {
            let mut poll = self.poll.borrow_mut();
            let mut events = self.events.borrow_mut();
            poll.poll(&mut events, None).unwrap();

            for event in events.iter() {
                match event.token() {
                    WAKER_TOKEN => {
                        while let Some(task) = self.queue.pop() {
                            let done = task.poll();
                            if done {
                                self.active.fetch_sub(1, Ordering::SeqCst);
                                self.waker.wake().unwrap();
                            }
                        }
                    }
                    _token => {
                        todo!()
                    }
                }
            }
        }
    }

    fn spawn_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        Task::spawn(future, self.waker.clone(), &self.queue);
        self.active.fetch_add(1, Ordering::SeqCst);
    }
}
