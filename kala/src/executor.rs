use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{
    join_handle::{JoinHandle, Shared}, queue::ConcurrentQueue, reactor::REACTOR, task::Task
};

pub struct Executor {
    queue: ConcurrentQueue<Arc<Task>>,
    active: AtomicUsize,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    pub fn new() -> Self {
        let queue = ConcurrentQueue::new();
        Self {
            queue,
            active: AtomicUsize::new(0),
        }
    }

    pub fn block_on<F, T>(&mut self, future: F) -> T
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

    pub fn run(&mut self) {
        while self.active.load(Ordering::SeqCst) > 0 {
            REACTOR.with_borrow_mut(|reactor| reactor.run_blocking());
            while let Some(task) = self.queue.pop() {
                let done = task.poll();
                if done {
                    self.active.fetch_sub(1, Ordering::SeqCst);
                }
            }
        }
    }

    fn spawn_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        let waker = REACTOR.with_borrow(|reactor| reactor.waker());
        Task::spawn(future, waker, &self.queue);
        self.active.fetch_add(1, Ordering::SeqCst);
    }
}
