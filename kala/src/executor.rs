use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};

use crate::{blocking_queue::BlockingQueue, task::Task};

pub struct Executor {
    queue: BlockingQueue<Arc<Task>>,
    active: AtomicUsize,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    pub fn new() -> Self {
        Self {
            queue: BlockingQueue::new(),
            active: AtomicUsize::new(0),
        }
    }
    
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static
    {
        self.active.fetch_add(1, Ordering::SeqCst);
        Task::spawn(future, &self.queue)
    }
    
    pub fn run(&self) {
        while self.active.load(Ordering::SeqCst) > 0 {
            let task = self.queue.pop_blocking();
            let done = task.poll();
            if done {
                self.active.fetch_sub(1, Ordering::SeqCst);
                self.queue.notify_one();
            }
        }
    }
}