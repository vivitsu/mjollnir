use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};

use crate::{blocking_queue::BlockingQueue, join_handle::{JoinHandle, Shared}, task::Task};

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
 
        Task::spawn(wrapper, &self.queue);
        self.active.fetch_add(1, Ordering::SeqCst);
        
        JoinHandle {
            shared
        }
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