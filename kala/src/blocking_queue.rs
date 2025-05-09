use std::sync::{Arc, Condvar, Mutex};

use crate::queue::ConcurrentQueue;

#[derive(Clone)]
pub(crate) struct BlockingQueue<T> {
    inner: ConcurrentQueue<T>,
    wait_lock: Arc<Mutex<()>>,
    cvar: Arc<Condvar>,
}

impl<T> BlockingQueue<T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: ConcurrentQueue::new(),
            wait_lock: Arc::new(Mutex::new(())),
            cvar: Arc::new(Condvar::new()),
        }
    }
    
    pub(crate) fn push(&self, value: T) {
        self.inner.push(value);
        self.cvar.notify_one();
    }
    
    pub(crate) fn pop_blocking(&self) -> T {
        if let Some(item) = self.inner.pop() {
            return item;
        }
        
        let mut guard = self.wait_lock.lock().unwrap();
        loop {
            if let Some(item) = self.inner.pop() {
                return item;
            }
            
            guard = self.cvar.wait(guard).unwrap();
            
        }
    }
    
    pub(crate) fn notify_one(&self) {
        self.cvar.notify_one();
    }
}