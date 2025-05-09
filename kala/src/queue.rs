use std::{collections::VecDeque, sync::{Arc, RwLock}};

#[derive(Clone)]
pub(crate) struct ConcurrentQueue<T> {
    queue: Arc<RwLock<VecDeque<T>>>,
}

impl<T> Default for ConcurrentQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConcurrentQueue<T> {
    pub(crate) fn new() -> Self {
        let queue = Arc::new(RwLock::new(VecDeque::new()));
        Self {
           queue 
        }
    }
    
    pub(crate) fn push(&self, value: T) {
        self.queue.write().unwrap().push_back(value);
    }
    
    pub(crate) fn pop(&self) -> Option<T> {
        self.queue.write().unwrap().pop_front()
    }
}