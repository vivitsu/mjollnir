use std::{pin::Pin, sync::{Arc, Mutex}, task::{Context, Poll, Waker}};

pub(crate) struct Shared<T> {
    result: Mutex<Option<T>>,
    waker: Mutex<Option<Waker>>,
}

impl<T> Shared<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Shared {
            result: Mutex::new(None),
            waker: Mutex::new(None),
        })
    }
    
    pub(crate) fn complete(&self, val: T) {
        *self.result.lock().unwrap() = Some(val);
        if let Some(w) = self.waker.lock().unwrap().take() {
            w.wake();
        }
    }
}

pub struct JoinHandle<T> {
    pub(crate) shared: Arc<Shared<T>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut slot = self.shared.result.lock().unwrap();
        if let Some(val) = slot.take() {
            Poll::Ready(val)
        } else {
            *self.shared.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}