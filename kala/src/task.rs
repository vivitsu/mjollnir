use std::{
    cell::RefCell,
    mem,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use crate::queue::ConcurrentQueue;
use mio::Waker as MioWaker;

type TaskPtr = *const ();

pub(crate) struct Task {
    future: RefCell<Pin<Box<dyn Future<Output = ()> + 'static>>>,
    task_queue: ConcurrentQueue<Arc<Task>>,
    reactor_waker: Arc<MioWaker>,
}

impl Task {
    pub(crate) fn schedule(self: &Arc<Self>) {
        self.task_queue.push(self.clone());
        self.reactor_waker.wake().unwrap();
    }

    pub(crate) fn poll(self: &Arc<Self>) -> bool {
        let raw = RawWaker::new(
            Arc::into_raw(self.clone()).cast::<()>(),
            create_arc_task_vtable(),
        );
        let waker = unsafe { Waker::from_raw(raw) };
        let mut ctx = Context::from_waker(&waker);

        let mut fut = self.future.borrow_mut();
        match fut.as_mut().poll(&mut ctx) {
            Poll::Pending => false,
            Poll::Ready(()) => true,
        }
    }

    pub(crate) fn spawn<F>(future: F, reactor_waker: Arc<MioWaker>, task_queue: &ConcurrentQueue<Arc<Task>>)
    where
        F: Future<Output = ()> + 'static,
    {
        #[allow(clippy::arc_with_non_send_sync)]
        let task = Arc::new(Task {
            future: RefCell::new(Box::pin(future)),
            task_queue: task_queue.clone(),
            reactor_waker: reactor_waker.clone(),
        });

        task_queue.push(task);
        reactor_waker.wake().unwrap();
    }
}

unsafe fn clone_arc_task_raw(data: TaskPtr) -> RawWaker {
    let _arc = mem::ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Task>()) });
    let _arc_clone: mem::ManuallyDrop<_> = _arc.clone();
    RawWaker::new(data, create_arc_task_vtable())
}

unsafe fn wake_arc_task_raw(data: TaskPtr) {
    let arc = unsafe { Arc::from_raw(data.cast::<Task>()) };
    Task::schedule(&arc);
}

unsafe fn wake_arc_task_by_ref_raw(data: TaskPtr) {
    let arc = mem::ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Task>()) });
    Task::schedule(&arc);
}

unsafe fn drop_arc_task_raw(data: TaskPtr) {
    drop(unsafe { Arc::from_raw(data.cast::<Task>()) });
}

fn create_arc_task_vtable() -> &'static RawWakerVTable {
    &RawWakerVTable::new(
        clone_arc_task_raw,
        wake_arc_task_raw,
        wake_arc_task_by_ref_raw,
        drop_arc_task_raw,
    )
}
