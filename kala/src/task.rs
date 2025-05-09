use std::{cell::RefCell, mem, pin::Pin, sync::Arc, task::{Context, Poll, RawWaker, RawWakerVTable, Waker}};

use crate::blocking_queue::BlockingQueue;

type TaskPtr = *const ();

pub(crate) struct Task {
    future: RefCell<Pin<Box<dyn Future<Output = ()> + 'static>>>,
    task_queue: BlockingQueue<Arc<Task>>,
}

impl Task {
    pub(crate) fn schedule(self: &Arc<Self>) {
        self.task_queue.push(self.clone());
    }
    
    pub(crate) fn poll(self: &Arc<Self>) -> bool {
        let raw = RawWaker::new(Arc::into_raw(self.clone()).cast::<()>(), create_arc_task_vtable());
        let waker = unsafe { Waker::from_raw(raw) };
        let mut ctx = Context::from_waker(&waker);
        
        let mut fut = self.future.borrow_mut();
        match fut.as_mut().poll(&mut ctx) {
            Poll::Pending => false,
            Poll::Ready(()) => true,
        }
    }
   
    pub(crate) fn spawn<F>(future: F, task_queue: &BlockingQueue<Arc<Task>>) 
    where 
        F: Future<Output = ()> + 'static 
    {
        #[allow(clippy::arc_with_non_send_sync)]
        let task = Arc::new(Task {
            future: RefCell::new(Box::pin(future)),
            task_queue: task_queue.clone(), 
        });
        
        task_queue.push(task);
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
