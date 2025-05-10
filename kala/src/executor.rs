use std::{cell::RefCell, sync::{
    atomic::{AtomicUsize, Ordering}, Arc
}};

use crate::{
    join_handle::{JoinHandle, Shared}, queue::ConcurrentQueue, reactor::Reactor, task::Task
};

thread_local! {
    static CURRENT_EXECUTOR: RefCell<Option<Arc<Executor>>> = const { RefCell::new(None) };
}

pub fn enter_runtime_scope<F, R>(executor: Arc<Executor>, f: F) -> R
where
    F: FnOnce() -> R,
{
    CURRENT_EXECUTOR.with(|cell| {
        let mut opt_exec = cell.borrow_mut();
        let old_exec = opt_exec.take();
        *opt_exec = Some(executor);
        let result = f();
        *opt_exec = old_exec;
        result
    })
}

pub struct Executor {
    reactor: Reactor,
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
        let queue = ConcurrentQueue::new();
        let reactor = Reactor::new()?;
        Ok(Self {
            reactor,
            queue,
            active: AtomicUsize::new(0),
        })
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
            self.reactor.run_blocking();
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
        Task::spawn(future, self.reactor.waker(), &self.queue);
        self.active.fetch_add(1, Ordering::SeqCst);
    }
}

pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: Send + 'static,
{
    CURRENT_EXECUTOR.with(|cell| {
        match cell.borrow().as_ref() {
            Some(executor) => executor.spawn(future),
            None => panic!("spawn called outside of a #[kala::main] runtime"),
        }
    })
}
