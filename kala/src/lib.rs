mod queue;
mod blocking_queue;
mod task;

pub mod join_handle;
pub mod executor;

use std::{cell::RefCell, sync::Arc};

use executor::Executor;
use join_handle::JoinHandle;
pub use kala_macros::main;

thread_local! {
    pub static CURRENT_RUNTIME: RefCell<Option<Arc<Executor>>> = const { RefCell::new(None) };
}

pub fn spawn<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + 'static,
    T: Send + 'static,
{
    CURRENT_RUNTIME.with(|slot| {
       let opt = slot.borrow();
       let executor = opt.as_ref().expect("No runtime installed - did you forget #[kala::main]?");
       
       executor.spawn(future)
   }) 
}
