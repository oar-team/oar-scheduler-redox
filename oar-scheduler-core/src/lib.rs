use std::cell::OnceCell;
use crate::models::Job;
use crate::scheduler::slot::SlotSet;

pub mod models;
pub mod platform;
pub mod scheduler;

pub mod auto_bench_fct {
    pub use auto_bench_fct::print_bench_fct_hy_results;
    pub use auto_bench_fct::print_bench_fct_results;
}

thread_local! {
    pub(crate) static HOOKS_HANDLER: HooksManager = HooksManager::new();
}

pub(crate) struct HooksManager {
    hooks_handler: OnceCell<Box<dyn HooksHandler>>,
}
impl HooksManager {
    fn new() -> Self {
        HooksManager {
            hooks_handler: OnceCell::new(),
        }
    }
    fn set_hooks_handler<H>(&self, hooks_handler: H)
    where
        H: HooksHandler + 'static,
    {
        if let Some(_old) = self.hooks_handler.get() {
            panic!("Hooks handler is already set.");
        }
        let _ = self.hooks_handler.set(Box::new(hooks_handler));
    }

    pub fn hook_assign_job(&self, slot_set: &mut SlotSet, job: &mut Job, min_begin: Option<i64>) -> bool {
        if self.hooks_handler.get().is_none() {
            return false;
        }
        self.hooks_handler.get().unwrap().hook_assign_job(slot_set, job, min_begin)
    }
}
pub fn set_hooks_handler<H>(hooks_handler: H)
where
    H: HooksHandler + 'static,
{
    HOOKS_HANDLER.with(|hooks_manager| {
        hooks_manager.set_hooks_handler(hooks_handler);
    });
}

pub trait HooksHandler {
    fn hook_assign_job(&self, slot_set: &mut SlotSet, job: &mut Job, min_begin: Option<i64>) -> bool;
}

