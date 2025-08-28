use crate::model::job::{Job, Moldable, ProcSet};
use crate::platform::PlatformConfig;
use crate::scheduler::slotset::SlotSet;
use indexmap::IndexMap;
use std::cell::OnceCell;
use std::rc::Rc;

thread_local! {
    static HOOKS_HANDLER: Rc<HooksManager> = Rc::new(HooksManager::new());
}

pub trait HooksHandler {
    /// Overrides the job sorting process. This hook should sort the `waiting_jobs` in place.
    fn hook_sort(&self, platform_config: &PlatformConfig, queues: &Vec<String>, waiting_jobs: &mut IndexMap<u32, Job>) -> bool;

    /// Overrides the single job scheduling on a slot set process. This hook should define the `assignment` property of `Job`.
    /// It will override quotas, timesharing, and placeholders, but not container/inner jobs.
    /// The dependencies can be taken into account through the `min_begin` function parameter.
    fn hook_assign(&self, slot_set: &mut SlotSet, job: &mut Job, min_begin: Option<i64>) -> bool;

    /// Overrides the process of finding resources for a moldable job.
    /// The top-level `Option` indicated if the hook is active or not, and the inner `Option` indicates the find process result:
    /// either the job can’t be scheduled (`None`, no resources available), or it can be scheduled on the returned resources (`Some(ProcSet)`).
    fn hook_find(
        &self,
        slot_set: &SlotSet,
        job: &Job,
        moldable: &Moldable,
        min_begin: Option<i64>,
        available_resources: ProcSet,
    ) -> Option<Option<ProcSet>>;
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

    pub fn hook_sort(&self, platform_config: &PlatformConfig, queues: &Vec<String>, waiting_jobs: &mut IndexMap<u32, Job>) -> bool {
        if self.hooks_handler.get().is_none() {
            return false;
        }
        self.hooks_handler
            .get()
            .unwrap()
            .hook_sort(platform_config, queues, waiting_jobs)
    }
    pub fn hook_assign(&self, slot_set: &mut SlotSet, job: &mut Job, min_begin: Option<i64>) -> bool {
        if self.hooks_handler.get().is_none() {
            return false;
        }
        self.hooks_handler.get().unwrap().hook_assign(slot_set, job, min_begin)
    }
    pub fn hook_find(
        &self,
        slot_set: &SlotSet,
        job: &Job,
        moldable: &Moldable,
        min_begin: Option<i64>,
        available_resources: ProcSet,
    ) -> Option<Option<ProcSet>> {
        if self.hooks_handler.get().is_none() {
            return None;
        }
        self.hooks_handler
            .get()
            .unwrap()
            .hook_find(slot_set, job, moldable, min_begin, available_resources)
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
pub(crate) fn get_hooks_manager() -> Rc<HooksManager> {
    HOOKS_HANDLER.with(|hooks_manager| hooks_manager.clone())
}
