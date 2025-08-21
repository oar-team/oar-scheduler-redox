use log::debug;
use oar_scheduler_core::hooks::HooksHandler;
use oar_scheduler_core::models::{Job, Moldable, ProcSet};
use oar_scheduler_core::scheduler::slot::SlotSet;

pub struct Hooks {

}

impl Hooks {
    pub fn new() -> Option<Self> {
        None
        //Some(Self {})
    }
}

impl HooksHandler for Hooks {
    fn hook_assign(&self, slot_set: &mut SlotSet, job: &mut Job, min_begin: Option<i64>) -> bool {
        debug!("Assign hook called");
        false
    }
    fn hook_find(&self, slot_set: &SlotSet, job: &Job, moldable: &Moldable, min_begin: Option<i64>, available_resources: ProcSet) -> Option<Option<ProcSet>> {
        debug!("Find hook called");
        None
    }
}
