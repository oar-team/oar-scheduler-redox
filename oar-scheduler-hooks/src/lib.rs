use log::info;
use oar_scheduler_core::HooksHandler;
use oar_scheduler_core::models::Job;
use oar_scheduler_core::scheduler::slot::SlotSet;

pub struct Hooks {

}

impl Hooks {
    pub fn new() -> Self {
        Hooks {}
    }
}

impl HooksHandler for Hooks {
    fn hook_assign_job(&self, slot_set: &mut SlotSet, job: &mut Job, min_begin: Option<i64>) -> bool {
        info!("Assign job hook called");
        false
    }
}
