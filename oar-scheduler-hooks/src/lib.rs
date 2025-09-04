/*
 * Copyright (c) 2025 Clément GRENNERAT
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, version 3.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see https://www.gnu.org/licenses/.
 *
 */

use indexmap::IndexMap;
use log::debug;
use oar_scheduler_core::hooks::HooksHandler;
use oar_scheduler_core::model::job::{Job, Moldable, ProcSet};
use oar_scheduler_core::platform::PlatformConfig;
use oar_scheduler_core::scheduler::slotset::SlotSet;

pub struct Hooks {

}

impl Hooks {
    pub fn new() -> Option<Self> {
        None
        //Some(Self {})
    }
}

#[allow(unused_variables)]
impl HooksHandler for Hooks {
    fn hook_sort(&self, platform_config: &PlatformConfig, queues: &Vec<String>, waiting_jobs: &mut IndexMap<i64, Job>) -> bool {
        debug!("Sort hook called");
        false
    }
    fn hook_assign(&self, slot_set: &mut SlotSet, job: &mut Job, min_begin: Option<i64>) -> bool {
        debug!("Assign hook called");
        false
    }
    fn hook_find(&self, slot_set: &SlotSet, job: &Job, moldable: &Moldable, min_begin: Option<i64>, available_resources: ProcSet) -> Option<Option<ProcSet>> {
        debug!("Find hook called");
        None
    }
}
