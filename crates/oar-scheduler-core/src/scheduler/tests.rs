// The ported tests keep the name of the Python test they mirror, even when it is not
// snake case (e.g. `test_calendar_simple_slotSet_2`).
#![allow(non_snake_case)]

#[cfg(test)]
pub mod slot_test;
#[cfg(test)]
pub mod hierarchy_test;
#[cfg(test)]
mod quotas_test;
#[cfg(test)]
mod platform_mock;
#[cfg(test)]
mod time_sharing_test;
#[cfg(test)]
mod dependencies_test;
#[cfg(test)]
mod container_test;
#[cfg(test)]
mod placeholder_test;
#[cfg(test)]
mod quotas_parsing_test;
#[cfg(test)]
mod temporal_quotas_test;
#[cfg(test)]
mod oar3_scheduling_test;
