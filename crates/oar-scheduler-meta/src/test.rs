// The ported tests keep the name of the Python test they mirror, even when it is not
// snake case (e.g. `test_calendar_simple_slotSet_2`).
#![allow(non_snake_case)]

use dotenvy::dotenv;
use log::LevelFilter;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_db::Session;

#[cfg(test)]
mod energy_saving_test;
#[cfg(test)]
mod job_test;
#[cfg(test)]
mod meta_schedule_test;
#[cfg(test)]
mod queues_test;
#[cfg(test)]
mod quotas_test;
#[cfg(test)]
mod resources_test;
#[cfg(test)]
mod oar3_db_test;
#[cfg(test)]
mod oar3_all_in_one_others_test;
#[cfg(test)]
mod oar3_energy_saving_test;
#[cfg(test)]
mod oar3_fairshare_test;
#[cfg(test)]
mod oar3_all_in_one_quotas_test;
#[cfg(test)]
mod oar3_all_in_one_test;
#[cfg(test)]
mod oar3_metasched_test;
#[cfg(test)]
mod oar3_walltime_test;

#[cfg(test)]
fn setup_for_tests(use_sqlite_memory: bool) -> (Session, Configuration) {
    // Load .env file if present
    dotenv().ok();

    // Initialize logging
    env_logger::Builder::new()
        .is_test(true)
        .filter(None, LevelFilter::Trace)
        .try_init()
        .ok();

    // Load configuration
    let mut config = Configuration::load();

    // Initialize database connection
    if use_sqlite_memory {
        config.db_type = "sqlite".to_string();
        config.db_hostname = ":memory:".to_string();
    }
    let session = Session::new(&config);

    // Create schema
    if use_sqlite_memory {
        session.create_schema();
    }

    (session, config)
}
