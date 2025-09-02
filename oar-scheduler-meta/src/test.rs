use dotenvy::dotenv;
use log::LevelFilter;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_db::Session;

#[cfg(test)]
mod job_test;
#[cfg(test)]
mod queues_test;
#[cfg(test)]
mod quotas_test;
#[cfg(test)]
mod resources_test;

fn setup_for_tests() -> (Session, Configuration) {
    // Load .env file if present
    dotenv().ok();

    // Initialize logging
    env_logger::Builder::new()
        .is_test(true)
        .filter(None, LevelFilter::Info)
        .filter(Some("oar3_rust::scheduler::hierarchy"), LevelFilter::Debug)
        .try_init()
        .ok();

    // Load configuration
    let config = Configuration::load();

    // Initialize database connection
    let session = Session::new("sqlite::memory:", 1);

    // Create schema
    session.create_schema();

    (session, config)
}
