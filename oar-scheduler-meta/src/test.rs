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
