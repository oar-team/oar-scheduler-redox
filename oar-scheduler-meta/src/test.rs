use dotenvy::dotenv;
use log::LevelFilter;
use oar_scheduler_core::model::configuration::{Configuration, DEFAULT_CONFIG_FILE};
use oar_scheduler_db::Session;

#[cfg(test)]
mod resources;

async fn setup() -> (Session, Configuration) {
    // Load .env file if present
    dotenv().ok();

    // Initialize logging
    env_logger::Builder::new()
        .filter(None, LevelFilter::Info)
        .filter(Some("oar3_rust::scheduler::hierarchy"), LevelFilter::Debug)
        .init();

    // Load configuration
    let config = Configuration::load_from_file(DEFAULT_CONFIG_FILE);

    // Initialize database connection
    let session = Session::new("sqlite::memory:", 1).await;

    // Create schema
    session.create_schema().await;

    (session, config)
}
