mod platform;
mod queues_schedule;
mod meta_schedule;

use dotenvy::dotenv;
use log::LevelFilter;
use oar_scheduler_core::model::configuration::{Configuration, DEFAULT_CONFIG_FILE};
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::Session;
use platform::Platform;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
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

    // Create the platform instance
    let mut platform = Platform::from_database(session, config).await;

    // Meta scheduling
    meta_schedule::meta_schedule(&mut platform).await;

}





