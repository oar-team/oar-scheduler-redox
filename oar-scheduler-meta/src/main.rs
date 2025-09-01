mod platform;
mod queues_schedule;
mod meta_schedule;
mod test;

use dotenvy::dotenv;
use log::LevelFilter;
use oar_scheduler_core::model::configuration::Configuration;
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
    let config = Configuration::load();

    // Initialize database connection
    let session = Session::new("sqlite::memory:", 1).await;

    // Seed database for testing
    session.create_schema().await;

    // Create the platform instance
    let mut platform = Platform::from_database(session, config).await;

    // Meta scheduling
    meta_schedule::meta_schedule(&mut platform).await;

}
