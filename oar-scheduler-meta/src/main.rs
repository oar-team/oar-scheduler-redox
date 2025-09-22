mod platform;
mod queues_schedule;
mod meta_schedule;
#[cfg(test)]
mod test;

use dotenvy::dotenv;
use log::LevelFilter;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::Session;
use platform::Platform;

fn main() {
    // Load .env file if present
    dotenv().ok();

    // Initialize logging
    env_logger::Builder::new()
        .filter(None, LevelFilter::Debug)
        .init();

    // Load configuration
    let config = Configuration::load();

    // Initialize database connection
    let session = Session::new(&config);

    // Create the platform instance
    let mut platform = Platform::from_database(session, config);

    // Meta scheduling
    meta_schedule::meta_schedule(&mut platform);

}
