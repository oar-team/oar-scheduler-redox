mod platform;
mod queues_schedule;
mod meta_schedule;
mod notification;
mod walltime_change;
mod energy_saving;
mod greta;
mod zmq;
#[cfg(test)]
mod test;

use dotenvy::dotenv;
use log::LevelFilter;
use oar_scheduler_core::model::configuration::Configuration;
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
