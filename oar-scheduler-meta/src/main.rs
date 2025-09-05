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

fn main() {
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
    let session = Session::new(&config);

    // Create the platform instance
    let mut platform = Platform::from_database(session, config);

    // Meta scheduling
    meta_schedule::meta_schedule(&mut platform);

}
