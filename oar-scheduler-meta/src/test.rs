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
    let session = Session::new("sqlite::memory:");

    // Create schema
    session.create_schema();

    (session, config)
}
