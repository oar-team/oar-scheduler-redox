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

use crate::platform::Platform;
use crate::test::setup_for_tests;
use dotenvy::dotenv;
use log::{info, LevelFilter};
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::Session;

const OAR_CONFIG: &str = include_str!("../../oar_config.env");
const QUOTAS_CONFIG: &str = include_str!("../../quotas_config.json");

fn quotas_setup() -> Platform {
    // Create temp files for configs
    let oar_config_file = tempfile::NamedTempFile::new().expect("Failed to create temp file for oar config");
    std::fs::write(oar_config_file.path(), OAR_CONFIG).expect("Failed to write oar config to temp file");
    oar_config_file.path().to_str().unwrap().to_string();
    let quotas_config_file = tempfile::NamedTempFile::new().expect("Failed to create temp file for quotas config");
    std::fs::write(quotas_config_file.path(), QUOTAS_CONFIG).expect("Failed to write quotas config to temp file");
    quotas_config_file.path().to_str().unwrap().to_string();
    unsafe {
        std::env::set_var("OARCONFFILE", oar_config_file.path());
    }

    let (session, mut config) = setup_for_tests(true);
    info!("quotas config path: {}", quotas_config_file.path().to_str().unwrap());
    config.quotas_conf_file = Some(quotas_config_file.path().to_str().unwrap().to_string());

    Platform::from_database(session, config)
}

#[test]
fn quotas_loading_test() {
    let platform = quotas_setup();
    let quotas_config = &platform.get_platform_config().quotas_config;
    println!("Quotas config: {:?}", quotas_config);

    assert!(quotas_config.enabled);
    assert_eq!(quotas_config.default_rules.len(), 2);
    assert_eq!(quotas_config.tracked_job_types.as_ref(), &["*".into()]);
    assert!(quotas_config.calendar.is_some());
    let calendar = quotas_config.calendar.as_ref().unwrap();
    assert_eq!(calendar.ordered_periodicals().len(), 11); // 5 for workdays, 4 for workdays nights, 2 for weekends
    assert_eq!(calendar.ordered_oneshots().len(), 2);
    assert_eq!(calendar.rules_map.len(), 4);
}
