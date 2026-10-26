use crate::platform::Platform;
use log::info;
use oar_scheduler_core::model::configuration::Configuration;
use oar_scheduler_core::platform::PlatformTrait;
use oar_scheduler_db::Session;

const OAR_CONFIG: &str = include_str!("../../oar_config.env");
const QUOTAS_CONFIG: &str = include_str!("../../quotas_config.json");

fn quotas_setup() -> Platform {
    // Create a temp file for the quotas config.
    let quotas_config_file = tempfile::NamedTempFile::new().expect("Failed to create temp file for quotas config");
    std::fs::write(quotas_config_file.path(), QUOTAS_CONFIG).expect("Failed to write quotas config to temp file");

    // Build the configuration from the embedded oar config string, WITHOUT touching
    // any global environment variable (doing so would race with the other tests).
    let mut config = Configuration::load_from_str(OAR_CONFIG).expect("Failed to parse oar config");
    // Use an in-memory sqlite database and the freshly created quotas config file.
    config.db_type = "sqlite".to_string();
    config.db_hostname = ":memory:".to_string();
    config.quotas_conf_file = Some(quotas_config_file.path().to_str().unwrap().to_string());

    info!("quotas config path: {}", quotas_config_file.path().to_str().unwrap());

    let session = Session::new(&config);
    session.create_schema();

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
