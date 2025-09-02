use crate::platform::Platform;
use crate::test::setup_for_tests;
use oar_scheduler_core::model::job::{PlaceholderType, TimeSharingType};
use oar_scheduler_db::model::{get_waiting_jobs, NewJob};
use std::collections::HashMap;

fn insert_jobs_for_tests(platform: &Platform) {
    let j1 = NewJob {
        user: Some("user1".to_string()),
        queue_name: "default".to_string(),
        res: vec![(60, vec![("resource_id=1".to_string(), "".to_string())])],
        types: vec!["placeholder=test".to_string(), "timesharing=*,user".to_string()],
    }
        .insert(platform.session())
        .expect("insert job 1");

    let j2 = NewJob {
        user: Some("user2".to_string()),
        queue_name: "besteffort".to_string(),
        res: vec![(120, vec![("nodes=2/cpu=1".to_string(), "".to_string())])],
        types: vec!["besteffort".to_string(), "container".to_string()],
    }
        .insert(platform.session())
        .expect("insert job 2");

    let j3 = NewJob {
        user: Some("user3".to_string()),
        queue_name: "default".to_string(),
        res: vec![(30, vec![("nodes=1".to_string(), "".to_string())])],
        types: vec![],
    }
        .insert(platform.session())
        .expect("insert job 3");

    let j4 = NewJob {
        user: Some("user4".to_string()),
        queue_name: "default".to_string(),
        res: vec![(
            300,
            vec![
                ("switch=1/nodes=4".to_string(), "".to_string()),
                ("licence=20".to_string(), "lic_type = 'mathlab'".to_string()),
            ],
        )],
        types: vec!["container".to_string()],
    }
        .insert(platform.session())
        .expect("insert job 4");

    let j5 = NewJob {
        user: Some("user5".to_string()),
        queue_name: "besteffort".to_string(),
        res: vec![(90, vec![("nodes=3".to_string(), "".to_string())])],
        types: vec!["besteffort".to_string(), "inner=1".to_string()],
    }
        .insert(platform.session())
        .expect("insert job 5");

    assert!(j1 > 0 && j2 > 0 && j3 > 0 && j4 > 0 && j5 > 0);
}

#[test]
fn test_insert_and_retrieve_job() {
    let (session, config) = setup_for_tests();
    let platform = Platform::from_database(session, config);
    insert_jobs_for_tests(&platform);

    let default_jobs = get_waiting_jobs(&platform.session(), Some(vec!["default".to_string()]), "None".to_string()).unwrap();
    let besteffort_jobs = get_waiting_jobs(&platform.session(), Some(vec!["besteffort".to_string()]), "None".to_string()).unwrap();

    assert_eq!(default_jobs.len(), 3);
    assert_eq!(besteffort_jobs.len(), 2);

    let job_1 = &default_jobs[0];
    let job_2 = &besteffort_jobs[0];
    let job_3 = &default_jobs[1];
    let job_4 = &default_jobs[2];
    let job_5 = &besteffort_jobs[1];

    // Checking global properties
    assert_eq!(job_1.user.as_deref(), Some("user1"));
    assert_eq!(job_1.queue, "default".into());
    let job_1_types: HashMap<Box<str>, Option<Box<str>>> =
        [("placeholder".into(), Some("test".into())), ("timesharing".into(), Some("*,user".into()))]
            .into_iter()
            .collect();
    assert_eq!(job_1.types, job_1_types);
    assert_eq!(job_1.placeholder, PlaceholderType::Placeholder("test".into()));
    assert_eq!(job_1.time_sharing, Some(TimeSharingType::UserAll));

    assert_eq!(job_2.user.as_deref(), Some("user2"));
    assert_eq!(job_2.queue, "besteffort".into());
    let job_2_types: HashMap<Box<str>, Option<Box<str>>> = [("besteffort".into(), None), ("container".into(), None)].into_iter().collect();
    assert_eq!(job_2.types, job_2_types);

    assert_eq!(job_3.user.as_deref(), Some("user3"));
    assert_eq!(job_3.queue, "default".into());
    assert_eq!(job_3.types, HashMap::new());

    assert_eq!(job_4.user.as_deref(), Some("user4"));
    assert_eq!(job_4.queue, "default".into());
    let job_4_types: HashMap<Box<str>, Option<Box<str>>> = [("container".into(), None)].into_iter().collect();
    assert_eq!(job_4.types, job_4_types);

    assert_eq!(job_5.user.as_deref(), Some("user5"));
    assert_eq!(job_5.queue, "besteffort".into());
    let job_5_types: HashMap<Box<str>, Option<Box<str>>> = [("besteffort".into(), None), ("inner".into(), Some("1".into()))].into_iter().collect();
    assert_eq!(job_5.types, job_5_types);

    // Checking moldables
    // TODO: test moldables requests
}
