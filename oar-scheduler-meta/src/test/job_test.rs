use crate::platform::Platform;
use crate::test::setup_for_tests;
use oar_scheduler_db::model::NewJob;

fn insert_jobs_for_tests(platform: &Platform) {
    let j1 = NewJob {
        user: Some("user1".to_string()),
        queue_name: None,
        res: vec![(60, vec![("resource_id=1".to_string(), "".to_string())])],
        types: None,
    }
        .insert(platform.session())
        .expect("insert job 1");

    let j2 = NewJob {
        user: Some("user2".to_string()),
        queue_name: Some("default".to_string()),
        res: vec![(120, vec![("nodes=2/cpu=1".to_string(), "".to_string())])],
        types: Some(vec!["besteffort".to_string()]),
    }
        .insert(platform.session())
        .expect("insert job 2");

    let j3 = NewJob {
        user: Some("user3".to_string()),
        queue_name: Some("short".to_string()),
        res: vec![(30, vec![("nodes=1".to_string(), "".to_string())])],
        types: None,
    }
        .insert(platform.session())
        .expect("insert job 3");

    let j4 = NewJob {
        user: Some("user4".to_string()),
        queue_name: Some("long".to_string()),
        res: vec![
            (300, vec![
                ("switch=1/nodes=4".to_string(), "".to_string()),
                ("licence=20".to_string(), "lic_type = 'mathlab'".to_string()),
            ]),
        ],
        types: Some(vec!["container".to_string()]),
    }
        .insert(platform.session())
        .expect("insert job 4");

    let j5 = NewJob {
        user: Some("user5".to_string()),
        queue_name: None,
        res: vec![(90, vec![("nodes=3".to_string(), "".to_string())])],
        types: Some(vec!["besteffort".to_string(), "container".to_string()]),
    }
        .insert(platform.session())
        .expect("insert job 5");

    assert!(j1 > 0 && j2 > 0 && j3 > 0 && j4 > 0 && j5 > 0);
}

#[test]
fn test_insert_and_retrive_job() {
    let (session, config) = setup_for_tests();
    let mut platform = Platform::from_database(session, config);
    insert_jobs_for_tests(&platform);

    // TODO: test job retrival
}
