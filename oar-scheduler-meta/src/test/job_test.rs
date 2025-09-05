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
use oar_scheduler_core::model::job::{PlaceholderType, TimeSharingType};
use oar_scheduler_core::platform::Job;
use oar_scheduler_db::model::{JobDatabaseRequests, JobReservation, NewJob};
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
        res: vec![(120, vec![("nodes=1/cpu=2".to_string(), "".to_string()), ("nodes=1/cpu=3".to_string(), "lowpower=true".to_string())]), (30, vec![("nodes=1/cpu=8".to_string(), "".to_string())])],
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

    let default_jobs = Job::get_jobs(&platform.session(), Some(vec!["default".to_string()]), None, None).unwrap();
    let besteffort_jobs = Job::get_jobs(&platform.session(), Some(vec!["besteffort".to_string()]), Some(JobReservation::None), None).unwrap();

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

    let mld_1 = &job_1.moldables[0];
    let mut mld_2_1 = &job_2.moldables[0];
    let mut mld_2_2 = &job_2.moldables[1];
    let mld_3 = &job_3.moldables[0];
    let mld_4 = &job_4.moldables[0];
    let mld_5 = &job_5.moldables[0];
    if mld_2_1.walltime < mld_2_2.walltime {
        let mld_tmp = mld_2_1;
        mld_2_1 = mld_2_2;
        mld_2_2 = mld_tmp;
    }
    assert_eq!(mld_1.walltime, 60);
    assert_eq!(mld_2_1.walltime, 120);
    assert_eq!(mld_2_2.walltime, 30);
    assert_eq!(mld_3.walltime, 30);
    assert_eq!(mld_4.walltime, 300);
    assert_eq!(mld_5.walltime, 90);

    assert_eq!(mld_1.requests.0.len(), 1);
    assert_eq!(mld_2_1.requests.0.len(), 2);
    assert_eq!(mld_2_2.requests.0.len(), 1);
    assert_eq!(mld_3.requests.0.len(), 1);
    assert_eq!(mld_4.requests.0.len(), 2);
    assert_eq!(mld_5.requests.0.len(), 1);

    let req_1 = &mld_1.requests.0[0];
    let req_2_1_1 = &mld_2_1.requests.0[0];
    let req_2_1_2 = &mld_2_1.requests.0[1];
    let req_2_2 = &mld_2_2.requests.0[0];
    let req_3 = &mld_3.requests.0[0];
    let req_4_1 = &mld_4.requests.0[0];
    let req_4_2 = &mld_4.requests.0[1];
    let req_5 = &mld_5.requests.0[0];

    assert_eq!(req_1.level_nbs, Box::from([(Box::from("resource_id"), 1)]));
    assert_eq!(req_2_1_1.level_nbs, Box::from([(Box::from("nodes"), 1), (Box::from("cpu"), 2)]));
    assert_eq!(req_2_1_2.level_nbs, Box::from([(Box::from("nodes"), 1), (Box::from("cpu"), 3)]));
    assert_eq!(req_2_2.level_nbs, Box::from([(Box::from("nodes"), 1), (Box::from("cpu"), 8)]));
    assert_eq!(req_3.level_nbs, Box::from([(Box::from("nodes"), 1)]));
    assert_eq!(req_4_1.level_nbs, Box::from([(Box::from("switch"), 1), (Box::from("nodes"), 4)]));
    assert_eq!(req_4_2.level_nbs, Box::from([(Box::from("licence"), 20)]));
    assert_eq!(req_5.level_nbs, Box::from([(Box::from("nodes"), 3)]));

}
