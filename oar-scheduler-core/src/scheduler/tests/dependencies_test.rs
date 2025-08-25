use crate::models::{JobBuilder, Moldable};
use crate::platform::PlatformConfig;
use crate::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use crate::scheduler::scheduling;
use crate::scheduler::slotset::SlotSet;
use crate::scheduler::tests::platform_mock::generate_mock_platform_config;
use indexmap::indexmap;
use log::LevelFilter;
use std::collections::HashMap;
use std::rc::Rc;

fn dependencies_platform_config() -> Rc<PlatformConfig> {
    let platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    Rc::new(platform_config)
}

#[test]
fn test_find_slots_for_moldable_with_dependencies() {
    let platform_config = dependencies_platform_config();
    let available = platform_config.resource_set.default_intervals.clone();
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    // Job 1: no dependency
    let moldable1 = Moldable::new(1, 100, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 1)])]));
    let job1 = JobBuilder::new(1)
        .user("user1".into())
        .queue("default".into())
        .moldable(moldable1)
        .build();

    // Job 2: depends on job 1
    let moldable2 = Moldable::new(2, 100, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 1)])]));
    let job2 = JobBuilder::new(2)
        .user("user2".into())
        .queue("default".into())
        .moldable(moldable2)
        .add_valid_dependency(1)
        .build();

    let mut jobs = indexmap![1 => job1, 2 => job2];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs);
    let j1 = &jobs[0];
    let j2 = &jobs[1];
    println!("J1 assignment: {:?}", j1.assignment);
    println!("J2 assignment: {:?}", j2.assignment);
    assert!(j1.assignment.is_some(), "Job 1 is not scheduled");
    assert!(j2.assignment.is_some(), "Job 2 is not scheduled");
    let sched1 = j1.assignment.as_ref().unwrap();
    let sched2 = j2.assignment.as_ref().unwrap();
    assert!(sched2.begin >= sched1.end + 1, "Job 2 does not starts after the end of Job 1");
}

#[test]
fn test_find_slots_for_moldable_with_container_and_inner_jobs() {
    env_logger::Builder::new()
        .is_test(true)
        .filter(None, LevelFilter::Debug)
        .init();


    let platform_config = dependencies_platform_config();
    let available = platform_config.resource_set.default_intervals.clone();
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    // Container job
    let moldable_container = Moldable::new(10, 200, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]));
    let job_container = JobBuilder::new(10)
        .user("container_user".into())
        .queue("default".into())
        .add_type_key("container".into())
        .moldable(moldable_container)
        .build();

    // Inner job
    let moldable_inner = Moldable::new(11, 100, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 1)])]));
    let job_inner = JobBuilder::new(11)
        .user("inner_user".into())
        .queue("default".into())
        .add_type("inner".into(), "10".into())
        .moldable(moldable_inner)
        .build();

    // Normal job depending on the inner job.
    let moldable_normal = Moldable::new(12, 50, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 1)])]));
    let job_normal = JobBuilder::new(12)
        .user("normal_user".into())
        .queue("default".into())
        .moldable(moldable_normal)
        .add_valid_dependency(11)
        .build();

    let mut jobs = indexmap![10 => job_container, 11 => job_inner, 12 => job_normal];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs);
    let j_container = &jobs[0];
    let j_inner = &jobs[1];
    let j_normal = &jobs[2];

    assert!(j_container.assignment.is_some(), "Container job is not scheduled");
    assert!(j_inner.assignment.is_some(), "Inner job is not scheduled");
    assert!(j_normal.assignment.is_some(), "Normal job is not scheduled");
    let sched_container = j_container.assignment.as_ref().unwrap();
    let sched_inner = j_inner.assignment.as_ref().unwrap();
    let sched_normal = j_normal.assignment.as_ref().unwrap();
    println!("Sched container: {:?}", sched_container);
    println!("Sched inner: {:?}", sched_inner);
    println!("Sched normal: {:?}", sched_normal);

    assert_eq!(sched_normal.begin, 100, "Normal job should start right after the inner job, at begin = 100");
}

