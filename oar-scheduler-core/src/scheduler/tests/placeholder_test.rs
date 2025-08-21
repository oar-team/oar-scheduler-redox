use crate::models::*;
use crate::platform::PlatformConfig;
use crate::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use crate::scheduler::scheduling::*;
use crate::scheduler::slot::*;
use crate::scheduler::tests::platform_mock::generate_mock_platform_config;
use indexmap::indexmap;
use std::collections::HashMap;
use std::rc::Rc;

fn setup_platform() -> Rc<PlatformConfig> {
    let platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    Rc::new(platform_config)
}

#[test]
fn placeholder_claim_and_regular_job() {
    // A placeholder job claims resources, and a regular job is scheduled outside the placeholder.
    let platform_config = setup_platform();
    let available = platform_config.resource_set.default_intervals.clone();
    let mut all_ss = HashMap::from([("default".into(), SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000))]);

    let placeholder_job = JobBuilder::new(0)
        .name("ph1".into())
        .placeholder(PlaceholderType::Placeholder("ph1".into()))
        .moldable_auto(0, 50, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 4)])]))
        .build();

    let regular_job = JobBuilder::new(1)
        .name("reg".into())
        .placeholder(PlaceholderType::Allow("ph0".into()))
        .moldable_auto(1, 30, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 5)])]))
        .build();

    let mut jobs = indexmap![0 => placeholder_job, 1 => regular_job];
    schedule_jobs(&mut all_ss, &mut jobs);

    // The regular job should be scheduled outside the placeholder's claimed interval.
    assert!(jobs.get(&0).unwrap().assignment.is_some(), "Placeholder job should be scheduled");
    assert!(jobs.get(&1).unwrap().assignment.is_some(), "Regular job should be scheduled");
    let ph_assignment = jobs.get(&0).unwrap().assignment.as_ref().unwrap();
    let reg_assignment = jobs.get(&1).unwrap().assignment.as_ref().unwrap();
    assert_eq!(reg_assignment.begin, ph_assignment.end + 1, "Regular job should start after placeholder ends");
    assert_eq!(reg_assignment.end, 79, "Placeholder job should end at 79");
}

#[test]
fn allow_job_fully_inside_placeholder() {
    // An allow job is scheduled fully inside a placeholder job's claimed resources.
    let platform_config = setup_platform();
    let available = platform_config.resource_set.default_intervals.clone();
    let mut all_ss = HashMap::from([("default".into(), SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000))]);

    let placeholder_job = JobBuilder::new(0)
        .name("ph2".into())
        .placeholder(PlaceholderType::Placeholder("ph2".into()))
        .moldable_auto(0, 60, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 3)])]))
        .build();

    let allow_job = JobBuilder::new(1)
        .name("allow".into())
        .placeholder(PlaceholderType::Allow("ph2".into()))
        .moldable_auto(10, 40, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]))
        .build();

    let mut jobs = indexmap![0 => placeholder_job, 1 => allow_job];
    schedule_jobs(&mut all_ss, &mut jobs);

    assert!(jobs.get(&0).unwrap().assignment.is_some(), "Placeholder job should be scheduled");
    assert!(jobs.get(&1).unwrap().assignment.is_some(), "Allow job should be scheduled");
    let ph_assignment = jobs.get(&0).unwrap().assignment.as_ref().unwrap();
    let allow_assignment = jobs.get(&1).unwrap().assignment.as_ref().unwrap();
    assert!(allow_assignment.begin >= ph_assignment.begin, "Allow job should start after or at placeholder begin");
    assert!(allow_assignment.end <= ph_assignment.end, "Allow job should end before or at placeholder end");
    assert!(allow_assignment.proc_set.is_subset(&ph_assignment.proc_set), "Allow job should use subset of placeholder resources");
}

#[test]
fn allow_job_partially_inside_placeholder() {
    // An allow job is scheduled partially inside and partially outside a placeholder job's claimed resources.
    let platform_config = setup_platform();
    let available = platform_config.resource_set.default_intervals.clone();
    let mut all_ss = HashMap::from([("default".into(), SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000))]);

    let placeholder_job = JobBuilder::new(0)
        .name("ph3".into())
        .placeholder(PlaceholderType::Placeholder("ph3".into()))
        .moldable_auto(0, 30, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]))
        .build();

    let allow_job = JobBuilder::new(1)
        .name("allow".into())
        .placeholder(PlaceholderType::Allow("ph3".into()))
        .moldable_auto(0, 50, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 3)])]))
        .build();

    let mut jobs = indexmap![0 => placeholder_job, 1 => allow_job];
    schedule_jobs(&mut all_ss, &mut jobs);

    assert!(jobs.get(&0).unwrap().assignment.is_some(), "Placeholder job should be scheduled");
    assert!(jobs.get(&1).unwrap().assignment.is_some(), "Allow job should be scheduled");
    let ph_assignment = jobs.get(&0).unwrap().assignment.as_ref().unwrap();
    let allow_assignment = jobs.get(&1).unwrap().assignment.as_ref().unwrap();
    assert_eq!(allow_assignment.begin, ph_assignment.begin, "Allow job should start at placeholder begin");
    assert!(allow_assignment.end > ph_assignment.end, "Allow job should end after placeholder end");
    assert_eq!(allow_assignment.proc_set, ProcSet::from_iter(1..=96), "Allow job should use the proc_set [1..=96]");
}

#[test]
fn allow_job_outside_placeholder() {
    // An allow job is scheduled completely outside the placeholder job's claimed resources.
    let platform_config = setup_platform();
    let available = platform_config.resource_set.default_intervals.clone();
    let mut all_ss = HashMap::from([("default".into(), SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000))]);

    let placeholder_job = JobBuilder::new(0)
        .name("ph4".into())
        .placeholder(PlaceholderType::Placeholder("ph4".into()))
        .moldable_auto(0, 20, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 5)])]))
        .build();

    let allow_job1 = JobBuilder::new(1)
        .name("allow".into())
        .placeholder(PlaceholderType::Allow("ph4".into()))
        .moldable_auto(1, 30, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 6)])]))
        .build();

    let allow_job2 = JobBuilder::new(2)
        .name("allow".into())
        .placeholder(PlaceholderType::Allow("ph4".into()))
        .moldable_auto(2, 30, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 3)])]))
        .build();

    let mut jobs = indexmap![0 => placeholder_job, 1 => allow_job1, 2 => allow_job2];
    schedule_jobs(&mut all_ss, &mut jobs);

    assert!(jobs.get(&0).unwrap().assignment.is_some(), "Placeholder job should be scheduled");
    assert!(jobs.get(&1).unwrap().assignment.is_some(), "Allow job1 should be scheduled");
    assert!(jobs.get(&2).unwrap().assignment.is_some(), "Allow job2 should be scheduled");
    let _ph_assignment = jobs.get(&0).unwrap().assignment.as_ref().unwrap();
    let allow1_assignment = jobs.get(&1).unwrap().assignment.as_ref().unwrap();
    let allow2_assignment = jobs.get(&2).unwrap().assignment.as_ref().unwrap();
    assert_eq!(allow1_assignment.begin, 0, "Allow job 1 should start at 0");
    assert_eq!(allow1_assignment.end, 29, "Allow job 1 should end at 30");
    assert_eq!(allow2_assignment.begin, 30, "Allow job 2 should start at 30");
    assert_eq!(allow2_assignment.end, 59, "Allow job 2 should end at 61");
    assert_eq!(allow2_assignment.proc_set, ProcSet::from_iter(1..=96), "Allow job 2 should have proc_set [1..=96]");
}
