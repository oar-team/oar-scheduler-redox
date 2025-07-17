#![allow(unused_imports)]
use crate::benchmark::platform_mock::generate_mock_platform_config;
use crate::models::models::{Job, Moldable, ProcSet, ScheduledJobData};
use crate::platform::PlatformConfig;
use crate::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use crate::scheduler::tree_slot::TreeSlotSet;
use std::rc::Rc;

#[allow(dead_code)]
fn get_platform_config() -> Rc<PlatformConfig> {
    // HierarchyRequests in here are only made on cores, then we can keep a single switch, single node, and single CPU.
    Rc::new(generate_mock_platform_config(false, 10, 10, 10, 10, false))
}

#[test]
pub fn test_claim_node_for_moldable_1() {

    let mut ss = TreeSlotSet::from_platform_config(get_platform_config(), 0, 100);
    ss.to_table(true).printstd();

    let req1 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=10]), vec![("cores".into(), 5)])
    ]);
    let req2 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=10]), vec![("cores".into(), 6)])
    ]);

    // Fake job, not used anyway since quotas are disabled.
    let mut job = Job::new(
        0,
        "test_job".to_string(),
        "test_job".to_string(),
        "test_job".to_string(),
        vec![],
        vec![],
    );
    let m1 = Moldable::new(10, req1);
    let m2 = Moldable::new(10, req2);

    let (node1, ps1) = ss.find_node_for_moldable(&m1, &job).unwrap();
    assert_eq!(node1.begin(), 0);
    assert_eq!(node1.end(), 100);
    job.scheduled_data = Some(ScheduledJobData::new(0, 9, ps1.clone(), 0));
    ss.claim_node_for_scheduled_job(node1.node_id(), &job);
    ss.to_table(true).printstd();

    let (node2, ps2) = ss.find_node_for_moldable(&m2, &job).unwrap();
    assert_eq!(node2.begin(), 10);
    assert_eq!(node2.end(), 100);
    job.scheduled_data = Some(ScheduledJobData::new(10, 19, ps2.clone(), 0));
    ss.claim_node_for_scheduled_job(node2.node_id(), &job);
    ss.to_table(true).printstd();
}

#[test]
pub fn test_claim_node_for_moldable_2() {
    let mut ss = TreeSlotSet::from_platform_config(get_platform_config(), 0, 100);
    ss.to_table(true).printstd();

    let req1 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=10]), vec![("cores".into(), 5)])
    ]);
    let req2 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=10]), vec![("cores".into(), 5)])
    ]);

    // Fake job, not used anyway since quotas are disabled.
    let mut job = Job::new(
        0,
        "test_job".to_string(),
        "test_job".to_string(),
        "test_job".to_string(),
        vec![],
        vec![],
    );
    let m1 = Moldable::new(10, req1);
    let m2 = Moldable::new(10, req2);

    let (node1, ps1) = ss.find_node_for_moldable(&m1, &job).unwrap();
    assert_eq!(node1.begin(), 0);
    assert_eq!(node1.end(), 100);
    job.scheduled_data = Some(ScheduledJobData::new(0, 9, ps1.clone(), 0));
    ss.claim_node_for_scheduled_job(node1.node_id(), &job);
    ss.to_table(true).printstd();

    let (node2, ps2) = ss.find_node_for_moldable(&m2, &job).unwrap();
    assert_eq!(node2.begin(), 0);
    assert_eq!(node2.end(), 100);
    job.scheduled_data = Some(ScheduledJobData::new(10, 19, ps2.clone(), 0));
    ss.claim_node_for_scheduled_job(node2.node_id(), &job);
    ss.to_table(true).printstd();
}

#[test]
pub fn test_claim_node_for_moldable_3() {
    let mut ss = TreeSlotSet::from_platform_config(get_platform_config(), 0, 100);
    ss.to_table(true).printstd();

    let req1 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=10]), vec![("cores".into(), 5)])
    ]);
    let req2 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=8]), vec![("cores".into(), 5)])
    ]);
    // Fake job, not used anyway since quotas are disabled.
    let mut job = Job::new(
        0,
        "test_job".to_string(),
        "test_job".to_string(),
        "test_job".to_string(),
        vec![],
        vec![],
    );
    let m1 = Moldable::new(10, req1);
    let m2 = Moldable::new(10, req2);

    let (node1, ps1) = ss.find_node_for_moldable(&m1, &job).unwrap();
    assert_eq!(node1.begin(), 0);
    assert_eq!(node1.end(), 100);
    job.scheduled_data = Some(ScheduledJobData::new(0, 9, ps1.clone(), 0));
    ss.claim_node_for_scheduled_job(node1.node_id(), &job);
    ss.to_table(true).printstd();

    let (node2, ps2) = ss.find_node_for_moldable(&m2, &job).unwrap();
    assert_eq!(node2.begin(), 10);
    assert_eq!(node2.end(), 100);
    job.scheduled_data = Some(ScheduledJobData::new(10, 19, ps2.clone(), 0));
    ss.claim_node_for_scheduled_job(node2.node_id(), &job);
    ss.to_table(true).printstd();
}
