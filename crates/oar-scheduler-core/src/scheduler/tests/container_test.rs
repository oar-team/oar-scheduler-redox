use crate::model::job::{JobBuilder, Moldable};
use crate::platform::PlatformConfig;
use crate::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use crate::scheduler::scheduling;
use crate::scheduler::slotset::SlotSet;
use crate::scheduler::tests::platform_mock::generate_mock_platform_config;
use indexmap::indexmap;
use std::collections::HashMap;
use std::rc::Rc;

fn container_platform_config() -> Rc<PlatformConfig> {
    let platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    Rc::new(platform_config)
}

#[test]
fn test_single_inner_job_in_container() {
    let platform_config = container_platform_config();
    let available = platform_config.resource_set.default_resources.clone();
    let mut all_ss = HashMap::from([("default".into(), SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000))]);

    // Container job: slotset name "sub1", runs from 100 to 300
    let moldable_container = Moldable::new(100, 200, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]));
    let job_container = JobBuilder::new(10)
        .user("container_user".into())
        .queue("default".into())
        .add_type("container".into(), "sub1".into())
        .moldable(moldable_container)
        .build();

    // Inner job: slotset name "sub1", runs for 100, should fit inside container
    let moldable_inner = Moldable::new(101, 100, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 1)])]));
    let job_inner = JobBuilder::new(11)
        .user("inner_user".into())
        .queue("default".into())
        .add_type("inner".into(), "sub1".into())
        .moldable(moldable_inner)
        .build();

    let mut jobs = indexmap![10 => job_container, 11 => job_inner];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs);
    let j_container = &jobs[0];
    let j_inner = &jobs[1];
    assert!(j_container.assignment.is_some(), "Container job should be scheduled");
    assert!(j_inner.assignment.is_some(), "Inner job should be scheduled");
    let sched_container = j_container.assignment.as_ref().unwrap();
    let sched_inner = j_inner.assignment.as_ref().unwrap();
    assert!(sched_inner.begin >= sched_container.begin, "Inner job should start after the container job begins");
    assert!(sched_inner.end <= sched_container.end, "Inner job should finish before the container job ends");
    assert!(sched_inner.resources.is_subset(&sched_container.resources), "Inner job should use a subset of the container job's resources");
}

#[test]
fn test_inner_job_in_two_disjoint_containers_same_slotset_name() {
    let platform_config = container_platform_config();
    let available = platform_config.resource_set.default_resources.clone();
    let mut all_ss = HashMap::from([("default".into(), SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000))]);

    // Container 1: slotset "sub2", 0-99
    let moldable_c1 = Moldable::new(200, 100, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]));
    let job_c1 = JobBuilder::new(20)
        .add_type("container".into(), "sub2".into())
        .moldable(moldable_c1)
        .build();
    // Container 2: slotset "sub2", 200-399
    let moldable_c2 = Moldable::new(201, 200, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]));
    let job_c2 = JobBuilder::new(21)
        .add_type("container".into(), "sub2".into())
        .moldable(moldable_c2)
        .build();
    // Inner job: slotset "sub2", duration 150, only fits in the second container
    let moldable_inner = Moldable::new(202, 150, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 1)])]));
    let job_inner = JobBuilder::new(22)
        .add_type("inner".into(), "sub2".into())
        .moldable(moldable_inner)
        .build();
    let mut jobs = indexmap![20 => job_c1, 21 => job_c2, 22 => job_inner];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs);
    let j_c1 = &jobs[0];
    let j_c2 = &jobs[1];
    let j_inner = &jobs[2];
    assert!(j_c1.assignment.is_some());
    assert!(j_c2.assignment.is_some());
    assert!(j_inner.assignment.is_some());
    let sched_c2 = j_c2.assignment.as_ref().unwrap();
    let sched_inner = j_inner.assignment.as_ref().unwrap();
    assert!(sched_inner.begin >= sched_c2.begin, "Inner job should start after the second container job begins");
    assert!(sched_inner.end <= sched_c2.end, "Inner job should finish before the second container job ends");
    assert!(sched_inner.resources.is_subset(&sched_c2.resources), "Inner job should use a subset of the second container job's resources");
}

#[test]
fn test_inner_job_in_two_overlapping_containers_same_slotset_name() {
    let platform_config = container_platform_config();
    let available = platform_config.resource_set.default_resources.clone();
    let mut all_ss = HashMap::from([("default".into(), SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000))]);

    // Container 1: sub-slot_set "sub3", 0-199, ressources 1-16
    let moldable_c1 = Moldable::new(1, 200, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("cpus".into(), 16)])]));
    let job_c1 = JobBuilder::new(1)
        .add_type("container".into(), "sub3".into())
        .moldable(moldable_c1)
        .build();
    // Regular 1: 0-99, ressources 17-24
    let moldable_regular1 = Moldable::new(2, 100, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("cpus".into(), 8)])]));
    let job_r1 = JobBuilder::new(2)
        .moldable(moldable_regular1)
        .build();
    // Container 2: sub-slot_set "sub3" "sub3", 100-299, ressources 17-32
    let moldable_c2 = Moldable::new(3, 200, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("cpus".into(), 16)])]));
    let job_c2 = JobBuilder::new(3)
        .add_type("container".into(), "sub3".into())
        .moldable(moldable_c2)
        .build();
    // Inner job: slotset "sub3", durée 50, doit être planifié dans la zone commune 200-299, ressources 8-16
    let moldable_inner = Moldable::new(4, 70, HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("cpus".into(), 20)])]));
    let job_inner = JobBuilder::new(4)
        .add_type("inner".into(), "sub3".into())
        .moldable(moldable_inner)
        .build();
    let mut jobs = indexmap![1 => job_c1, 2 => job_r1, 3 => job_c2, 4 => job_inner];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs);
    let j_c1 = &jobs[0];
    let j_r1 = &jobs[1];
    let j_c2 = &jobs[2];
    let j_inner = &jobs[3];
    assert!(j_c1.assignment.is_some());
    assert!(j_r1.assignment.is_some());
    assert!(j_c2.assignment.is_some());
    assert!(j_inner.assignment.is_some());
    let sched_c1 = j_c1.assignment.as_ref().unwrap();
    let _sched_r1 = j_r1.assignment.as_ref().unwrap();
    let sched_c2 = j_c2.assignment.as_ref().unwrap();
    let sched_inner = j_inner.assignment.as_ref().unwrap();

    assert_eq!(sched_c2.begin, 100, "Second container job should start right after the regular job, at time 100");
    assert_eq!(sched_inner.begin, 100, "Inner job should start at the beginning of the common area, at time 100");
    assert_eq!(sched_inner.end, 169, "Inner job should end at time 169, which is 70 after it started");
    assert!(sched_inner.resources.is_subset(&(&sched_c2.resources | &sched_c1.resources)), "Inner job should use a subset of the c1 and c2 container jobs' resources");
}
