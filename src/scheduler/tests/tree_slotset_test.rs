use crate::models::models::{Moldable, ProcSet, ScheduledJobData};
use crate::scheduler::hierarchy::{Hierarchy, HierarchyRequest, HierarchyRequests};
use crate::scheduler::tree_slotset::TreeSlotSet;

fn get_hierarchy() -> Hierarchy {
    Hierarchy::new()
        .add_partition("switch".into(), Box::from([ProcSet::from_iter(1..=16), ProcSet::from_iter(17..=32)]))
        .add_partition("node".into(), Box::from([ProcSet::from_iter(1..=16), ProcSet::from_iter(16..=32)]))
        .add_partition("core".into(), (1..=32).map(|i| ProcSet::from_iter(i..=i)).collect::<Box<[ProcSet]>>())
}

#[test]
pub fn test_claim_node_for_moldable_1() {
    let mut ss = TreeSlotSet::from_proc_set(ProcSet::from_iter([1..=10]), 0, 100);
    ss.to_table(true).printstd();

    let h = get_hierarchy();
    let req1 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=10]), vec![("core".into(), 5)])
    ]);
    let req2 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=10]), vec![("core".into(), 6)])
    ]);

    let m1 = Moldable::new(10, req1);
    let m2 = Moldable::new(10, req2);

    let (node1, ps1) = ss.find_node_for_moldable(&m1, &h).unwrap();
    assert_eq!(node1.begin(), 0);
    assert_eq!(node1.end(), 100);
    ss.claim_node_for_scheduled_job(node1.node_id(), &ScheduledJobData::new(0, 9, ps1, 0));
    ss.to_table(true).printstd();

    let (node2, ps2) = ss.find_node_for_moldable(&m2, &h).unwrap();
    assert_eq!(node2.begin(), 10);
    assert_eq!(node2.end(), 100);
    ss.claim_node_for_scheduled_job(node2.node_id(), &ScheduledJobData::new(10, 19, ps2, 0));
    ss.to_table(true).printstd();
}

#[test]
pub fn test_claim_node_for_moldable_2() {
    let mut ss = TreeSlotSet::from_proc_set(ProcSet::from_iter([1..=10]), 0, 100);
    ss.to_table(true).printstd();

    let h = get_hierarchy();
    let req1 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=10]), vec![("core".into(), 5)])
    ]);
    let req2 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=10]), vec![("core".into(), 5)])
    ]);

    let m1 = Moldable::new(10, req1);
    let m2 = Moldable::new(10, req2);

    let (node1, ps1) = ss.find_node_for_moldable(&m1, &h).unwrap();
    assert_eq!(node1.begin(), 0);
    assert_eq!(node1.end(), 100);
    ss.claim_node_for_scheduled_job(node1.node_id(), &ScheduledJobData::new(0, 9, ps1, 0));
    ss.to_table(true).printstd();

    let (node2, ps2) = ss.find_node_for_moldable(&m2, &h).unwrap();
    assert_eq!(node2.begin(), 0);
    assert_eq!(node2.end(), 100);
    ss.claim_node_for_scheduled_job(node2.node_id(), &ScheduledJobData::new(10, 19, ps2, 0));
    ss.to_table(true).printstd();
}

#[test]
pub fn test_claim_node_for_moldable_3() {
    let mut ss = TreeSlotSet::from_proc_set(ProcSet::from_iter([1..=10]), 0, 100);
    ss.to_table(true).printstd();

    let h = get_hierarchy();
    let req1 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=10]), vec![("core".into(), 5)])
    ]);
    let req2 = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(ProcSet::from_iter([1..=8]), vec![("core".into(), 5)])
    ]);

    let m1 = Moldable::new(10, req1);
    let m2 = Moldable::new(10, req2);

    let (node1, ps1) = ss.find_node_for_moldable(&m1, &h).unwrap();
    assert_eq!(node1.begin(), 0);
    assert_eq!(node1.end(), 100);
    ss.claim_node_for_scheduled_job(node1.node_id(), &ScheduledJobData::new(0, 9, ps1, 0));
    ss.to_table(true).printstd();

    let (node2, ps2) = ss.find_node_for_moldable(&m2, &h).unwrap();
    assert_eq!(node2.begin(), 10);
    assert_eq!(node2.end(), 100);
    ss.claim_node_for_scheduled_job(node2.node_id(), &ScheduledJobData::new(10, 19, ps2, 0));
    ss.to_table(true).printstd();
}
