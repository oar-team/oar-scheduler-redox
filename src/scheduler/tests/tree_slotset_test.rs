use crate::models::models::Moldable;
use crate::scheduler::slot::ProcSet;
use crate::scheduler::tree_slotset::TreeSlotSet;

#[test]
pub fn test_claim_node_for_moldable() {
    let mut ss = TreeSlotSet::from_proc_set(ProcSet::from_iter([1..=10]), 0, 100);
    ss.to_table(true).printstd();

    let m1 = Moldable::new(10, ProcSet::from_iter([1..=5]));
    let m2 = Moldable::new(10, ProcSet::from_iter([3..=7]));

    let node1 = ss.find_node_for_moldable(&m1).unwrap();
    assert_eq!(node1.begin(), 0);
    assert_eq!(node1.end(), 100);
    ss.claim_node_for_moldable(node1.node_id(), &m1);
    ss.to_table(true).printstd();

    let node2 = ss.find_node_for_moldable(&m2).unwrap();
    assert_eq!(node2.begin(), 10);
    assert_eq!(node2.end(), 100);
    ss.claim_node_for_moldable(node2.node_id(), &m2);
    ss.to_table(true).printstd();
}
