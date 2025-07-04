use crate::models::models::Moldable;
use crate::scheduler::slot::ProcSet;
use crate::scheduler::tree_slotset::{TreeNode, TreeSlotSet};

#[test]
pub fn test_claim_node_for_moldable() {
    let mut ss = TreeSlotSet::from_proc_set(ProcSet::from_iter([1..=10]), 0, 100);
    ss.to_table().printstd();

    let m1 = Moldable::new(10, ProcSet::from_iter([1..=5]));
    let m2 = Moldable::new(10, ProcSet::from_iter([3..=7]));

    let node1 = ss.find_node_for_moldable(&m1).unwrap();
    ss.claim_node_for_moldable(node1.node_id(), &m1);
    ss.to_table().printstd();


    let node2 = ss.find_node_for_moldable(&m2).unwrap();
    ss.claim_node_for_moldable(node2.node_id(), &m2);
    ss.to_table().printstd();

    panic!("Test")

}
