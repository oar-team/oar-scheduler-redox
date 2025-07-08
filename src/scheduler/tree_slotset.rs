use crate::models::models::ProcSet;
use crate::models::models::{Moldable, ProcSetCoresOp, ScheduledJobData};
use log::{debug, info};
use prettytable::{format, row, Table};
use slab_tree::*;

/// A slot is a time interval storing the available resources described as a ProcSet.
/// The time interval is [b, e] (b and e included, in epoch seconds).
#[derive(Clone, Debug)]
pub struct TreeSlot {
    begin: i64,
    end: i64,
    proc_set: ProcSet,
}

impl TreeSlot {
    pub fn new(begin: i64, end: i64, proc_set: ProcSet) -> TreeSlot {
        TreeSlot { begin, end, proc_set }
    }
    pub fn duration(&self) -> i64 {
        self.end - self.begin + 1
    }
    pub fn sub_resources(&mut self, proc_set: &ProcSet) {
        self.proc_set = &self.proc_set - proc_set;
    }
}

#[derive(Clone, Debug)]
pub struct TreeNode {
    slot: TreeSlot,          // If not a leaf, stores the intersection of the childrens proc_sets
    node_id: Option<NodeId>, // Nodes are never deleted, then it is safe to store the node_id in each node
    is_leaf: bool,
    proc_set_union: ProcSet,
}
pub enum FitState {
    None,
    MaybeChildren,
    Fit(ProcSet),
}
impl TreeNode {
    pub fn new_leaf(slot: TreeSlot) -> TreeNode {
        TreeNode {
            proc_set_union: slot.proc_set.clone(),
            slot,
            node_id: None,
            is_leaf: true,
        }
    }

    pub fn slot(&self) -> &TreeSlot {
        &self.slot
    }
    pub fn slot_mut(&mut self) -> &mut TreeSlot {
        &mut self.slot
    }
    pub fn set_node_id(&mut self, node_id: NodeId) {
        self.node_id = Some(node_id);
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id.unwrap()
    }
    pub fn begin(&self) -> i64 {
        self.slot.begin
    }
    pub fn end(&self) -> i64 {
        self.slot.end
    }
    pub fn proc_set(&self) -> &ProcSet {
        &self.slot.proc_set
    }
    pub fn duration(&self) -> i64 {
        self.slot.duration()
    }
    pub fn sub_resources(&mut self, proc_set: &ProcSet) {
        self.slot.sub_resources(proc_set);
    }
    pub fn sub_union_resources(&mut self, proc_set: &ProcSet) {
        self.proc_set_union = &self.proc_set_union - proc_set;
    }

    pub fn fit_state(&self, moldable: &Moldable) -> FitState {
        if moldable.walltime <= self.slot.duration() {
            if self.is_leaf || moldable.walltime == self.slot.duration() {
                // Needs to fit for sure because is_leaf or no children will fit as they have a duration strictly smaller
                return self.fit_moldable_otherwise(moldable, FitState::None);
            }
            // Check that it might fit on children
            let union_filtered_proc_set = &self.proc_set_union & &moldable.filter_proc_set;
            if union_filtered_proc_set.core_count() >= moldable.core_count {
                return self.fit_moldable_otherwise(moldable, FitState::MaybeChildren);
            }
        }
        FitState::None
    }
    fn fit_moldable_otherwise(&self, moldable: &Moldable, otherwise: FitState) -> FitState {
        let intersection_filtered_proc_set = &self.slot.proc_set & &moldable.filter_proc_set;
        if let Some(proc_set) = intersection_filtered_proc_set.sub_proc_set_with_cores(moldable.core_count) {
            FitState::Fit(proc_set)
        } else {
            otherwise
        }
    }
}

/// A SlotSet is a collection of Slots ordered by time.
/// It is a tree of TreeSlot, each node having no more than 2 children
#[derive(Debug)]
pub struct TreeSlotSet {
    tree: Tree<TreeNode>,
}
impl TreeSlotSet {
    /// Convert the tree structure to a table for display
    pub fn to_table(&self, show_nodes: bool) -> Table {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_CLEAN);
        table.add_row(row![
            buFc->"Indent",
            buFc->"Is Leaf",
            buFc->"Begin (epoch)",
            buFc->"End (epoch)",
            buFc->"Size (days)",
            buFc->"ProcSet",
            buFc->"ProcSet Union"
        ]);

        // Perform an in-order traversal of the tree
        let root_id = self.tree.root_id().unwrap();
        let root = self.tree.get(root_id).unwrap();
        self.add_node_to_table(&root, &mut table, 0, show_nodes);

        table
    }

    /// Helper function to recursively add nodes to the table
    fn add_node_to_table(&self, node: &NodeRef<TreeNode>, table: &mut Table, indent: usize, show_nodes: bool) {
        // Traverse left subtree if exists
        if let Some(left) = node.first_child() {
            self.add_node_to_table(&left, table, indent + 1, show_nodes);
        }

        // Add current node to the table
        let node_data = node.data();
        if show_nodes || node_data.is_leaf {
            table.add_row(row![
                indent,
                node_data.is_leaf,
                node_data.begin(),
                node_data.end(),
                format!("{:.2}", (node_data.duration() as f32) / 3600.0 / 24.0),
                node_data.proc_set(),
                &node_data.proc_set_union
            ]);
        }

        // Traverse right subtree if exists
        if let Some(right) = node.last_child() {
            self.add_node_to_table(&right, table, indent + 1, show_nodes);
        }
    }
    pub fn from_slot(slot: TreeSlot) -> TreeSlotSet {
        let mut tree = TreeBuilder::new().with_root(TreeNode::new_leaf(slot)).build();
        let root_id = tree.root_id().unwrap();
        tree.root_mut().unwrap().data().set_node_id(root_id);
        TreeSlotSet { tree }
    }
    pub fn from_proc_set(proc_set: ProcSet, begin: i64, end: i64) -> TreeSlotSet {
        Self::from_slot(TreeSlot::new(begin, end, proc_set))
    }

    /// ScheduledJobData begin should be equal to the beginning of the slot NodeId, and ending should be <= end of NodeId
    pub fn claim_node_for_scheduled_job(&mut self, node_id: NodeId, job: &ScheduledJobData) {
        let mut node = self.tree.get_mut(node_id).unwrap();

        let tree_node = node.data().clone();
        Self::claim_node_for_scheduled_job_rec(node, &job.proc_set, job.end + 1);
        debug!(
            "Placing moldable of length {} (ps: {}) on node {}-{} ps: {}, psu: {}",
            job.end - job.begin + 1,
            job.proc_set,
            tree_node.begin(),
            tree_node.end(),
            tree_node.proc_set(),
            tree_node.proc_set_union
        );
        if log::log_enabled!(log::Level::Trace) {
            self.to_table(false).printstd();
        }
    }
    fn claim_node_for_scheduled_job_rec(mut node: NodeMut<TreeNode>, proc_set: &ProcSet, split_before: i64) {
        let last_child_end = node.last_child().map(|mut child| child.data().end());
        let tree_node = node.data();
        let original_proc_set = tree_node.slot().proc_set.clone();
        tree_node.slot_mut().sub_resources(proc_set);

        if tree_node.is_leaf {
            if tree_node.slot().end >= split_before {
                // Split the node into two new children
                tree_node.is_leaf = false;
                let left_child = TreeNode::new_leaf(TreeSlot::new(tree_node.begin(), split_before - 1, tree_node.proc_set().clone()));
                let right_child = TreeNode::new_leaf(TreeSlot::new(split_before, tree_node.end(), original_proc_set));
                node.append(left_child);
                node.append(right_child);
                let left_child_id = node.first_child().unwrap().node_id();
                let right_child_id = node.last_child().unwrap().node_id();
                node.first_child().unwrap().data().set_node_id(left_child_id);
                node.last_child().unwrap().data().set_node_id(right_child_id);
                // Union is unchanged
            } else {
                // Taking the full leaf
                tree_node.proc_set_union = tree_node.proc_set().clone();
            }
        } else {
            // Union loses the proc_set only if all children are taken by the moldable
            if last_child_end.unwrap() < split_before - 1 {
                tree_node.sub_union_resources(proc_set);
            }

            Self::claim_node_for_scheduled_job_rec(node.first_child().unwrap(), proc_set, split_before);

            let mut last_child = node.last_child().unwrap();
            if last_child.data().begin() < split_before {
                Self::claim_node_for_scheduled_job_rec(last_child, proc_set, split_before);
            }
        }
    }

    pub fn find_node_for_moldable(&self, moldable: &Moldable) -> Option<(&TreeNode, ProcSet)> {
        let (count, node_id_proc_set) = Self::find_node_for_moldable_rec(self.tree.root().unwrap(), moldable);
        debug!("Found node for moldable iterating over {} nodes", count);
        node_id_proc_set.map(|(node_id, proc_set)| (self.tree.get(node_id).unwrap().data(), proc_set))
    }
    fn find_node_for_moldable_rec(node: NodeRef<TreeNode>, moldable: &Moldable) -> (usize, Option<(NodeId, ProcSet)>) {
        match node.data().fit_state(moldable) {
            FitState::Fit(proc_set) => return (1, Some((node.node_id(), proc_set))),
            FitState::MaybeChildren => {
                for child in node.children() {
                    let (count, child) = Self::find_node_for_moldable_rec(child, moldable);
                    if let Some(child) = child {
                        return (1 + count, Some(child));
                    }
                }
            }
            FitState::None => return (1, None),
        }
        (1, None)
    }

    pub fn count_leaves_and_nodes(&self) -> (usize, usize) {
        self.tree.root().unwrap().traverse_level_order().fold((0, 0), |(leaves, nodes), node| match node.data().is_leaf {
            true => (leaves + 1, nodes + 1),
            false => (leaves, nodes + 1),
        })
    }
}
