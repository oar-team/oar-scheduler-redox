use crate::models::models::Moldable;
use crate::models::models::{Job, ProcSet, ProcSetCoresOp};
use crate::platform::PlatformConfig;
use crate::scheduler::quotas::{check_quotas, Quotas};
use log::{debug, info};
use oar3_rust_macros::benchmark;
use prettytable::{format, row, Table};
use ptree::print_tree;
use slab_tree::*;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::rc::Rc;

/// A slot is a time interval storing the available resources described as a ProcSet.
/// The time interval is [b, e] (b and e included, in epoch seconds).
#[derive(Clone)]
pub struct TreeSlot {
    begin: i64,
    end: i64,
    proc_set: ProcSet,
    quotas: Quotas,
    platform_config: Rc<PlatformConfig>,
}
impl Debug for TreeSlot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TreeSlot {{ begin: {}, end: {}, proc_set: {} }}", self.begin, self.end, self.proc_set)
    }
}

impl TreeSlot {
    pub fn new(platform_config: Rc<PlatformConfig>, begin: i64, end: i64, proc_set: ProcSet, quotas: Option<Quotas>) -> TreeSlot {
        TreeSlot {
            begin,
            end,
            proc_set,
            quotas: quotas.unwrap_or(Quotas::new(Rc::clone(&platform_config))),
            platform_config,
        }
    }
    pub fn duration(&self) -> i64 {
        self.end - self.begin + 1
    }
    pub fn begin(&self) -> i64 {
        self.begin
    }
    pub fn end(&self) -> i64 {
        self.end
    }
    pub fn proc_set(&self) -> &ProcSet {
        &self.proc_set
    }
}

/// Represent a node of the tree that stores the slots.
/// Can either be a leaf node (a slot `TreeSlot`) or a node with two children.
/// The node contains a `TreeSlot` that stores the intersection of the proc_sets of its children,
/// and a ProcSet `proc_set_union` that stores the union of the proc_sets of all its children.
#[derive(Clone, Debug)]
pub struct TreeNode {
    slot: TreeSlot,          // If not a leaf, stores the intersection of the childrens proc_sets
    node_id: Option<NodeId>, // Nodes are never deleted, then it is safe to store the node_id in each node
    is_leaf: bool,
    proc_set_union: ProcSet,
    quotas_union: Quotas,
}
pub enum FitState {
    None,
    MaybeChildren,
    Fit(ProcSet),
}
impl TreeNode {
    /// Creates a new leaf node with the given slot.
    /// The `proc_set_union` is initialized to the slot's proc_set as it is a leaf node.
    /// [`TreeNode::set_node_id`] should be called after the node is added to the tree to set the node_id.
    /// Indeed, the node_id field is used for methods to return a `TreeNode` without needing to pass the node_id around.
    pub fn new_leaf(slot: TreeSlot) -> TreeNode {
        TreeNode {
            proc_set_union: slot.proc_set.clone(),
            quotas_union: slot.quotas.clone(),
            slot,
            node_id: None,
            is_leaf: true,
        }
    }

    /// Duplicates the current node, creating a new `TreeNode` with the same slot and properties,
    /// but setting `is_leaf` to true, and setting its unions equals to the intersection of its parent node.
    pub fn duplicate_for_leaf(&self, begin: i64, end: i64) -> TreeNode {
        TreeNode {
            slot: TreeSlot::new(
                Rc::clone(&self.slot.platform_config),
                begin,
                end,
                self.slot.proc_set.clone(),
                Some(self.slot.quotas.clone()),
            ),
            node_id: None, // Node ID will be set right after the node is added to the tree
            is_leaf: true,
            proc_set_union: self.slot.proc_set.clone(),
            quotas_union: self.slot.quotas.clone(),
        }
    }

    pub fn slot(&self) -> &TreeSlot {
        &self.slot
    }
    #[allow(dead_code)]
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
    pub fn platform_config(&self) -> &PlatformConfig {
        &self.slot.platform_config
    }
    pub fn sub_resources(&mut self, proc_set: &ProcSet) {
        self.slot.proc_set = &self.slot.proc_set - proc_set;
    }
    pub fn sub_union_resources(&mut self, proc_set: &ProcSet) {
        self.proc_set_union = &self.proc_set_union - proc_set;
    }
    /// Increment the intersection quotas of this node for a scheduled job. Automatically ignores the request if they are not enabled.
    pub fn increment_quotas(&mut self, job: &Job) {
        if !self.slot.platform_config.quotas_config.enabled {
            return;
        }
        self.slot.quotas.increment_for_job(job, self.duration(), job.resource_count().unwrap());
    }
    /// Increment the union quotas of this node for a scheduled job. Automatically ignores the request if they are not enabled.
    pub fn increment_union_quotas(&mut self, job: &Job) {
        if !self.slot.platform_config.quotas_config.enabled {
            return;
        }
        self.quotas_union.increment_for_job(job, self.duration(), job.resource_count().unwrap());
    }

    /// Returns how could a moldable fit in this node and its children.
    /// Return [`FitState::None`] if the moldable cannot fit in this node or its children.
    /// Return [`FitState::MaybeChildren`] if the moldable might fit in the children. The children can then be traversed to find a smaller fitting node.
    /// Return [`FitState::Fit(proc_set)`] if the moldable fits in this node, and the `proc_set` is the resources that can be claimed for the moldable.
    #[benchmark]
    pub fn fit_state(&self, moldable: &Moldable, job: &Job, quotas_hit_count: &mut u32) -> FitState {
        let hierarchy = &self.slot.platform_config.resource_set.hierarchy;
        if moldable.walltime <= self.slot.duration() {
            // Needs to fit without considering the MaybeChildren option because is_leaf or because no children will be large enough for the walltime.
            if self.is_leaf || moldable.walltime == self.slot.duration() {
                return self.fit_state_in_intersection(&moldable, job, FitState::None, quotas_hit_count);
            }
            // Check that it might fit on children

            // Check first the union, then the intersection.
            return hierarchy
                .request(&self.proc_set_union, &moldable.requests)
                // TODO: check union quotas. This is not required, it might increase the traversed nodes, but a benchmark is required to check if checking quotas on union is worth it.
                .map(|_| {
                    // Fits on the union: either it fits the intersection or it returns MaybeChildren
                    self.fit_state_in_intersection(&moldable, job, FitState::MaybeChildren, quotas_hit_count)
                })
                .unwrap_or(FitState::None); // Do not fit the union

            // Check first the intersection, then the union.
            // let state = self.fit_state_in_intersection(&moldable, job, walltime, FitState::MaybeChildren, quotas_hit_count);
            // if let FitState::MaybeChildren = state {
            //     // If it does not fit in the intersection, check the union.
            //     return hierarchy // No quotas check on the union.
            //         .request(&self.proc_set_union, &moldable.requests)
            //         .map(|_proc_set| FitState::MaybeChildren)
            //         .unwrap_or(FitState::None);
            // }
            // return state
        }
        FitState::None
    }

    /// Utility function for `TreeNode::fit_state`.
    /// Checks the fit state of a job in the intersection of the proc_set and the moldable requests.
    #[benchmark]
    fn fit_state_in_intersection(&self, moldable: &Moldable, job: &Job, no_fit_state: FitState, quotas_hit_count: &mut u32) -> FitState {
        self.slot
            .platform_config
            .resource_set
            .hierarchy
            .request(&self.slot.proc_set, &moldable.requests)
            .and_then(|proc_set| {
                // Checking quotas
                if self.platform_config().quotas_config.enabled {
                    // TODO: To support temporal quotas, the unions and intersections should be a HashMap<rules_id, Quotas>
                    let res = check_quotas(
                        HashMap::from([(-1, (self.slot.quotas.clone(), self.duration()))]),
                        job,
                        proc_set.core_count(),
                    );
                    if let Some((msg, rule, limit)) = res {
                        *quotas_hit_count += 1;
                        debug!(
                            "Quotas limitation reached for job {}: {}, rule: {:?}, limit: {}",
                            job.id, msg, rule, limit
                        );
                        return None;
                    }
                }
                Some(FitState::Fit(proc_set))
            })
            .unwrap_or(no_fit_state)
    }
}

/// A SlotSet is a collection of Slots ordered by time.
/// It is a tree of TreeNode, each node being either a leaf or a node with two children.
pub struct TreeSlotSet {
    tree: Tree<TreeNode>,
    platform_config: Rc<PlatformConfig>,
}
impl Debug for TreeSlotSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TreeSlotSet {{ tree: {:?} }}", self.tree)
    }
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
    /// Helper function to recursively add nodes to a table for display
    fn add_node_to_table(&self, node: &NodeRef<TreeNode>, table: &mut Table, indent: usize, show_nodes: bool) {
        // Add the current node to the table
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

        // Traverse left subtree if exists
        if let Some(left) = node.first_child() {
            self.add_node_to_table(&left, table, indent + 1, show_nodes);
        }
        // Traverse right subtree if exists
        if let Some(right) = node.last_child() {
            self.add_node_to_table(&right, table, indent + 1, show_nodes);
        }
    }
    /// Prints the tree as a hierarchical structure.
    pub fn print_tree(&self) {
        let mut tree = ptree::TreeBuilder::new("Tree Slot Set".to_string());
        let mut node_stack: Vec<Option<NodeId>> = vec![self.tree.root_id()];
        while let Some(node_id) = node_stack.pop() {
            if let Some(node_id) = node_id {
                let node = self.tree.get(node_id).unwrap();
                let text = format!(
                    "{}->{} (i: {} u: {})",
                    node.data().begin(),
                    node.data().end(),
                    node.data().slot.proc_set,
                    node.data().proc_set_union
                );
                if node.data().is_leaf {
                    tree.add_empty_child(text);
                } else {
                    tree.begin_child(text);
                    node_stack.push(None);
                    node_stack.push(Some(self.tree.get(node_id).unwrap().last_child().unwrap().node_id()));
                    node_stack.push(Some(self.tree.get(node_id).unwrap().first_child().unwrap().node_id()));
                }
            } else {
                tree.end_child();
            }
        }
        print_tree(&tree.build()).unwrap();
    }

    #[allow(dead_code)]
    fn get_node_levels(&self) -> HashMap<u32, Vec<&TreeNode>> {
        let root_id = self.tree.root_id().unwrap();
        let mut map = HashMap::new();
        self.get_node_children_levels(root_id, 0, &mut map);
        map
    }
    fn get_node_children_levels<'a>(&'a self, node_id: NodeId, depth: u32, mut map: &mut HashMap<u32, Vec<&'a TreeNode>>) {
        let node = self.tree.get(node_id).unwrap();
        map.entry(depth).or_insert_with(Vec::new).push(node.data());
        if let Some(child) = node.first_child() {
            self.get_node_children_levels(child.node_id(), depth + 1, &mut map);
        }
        if let Some(child) = node.last_child() {
            self.get_node_children_levels(child.node_id(), depth + 1, &mut map);
        }
    }

    /// Builds a new TreeSlotSet with a single slot and a single root-leaf node.
    pub fn from_slot(slot: TreeSlot) -> TreeSlotSet {
        let platform_config = Rc::clone(&slot.platform_config);
        let mut tree = TreeBuilder::new().with_root(TreeNode::new_leaf(slot)).build();
        let root_id = tree.root_id().unwrap();
        tree.root_mut().unwrap().data().set_node_id(root_id);
        TreeSlotSet { tree, platform_config }
    }
    /// Builds a new TreeSlotSet with a single slot and a single root-leaf node, with the proc set `platform_config.resource_set.default_intervals`.
    pub fn from_platform_config(platform_config: Rc<PlatformConfig>, begin: i64, end: i64) -> TreeSlotSet {
        let proc_set = platform_config.resource_set.default_intervals.clone();
        Self::from_slot(TreeSlot::new(platform_config, begin, end, proc_set, None))
    }

    /// Subtract resources used by `job` to the node `node_id`. `job` must be scheduled.
    /// It will traverse the node parents and children, and may split a leaf node containing the ending of the scheduled job.
    /// The scheduled job should fit in the node `node_id` and its beginning should be equal to the beginning of the node `node_id`.
    pub fn claim_node_for_scheduled_job(&mut self, node_id: NodeId, job: &Job) {
        let scheduled_data = job.scheduled_data.as_ref().expect("Job must be scheduled to claim resources");
        let mut node = self.tree.get_mut(node_id).unwrap();

        let tree_node = node.data().clone();
        // Update parent intersections
        Self::claim_node_for_scheduled_job_up_rec(&mut node, &job, &scheduled_data.proc_set);
        // Update children intersections and unions, and split a leaf node if required.
        Self::claim_node_for_scheduled_job_down_rec(node, &job, &scheduled_data.proc_set, scheduled_data.end + 1);
        debug!(
            "Placing moldable of length {} (ps: {}) on node {}-{} ps: {}, psu: {}",
            scheduled_data.end - scheduled_data.begin + 1,
            scheduled_data.proc_set,
            tree_node.begin(),
            tree_node.end(),
            tree_node.proc_set(),
            tree_node.proc_set_union
        );
    }
    /// Helper recursive function to claim intersection resources in parents for a scheduled job, see [`TreeSlotSet::claim_node_for_scheduled_job`].
    fn claim_node_for_scheduled_job_up_rec(node: &mut NodeMut<TreeNode>, job: &Job, job_proc_set: &ProcSet) {
        if let Some(mut parent) = node.parent() {
            parent.data().sub_resources(job_proc_set);
            parent.data().increment_quotas(job);
            Self::claim_node_for_scheduled_job_up_rec(&mut parent, job, job_proc_set);
        }
    }
    /// Helper recursive function to claim resources in children for a scheduled job, see [`TreeSlotSet::claim_node_for_scheduled_job`].
    fn claim_node_for_scheduled_job_down_rec(mut node: NodeMut<TreeNode>, job: &Job, job_proc_set: &ProcSet, split_before: i64) {
        let last_child_end = node.last_child().map(|mut child| child.data().end());
        let tree_node = node.data();

        if tree_node.is_leaf {
            if tree_node.slot().end >= split_before {
                // Split the node creating its two children
                tree_node.is_leaf = false;
                let right_child = tree_node.duplicate_for_leaf(split_before, tree_node.end());
                tree_node.sub_resources(job_proc_set);
                tree_node.increment_quotas(&job);
                let left_child = tree_node.duplicate_for_leaf(tree_node.begin(), split_before - 1);

                node.append(left_child);
                node.append(right_child);
                let left_child_id = node.first_child().unwrap().node_id();
                let right_child_id = node.last_child().unwrap().node_id();
                node.first_child().unwrap().data().set_node_id(left_child_id);
                node.last_child().unwrap().data().set_node_id(right_child_id);
                // The union is unchanged
            } else {
                // Taking the full leaf
                tree_node.sub_resources(job_proc_set);
                tree_node.increment_quotas(&job);
                tree_node.proc_set_union = tree_node.proc_set().clone();
                tree_node.quotas_union = tree_node.slot().quotas.clone();
            }
        } else {
            tree_node.sub_resources(job_proc_set);
            tree_node.increment_quotas(&job);

            // The union loses the proc_set/increments the quotas only if all children are taken by the moldable
            if last_child_end.unwrap() < split_before - 1 {
                tree_node.sub_union_resources(job_proc_set);
                tree_node.increment_union_quotas(&job);
            }

            Self::claim_node_for_scheduled_job_down_rec(node.first_child().unwrap(), &job, job_proc_set, split_before);

            let mut last_child = node.last_child().unwrap();
            if last_child.data().begin() < split_before {
                Self::claim_node_for_scheduled_job_down_rec(last_child, &job, job_proc_set, split_before);
            }
        }
    }

    pub fn find_node_for_moldable(&self, moldable: &Moldable, job: &Job) -> Option<(&TreeNode, ProcSet, u32)> {
        let mut quotas_hit_count = 0;
        let (count, node_id_proc_set) = self.find_node_for_moldable_rec(self.tree.root().unwrap(), moldable, job, &mut quotas_hit_count);
        node_id_proc_set.map(|(node_id, proc_set)| (self.tree.get(node_id).unwrap().data(), proc_set, quotas_hit_count))
    }
    /// Helper recursive function to find a node for moldable, see [`TreeSlotSet::find_node_for_moldable`].
    #[benchmark]
    fn find_node_for_moldable_rec(
        &self,
        node: NodeRef<TreeNode>,
        moldable: &Moldable,
        job: &Job,
        quotas_hit_count: &mut u32,
    ) -> (usize, Option<(NodeId, ProcSet)>) {
        match node.data().fit_state(moldable, job, quotas_hit_count) {
            FitState::Fit(proc_set) => {
                // Find the smallest children node with the same beginning.
                let mut child_node_id = node.node_id();
                while let Some(child) = self.tree.get(child_node_id).unwrap().first_child() {
                    if child.data().duration() < moldable.walltime {
                        break;
                    }
                    child_node_id = child.node_id();
                }
                if child_node_id == node.node_id() {
                    return (1, Some((child_node_id, proc_set)))
                }
                // Compute again the proc_set for the children node in case a more isolated proc_set could be found.
                return self.find_node_for_moldable_rec(self.tree.get(child_node_id).unwrap(), moldable, job, quotas_hit_count);
            },
            FitState::MaybeChildren => {
                for child in node.children() {
                    let (count, child) = self.find_node_for_moldable_rec(child, moldable, job, quotas_hit_count);
                    if let Some(child) = child {
                        return (1 + count, Some(child));
                    }
                }
            }
            FitState::None => return (1, None),
        }
        (1, None)
    }

    /// Returns the number of leaves slots.
    pub fn count_leaves(&self) -> usize {
        self.tree
            .root()
            .unwrap()
            .traverse_level_order()
            .filter(|node| node.data().is_leaf)
            .count()
    }

    #[allow(dead_code)]
    pub fn leaf_slot_at(&self, time: i64) -> Option<&TreeSlot> {
        self.tree
            .root()
            .unwrap()
            .traverse_level_order()
            .find(|node| node.data().is_leaf && node.data().begin() <= time && node.data().end() >= time)
            .map(|node| &node.data().slot)
    }
}
