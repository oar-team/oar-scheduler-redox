use std::collections::HashMap;

use crate::scheduler::slotset::SlotSet;
use crate::scheduler::slot::Slot;
use crate::model::job::ProcSet;

const ORDER_KEY_GAP: i128 = 1_i128 << 32;

#[derive(Debug, Clone)]
struct SlotOrderTreeNode {
    slot_id: i32,
    order_key: i128,
    priority: u64,

    parent: Option<usize>,
    left: Option<usize>,
    right: Option<usize>,

    leaf_proc_set: ProcSet,
    subtree_proc_set: ProcSet,

    subtree_min_key: i128,
    subtree_max_key: i128,
}

impl SlotOrderTreeNode {
    fn new(slot_id: i32, order_key: i128, priority: u64, leaf_proc_set: ProcSet) -> Self {
        Self {
            slot_id,
            order_key,
            priority,

            parent: None,
            left: None,
            right: None,

            subtree_min_key: order_key,
            subtree_max_key: order_key,

            subtree_proc_set: leaf_proc_set.clone(),
            leaf_proc_set,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SlotOrderTree {
    root: Option<usize>,
    nodes: Vec<SlotOrderTreeNode>,
    slot_id_to_node: HashMap<i32, usize>,
    rng_state: u64,
}

impl SlotOrderTree {
    pub fn new() -> Self {
        Self {
            root: None,
            nodes: Vec::new(),
            slot_id_to_node: HashMap::new(),
            rng_state: 0x9E3779B97F4A7C15,
        }
    }

    pub fn build_from_slotset(slotset: &SlotSet) -> Self {
        let mut tree = Self::new();

        for (idx, slot) in slotset.iter().enumerate() {
            let order_key = (idx as i128) * ORDER_KEY_GAP;
            tree.insert_new_slot(slot.id(), order_key, slot.proc_set().clone());
        }

        tree
    }

    pub fn contains_slot(&self, slot_id: i32) -> bool {
        self.slot_id_to_node.contains_key(&slot_id)
    }

    pub fn order_key_of(&self, slot_id: i32) -> Option<i128> {
        self.slot_id_to_node
            .get(&slot_id)
            .map(|node_idx| self.nodes[*node_idx].order_key)
    }

    pub fn inorder_slot_ids(&self) -> Vec<i32> {
        let mut out = Vec::new();
        self.collect_inorder_slot_ids(self.root, &mut out);
        out
    }

    pub fn update_slot_proc_set(&mut self, slot_id: i32, new_proc_set: ProcSet) {
        let node_idx = *self
            .slot_id_to_node
            .get(&slot_id)
            .expect("SlotOrderTree::update_slot_proc_set: unknown slot id");

        self.nodes[node_idx].leaf_proc_set = new_proc_set;
        self.pull_to_root(node_idx);
    }

    pub fn insert_after(
        &mut self,
        left_slot_id: i32,
        new_slot_id: i32,
        new_proc_set: ProcSet,
        right_neighbor_slot_id: Option<i32>,
    ) {
        self.insert_between(
            Some(left_slot_id),
            new_slot_id,
            new_proc_set,
            right_neighbor_slot_id,
        );
    }

    pub fn remove_slot(&mut self, slot_id: i32) {
        if !self.slot_id_to_node.contains_key(&slot_id) {
            return;
        }

        let mut items = self
            .inorder_items()
            .into_iter()
            .filter(|(sid, _, _)| *sid != slot_id)
            .collect::<Vec<_>>();

        self.root = None;
        self.nodes.clear();
        self.slot_id_to_node.clear();

        for (slot_id, order_key, proc_set) in items.drain(..) {
            self.insert_new_slot(slot_id, order_key, proc_set);
        }
    }

    pub fn range_proc_set(&self, left_slot_id: i32, right_slot_id: i32) -> ProcSet {
        let left_key = self
            .order_key_of(left_slot_id)
            .expect("SlotOrderTree::range_proc_set: left slot missing");
        let right_key = self
            .order_key_of(right_slot_id)
            .expect("SlotOrderTree::range_proc_set: right slot missing");

        let (from, to) = if left_key <= right_key {
            (left_key, right_key)
        } else {
            (right_key, left_key)
        };

        self.query_range(self.root, from, to)
            .expect("SlotOrderTree::range_proc_set: empty query result for valid slot range")
    }

    fn insert_new_slot(&mut self, slot_id: i32, order_key: i128, proc_set: ProcSet) {
        let priority = self.next_priority();
        let node_idx = self.nodes.len();

        self.nodes.push(SlotOrderTreeNode::new(
            slot_id,
            order_key,
            priority,
            proc_set,
        ));

        self.slot_id_to_node.insert(slot_id, node_idx);
        self.root = self.insert_rec(self.root, node_idx);

        if let Some(root_idx) = self.root {
            self.nodes[root_idx].parent = None;
        }
    }

    fn insert_rec(&mut self, root: Option<usize>, new_idx: usize) -> Option<usize> {
        match root {
            None => Some(new_idx),
            Some(root_idx) => {
                let new_key = self.nodes[new_idx].order_key;
                let root_key = self.nodes[root_idx].order_key;

                if new_key < root_key {
                    let left = self.nodes[root_idx].left;
                    let new_left = self.insert_rec(left, new_idx);
                    self.nodes[root_idx].left = new_left;

                    if let Some(child_idx) = new_left {
                        self.nodes[child_idx].parent = Some(root_idx);
                    }

                    if self.priority_of(new_left) > self.nodes[root_idx].priority {
                        let rotated = self.rotate_right(root_idx);
                        Some(rotated)
                    } else {
                        self.pull(root_idx);
                        Some(root_idx)
                    }
                } else {
                    let right = self.nodes[root_idx].right;
                    let new_right = self.insert_rec(right, new_idx);
                    self.nodes[root_idx].right = new_right;

                    if let Some(child_idx) = new_right {
                        self.nodes[child_idx].parent = Some(root_idx);
                    }

                    if self.priority_of(new_right) > self.nodes[root_idx].priority {
                        let rotated = self.rotate_left(root_idx);
                        Some(rotated)
                    } else {
                        self.pull(root_idx);
                        Some(root_idx)
                    }
                }
            }
        }
    }

    fn rotate_left(&mut self, root_idx: usize) -> usize {
        let right_idx = self.nodes[root_idx]
            .right
            .expect("SlotOrderTree::rotate_left: missing right child");

        let old_parent = self.nodes[root_idx].parent;
        let right_left = self.nodes[right_idx].left;

        self.nodes[root_idx].right = right_left;
        if let Some(child_idx) = right_left {
            self.nodes[child_idx].parent = Some(root_idx);
        }

        self.nodes[right_idx].left = Some(root_idx);
        self.nodes[right_idx].parent = old_parent;
        self.nodes[root_idx].parent = Some(right_idx);

        self.pull(root_idx);
        self.pull(right_idx);

        right_idx
    }

    fn rotate_right(&mut self, root_idx: usize) -> usize {
        let left_idx = self.nodes[root_idx]
            .left
            .expect("SlotOrderTree::rotate_right: missing left child");

        let old_parent = self.nodes[root_idx].parent;
        let left_right = self.nodes[left_idx].right;

        self.nodes[root_idx].left = left_right;
        if let Some(child_idx) = left_right {
            self.nodes[child_idx].parent = Some(root_idx);
        }

        self.nodes[left_idx].right = Some(root_idx);
        self.nodes[left_idx].parent = old_parent;
        self.nodes[root_idx].parent = Some(left_idx);

        self.pull(root_idx);
        self.pull(left_idx);

        left_idx
    }

    fn pull(&mut self, idx: usize) {
        let left = self.nodes[idx].left;
        let right = self.nodes[idx].right;

        let mut subtree_proc_set = self.nodes[idx].leaf_proc_set.clone();
        let mut subtree_min_key = self.nodes[idx].order_key;
        let mut subtree_max_key = self.nodes[idx].order_key;

        if let Some(left_idx) = left {
            subtree_proc_set = self.nodes[left_idx].subtree_proc_set.clone() & subtree_proc_set;
            subtree_min_key = self.nodes[left_idx].subtree_min_key;
        }

        if let Some(right_idx) = right {
            subtree_proc_set = subtree_proc_set & self.nodes[right_idx].subtree_proc_set.clone();
            subtree_max_key = self.nodes[right_idx].subtree_max_key;
        }

        self.nodes[idx].subtree_proc_set = subtree_proc_set;
        self.nodes[idx].subtree_min_key = subtree_min_key;
        self.nodes[idx].subtree_max_key = subtree_max_key;
    }

    fn query_range(&self, node: Option<usize>, from: i128, to: i128) -> Option<ProcSet> {
        let idx = node?;
        let n = &self.nodes[idx];

        if n.subtree_max_key < from || n.subtree_min_key > to {
            return None;
        }

        if from <= n.subtree_min_key && n.subtree_max_key <= to {
            return Some(n.subtree_proc_set.clone());
        }

        let left = self.query_range(n.left, from, to);
        let self_value = if from <= n.order_key && n.order_key <= to {
            Some(n.leaf_proc_set.clone())
        } else {
            None
        };
        let right = self.query_range(n.right, from, to);

        Self::combine_procsets(left, self_value, right)
    }

    fn combine_procsets(
        a: Option<ProcSet>,
        b: Option<ProcSet>,
        c: Option<ProcSet>,
    ) -> Option<ProcSet> {
        let mut acc: Option<ProcSet> = None;

        for item in [a, b, c] {
            if let Some(ps) = item {
                acc = Some(match acc {
                    None => ps,
                    Some(prev) => prev & ps,
                });
            }
        }

        acc
    }

    fn collect_inorder_slot_ids(&self, node: Option<usize>, out: &mut Vec<i32>) {
        if let Some(idx) = node {
            self.collect_inorder_slot_ids(self.nodes[idx].left, out);
            out.push(self.nodes[idx].slot_id);
            self.collect_inorder_slot_ids(self.nodes[idx].right, out);
        }
    }

    fn inorder_items(&self) -> Vec<(i32, i128, ProcSet)> {
        let mut out = Vec::new();
        self.collect_inorder_items(self.root, &mut out);
        out
    }

    fn collect_inorder_items(&self, node: Option<usize>, out: &mut Vec<(i32, i128, ProcSet)>) {
        if let Some(idx) = node {
            self.collect_inorder_items(self.nodes[idx].left, out);
            out.push((
                self.nodes[idx].slot_id,
                self.nodes[idx].order_key,
                self.nodes[idx].leaf_proc_set.clone(),
            ));
            self.collect_inorder_items(self.nodes[idx].right, out);
        }
    }

    fn relabel_all_keys(&mut self) {
        let items = self.inorder_items();

        self.root = None;
        self.nodes.clear();
        self.slot_id_to_node.clear();

        for (idx, (slot_id, _old_key, proc_set)) in items.into_iter().enumerate() {
            let new_key = (idx as i128) * ORDER_KEY_GAP;
            self.insert_new_slot(slot_id, new_key, proc_set);
        }

        if let Some(root_idx) = self.root {
            self.nodes[root_idx].parent = None;
        }
    }

    fn recompute_all(&mut self) {
        self.root = self.recompute_subtree(self.root);
    }

    fn recompute_subtree(&mut self, node: Option<usize>) -> Option<usize> {
        let idx = node?;
        let left = self.nodes[idx].left;
        let right = self.nodes[idx].right;

        self.nodes[idx].left = self.recompute_subtree(left);
        self.nodes[idx].right = self.recompute_subtree(right);
        self.pull(idx);

        Some(idx)
    }

    fn priority_of(&self, node: Option<usize>) -> u64 {
        node.map(|idx| self.nodes[idx].priority).unwrap_or(0)
    }

    fn next_priority(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.rng_state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng_state = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    pub fn insert_between(
        &mut self,
        left_neighbor_slot_id: Option<i32>,
        new_slot_id: i32,
        new_proc_set: ProcSet,
        right_neighbor_slot_id: Option<i32>,
    ) {
        let new_key = match (left_neighbor_slot_id, right_neighbor_slot_id) {
            (Some(left_slot_id), Some(right_slot_id)) => {
                let left_key = self
                    .order_key_of(left_slot_id)
                    .expect("SlotOrderTree::insert_between: left slot missing");
                let right_key = self
                    .order_key_of(right_slot_id)
                    .expect("SlotOrderTree::insert_between: right slot missing");

                if right_key - left_key <= 1 {
                    self.relabel_all_keys();

                    let left_key = self
                        .order_key_of(left_slot_id)
                        .expect("SlotOrderTree::insert_between: left slot missing after relabel");
                    let right_key = self
                        .order_key_of(right_slot_id)
                        .expect("SlotOrderTree::insert_between: right slot missing after relabel");

                    (left_key + right_key) / 2
                } else {
                    (left_key + right_key) / 2
                }
            }

            (Some(left_slot_id), None) => {
                let left_key = self
                    .order_key_of(left_slot_id)
                    .expect("SlotOrderTree::insert_between: left slot missing");
                left_key + ORDER_KEY_GAP
            }

            (None, Some(right_slot_id)) => {
                let right_key = self
                    .order_key_of(right_slot_id)
                    .expect("SlotOrderTree::insert_between: right slot missing");
                right_key - ORDER_KEY_GAP
            }

            (None, None) => 0,
        };

        self.insert_new_slot(new_slot_id, new_key, new_proc_set);
    }

    fn pull_to_root(&mut self, start_idx: usize) {
        let mut cur = Some(start_idx);

        while let Some(idx) = cur {
            self.pull(idx);
            cur = self.nodes[idx].parent;
        }
    }
}