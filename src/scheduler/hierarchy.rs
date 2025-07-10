use crate::models::models::ProcSet;
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hierarchy {
    Node(Vec<(ProcSet, Hierarchy)>),
    Leaf(Vec<ProcSet>),
}

fn test() {
    let test = Hierarchy::Node(vec![
        (
            ProcSet::from_iter([1..=16]),
            Hierarchy::Leaf(vec![ProcSet::from_iter([1..=8]), ProcSet::from_iter([9..=16])]),
        ),
        (
            ProcSet::from_iter([17..=32]),
            Hierarchy::Leaf(vec![
                ProcSet::from_iter([17..=20]),
                ProcSet::from_iter([21..=24]),
                ProcSet::from_iter([25..=32]),
            ]),
        ),
    ]);
}

impl Hierarchy {
    pub fn find_resource_hierarchies_scattered(&self, available_proc_set: &ProcSet, level_requests: &[u32]) -> Option<ProcSet> {
        let proc_sets = match self {
            Hierarchy::Node(children) => children
                .iter()
                .filter_map(|(proc_set, hierarchy)| {
                    if level_requests.len() < 2 {
                        if proc_set.is_subset(&available_proc_set) {
                            return Some(Cow::Borrowed(proc_set));
                        } else {
                            return None;
                        }
                    }
                    hierarchy
                        .find_resource_hierarchies_scattered(&available_proc_set, &level_requests[1..level_requests.len()])
                        .map(|proc_set| Cow::Owned(proc_set))
                })
                .take(level_requests[0] as usize)
                .collect::<Vec<_>>(),
            Hierarchy::Leaf(proc_sets) => proc_sets
                .iter()
                .filter(|proc_set| proc_set.is_subset(&available_proc_set))
                .take(level_requests[0] as usize)
                .map(|proc_set| Cow::Borrowed(proc_set))
                .collect::<Vec<_>>(),
        };
        if proc_sets.len() < level_requests[0] as usize {
            None
        } else {
            Some(proc_sets.iter().fold(ProcSet::new(), |acc, proc_set| acc | proc_set.as_ref()))
        }
    }
}
