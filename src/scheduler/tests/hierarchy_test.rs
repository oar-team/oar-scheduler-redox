use crate::models::models::ProcSet;
use std::ops::RangeInclusive;
use crate::scheduler::hierarchy::Hierarchy;

#[allow(dead_code)]
fn procset(range: RangeInclusive<u32>) -> ProcSet {
    ProcSet::from_iter([range])
}
#[allow(dead_code)]
fn leaf(proc_sets: Vec<RangeInclusive<u32>>) -> Hierarchy {
    Hierarchy::Leaf(proc_sets.into_iter().map(procset).collect())
}
#[allow(dead_code)]
fn node(children: Vec<(RangeInclusive<u32>, Hierarchy)>) -> Hierarchy {
    Hierarchy::Node(
        children
            .into_iter()
            .map(|(range, hierarchy)| (procset(range), hierarchy))
            .collect(),
    )
}

#[test]
fn test_find_resource_hierarchies_scattered1() {
    // Single level hierarchy
    let h = leaf(vec![1..=16, 17..=32]);
    let available = procset(1..=32);
    let result = h.find_resource_hierarchies_scattered(&available, &[2]);
    assert_eq!(result, Some(procset(1..=32)));
}

#[test]
fn test_find_resource_hierarchies_scattered2() {
    // Two level hierarchy
    let h = node(vec![
        (1..=16, leaf(vec![1..=8, 9..=16])),
        (17..=32, leaf(vec![17..=24, 25..=32])),
    ]);

    let result = h.find_resource_hierarchies_scattered(&procset(1..=32), &[2, 1]);
    assert_eq!(result, Some(procset(1..=8) | procset(17..=24)));
}

#[test]
fn test_find_resource_hierarchies_scattered3() {
    // Two level hierarchy with partial availability
    let h = node(vec![
        (1..=16, leaf(vec![1..=8, 9..=16])),
        (17..=32, leaf(vec![17..=24, 25..=32])),
    ]);

    let available = procset(1..=12) | procset(17..=28);
    let result = h.find_resource_hierarchies_scattered(&available, &[2, 1]);
    assert_eq!(result, Some(procset(1..=8) | procset(17..=24)));
}

#[test]
fn test_find_resource_hierarchies_scattered4() {
    // Three level hierarchy
    let h = node(vec![
        (1..=16, node(vec![
            (1..=8, leaf(vec![1..=4, 5..=8])),
            (9..=16, leaf(vec![9..=12, 13..=16])),
        ])),
        (17..=32, node(vec![
            (17..=24, leaf(vec![17..=20, 21..=24])),
            (25..=32, leaf(vec![25..=28, 29..=32])),
        ])),
    ]);

    let result = h.find_resource_hierarchies_scattered(&procset(1..=32), &[2, 1, 1]);
    assert_eq!(result, Some(procset(1..=4) | procset(17..=20)));
}

#[test]
fn test_find_resource_hierarchies_scattered5() {
    let h = node(vec![
        (1..=32, node(vec![
            (1..=16, node(vec![
                (1..=8, leaf(vec![1..=2, 3..=4, 5..=8])),
                (9..=16, leaf(vec![9..=12, 13..=16])),
            ])),
            (17..=32, node(vec![
                (17..=24, leaf(vec![17..=19, 20..=22, 22..=24])),
                (25..=32, leaf(vec![25..=27, 28..=30, 31..=32])),
            ])),
        ])),
        (33..=64, node(vec![
            (33..=48, node(vec![
                (33..=40, leaf(vec![33..=34, 35..=37, 38..=41])),
                (42..=49, leaf(vec![42..=45, 46..=47, 48..=49])),
            ])),
            (50..=64, node(vec![
                (50..=57, leaf(vec![50..=52, 53..=54, 55..=58])),
                (59..=64, leaf(vec![59..=61, 62..=63, 64..=64])),
            ])),
        ])),
    ]);

    let result = h.find_resource_hierarchies_scattered(&procset(1..=64), &[2, 2, 1, 1]);
    assert_eq!(result, Some(procset(1..=2) | procset(17..=19) | procset(33..=34) | procset(50..=52)));
}

#[test]
fn test_find_resource_hierarchies_scattered6() {
    let h = node(vec![
        (1..=16, node(vec![
            (1..=8, leaf(vec![1..=4, 5..=8])),
            (9..=16, leaf(vec![9..=12, 13..=16])),
        ])),
        (17..=32, node(vec![
            (17..=24, leaf(vec![17..=20, 21..=24])),
            (25..=32, leaf(vec![25..=28, 29..=32])),
        ])),
    ]);

    // Test with [2, 2, 1] levels
    let result = h.find_resource_hierarchies_scattered(&procset(1..=32), &[2, 2, 1]);
    assert_eq!(
        result,
        Some(procset(1..=4) | procset(9..=12) | procset(17..=20) | procset(25..=28))
    );

    // Test with [1, 2, 1] levels
    let result = h.find_resource_hierarchies_scattered(&procset(1..=32), &[1, 2, 1]);
    assert_eq!(result, Some(procset(1..=4) | procset(9..=12)));
}
