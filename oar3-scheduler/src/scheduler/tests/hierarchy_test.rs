use crate::models::ProcSet;
use crate::scheduler::hierarchy::{Hierarchy, HierarchyRequest, HierarchyRequests};
use crate::scheduler::tests::platform_mock::generate_mock_platform_config;
use std::ops::RangeInclusive;

#[allow(dead_code)]
fn procsets(ranges: Box<[RangeInclusive<u32>]>) -> Box<[ProcSet]> {
    ranges.into_iter().map(|r| ProcSet::from_iter(r)).collect()
}
#[allow(dead_code)]
fn procset(range: RangeInclusive<u32>) -> ProcSet {
    ProcSet::from_iter(range)
}

#[test]
fn test_find_resource_hierarchies_scattered1() {
    // Single level hierarchy
    let h = Hierarchy::new().add_partition("switch".into(), procsets([1..=16, 17..=32].into()));
    let available = procset(1..=32);
    let result = h.find_resource_hierarchies_scattered(&available, &[("switch".into(), 2)]);
    assert_eq!(result, Some(procset(1..=32)));
}

#[test]
fn test_find_resource_hierarchies_scattered2() {
    // Two level hierarchy
    let h = Hierarchy::new()
        .add_partition("switch".into(), procsets([1..=16, 17..=32].into()))
        .add_partition("node".into(), procsets([1..=8, 9..=16, 17..=24, 25..=32].into()));

    let available = procset(1..=32);
    let result = h.find_resource_hierarchies_scattered(&available, &[("switch".into(), 2), ("node".into(), 1)]);
    assert_eq!(result, Some(procset(1..=8) | procset(17..=24)));
}

#[test]
fn test_find_resource_hierarchies_scattered3() {
    // Two level hierarchy with partial availability
    let h = Hierarchy::new()
        .add_partition("switch".into(), procsets([1..=16, 17..=32].into()))
        .add_partition("node".into(), procsets([1..=8, 9..=16, 17..=24, 25..=32].into()));

    let available = procset(1..=12) | procset(17..=28);
    let result = h.find_resource_hierarchies_scattered(&available, &[("switch".into(), 2), ("node".into(), 1)]);
    assert_eq!(result, Some(procset(1..=8) | procset(17..=24)));
}

#[test]
fn test_find_resource_hierarchies_scattered4() {
    // Three level hierarchy
    let h = Hierarchy::new()
        .add_partition("switch".into(), procsets([1..=16, 17..=32].into()))
        .add_partition("node".into(), procsets([1..=8, 9..=16, 17..=24, 25..=32].into()))
        .add_partition(
            "cores".into(),
            procsets([1..=4, 5..=8, 9..=12, 13..=16, 17..=20, 21..=24, 25..=28, 29..=32].into()),
        );

    let result = h.find_resource_hierarchies_scattered(&procset(1..=32), &[("switch".into(), 2), ("node".into(), 1), ("cores".into(), 1)]);
    assert_eq!(result, Some(procset(1..=4) | procset(17..=20)));
}

#[test]
fn test_find_resource_hierarchies_scattered5() {
    let h = Hierarchy::new()
        .add_partition("switch".into(), procsets([1..=32, 33..=64].into()))
        .add_partition("node".into(), procsets([1..=16, 17..=32, 33..=49, 50..=64].into()))
        .add_partition(
            "cpus".into(),
            procsets([1..=8, 9..=16, 17..=24, 25..=32, 33..=41, 42..=29, 50..=58, 51..=64].into()),
        )
        .add_partition(
            "cores".into(),
            procsets(
                [
                    1..=2,
                    3..=4,
                    5..=8,
                    9..=16,
                    10..=12,
                    12..=16,
                    17..=19,
                    20..=22,
                    22..=24,
                    25..=27,
                    28..=30,
                    31..=32,
                    33..=34,
                    35..=37,
                    38..=41,
                    42..=45,
                    46..=47,
                    48..=49,
                    50..=52,
                    53..=54,
                    55..=58,
                    59..=61,
                    62..=63,
                    64..=64,
                ]
                .into(),
            ),
        );

    let result = h.find_resource_hierarchies_scattered(
        &procset(1..=64),
        &[("switch".into(), 2), ("node".into(), 2), ("cpus".into(), 1), ("cores".into(), 1)],
    );
    assert_eq!(result, Some(procset(1..=2) | procset(17..=19) | procset(33..=34) | procset(50..=52)));
}

#[test]
fn test_find_resource_hierarchies_scattered6() {
    let h = Hierarchy::new()
        .add_partition("switch".into(), procsets([1..=16, 17..=32].into()))
        .add_partition("node".into(), procsets([1..=8, 9..=16, 17..=24, 25..=32].into()))
        .add_partition(
            "cpus".into(),
            procsets([1..=4, 5..=8, 9..=12, 13..=16, 17..=20, 21..=24, 25..=28, 29..=32].into()),
        );

    // Test with [2, 2, 1] levels
    let result = h.find_resource_hierarchies_scattered(&procset(1..=32), &[("switch".into(), 2), ("node".into(), 2), ("cpus".into(), 1)]);
    assert_eq!(result, Some(procset(1..=4) | procset(9..=12) | procset(17..=20) | procset(25..=28)));

    // Test with [1, 2, 1] levels
    let result = h.find_resource_hierarchies_scattered(&procset(1..=32), &[("switch".into(), 1), ("node".into(), 2), ("cpus".into(), 1)]);
    assert_eq!(result, Some(procset(1..=4) | procset(9..=12)));
}

#[test]
fn test_hierarchy_from_platform() {
    let platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    // 1 node = 32 cores.

    let available = ProcSet::from_iter([1..=256]);

    let req = HierarchyRequests::from_requests(vec![
        HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])
    ]);

    println!("requesting {:?}, hierarchy {:?}", req, platform_config.resource_set.hierarchy);

    let proc_set = platform_config.resource_set.hierarchy.request(&available, &req);
    let proc_set_2 = platform_config.resource_set.hierarchy.find_resource_hierarchies_scattered(&available, &[("nodes".into(), 2)]);

    assert!(proc_set.is_some());
    assert!(proc_set_2.is_some());
    let proc_set = proc_set.unwrap();
    let proc_set_2 = proc_set_2.unwrap();
    assert_eq!(proc_set_2, ProcSet::from_iter([1..=64]));
    assert_eq!(proc_set, ProcSet::from_iter([1..=64]));
}
