use crate::scheduler::tests::platform_mock::generate_mock_platform_config;
use crate::models::{JobBuilder, ScheduledJobData, ProcSet};
use crate::scheduler::slot::{Slot, SlotSet};
use std::collections::HashMap;
use std::rc::Rc;

pub fn get_test_slot_set() -> SlotSet {
    let platform_config = Rc::new(generate_mock_platform_config(false, 100, 48, 4, 64, false));

    let s1: Slot = Slot::new(Rc::clone(&platform_config), 1, None, Some(2), 0, 9, ProcSet::from_iter([1..=32]), None);
    let s2: Slot = Slot::new(Rc::clone(&platform_config), 2, Some(1), Some(3), 10, 19, ProcSet::from_iter([1..=16, 28..=32]), None);
    let s3: Slot = Slot::new(Rc::clone(&platform_config), 3, Some(2), None, 20, 29, ProcSet::from_iter([1..=8, 30..=32]), None);

    let slots = HashMap::from([(1, s1), (2, s2), (3, s3)]);
    SlotSet::from_map(Rc::clone(&platform_config), slots, 1)
}

#[test]
pub fn test_slot_id_at() {
    let ss = get_test_slot_set();

    assert_eq!(ss.slot_id_at(5, None).unwrap(), 1);
    assert_eq!(ss.slot_id_at(16, None).unwrap(), 2);
    assert_eq!(ss.slot_id_at(25, None).unwrap(), 3);
}

#[test]
pub fn test_split() {
    let mut ss = get_test_slot_set();

    ss.find_and_split_at(5, true);
    assert_eq!(ss.slot_id_at(4, None).unwrap(), 4);
    assert_eq!(ss.slot_id_at(5, None).unwrap(), 1);

    ss.find_and_split_at(1, true);
    assert_eq!(ss.slot_id_at(0, None).unwrap(), 5);
    assert_eq!(ss.slot_id_at(1, None).unwrap(), 4);

    ss.find_and_split_at(29, true);
    assert_eq!(ss.slot_id_at(28, None).unwrap(), 6);
    assert_eq!(ss.slot_id_at(29, None).unwrap(), 3);

    ss.find_and_split_at(28, false);
    assert_eq!(ss.slot_id_at(27, None).unwrap(), 6);
    assert_eq!(ss.slot_id_at(28, None).unwrap(), 7);
}

#[test]
pub fn test_get_encompassing_range() {
    let ss = get_test_slot_set();
    assert_eq!(ss.get_encompassing_range(5, 16, None).map(|(s1, s2)| (s1.id(), s2.id())), Some((1, 2)));
    assert_eq!(ss.get_encompassing_range(5, 25, None).map(|(s1, s2)| (s1.id(), s2.id())), Some((1, 3)));
}

#[test]
pub fn test_get_encompassing_range_strict() {
    let ss = get_test_slot_set();
    assert_eq!(ss.get_encompassing_range_strict(5, 16, None).map(|(s1, s2)| (s1.id(), s2.id())), None);
    assert_eq!(ss.get_encompassing_range_strict(5, 25, None).map(|(s1, s2)| (s1.id(), s2.id())), None);
    assert_eq!(
        ss.get_encompassing_range_strict(10, 15, None).map(|(s1, s2)| (s1.id(), s2.id())),
        Some((1, 3))
    );
}

#[test]
pub fn test_iter() {
    let ss = get_test_slot_set();

    let mut it = ss.iter();
    assert_eq!(it.next().map(|s| s.id()), Some(1));
    assert_eq!(it.next().map(|s| s.id()), Some(2));
    assert_eq!(it.next().map(|s| s.id()), Some(3));
    assert_eq!(it.next().map(|s| s.id()), None);
}

#[test]
pub fn test_iter_rev() {
    let ss = get_test_slot_set();
    let mut it = ss.iter().rev();
    assert_eq!(it.next().map(|s| s.id()), Some(3));
    assert_eq!(it.next().map(|s| s.id()), Some(2));
    assert_eq!(it.next().map(|s| s.id()), Some(1));
    assert_eq!(it.next().map(|s| s.id()), None);
}

#[test]
pub fn test_iter_between() {
    let ss = get_test_slot_set();
    let mut it = ss.iter().start_at(1).end_at(2);
    assert_eq!(it.next().map(|s| s.id()), Some(1));
    assert_eq!(it.next().map(|s| s.id()), Some(2));
    assert_eq!(it.next().map(|s| s.id()), None);

    let mut it = ss.iter().start_at(2);
    assert_eq!(it.next().map(|s| s.id()), Some(2));
    assert_eq!(it.next().map(|s| s.id()), Some(3));
    assert_eq!(it.next().map(|s| s.id()), None);

    let mut it = ss.iter().between(2, 2);
    assert_eq!(it.next().map(|s| s.id()), Some(2));
    assert_eq!(it.next().map(|s| s.id()), None);

    let mut it = ss.iter().between(0, 2);
    assert_eq!(it.next().map(|s| s.id()), None);
}

#[test]
pub fn test_iter_between_rev() {
    let ss = get_test_slot_set();
    let mut it = ss.iter().end_at(2).rev();
    assert_eq!(it.next().map(|s| s.id()), Some(2));
    assert_eq!(it.next().map(|s| s.id()), Some(1));
    assert_eq!(it.next().map(|s| s.id()), None);

    let mut it = ss.iter().between(2, 3).rev();
    assert_eq!(it.next().map(|s| s.id()), Some(3));
    assert_eq!(it.next().map(|s| s.id()), Some(2));
    assert_eq!(it.next().map(|s| s.id()), None);

    let mut it = ss.iter().start_at(2).end_at(2).rev();
    assert_eq!(it.next().map(|s| s.id()), Some(2));
    assert_eq!(it.next().map(|s| s.id()), None);

    let mut it = ss.iter().start_at(1).end_at(3).rev();
    assert_eq!(it.next().map(|s| s.id()), Some(3));
    assert_eq!(it.next().map(|s| s.id()), Some(2));
    assert_eq!(it.next().map(|s| s.id()), Some(1));
    assert_eq!(it.next().map(|s| s.id()), None);
}

#[test]
pub fn test_iter_between_with_width() {
    let ss = get_test_slot_set();

    let mut it = ss.iter().between(1, 2).with_width(10);
    assert_eq!(it.next().map(|(s1, s2)| (s1.id(), s2.id())), Some((1, 1)));
    assert_eq!(it.next().map(|(s1, s2)| (s1.id(), s2.id())), Some((2, 2)));
    assert_eq!(it.next().map(|(s1, s2)| (s1.id(), s2.id())), None);

    let mut it = ss.iter().start_at(2).with_width(11);
    assert_eq!(it.next().map(|(s1, s2)| (s1.id(), s2.id())), Some((2, 3)));
    assert_eq!(it.next().map(|(s1, s2)| (s1.id(), s2.id())), None);

    let mut it = ss.iter().between(1, 3).with_width(20);
    assert_eq!(it.next().map(|(s1, s2)| (s1.id(), s2.id())), Some((1, 2)));
    assert_eq!(it.next().map(|(s1, s2)| (s1.id(), s2.id())), Some((2, 3)));
    assert_eq!(it.next().map(|(s1, s2)| (s1.id(), s2.id())), None);
}

#[test]
pub fn test_split_slots() {
    let mut ss = get_test_slot_set();
    let scheduled_job_data = ScheduledJobData::new(5, 14, ProcSet::from_iter([4..=6]), 0);
    let job = JobBuilder::new(1).scheduled(scheduled_job_data).build();
    ss.split_slots_for_job_and_update_resources(&job, true, None);

    assert_eq!(ss.slot_at(4, None).unwrap().proc_set().clone(), ProcSet::from_iter([1..=32]));
    assert_eq!(ss.slot_at(5, None).unwrap().proc_set().clone(), ProcSet::from_iter([1..=3, 7..=32]));
    assert_eq!(ss.slot_at(9, None).unwrap().proc_set().clone(), ProcSet::from_iter([1..=3, 7..=32]));
    assert_eq!(
        ss.slot_at(10, None).unwrap().proc_set().clone(),
        ProcSet::from_iter([1..=3, 7..=16, 28..=32])
    );
    assert_eq!(
        ss.slot_at(14, None).unwrap().proc_set().clone(),
        ProcSet::from_iter([1..=3, 7..=16, 28..=32])
    );
    assert_eq!(ss.slot_at(15, None).unwrap().proc_set().clone(), ProcSet::from_iter([1..=16, 28..=32]));
}

#[test]
pub fn test_intersect_slots_intervals() {
    let ss = get_test_slot_set();
    assert_eq!(ss.intersect_slots_intervals(1, 2), ProcSet::from_iter([1..=16, 28..=32]));
    assert_eq!(ss.intersect_slots_intervals(2, 2), ProcSet::from_iter([1..=16, 28..=32]));
    assert_eq!(ss.intersect_slots_intervals(1, 3), ProcSet::from_iter([1..=8, 30..=32]));
}
