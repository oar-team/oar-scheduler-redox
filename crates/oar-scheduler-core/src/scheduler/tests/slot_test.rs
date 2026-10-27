use crate::model::job::{Job, JobAssignment, JobBuilder, PlaceholderType, ProcSet};
use crate::platform::PlatformConfig;
use crate::scheduler::slot::Slot;
use crate::scheduler::slotset::{SlotIterator, SlotSet};
use crate::scheduler::tests::platform_mock::generate_mock_platform_config;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Python `MAX_TIME` (`oar.kao.slot.MAX_TIME`).
const MAX_TIME: i64 = 2_147_483_648;

fn mock_platform_config(res_count: u32) -> Rc<PlatformConfig> {
    Rc::new(generate_mock_platform_config(false, res_count, 8, 4, 8, false))
}

/// Python `JobPseudo(id, start_time, walltime, res_set)`.
/// In Rust the assignment end is inclusive, hence `start_time + walltime - 1`.
fn pseudo_job(id: i64, start_time: i64, walltime: i64, res_set: ProcSet) -> Job {
    JobBuilder::new(id)
        .assign(JobAssignment::new(start_time, start_time + walltime - 1, res_set, 0))
        .build()
}

fn new_slot(pc: &Rc<PlatformConfig>, id: i32, prev: Option<i32>, next: Option<i32>, begin: i64, end: i64, proc_set: ProcSet) -> Slot {
    Slot::new(Rc::clone(pc), id, prev, next, begin, end, proc_set, None)
}

/// Port of the Python `compare_slots_val_ref`: compares the slots in linked-list order with a
/// reference vector of `(begin, end, proc_set)`.
fn compare_slots_val_ref(ss: &SlotSet, v: &[(i64, i64, ProcSet)]) -> bool {
    let mut i = 0;
    for slot in ss.iter() {
        if i >= v.len() {
            println!("NOT EQUAL (test/ref): the SlotSet has more slots than the reference vector");
            return false;
        }
        let (b, e, itvs) = &v[i];
        if slot.begin() != *b || slot.end() != *e || slot.proc_set() != itvs {
            println!(
                "NOT EQUAL (test/ref): begin: {}, {}, end: {}, {}, itvs: {}, {}",
                slot.begin(),
                b,
                slot.end(),
                e,
                slot.proc_set(),
                itvs
            );
            return false;
        }
        i += 1;
    }
    if i != v.len() {
        println!("NOT EQUAL (test/ref): expected {} slots, got {}", v.len(), i);
        return false;
    }
    true
}

/// Port of the Python `check_slot_integrity`: verifies the boundaries, the absence of duplicate
/// links, the contiguity of the slots, and that the linked list is consistent.
fn check_slot_integrity(ss: &SlotSet) {
    let mut prevs: HashSet<Option<i32>> = HashSet::new();
    let mut nexts: HashSet<Option<i32>> = HashSet::new();
    let mut prev_e: Option<i64> = None;
    let mut slot = ss.first_slot();
    while let Some(current) = slot {
        // Check boundaries
        assert!(current.begin() <= current.end(), "slot {} has begin > end", current.id());
        // Check for duplicates
        assert!(
            !nexts.contains(&current.next()) && !prevs.contains(&current.prev()),
            "duplicate prev/next link at slot {}",
            current.id()
        );
        // Check contiguity
        if let Some(previous_end) = prev_e {
            assert_eq!(previous_end + 1, current.begin(), "slots are not contiguous at slot {}", current.id());
        }
        if let Some(prev_id) = current.prev() {
            assert_eq!(
                ss.get_slot(prev_id).and_then(|prev_slot| prev_slot.next()),
                Some(current.id()),
                "broken prev link at slot {}",
                current.id()
            );
        }
        if let Some(next_id) = current.next() {
            assert_eq!(
                ss.get_slot(next_id).and_then(|next_slot| next_slot.prev()),
                Some(current.id()),
                "broken next link at slot {}",
                current.id()
            );
        }
        prev_e = Some(current.end());
        prevs.insert(current.prev());
        nexts.insert(current.next());
        slot = current.next().and_then(|id| ss.get_slot(id));
    }
}

/// Python `SlotSet.traverse_id(start, end)`: `(0, 0)` traverses the whole set, a zero id means
/// "from the first slot"/"until the last slot".
fn traverse_iter<'a>(ss: &'a SlotSet, start: i32, end: i32) -> SlotIterator<'a> {
    if start == 0 && end == 0 {
        ss.iter()
    } else if start == 0 {
        ss.iter().end_at(end)
    } else if end == 0 {
        ss.iter().start_at(start)
    } else {
        ss.iter().between(start, end)
    }
}

fn traverse_ids(ss: &SlotSet, start: i32, end: i32) -> Vec<i32> {
    traverse_iter(ss, start, end).map(|slot| slot.id()).collect()
}

/// Python `SlotSet.traverse_with_width(width, start_id, end_id)`, yielding `(start_id, end_id)`.
fn traverse_with_width_ids(ss: &SlotSet, width: i64, start_id: i32, end_id: i32) -> Vec<(i32, i32)> {
    traverse_iter(ss, start_id, end_id)
        .with_width(width)
        .map(|(start_slot, end_slot)| (start_slot.id(), end_slot.id()))
        .collect()
}

/// Python `_reference_traverse_with_width`: the original nested-loop semantics.
fn reference_traverse_with_width_ids(ss: &SlotSet, width: i64, start_id: i32, end_id: i32) -> Vec<(i32, i32)> {
    assert!(width > 0);
    let mut result = Vec::new();
    for start_slot in traverse_iter(ss, start_id, end_id) {
        let begin_time = start_slot.begin();
        for end_slot in traverse_iter(ss, start_slot.id(), end_id) {
            if end_slot.end() - begin_time + 1 >= width {
                result.push((start_slot.id(), end_slot.id()));
                break;
            }
        }
    }
    result
}

/// Small deterministic PRNG used by the randomized `traverse_with_width` test.
struct TestRng(u64);

impl TestRng {
    fn new(seed: u64) -> Self {
        TestRng(seed)
    }
    fn next_u32(&mut self) -> u32 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) as u32
    }
    /// Inclusive random integer in `[min, max]`, like Python's `random.Random.randint`.
    fn randint(&mut self, min: u32, max: u32) -> u32 {
        min + self.next_u32() % (max - min + 1)
    }
}

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
    let scheduled_job_data = JobAssignment::new(5, 14, ProcSet::from_iter([4..=6]), 0);
    let job = JobBuilder::new(1).assign(scheduled_job_data).build();
    ss.split_slots_for_job_and_update_resources(&job, true, true, None);

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
pub fn test_split_slots_outside() {
    let mut ss = get_test_slot_set();
    let scheduled_job_data = JobAssignment::new(-5, 14, ProcSet::from_iter([4..=6]), 0);
    let job = JobBuilder::new(1).assign(scheduled_job_data).build();
    ss.split_slots_for_job_and_update_resources(&job, true, true, None);

    assert_eq!(ss.slot_at(4, None).unwrap().proc_set().clone(), ProcSet::from_iter([1..=3, 7..=32]));
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
    assert_eq!(ss.intersect_slots_intervals(1, 2, None, None, &PlaceholderType::None), ProcSet::from_iter([1..=16, 28..=32]));
    assert_eq!(ss.intersect_slots_intervals(2, 2, None, None, &PlaceholderType::None), ProcSet::from_iter([1..=16, 28..=32]));
    assert_eq!(ss.intersect_slots_intervals(1, 3, None, None, &PlaceholderType::None), ProcSet::from_iter([1..=8, 30..=32]));
}

// -------------------------------------------------------------------------------------------------
// Port of `oar3/tests/kao/test_slot.py`.
// -------------------------------------------------------------------------------------------------

/// Python `test_intersec_itvs_slots`.
#[test]
pub fn test_intersec_itvs_slots() {
    let pc = mock_platform_config(32);
    let s1 = new_slot(&pc, 1, None, Some(2), 1, 10, ProcSet::from_iter([1..=32]));
    let s2 = new_slot(&pc, 2, Some(1), Some(3), 11, 20, ProcSet::from_iter([1..=16, 24..=28]));
    let s3 = new_slot(&pc, 3, Some(2), None, 21, 30, ProcSet::from_iter([1..=8, 12..=26]));
    let ss = SlotSet::from_map(Rc::clone(&pc), HashMap::from([(1, s1), (2, s2), (3, s3)]), 1);

    assert_eq!(
        ss.intersect_slots_intervals(1, 3, None, None, &PlaceholderType::None),
        ProcSet::from_iter([1..=8, 12..=16, 24..=26])
    );
}

/// Python `test_split_slots_ab`.
#[test]
pub fn test_split_slots_ab() {
    // Python's `split_slots` splits at `start_time + walltime` only when the slot end is strictly
    // greater, so it over-subtracts one extra time unit here. The Rust API always splits at
    // `assignment.end + 1`, hence the additional `(25, 25)` slot.
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 1, 25, ProcSet::from_iter([1..=32])));
    let j1 = pseudo_job(1, 5, 20, ProcSet::from_iter([10..=20]));

    ss.split_slots_for_job_and_update_resources(&j1, false, true, Some(1));

    let v = [
        (1, 4, ProcSet::from_iter([1..=32])),
        (5, 24, ProcSet::from_iter([1..=9, 21..=32])),
        (25, 25, ProcSet::from_iter([1..=32])),
    ];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Python `test_split_slots_abc`.
#[test]
pub fn test_split_slots_abc() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 1, 20, ProcSet::from_iter([1..=32])));
    let j1 = pseudo_job(1, 5, 10, ProcSet::from_iter([10..=20]));

    ss.split_slots_for_job_and_update_resources(&j1, false, true, Some(1));

    let v = [
        (1, 4, ProcSet::from_iter([1..=32])),
        (5, 14, ProcSet::from_iter([1..=9, 21..=32])),
        (15, 20, ProcSet::from_iter([1..=32])),
    ];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Python `test_split_slots_b`.
#[test]
pub fn test_split_slots_b() {
    // Python's `split_slots` does not split when `slot.e == start_time + walltime`, so it subtracts
    // the job resources from the whole `(1, 21)` slot and over-subtracts time unit 21. The Rust API
    // splits at `assignment.end + 1` and leaves time unit 21 with its full resources.
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 1, 21, ProcSet::from_iter([1..=32])));
    let j1 = pseudo_job(1, 1, 20, ProcSet::from_iter([10..=20]));

    ss.split_slots_for_job_and_update_resources(&j1, false, true, Some(1));

    let v = [
        (1, 20, ProcSet::from_iter([1..=9, 21..=32])),
        (21, 21, ProcSet::from_iter([1..=32])),
    ];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Python `test_split_slots_bc`.
#[test]
pub fn test_split_slots_bc() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 1, 20, ProcSet::from_iter([1..=32])));
    let j1 = pseudo_job(1, 1, 10, ProcSet::from_iter([10..=20]));

    ss.split_slots_for_job_and_update_resources(&j1, false, true, Some(1));

    let v = [
        (1, 10, ProcSet::from_iter([1..=9, 21..=32])),
        (11, 20, ProcSet::from_iter([1..=32])),
    ];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Python `test_bug_split_slots`.
#[test]
pub fn test_bug_split_slots() {
    // The Python slot literal for s2 declares `prev=1` while s4 (whose next is 2) is the actual
    // previous slot, so the Python linked list is inconsistent. `SlotSet::from_map` validates the
    // links, hence s2's prev is set to 4 here. The traversal order (1 -> 4 -> 2) is unchanged.
    let pc = mock_platform_config(32);
    let s1 = new_slot(&pc, 1, None, Some(4), 20, 69, ProcSet::from_iter([16..=32]));
    let s2 = new_slot(&pc, 2, Some(4), None, 2147483600, 2147483647, ProcSet::new());
    let s4 = new_slot(&pc, 4, Some(1), Some(2), 70, 2147483599, ProcSet::from_iter([1..=32]));
    let mut ss = SlotSet::from_map(Rc::clone(&pc), HashMap::from([(1, s1), (2, s2), (4, s4)]), 1);

    let j2 = pseudo_job(2, 20, 60, ProcSet::from_iter([16..=30]));
    ss.split_slots_for_job_and_update_resources(&j2, false, true, Some(1));

    let v = [
        (20, 69, ProcSet::from_iter([31..=32])),
        (70, 79, ProcSet::from_iter([1..=15, 31..=32])),
        (80, 2147483599, ProcSet::from_iter([1..=32])),
        (2147483600, 2147483647, ProcSet::new()),
    ];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Python `test_add_split_slots_jobs_one_job`.
#[test]
pub fn test_add_split_slots_jobs_one_job() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 10, MAX_TIME, ProcSet::new()));
    let j = pseudo_job(1, 5, 10, ProcSet::from_iter([10..=50]));

    ss.split_slots_for_jobs_and_update_resources(&vec![&j], false, false, None);

    let v = [
        (10, 14, ProcSet::from_iter([10..=50])),
        (15, MAX_TIME, ProcSet::new()),
    ];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Python `test_add_split_slots_jobs_2_jobs_1` (no assertion in the reference test).
#[test]
pub fn test_add_split_slots_jobs_2_jobs_1() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 10, MAX_TIME, ProcSet::new()));
    let j1 = pseudo_job(1, 100, 30, ProcSet::from_iter([10..=20]));
    let j2 = pseudo_job(2, 20, 200, ProcSet::from_iter([40..=50]));

    ss.split_slots_for_jobs_and_update_resources(&vec![&j1], false, false, None);
    ss.split_slots_for_jobs_and_update_resources(&vec![&j2], false, false, None);
    ss.split_slots_for_jobs_and_update_resources(&vec![&j2], false, false, None);

    check_slot_integrity(&ss);
    assert_eq!(ss.slot_count(), 5);
}

/// Python `test_add_split_slots_jobs_2_jobs_2`.
#[test]
pub fn test_add_split_slots_jobs_2_jobs_2() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 10, MAX_TIME, ProcSet::new()));
    let j1 = pseudo_job(1, 100, 30, ProcSet::from_iter([10..=20]));
    let j2 = pseudo_job(2, 20, 200, ProcSet::from_iter([40..=50]));

    ss.split_slots_for_jobs_and_update_resources(&vec![&j2, &j1], false, false, None);

    let v = [
        (10, 19, ProcSet::new()),
        (20, 99, ProcSet::from_iter([40..=50])),
        (100, 129, ProcSet::from_iter([10..=20, 40..=50])),
        (130, 219, ProcSet::from_iter([40..=50])),
        (220, MAX_TIME, ProcSet::new()),
    ];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Python `test_add_split_slots_jobs_2_jobs_2_2`.
#[test]
pub fn test_add_split_slots_jobs_2_jobs_2_2() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 0, MAX_TIME, ProcSet::from_iter([1..=20])));
    let j1 = pseudo_job(1, 0, 300, ProcSet::from_iter([1..=10]));
    let j2 = pseudo_job(2, 0, 300, ProcSet::from_iter([11..=20]));

    ss.split_slots_for_jobs_and_update_resources(&vec![&j1], false, true, None);
    ss.split_slots_for_jobs_and_update_resources(&vec![&j2], false, true, None);

    let v = [
        (0, 299, ProcSet::new()),
        (300, MAX_TIME, ProcSet::from_iter([1..=20])),
    ];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Shared body of the Python `test_slots_and_jobs` parametrized cases.
fn split_jobs_and_check_integrity(jobs: Vec<Job>) {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 1, 300, ProcSet::from_iter([1..=32])));
    let job_refs: Vec<&Job> = jobs.iter().collect();
    ss.split_slots_for_jobs_and_update_resources(&job_refs, false, true, None);
    check_slot_integrity(&ss);
}

/// Python `test_slots_and_jobs[same job]`.
#[test]
pub fn test_slots_and_jobs_same_job() {
    split_jobs_and_check_integrity(vec![
        pseudo_job(1, 5, 10, ProcSet::from_iter([1..=1])),
        pseudo_job(2, 5, 10, ProcSet::from_iter([1..=1])),
        pseudo_job(2, 5, 10, ProcSet::from_iter([1..=1])),
        pseudo_job(2, 5, 10, ProcSet::from_iter([1..=1])),
    ]);
}

/// Python `test_slots_and_jobs[same start time]`.
#[test]
pub fn test_slots_and_jobs_same_start_time() {
    split_jobs_and_check_integrity(vec![
        pseudo_job(1, 5, 10, ProcSet::from_iter([1..=1])),
        pseudo_job(2, 5, 20, ProcSet::from_iter([1..=1])),
    ]);
}

/// Python `test_slots_and_jobs[same start time with inverted endings]`.
#[test]
pub fn test_slots_and_jobs_same_start_time_with_inverted_endings() {
    split_jobs_and_check_integrity(vec![
        pseudo_job(2, 5, 20, ProcSet::from_iter([1..=1])),
        pseudo_job(1, 5, 10, ProcSet::from_iter([1..=1])),
    ]);
}

/// Python `test_slots_and_jobs[stairs]`.
#[test]
pub fn test_slots_and_jobs_stairs() {
    split_jobs_and_check_integrity(vec![
        pseudo_job(1, 5, 10, ProcSet::from_iter([1..=1])),
        pseudo_job(1, 10, 10, ProcSet::from_iter([2..=2])),
        pseudo_job(1, 15, 10, ProcSet::from_iter([3..=3])),
        pseudo_job(1, 20, 10, ProcSet::from_iter([4..=4])),
    ]);
}

/// Python `test_slots_and_jobs_corner_cases`.
#[test]
pub fn test_slots_and_jobs_corner_cases() {
    let pc = mock_platform_config(32);
    let s1 = new_slot(&pc, 1, None, Some(2), 50, 249, ProcSet::from_iter([1..=32]));
    let s2 = new_slot(&pc, 2, Some(1), None, 250, 499, ProcSet::from_iter([1..=32]));
    let mut ss = SlotSet::from_map(Rc::clone(&pc), HashMap::from([(1, s1), (2, s2)]), 1);
    let j = pseudo_job(1, 249, 1, ProcSet::from_iter([1..=1]));

    ss.split_slots_for_jobs_and_update_resources(&vec![&j], false, true, None);

    check_slot_integrity(&ss);
    assert_eq!(ss.slot_count(), 3);
    assert_eq!(ss.slot_at(249, None).unwrap().proc_set(), &ProcSet::from_iter([2..=32]));
}

/// Slots used by the Python `test_slot_id_at` parametrized cases.
fn slot_id_at_slotset(pc: &Rc<PlatformConfig>) -> SlotSet {
    let s1 = new_slot(pc, 1, None, Some(2), 50, 249, ProcSet::from_iter([1..=32]));
    let s2 = new_slot(pc, 2, Some(1), Some(3), 250, 499, ProcSet::from_iter([1..=32]));
    let s3 = new_slot(pc, 3, Some(2), None, 500, 1000, ProcSet::from_iter([1..=32]));
    SlotSet::from_map(Rc::clone(pc), HashMap::from([(1, s1), (2, s2), (3, s3)]), 1)
}

/// Python `test_slot_id_at[5]`: `slot_id_at` returns 0 when not found (Rust `None`).
#[test]
pub fn test_slot_id_at_5() {
    let pc = mock_platform_config(32);
    let ss = slot_id_at_slotset(&pc);
    assert_eq!(ss.slot_id_at(5, None), None);
}

/// Python `test_slot_id_at[250]`.
#[test]
pub fn test_slot_id_at_250() {
    let pc = mock_platform_config(32);
    let ss = slot_id_at_slotset(&pc);
    assert_eq!(ss.slot_id_at(250, None), Some(2));
}

/// Python `test_slot_id_at[499]`.
#[test]
pub fn test_slot_id_at_499() {
    let pc = mock_platform_config(32);
    let ss = slot_id_at_slotset(&pc);
    assert_eq!(ss.slot_id_at(499, None), Some(2));
}

/// Python `test_slot_id_at[500]`.
#[test]
pub fn test_slot_id_at_500() {
    let pc = mock_platform_config(32);
    let ss = slot_id_at_slotset(&pc);
    assert_eq!(ss.slot_id_at(500, None), Some(3));
}

/// Python `test_slot_id_at[1500]`.
#[test]
pub fn test_slot_id_at_1500() {
    let pc = mock_platform_config(32);
    let ss = slot_id_at_slotset(&pc);
    assert_eq!(ss.slot_id_at(1500, None), None);
}

/// Python `test_slot_id_at[25]`.
#[test]
pub fn test_slot_id_at_25() {
    let pc = mock_platform_config(32);
    let ss = slot_id_at_slotset(&pc);
    assert_eq!(ss.slot_id_at(25, None), None);
}

/// Slots used by the Python `test_get_encompassing_range` parametrized cases.
fn encompassing_range_slotset(pc: &Rc<PlatformConfig>) -> SlotSet {
    let s1 = new_slot(pc, 1, None, Some(2), 50, 249, ProcSet::from_iter([1..=32]));
    let s2 = new_slot(pc, 2, Some(1), Some(3), 250, 499, ProcSet::from_iter([1..=32]));
    let s3 = new_slot(pc, 3, Some(2), None, 500, 1000, ProcSet::from_iter([1..=32]));
    SlotSet::from_map(Rc::clone(pc), HashMap::from([(1, s1), (2, s2), (3, s3)]), 1)
}

fn encompassing_ids(ss: &SlotSet, begin: i64, end: i64) -> Option<(i32, i32)> {
    ss.get_encompassing_range(begin, end, None).map(|(begin_slot, end_slot)| (begin_slot.id(), end_slot.id()))
}

/// Python `test_get_encompassing_range[(50, 50)]`.
#[test]
pub fn test_get_encompassing_range_50_50() {
    let pc = mock_platform_config(32);
    let ss = encompassing_range_slotset(&pc);
    assert_eq!(encompassing_ids(&ss, 50, 50), Some((1, 1)));
}

/// Python `test_get_encompassing_range[(50, 249)]`.
#[test]
pub fn test_get_encompassing_range_50_249() {
    let pc = mock_platform_config(32);
    let ss = encompassing_range_slotset(&pc);
    assert_eq!(encompassing_ids(&ss, 50, 249), Some((1, 1)));
}

/// Python `test_get_encompassing_range[(249, 249)]`.
#[test]
pub fn test_get_encompassing_range_249_249() {
    let pc = mock_platform_config(32);
    let ss = encompassing_range_slotset(&pc);
    assert_eq!(encompassing_ids(&ss, 249, 249), Some((1, 1)));
}

/// Python `test_get_encompassing_range[(50, 1000)]`.
#[test]
pub fn test_get_encompassing_range_50_1000() {
    let pc = mock_platform_config(32);
    let ss = encompassing_range_slotset(&pc);
    assert_eq!(encompassing_ids(&ss, 50, 1000), Some((1, 3)));
}

/// Python `test_get_encompassing_range[(499, 500)]`.
#[test]
pub fn test_get_encompassing_range_499_500() {
    let pc = mock_platform_config(32);
    let ss = encompassing_range_slotset(&pc);
    assert_eq!(encompassing_ids(&ss, 499, 500), Some((2, 3)));
}

/// Slots used by the Python `test_traverse` parametrized cases (chain order is 2 -> 1 -> 7).
fn traverse_slotset(pc: &Rc<PlatformConfig>) -> SlotSet {
    let s2 = new_slot(pc, 2, None, Some(1), 50, 249, ProcSet::from_iter([1..=32]));
    let s1 = new_slot(pc, 1, Some(2), Some(7), 250, 499, ProcSet::from_iter([1..=32]));
    let s7 = new_slot(pc, 7, Some(1), None, 500, 1000, ProcSet::from_iter([1..=32]));
    SlotSet::from_map(Rc::clone(pc), HashMap::from([(2, s2), (1, s1), (7, s7)]), 2)
}

/// Python `test_traverse[0-0]` (the default traversal).
#[test]
pub fn test_traverse_default() {
    let pc = mock_platform_config(32);
    let ss = traverse_slotset(&pc);
    assert_eq!(traverse_ids(&ss, 0, 0), vec![2, 1, 7]);
}

/// Python `test_traverse[2-7]`.
#[test]
pub fn test_traverse_2_7() {
    let pc = mock_platform_config(32);
    let ss = traverse_slotset(&pc);
    assert_eq!(traverse_ids(&ss, 2, 7), vec![2, 1, 7]);
}

/// Python `test_traverse[2-2]` (the Python `-1` entries are only zip padding).
#[test]
pub fn test_traverse_2_2() {
    let pc = mock_platform_config(32);
    let ss = traverse_slotset(&pc);
    assert_eq!(traverse_ids(&ss, 2, 2), vec![2]);
}

/// Python `test_traverse[7-2]` (an end id before the start id traverses to the end of the chain).
#[test]
pub fn test_traverse_7_2() {
    let pc = mock_platform_config(32);
    let ss = traverse_slotset(&pc);
    assert_eq!(traverse_ids(&ss, 7, 2), vec![7]);
}

/// Python `test_traverse[1-7]`.
#[test]
pub fn test_traverse_1_7() {
    let pc = mock_platform_config(32);
    let ss = traverse_slotset(&pc);
    assert_eq!(traverse_ids(&ss, 1, 7), vec![1, 7]);
}

/// Slots used by the Python `test_traverse_width` parametrized cases.
fn traverse_width_slotset(pc: &Rc<PlatformConfig>) -> SlotSet {
    let mut ss = SlotSet::from_slot(new_slot(pc, 1, None, None, 1, 1000, ProcSet::from_iter([1..=32])));
    ss.find_and_split_at(10, false);
    ss.find_and_split_at(11, false);
    ss.find_and_split_at(50, false);
    ss.find_and_split_at(500, false);
    ss
}

/// Python `test_traverse_width[15]`.
#[test]
pub fn test_traverse_width_15() {
    let pc = mock_platform_config(32);
    let ss = traverse_width_slotset(&pc);
    assert_eq!(traverse_with_width_ids(&ss, 15, 0, 0), vec![(1, 3), (2, 3), (3, 3), (4, 4), (5, 5)]);
}

/// Python `test_traverse_width[50]`.
#[test]
pub fn test_traverse_width_50() {
    let pc = mock_platform_config(32);
    let ss = traverse_width_slotset(&pc);
    assert_eq!(traverse_with_width_ids(&ss, 50, 0, 0), vec![(1, 4), (2, 4), (3, 4), (4, 4), (5, 5)]);
}

/// Python `test_traverse_width[20000]` (the Python `(-1, -1)` entry is only zip padding: no pair).
#[test]
pub fn test_traverse_width_20000() {
    let pc = mock_platform_config(32);
    let ss = traverse_width_slotset(&pc);
    assert!(traverse_with_width_ids(&ss, 20000, 0, 0).is_empty());
}

/// Python `test_split_at_before[1]`.
#[test]
#[should_panic(expected = "not in the slot time range")]
pub fn test_split_at_before_1() {
    // Python's `split_at_before` creates a size-1 slot when the split date equals the slot begin.
    // The Rust `split_at` requires `time > slot.begin` and panics instead.
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 1, 100, ProcSet::from_iter([1..=32])));
    ss.split_at(1, 1, true);
}

/// Python `test_split_at_before[100]`.
#[test]
pub fn test_split_at_before_100() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 1, 100, ProcSet::from_iter([1..=32])));
    ss.split_at(1, 100, true);

    check_slot_integrity(&ss);
    assert_eq!(ss.slot_count(), 2);
    assert_eq!((ss.first_slot().unwrap().begin(), ss.first_slot().unwrap().end()), (1, 99));
    assert_eq!((ss.last_slot().unwrap().begin(), ss.last_slot().unwrap().end()), (100, 100));
}

/// Python `test_split_at_before[50]`.
#[test]
pub fn test_split_at_before_50() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 1, 100, ProcSet::from_iter([1..=32])));
    ss.split_at(1, 50, true);

    check_slot_integrity(&ss);
    assert_eq!(ss.slot_count(), 2);
    assert_eq!((ss.first_slot().unwrap().begin(), ss.first_slot().unwrap().end()), (1, 49));
    assert_eq!((ss.last_slot().unwrap().begin(), ss.last_slot().unwrap().end()), (50, 100));
}

/// Python `test_split_at_after[1]`.
#[test]
#[should_panic(expected = "not in the slot time range")]
pub fn test_split_at_after_1() {
    // Python's `split_at_after` keeps a size-1 slot when the split date equals the slot begin.
    // The Rust `split_at` requires `time > slot.begin` and panics instead.
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 1, 100, ProcSet::from_iter([1..=32])));
    ss.split_at(1, 1, false);
}

/// Python `test_split_at_after[100]`.
#[test]
pub fn test_split_at_after_100() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 1, 100, ProcSet::from_iter([1..=32])));
    ss.split_at(1, 100, false);

    check_slot_integrity(&ss);
    assert_eq!(ss.slot_count(), 2);
    assert_eq!((ss.first_slot().unwrap().begin(), ss.first_slot().unwrap().end()), (1, 99));
    assert_eq!((ss.last_slot().unwrap().begin(), ss.last_slot().unwrap().end()), (100, 100));
}

/// Python `test_split_at_after[50]`.
#[test]
pub fn test_split_at_after_50() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 1, 100, ProcSet::from_iter([1..=32])));
    ss.split_at(1, 50, false);

    check_slot_integrity(&ss);
    assert_eq!(ss.slot_count(), 2);
    assert_eq!((ss.first_slot().unwrap().begin(), ss.first_slot().unwrap().end()), (1, 49));
    assert_eq!((ss.last_slot().unwrap().begin(), ss.last_slot().unwrap().end()), (50, 100));
}

/// Python `test_split_slots_jobs_job_1` (job strictly inside the slot).
#[test]
pub fn test_split_slots_jobs_job_1() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 10, 50, ProcSet::from_iter([1..=32])));
    let j1 = pseudo_job(1, 5, 20, ProcSet::from_iter([10..=20]));

    ss.split_slots_for_jobs_and_update_resources(&vec![&j1], false, true, None);

    let v = [
        (10, 24, ProcSet::from_iter([1..=9, 21..=32])),
        (25, 50, ProcSet::from_iter([1..=32])),
    ];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Python `test_split_slots_jobs_job_2` (job before the slot).
#[test]
pub fn test_split_slots_jobs_job_2() {
    // The job ends before the slot begins: Python's `split_slots` would fail its `split_at_after`
    // assertion (the insertion date is outside the slot); the Rust API simply returns `None` and
    // leaves the SlotSet unchanged. The reference expectation is unchanged slots.
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 40, 100, ProcSet::from_iter([1..=32])));
    let j1 = pseudo_job(1, 5, 20, ProcSet::from_iter([10..=20]));

    ss.split_slots_for_jobs_and_update_resources(&vec![&j1], false, true, None);

    let v = [(40, 100, ProcSet::from_iter([1..=32]))];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Python `test_split_slots_jobs_job_3` (job overlapping the end of the slot).
#[test]
pub fn test_split_slots_jobs_job_3() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 10, 50, ProcSet::from_iter([1..=32])));
    let j1 = pseudo_job(1, 45, 20, ProcSet::from_iter([10..=20]));

    ss.split_slots_for_jobs_and_update_resources(&vec![&j1], false, true, None);

    let v = [
        (10, 44, ProcSet::from_iter([1..=32])),
        (45, 50, ProcSet::from_iter([1..=9, 21..=32])),
    ];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Python `test_split_slots_jobs_job_4` (job after the slot).
#[test]
pub fn test_split_slots_jobs_job_4() {
    let pc = mock_platform_config(32);
    let mut ss = SlotSet::from_slot(new_slot(&pc, 1, None, None, 10, 50, ProcSet::from_iter([1..=32])));
    let j1 = pseudo_job(1, 60, 20, ProcSet::from_iter([10..=20]));

    ss.split_slots_for_jobs_and_update_resources(&vec![&j1], false, true, None);

    let v = [(10, 50, ProcSet::from_iter([1..=32]))];
    assert!(compare_slots_val_ref(&ss, &v));
}

/// Python `_build_chain_slotset`: 7 slots with intervals (1,1), (2,4), (5,10), (11,11),
/// (12,25), (26,30), (31,31).
fn build_chain_slotset(pc: &Rc<PlatformConfig>) -> SlotSet {
    let intervals = [(1, 1), (2, 4), (5, 10), (11, 11), (12, 25), (26, 30), (31, 31)];
    let mut slots = HashMap::new();
    for (index, (begin, end)) in intervals.iter().enumerate() {
        let id = (index + 1) as i32;
        let prev = if index == 0 { None } else { Some(id - 1) };
        let next = if index + 1 == intervals.len() { None } else { Some(id + 1) };
        slots.insert(
            id,
            new_slot(pc, id, prev, next, *begin, *end, ProcSet::from_iter([1..=32])),
        );
    }
    SlotSet::from_map(Rc::clone(pc), slots, 1)
}

/// Python `test_traverse_with_width_explicit_chain`.
#[test]
pub fn test_traverse_with_width_explicit_chain() {
    let pc = mock_platform_config(32);
    let ss = build_chain_slotset(&pc);

    // Width that every slot satisfies on its own
    assert_eq!(
        traverse_with_width_ids(&ss, 1, 0, 0),
        vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7)]
    );

    // Miscellaneous widths exercising the backward-persistent end pointer
    assert_eq!(
        traverse_with_width_ids(&ss, 3, 0, 0),
        vec![(1, 2), (2, 2), (3, 3), (4, 5), (5, 5), (6, 6)]
    );
    assert_eq!(
        traverse_with_width_ids(&ss, 5, 0, 0),
        vec![(1, 3), (2, 3), (3, 3), (4, 5), (5, 5), (6, 6)]
    );
    assert_eq!(traverse_with_width_ids(&ss, 14, 0, 0), vec![(1, 5), (2, 5), (3, 5), (4, 5), (5, 5)]);

    // Width that only fits for the first start, ending at the very last slot
    assert_eq!(traverse_with_width_ids(&ss, 31, 0, 0), vec![(1, 7)]);

    // Width that never fits
    assert!(traverse_with_width_ids(&ss, 100, 0, 0).is_empty());

    // end_id bounds the traversal
    assert_eq!(traverse_with_width_ids(&ss, 14, 0, 5), vec![(1, 5), (2, 5), (3, 5), (4, 5), (5, 5)]);
    assert_eq!(traverse_with_width_ids(&ss, 20, 0, 5), vec![(1, 5), (2, 5), (3, 5)]);

    // start_id in the middle
    assert_eq!(traverse_with_width_ids(&ss, 5, 3, 0), vec![(3, 3), (4, 5), (5, 5), (6, 6)]);
    assert_eq!(traverse_with_width_ids(&ss, 14, 2, 0), vec![(2, 5), (3, 5), (4, 5), (5, 5)]);

    // start_id == end_id
    assert_eq!(traverse_with_width_ids(&ss, 1, 4, 4), vec![(4, 4)]);
    assert!(traverse_with_width_ids(&ss, 5, 4, 4).is_empty());

    // Unknown start id behaves like an empty traversal
    assert!(traverse_with_width_ids(&ss, 1, 99, 0).is_empty());
    // Divergence: Python validates `end_id` and returns an empty traversal for an unknown id.
    // The Rust `end_at` does not validate the id, so an unknown end id behaves like the end of
    // the linked list and the traversal yields every slot.
    assert_eq!(
        traverse_with_width_ids(&ss, 1, 0, 99),
        vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7)]
    );
}

/// Python `test_traverse_with_width_randomized_matches_original`.
#[test]
pub fn test_traverse_with_width_randomized_matches_original() {
    let pc = mock_platform_config(32);
    let mut rng = TestRng::new(20240607);
    for _ in 0..2000 {
        let n_slots = rng.randint(1, 12);
        let mut begin = rng.randint(0, 10) as i64;
        let mut slots = HashMap::new();
        for index in 1..=n_slots {
            let id = index as i32;
            let end = begin + rng.randint(0, 8) as i64;
            let prev = if index == 1 { None } else { Some(id - 1) };
            let next = if index == n_slots { None } else { Some(id + 1) };
            slots.insert(
                id,
                new_slot(&pc, id, prev, next, begin, end, ProcSet::from_iter([1..=32])),
            );
            begin = end + 1 + rng.randint(0, 3) as i64;
        }
        let ss = SlotSet::from_map(Rc::clone(&pc), slots, 1);
        let width = rng.randint(1, 40) as i64;
        let start_id = rng.randint(0, n_slots) as i32;
        let end_id = rng.randint(0, n_slots) as i32;

        let got = traverse_with_width_ids(&ss, width, start_id, end_id);
        let expected = reference_traverse_with_width_ids(&ss, width, start_id, end_id);
        assert_eq!(
            got, expected,
            "mismatch width={} start_id={} end_id={}",
            width, start_id, end_id
        );
    }
}
