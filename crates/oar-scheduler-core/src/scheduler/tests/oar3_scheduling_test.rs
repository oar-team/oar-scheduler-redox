//! Port of the OAR3 Python scheduling tests.
//!
//! Each test is a port of a test from `oar3/tests/kao/test_scheduling.py` (and
//! `test_scheduling_basic.py`). The Python implementation of the scheduler
//! (`oar/kao/scheduling.py`) works on `JobPseudo` objects and on a dictionary of
//! `SlotSet`s; the Rust implementation works on `Job` with an assignment and on a
//! `HashMap<Box<str>, SlotSet>`.
//!
//! Known and intentional divergences with Python, documented per test:
//!   - Resource placement: when several allocations match a request, the Rust
//!     implementation keeps the *lowest* resource ids (first fit) whereas OAR3
//!     keeps the highest ones (e.g. `[1..=16]` instead of `[17..=32]` for a two
//!     nodes request on a 32 cores platform). The expected slots are therefore
//!     mirrored compared to the Python assertions.
//!   - Python has no `assignment` end sentinel of `-1` for unscheduled jobs: a
//!     job that could not be scheduled has no assignment at all in Rust.

use crate::model::job::{Job, JobAssignment, JobBuilder, Moldable, PlaceholderType, ProcSet, TimeSharingType};
use crate::platform::PlatformConfig;
use crate::scheduler::hierarchy::{Hierarchy, HierarchyRequest, HierarchyRequests};
use crate::scheduler::scheduling;
use crate::scheduler::slot::Slot;
use crate::scheduler::slotset::SlotSet;
use crate::scheduler::tests::platform_mock::generate_mock_platform_config;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::rc::Rc;

/// Python `oar.kao.slot.MAX_TIME`.
const MAX_TIME: i64 = 2_147_483_648;

fn procsets(ranges: Box<[RangeInclusive<u32>]>) -> Box<[ProcSet]> {
    ranges.into_iter().map(|r| ProcSet::from_iter(r)).collect()
}

fn available() -> ProcSet {
    ProcSet::from_iter([1..=32])
}

/// Builds a mock platform configuration with 32 cores, 4 nodes of 8 cores and 8
/// cpus of 4 cores, and the `node` hierarchy used by the Python tests.
fn pc_32() -> Rc<PlatformConfig> {
    let mut pc = generate_mock_platform_config(false, 32, 8, 4, 2, false);
    pc.resource_set.hierarchy = Hierarchy::new()
        .add_partition("node".into(), procsets([1..=8, 9..=16, 17..=24, 25..=32].into()))
        .add_partition("switch".into(), procsets([1..=16, 17..=32].into()));
    Rc::new(pc)
}

/// Platform configuration with the `node`, `switch` and `gpu` partitions used by
/// `test_assign_resources_mld_job_split_slots_2` and `_3` in Python.
fn pc_32_with_gpu() -> Rc<PlatformConfig> {
    let mut pc = generate_mock_platform_config(false, 32, 8, 4, 2, false);
    pc.resource_set.hierarchy = Hierarchy::new()
        .add_partition("node".into(), procsets([1..=8, 9..=16, 17..=24, 25..=32].into()))
        .add_partition("switch".into(), procsets([1..=16, 17..=21].into()))
        .add_partition("gpu".into(), procsets([22..=32].into()));
    Rc::new(pc)
}

/// Builds a `HierarchyRequests` from `(level name, number of resources)` pairs,
/// constrained to the whole platform.
fn requests(levels: &[(&str, u32)]) -> HierarchyRequests {
    HierarchyRequests::from_requests(vec![HierarchyRequest::new(
        available(),
        levels.iter().map(|(name, n)| (Box::from(*name), *n)).collect(),
    )])
}

/// Builds a moldable whose requests are constrained to the whole platform.
fn moldable(id: i64, walltime: i64, levels: &[(&str, u32)]) -> Moldable {
    Moldable::new(id, walltime, requests(levels))
}

/// Builds a `Job` with a single moldable, as Python's `JobPseudo(mld_res_rqts=[...])`.
fn job(id: i64, walltime: i64, levels: &[(&str, u32)]) -> Job {
    JobBuilder::new(id).moldable(moldable(id, walltime, levels)).build()
}

/// A job used to represent a job that was already scheduled in a previous round
/// (Python's `JobPseudo(start_time=..., walltime=..., res_set=...)`).
fn pre_scheduled_job(id: i64, start_time: i64, walltime: i64, res_set: ProcSet) -> JobBuilder {
    JobBuilder::new(id).assign(JobAssignment::new(start_time, start_time + walltime - 1, res_set, 0))
}

fn slot_set(pc: &Rc<PlatformConfig>, begin: i64, end: i64) -> SlotSet {
    SlotSet::from_platform_config(Rc::clone(pc), begin, end)
}

fn schedule(all_ss: &mut HashMap<Box<str>, SlotSet>, jobs: Vec<Job>) -> IndexMap<i64, Job> {
    let mut waiting: IndexMap<i64, Job> = jobs.into_iter().map(|j| (j.id, j)).collect();
    scheduling::schedule_jobs(all_ss, &mut waiting);
    waiting
}

fn slots(ss: &SlotSet) -> Vec<(i64, i64, ProcSet)> {
    ss.iter().map(|s| (s.begin(), s.end(), s.proc_set().clone())).collect()
}

/// Python's `compare_slots_val_ref`: compares the whole slot chain, in order.
fn assert_slots(ss: &SlotSet, expected: &[(i64, i64, ProcSet)]) {
    assert_eq!(slots(ss), expected);
}

fn assignment(jobs: &IndexMap<i64, Job>, id: i64) -> JobAssignment {
    jobs.get(&id).and_then(|j| j.assignment.clone()).unwrap_or_else(|| panic!("job {} was not scheduled", id))
}

/// Python's `set_slots_with_prev_scheduled_jobs`: adds the resources of the
/// already scheduled jobs to the slot sets (`sub=false` for container jobs, which
/// *reserve* resources in their own slot set), then removes them from the slot set
/// they run in.
fn set_slots_with_prev_scheduled_jobs(all_ss: &mut HashMap<Box<str>, SlotSet>, jobs: &[Job]) {
    let pc = all_ss.get(&Box::from("default")).unwrap().get_platform_config().clone();
    let mut jobs_slotsets: HashMap<Box<str>, Vec<Job>> = HashMap::new();

    for job in jobs {
        if job.types.contains_key(&Box::from("container")) {
            let ss_name: Box<str> = job
                .types
                .get(&Box::from("container"))
                .and_then(|name| name.clone())
                .unwrap_or_else(|| Box::from(job.id.to_string()));
            if !all_ss.contains_key(&ss_name) {
                let default_ss = all_ss.get(&Box::from("default")).unwrap();
                let slot = Slot::new(Rc::clone(&pc), 1, None, None, default_ss.begin(), default_ss.end(), ProcSet::new(), None);
                all_ss.insert(ss_name.clone(), SlotSet::from_slot(slot));
            }
            // Add the container's resources to its own slot set.
            all_ss.get_mut(&ss_name).unwrap().split_slots_for_jobs_and_update_resources(&vec![job], false, false, None);
        }
        jobs_slotsets.entry(job.slot_set_name()).or_default().push(job.clone());
    }

    for (ss_name, jobs) in jobs_slotsets {
        if let Some(slot_set) = all_ss.get_mut(&ss_name) {
            slot_set.split_slots_for_jobs_and_update_resources(&jobs.iter().collect(), false, true, None);
        }
    }
}

// ---------------------------------------------------------------------------
// test_scheduling_basic.py / test_scheduling.py: assign resources to a moldable
// ---------------------------------------------------------------------------

#[test]
fn test_assign_resources_mld_job_split_slots_1() {
    // Python expects [(0, 59, 17..=32), (60, 100, 1..=32)]: the Rust scheduler
    // takes the first fitting resources (1..=16) instead of the last ones.
    let pc = pc_32();
    let mut ss = slot_set(&pc, 0, 100);
    let mut job = job(1, 60, &[("node", 2)]);
    scheduling::schedule_job(&mut ss, &mut job, None);

    let a = job.assignment.expect("the job should be scheduled");
    assert_eq!((a.begin, a.end, a.resources), (0, 59, ProcSet::from_iter([1..=16])));
    assert_slots(
        &ss,
        &[(0, 59, ProcSet::from_iter([17..=32])), (60, 100, available())],
    );
}

#[test]
fn test_assign_resources_mld_job_split_slots_2() {
    // The job requests 2 nodes and 1 gpu. Python schedules it with the 17..=21
    // allocation (which does not even satisfy the request), the Rust
    // implementation does not find any valid allocation and leaves the slot set
    // untouched. Divergence documented here: this Python expectation is
    // considered dubious (17..=21 is a fifth of the platform, not 2 nodes).
    let pc = pc_32_with_gpu();
    let mut ss = slot_set(&pc, 0, 100);
    let mut requested = JobBuilder::new(1)
        .moldable(Moldable::new(1, 60, requests(&[("node", 2), ("gpu", 1)])))
        .build();
    scheduling::schedule_job(&mut ss, &mut requested, None);

    assert!(requested.assignment.is_none(), "the Rust implementation finds no allocation for this request");
    assert_slots(&ss, &[(0, 100, available())]);
}

#[test]
fn test_assign_resources_mld_job_split_slots_3() {
    // Job which cannot be satisfied: only one gpu is available while the job
    // requests two of them. The slot set must be left unchanged.
    let pc = pc_32_with_gpu();
    let mut ss = slot_set(&pc, 0, 100);
    let mut requested = JobBuilder::new(1)
        .moldable(Moldable::new(1, 60, requests(&[("node", 2), ("gpu", 2)])))
        .build();
    scheduling::schedule_job(&mut ss, &mut requested, None);

    assert!(requested.assignment.is_none());
    assert_slots(&ss, &[(0, 100, available())]);
}

#[test]
fn test_schedule_id_jobs_ct_1() {
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 100))]);
    let jobs = schedule(&mut all_ss, vec![job(1, 60, &[("node", 2)])]);

    // Python expects 17..=32 (last fit), Rust gives 1..=16 (first fit).
    let a = assignment(&jobs, 1);
    assert_eq!((a.begin, a.end, a.resources.clone()), (0, 59, ProcSet::from_iter([1..=16])));
    assert_slots(
        all_ss.get("default").unwrap(),
        &[(0, 59, ProcSet::from_iter([17..=32])), (60, 100, available())],
    );
}

// ---------------------------------------------------------------------------
// Containers
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_container1() {
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 100))]);

    let container = JobBuilder::new(1).add_type_key("container".into()).moldable(moldable(1, 80, &[("node", 2)])).build();
    let inner = JobBuilder::new(2).add_type("inner".into(), "1".into()).moldable(moldable(1, 30, &[("node", 1)])).build();

    let jobs = schedule(&mut all_ss, vec![container, inner]);

    // Python expects the inner job to get 1..=8.
    assert_eq!(assignment(&jobs, 2).resources, ProcSet::from_iter([1..=8]));
}

#[test]
fn test_schedule_container_error1() {
    // The inner job's resources constraint (17..=32) does not intersect the
    // container's resources: it cannot be scheduled.
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 100))]);

    let container = JobBuilder::new(1).add_type_key("container".into()).moldable(moldable(1, 60, &[("node", 2)])).build();
    let inner = JobBuilder::new(2)
        .add_type("inner".into(), "1".into())
        .moldable(Moldable::new(
            1,
            30,
            HierarchyRequests::from_requests(vec![HierarchyRequest::new(ProcSet::from_iter([17..=32]), vec![("node".into(), 1)])]),
        ))
        .build();

    let jobs = schedule(&mut all_ss, vec![container, inner]);

    // Python asserts `j2.start_time == -1`.
    assert!(jobs.get(&2).unwrap().assignment.is_none());
}

#[test]
fn test_schedule_container_error2() {
    // The inner job requests more resources than the container owns.
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 100))]);

    let container = JobBuilder::new(1).add_type_key("container".into()).moldable(moldable(1, 60, &[("node", 2)])).build();
    let inner = JobBuilder::new(2).add_type("inner".into(), "1".into()).moldable(moldable(1, 20, &[("node", 3)])).build();

    let jobs = schedule(&mut all_ss, vec![container, inner]);

    assert!(jobs.get(&2).unwrap().assignment.is_none());
}

#[test]
fn test_schedule_container_error3() {
    // The inner job's walltime exceeds the container's walltime.
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 100))]);

    let container = JobBuilder::new(1).add_type_key("container".into()).moldable(moldable(1, 60, &[("node", 2)])).build();
    let inner = JobBuilder::new(2).add_type("inner".into(), "1".into()).moldable(moldable(1, 70, &[("node", 1)])).build();

    let jobs = schedule(&mut all_ss, vec![container, inner]);

    assert!(jobs.get(&2).unwrap().assignment.is_none());
}

#[test]
fn test_schedule_container_recursif() {
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 100))]);

    let container = JobBuilder::new(1).add_type_key("container".into()).moldable(moldable(1, 80, &[("node", 2)])).build();
    let container_inner = JobBuilder::new(2)
        .add_type_key("container".into())
        .add_type("inner".into(), "1".into())
        .moldable(moldable(1, 50, &[("node", 2)]))
        .build();
    let inner = JobBuilder::new(3).add_type("inner".into(), "2".into()).moldable(moldable(1, 30, &[("node", 1)])).build();

    let jobs = schedule(&mut all_ss, vec![container, container_inner, inner]);

    // Python expects the inner job to get 1..=8.
    assert_eq!(assignment(&jobs, 3).resources, ProcSet::from_iter([1..=8]));
}

// ---------------------------------------------------------------------------
// Dependencies
// ---------------------------------------------------------------------------

#[test]
fn test_simple_dependency() {
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 1000))]);

    let first = job(1, 60, &[("node", 2)]);
    let second = JobBuilder::new(2)
        .add_dependency(1, "Waiting".into(), Some(0))
        .moldable(moldable(1, 80, &[("node", 2)]))
        .build();

    let jobs = schedule(&mut all_ss, vec![first, second]);

    assert_eq!(assignment(&jobs, 2).begin, 60);
}

#[test]
fn test_error_dependency() {
    // A dependency in error state is ignored: the job is scheduled at time 0.
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 1000))]);

    let first = job(1, 60, &[("node", 2)]);
    let second = JobBuilder::new(2)
        .add_dependency(1, "Error".into(), Some(0))
        .moldable(moldable(1, 80, &[("node", 2)]))
        .build();

    let jobs = schedule(&mut all_ss, vec![first, second]);

    assert_eq!(assignment(&jobs, 2).begin, 0);
}

#[test]
fn test_terminated_dependency() {
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 1000))]);

    let job = JobBuilder::new(2)
        .add_dependency(1, "Terminated".into(), Some(0))
        .moldable(moldable(1, 80, &[("node", 2)]))
        .build();

    let jobs = schedule(&mut all_ss, vec![job]);

    assert_eq!(assignment(&jobs, 2).begin, 0);
}

// ---------------------------------------------------------------------------
// Placeholders
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_placeholder1() {
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 1000))]);

    let placeholder = JobBuilder::new(1)
        .placeholder(PlaceholderType::Placeholder("yop".into()))
        .moldable(moldable(1, 80, &[("node", 4)]))
        .build();
    let regular = job(2, 50, &[("node", 4)]);
    let allow = JobBuilder::new(3)
        .placeholder(PlaceholderType::Allow("yop".into()))
        .moldable(moldable(1, 60, &[("node", 4)]))
        .build();

    let jobs = schedule(&mut all_ss, vec![placeholder, regular, allow]);

    assert_eq!(assignment(&jobs, 2).begin, 80);
    assert_eq!(assignment(&jobs, 3).begin, 0);
}

#[test]
fn test_schedule_placeholder2() {
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 1000))]);

    let first = job(1, 60, &[("node", 2)]);
    let allow = JobBuilder::new(2)
        .add_dependency(1, "Waiting".into(), Some(0))
        .placeholder(PlaceholderType::Allow("yop".into()))
        .moldable(moldable(1, 80, &[("node", 2)]))
        .build();

    let jobs = schedule(&mut all_ss, vec![first, allow]);

    assert_eq!(assignment(&jobs, 2).begin, 60);
}

// ---------------------------------------------------------------------------
// Time sharing
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_timesharing1() {
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 1000))]);

    let first = JobBuilder::new(1)
        .user("toto".into())
        .name("yop".into())
        .time_sharing(TimeSharingType::AllAll)
        .moldable(moldable(1, 60, &[("node", 4)]))
        .build();
    let second = JobBuilder::new(2)
        .user("toto".into())
        .name("yop".into())
        .time_sharing(TimeSharingType::AllAll)
        .moldable(moldable(1, 80, &[("node", 4)]))
        .build();

    let jobs = schedule(&mut all_ss, vec![first, second]);

    assert_eq!(assignment(&jobs, 1).begin, assignment(&jobs, 2).begin);
}

// ---------------------------------------------------------------------------
// Already scheduled jobs (`set_slots_with_prev_scheduled_jobs`)
// ---------------------------------------------------------------------------

#[test]
fn test_set_slots_with_prev_scheduled_jobs_1() {
    // Two already scheduled jobs are removed from the slots (this is what the
    // meta-scheduler does with the running jobs at the beginning of a round).
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), SlotSet::from_slot(Slot::new(Rc::clone(&pc), 1, None, None, 1, 100, available(), None)))]);

    let j1 = pre_scheduled_job(1, 5, 10, ProcSet::from_iter([10..=20])).build();
    let j2 = pre_scheduled_job(2, 30, 20, ProcSet::from_iter([5..=15, 20..=28])).build();
    set_slots_with_prev_scheduled_jobs(&mut all_ss, &[j1, j2]);

    assert_slots(
        all_ss.get("default").unwrap(),
        &[
            (1, 4, available()),
            (5, 14, ProcSet::from_iter([1..=9, 21..=32])),
            (15, 29, available()),
            (30, 49, ProcSet::from_iter([1..=4, 16..=19, 29..=32])),
            (50, 100, available()),
        ],
    );
}

#[test]
fn test_schedule_error_1() {
    // A job already occupies 1..=8 from 0 to 29: the next job requests 2 nodes
    // and gets 9..=24 in Python (17..=32 in Rust, because of the first-fit
    // resource placement).
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 10_000))]);

    let previous = pre_scheduled_job(2, 0, 30, ProcSet::from_iter([1..=8])).build();
    set_slots_with_prev_scheduled_jobs(&mut all_ss, &[previous]);

    let jobs = schedule(&mut all_ss, vec![job(4, 60, &[("node", 2)])]);

    assert_eq!(assignment(&jobs, 4).resources, ProcSet::from_iter([9..=24]));
}

#[test]
fn test_schedule_container_prev_sched() {
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 1000))]);

    let container = pre_scheduled_job(1, 200, 150, ProcSet::from_iter([7..=27]))
        .add_type_key("container".into())
        .build();
    let inner_prev = pre_scheduled_job(2, 210, 70, ProcSet::from_iter([9..=16]))
        .add_type("inner".into(), "1".into())
        .build();
    set_slots_with_prev_scheduled_jobs(&mut all_ss, &[container, inner_prev]);

    let inner = JobBuilder::new(3).add_type("inner".into(), "1".into()).moldable(moldable(1, 30, &[("node", 1)])).build();
    let jobs = schedule(&mut all_ss, vec![inner]);

    let a = assignment(&jobs, 3);
    assert_eq!(a.begin, 200);
    assert_eq!(a.resources, ProcSet::from_iter([17..=24]));
}

#[test]
fn test_schedule_container_prev_sched_recursif() {
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 1000))]);

    let container = pre_scheduled_job(1, 200, 150, ProcSet::from_iter([7..=27]))
        .add_type_key("container".into())
        .build();
    let container_inner = pre_scheduled_job(2, 210, 70, ProcSet::from_iter([15..=25]))
        .add_type_key("container".into())
        .add_type("inner".into(), "1".into())
        .build();
    set_slots_with_prev_scheduled_jobs(&mut all_ss, &[container, container_inner]);

    let inner = JobBuilder::new(3).add_type("inner".into(), "2".into()).moldable(moldable(1, 30, &[("node", 1)])).build();
    let jobs = schedule(&mut all_ss, vec![inner]);

    let a = assignment(&jobs, 3);
    assert_eq!(a.begin, 210);
    assert_eq!(a.resources, ProcSet::from_iter([17..=24]));
}

#[test]
fn test_schedule_w_temporally_fragmented_container() {
    // The "yop" container is fragmented in two intervals: the inner job must be
    // scheduled in the second one, which is big enough.
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 5000))]);

    let container1 = pre_scheduled_job(1, 200, 50, ProcSet::from_iter([7..=32]))
        .add_type("container".into(), "yop".into())
        .build();
    let container2 = pre_scheduled_job(2, 1000, 200, ProcSet::from_iter([15..=25]))
        .add_type("container".into(), "yop".into())
        .build();
    set_slots_with_prev_scheduled_jobs(&mut all_ss, &[container1, container2]);

    let inner = JobBuilder::new(3).add_type("inner".into(), "yop".into()).moldable(moldable(1, 100, &[("node", 1)])).build();
    let jobs = schedule(&mut all_ss, vec![inner]);

    let a = assignment(&jobs, 3);
    assert_eq!(a.begin, 1000);
    assert_eq!(a.resources, ProcSet::from_iter([17..=24]));
}

#[test]
fn test_schedule_placeholder_prev_sched() {
    let pc = pc_32();
    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 1000))]);

    let placeholder = pre_scheduled_job(1, 200, 150, ProcSet::from_iter([7..=27]))
        .placeholder(PlaceholderType::Placeholder("yop".into()))
        .build();
    set_slots_with_prev_scheduled_jobs(&mut all_ss, &[placeholder]);

    let regular = job(2, 150, &[("node", 4)]);
    let allow = JobBuilder::new(3)
        .placeholder(PlaceholderType::Allow("yop".into()))
        .moldable(moldable(1, 500, &[("node", 2)]))
        .build();
    let jobs = schedule(&mut all_ss, vec![regular, allow]);

    assert_eq!(assignment(&jobs, 3).resources, ProcSet::from_iter([1..=16]));
}

#[test]
fn test_schedule_error_2() {
    // A job occupies 1..=8 from 10 to 609 on a 16 cores platform: the two jobs
    // requesting 4 nodes can only start after it.
    let mut pc = generate_mock_platform_config(false, 16, 8, 1, 2, false);
    pc.resource_set.hierarchy = Hierarchy::new().add_partition(
        "node".into(),
        procsets([1..=2, 3..=4, 5..=6, 7..=8, 9..=10, 11..=12, 13..=14, 15..=16].into()),
    );
    let pc = Rc::new(pc);

    let mut all_ss = HashMap::from([("default".into(), slot_set(&pc, 0, 1000))]);

    let previous = pre_scheduled_job(1, 10, 600, ProcSet::from_iter([1..=8])).build();
    set_slots_with_prev_scheduled_jobs(&mut all_ss, &[previous]);

    let available = ProcSet::from_iter([1..=16]);
    let node_requests = |id: i64| {
        JobBuilder::new(id)
            .moldable(Moldable::new(
                1,
                60,
                HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("node".into(), 4)])]),
            ))
            .build()
    };
    let jobs = schedule(&mut all_ss, vec![node_requests(5), node_requests(6)]);

    // The nodes 5 to 8 (9..=16) are free from time 0: both jobs are scheduled
    // there, one after the other (the nodes 1 to 4 stay busy until 609).
    let j5 = assignment(&jobs, 5);
    let j6 = assignment(&jobs, 6);
    assert_eq!((j5.begin, j5.end, j5.resources), (0, 59, ProcSet::from_iter([9..=16])));
    assert_eq!((j6.begin, j6.end, j6.resources), (60, 119, ProcSet::from_iter([9..=16])));
}
