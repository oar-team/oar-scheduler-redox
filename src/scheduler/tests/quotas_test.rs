// Rust translation of Python quotas tests from oar.kao.quotas
// For tests not possible to implement yet, the Python code is left as a comment above a blank test function.
// Helper and config functions are provided for test clarity.

use crate::benchmark::platform_mock::generate_mock_platform_config;
use crate::models::models::{Job, Moldable, ProcSet, ProcSetCoresOp, ScheduledJobData};
use crate::platform::PlatformConfig;
use crate::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use crate::scheduler::quotas::*;
use crate::scheduler::scheduling_basic;
use crate::scheduler::slot::{Slot, SlotSet};
use std::collections::HashMap;
use std::rc::Rc;
use log::LevelFilter;

fn quotas_platform_config() -> Rc<PlatformConfig> {
    // Adjust as needed for your actual config
    let platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    Rc::new(platform_config)
}

// --- TESTS ---

#[test]
fn test_quotas_rules_from_json() {
    let quotas_rules_json = r#"{"*,*,*,john": [100, "ALL", "0.5*ALL"], "*,projA,*,*": ["34", "ALL", "2*ALL"]}"#;
    let quotas = quotas_map_from_json(quotas_rules_json, 100);

    assert_eq!(quotas.len(), 2);
    assert!(quotas.contains_key(&("*".into(), "*".into(), "*".into(), "john".into())));
    assert!(quotas.contains_key(&("*".into(), "projA".into(), "*".into(), "*".into())));
    assert_eq!(
        quotas[&("*".into(), "*".into(), "*".into(), "john".into())],
        QuotasValue::new(Some(100), Some(100), Some(50))
    );
    assert_eq!(
        quotas[&("*".into(), "projA".into(), "*".into(), "*".into())],
        QuotasValue::new(Some(34), Some(100), Some(200))
    );
}

#[test]
fn test_quotas_one_job_no_rules() {
    let platform_config = quotas_platform_config();

    let available = platform_config.resource_set.default_intervals.clone();
    let slot = Slot::new(Rc::clone(&platform_config), 1, None, None, available.clone(), 0, 1000);
    let ss = SlotSet::from_slot(slot);
    let mut all_ss = HashMap::from([("default".to_string(), ss)]);

    let moldable = Moldable::new(
        60,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]),
    );

    let job = Job::new(
        1,
        String::from("user"),
        String::from("project"),
        String::from("queue"),
        vec![String::from("type1")],
        vec![moldable],
    );

    scheduling_basic::schedule_jobs(&mut all_ss, &mut vec![job]);

    let ss = all_ss.get("default").unwrap();

    let s1 = ss.slot_at(0, None).unwrap();
    let s2 = ss.slot_at(60, None).unwrap();
    assert_eq!(s1.begin(), 0);
    assert_eq!(s1.end(), 59);
    assert_eq!(s1.proc_set(), &ProcSet::from_iter([65..=256]));
    assert_eq!(s2.begin(), 60);
    assert_eq!(s2.end(), 1000);
    assert_eq!(s2.proc_set(), &available);
}

#[test]
fn test_quotas_one_job_rule_nb_res_1() {
    let mut platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    platform_config.quotas_config = QuotasConfig::new(
        true,
        None,
        HashMap::from([(("*".into(), "*".into(), "*".into(), "/".into()), QuotasValue::new(Some(63), None, None))]),
        Box::new(["*".into()]),
    );
    let platform_config = Rc::new(platform_config);

    let available = platform_config.resource_set.default_intervals.clone();
    let ss = SlotSet::from_platform(Rc::clone(&platform_config), 0, 1000);
    let mut all_ss = HashMap::from([("default".to_string(), ss)]);

    let moldable = Moldable::new(
        60,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]),
    );

    let job = Job::new(
        1,
        String::from("user"),
        String::from("project"),
        String::from("queue"),
        vec![String::from("type1")],
        vec![moldable],
    );

    let mut jobs = vec![job];
    scheduling_basic::schedule_jobs(&mut all_ss, &mut jobs);

    println!("jobs: {:?}", jobs);

    // With quota of 1, job should not get any resources
    assert!(jobs[0].scheduled_data.is_none());
}

#[test]
fn test_quotas_one_job_rule_nb_res_2() {
    let mut platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    platform_config.quotas_config = QuotasConfig::new(
        true,
        None,
        HashMap::from([(("*".into(), "*".into(), "*".into(), "/".into()), QuotasValue::new(Some(64), None, None))]),
        Box::new(["*".into()]),
    );
    let platform_config = Rc::new(platform_config);

    let available = platform_config.resource_set.default_intervals.clone();
    let ss = SlotSet::from_platform(Rc::clone(&platform_config), 0, 1000);
    let mut all_ss = HashMap::from([("default".to_string(), ss)]);

    let moldable = Moldable::new(
        60,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]),
    );

    let job = Job::new(
        2,
        String::from("user"),
        String::from("project"),
        String::from("queue"),
        vec![String::from("type1")],
        vec![moldable],
    );

    let mut jobs = vec![job];
    scheduling_basic::schedule_jobs(&mut all_ss, &mut jobs);

    // With quota of 64, job should get scheduled on 64 cores
    let scheduled = &jobs[0].scheduled_data;
    assert!(scheduled.is_some());
    let sched = scheduled.as_ref().unwrap();
    assert_eq!(sched.proc_set.core_count(), 64);
}

#[test]
fn test_quotas_four_jobs_rule_1() {
    env_logger::Builder::new().filter(None, LevelFilter::Info).init();
    // Quotas: 16 procs max, except project "yop" (max 1 running job)
    let mut platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    platform_config.quotas_config = QuotasConfig::new(
        true,
        None,
        HashMap::from([
            (("*".into(), "*".into(), "*".into(), "/".into()), QuotasValue::new(Some(16), None, None)),
            (("*".into(), "yop".into(), "*".into(), "*".into()), QuotasValue::new(None, Some(1), None)),
        ]),
        Box::new(["*".into()]),
    );
    let platform_config = Rc::new(platform_config);
    let available = platform_config.resource_set.default_intervals.clone();
    let ss = SlotSet::from_platform(Rc::clone(&platform_config), 0, 10000);
    let mut all_ss = HashMap::from([("default".to_string(), ss)]);

    // First two jobs are already scheduled
    let job1 = Job::new_scheduled(
        1,
        "toto".into(),
        "".into(),
        "default".into(),
        vec![],
        vec![],
        ScheduledJobData::new(0, 19, ProcSet::from_iter(9..=24), 0),
    );
    let job2 = Job::new_scheduled(
        2,
        "lulu".into(),
        "yop".into(),
        "default".into(),
        vec![],
        vec![],
        ScheduledJobData::new(0, 49, ProcSet::from_iter(1..=8), 0),
    );
    let jobs = vec![&job1, &job2];
    // Insert scheduled jobs into slots
    let ss = all_ss.get_mut("default").unwrap();
    ss.split_slots_for_jobs_and_update_resources(&jobs, true, true, None);

    // Now schedule two more jobs
    let moldable_j3 = Moldable::new(
        10,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("cpus".into(), 1)])]),
    );
    let j3 = Job::new(3, "toto".into(), "".into(), "default".into(), vec![], vec![moldable_j3]);
    let moldable_j4 = Moldable::new(
        60,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("cpus".into(), 1)])]),
    );
    let j4 = Job::new(4, "lulu".into(), "yop".into(), "default".into(), vec![], vec![moldable_j4]);
    let mut jobs_new = vec![j3, j4];
    scheduling_basic::schedule_jobs(&mut all_ss, &mut jobs_new);
    let j3 = &jobs_new[0];
    let j4 = &jobs_new[1];
    // Check results
    assert!(j3.scheduled_data.is_some());
    assert!(j4.scheduled_data.is_some());
    let sched3 = j3.scheduled_data.as_ref().unwrap();
    let sched4 = j4.scheduled_data.as_ref().unwrap();
    assert_eq!(sched3.begin, 20);
    assert_eq!(sched3.proc_set, ProcSet::from_iter(9..=16));
    assert_eq!(sched4.begin, 50);
    assert_eq!(sched4.proc_set, ProcSet::from_iter(1..=8));
}

#[test]
fn test_quotas_three_jobs_rule_1() {
    env_logger::Builder::new().filter(None, LevelFilter::Info).init();
    // Quotas: 8 procs max
    let mut platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    platform_config.quotas_config = QuotasConfig::new(
        true,
        None,
        HashMap::from([(("*".into(), "*".into(), "*".into(), "/".into()), QuotasValue::new(Some(8), None, None))]),
        Box::new(["*".into()]),
    );
    let platform_config = Rc::new(platform_config);
    let available = platform_config.resource_set.default_intervals.clone();
    let ss = SlotSet::from_platform(Rc::clone(&platform_config), 0, 10000);
    let mut all_ss = HashMap::from([("default".to_string(), ss)]);

    // First job is already scheduled
    let job = Job::new_scheduled(
        1,
        "toto".into(),
        "".into(),
        "default".into(),
        vec![],
        vec![],
        ScheduledJobData::new(50, 149, ProcSet::from_iter(17..=24), 0),
    );
    let jobs = vec![&job];
    let ss = all_ss.get_mut("default").unwrap();
    ss.split_slots_for_jobs_and_update_resources(&jobs, true, true, None);

    // Now schedule two more jobs
    let moldable_j2 = Moldable::new(
        200,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 1)])]),
    );
    let j2 = Job::new(2, "toto".into(), "".into(), "default".into(), vec![], vec![moldable_j2]);
    let moldable_j3 = Moldable::new(
        100,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 1)])]),
    );
    let j3 = Job::new(3, "lulu".into(), "yop".into(), "default".into(), vec![], vec![moldable_j3]);
    let mut jobs_new = vec![j2, j3];
    scheduling_basic::schedule_jobs(&mut all_ss, &mut jobs_new);
    let j2 = &jobs_new[0];
    let j3 = &jobs_new[1];
    // Check results
    assert!(j2.scheduled_data.is_some());
    assert!(j3.scheduled_data.is_some());
    let sched2 = j2.scheduled_data.as_ref().unwrap();
    let sched3 = j3.scheduled_data.as_ref().unwrap();
    assert_eq!(sched2.begin, 150);
    assert_eq!(sched2.proc_set, ProcSet::from_iter(1..=8));
    assert_eq!(sched3.begin, 0);
    assert_eq!(sched3.proc_set, ProcSet::from_iter(1..=8));
}

#[test]
fn test_quotas_two_job_rules_nb_res_quotas_file() {
    /*
    Python:
    ...
    Quotas.enable(config)
    ...
    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)
    assert j1.res_set == ProcSet()
    assert j2.res_set == ProcSet(*[(1, 16)])
    */
    // Not yet implemented: quotas file config and enforcement
}

#[test]
fn test_quotas_two_jobs_job_type_proc() {
    /*
    Python:
    ...
    Quotas.enable(config)
    ...
    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)
    assert j1.start_time == 0
    assert j2.start_time == 50
    */
    // Not yet implemented: quotas job type enforcement
}
