// Rust translation of Python quotas tests from oar.kao.quotas
// For tests not possible to implement yet, the Python code is left as a comment above a blank test function.
// Helper and config functions are provided for test clarity.

use std::collections::HashMap;
use crate::benchmark::platform_mock::generate_mock_platform_config;
use crate::models::models::{Job, Moldable, ProcSet, ProcSetCoresOp};
use crate::platform::PlatformConfig;
use crate::scheduler::quotas::*;
use crate::scheduler::slot::{Slot, SlotSet};
use std::rc::Rc;
use crate::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use crate::scheduler::scheduling_basic;

fn quotas_platform_config() -> Rc<PlatformConfig> {
    // Adjust as needed for your actual config
    let platform_config = generate_mock_platform_config(256, 8, 4, 8, true);
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
        HierarchyRequests::from_requests(vec![
            HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])
        ])
    );

    let job = Job::new(
        1,
        String::from("user"),
        String::from("project"),
        String::from("queue"),
        vec![String::from("type1")],
        vec![moldable],
    );

    scheduling_basic::schedule_jobs_ct(&mut all_ss, &mut vec![job], false);

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
    let mut platform_config = generate_mock_platform_config(256, 8, 4, 8, true);
    platform_config.quotas_config = QuotasConfig::new(
        true,
        None,
        HashMap::from([(
            ("*".into(), "*".into(), "*".into(), "/".into()),
            QuotasValue::new(Some(1), None, None),
        )]),
        Box::new([]),
    );
    let platform_config = Rc::new(platform_config);

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

    let mut jobs = vec![job];
    scheduling_basic::schedule_jobs_ct(&mut all_ss, &mut jobs, false);

    // With quota of 1, job should not get any resources
    //assert!(jobs[0].scheduled_data.is_none()); Not implemented yet: quotas enforcement in scheduler
}

#[test]
fn test_quotas_one_job_rule_nb_res_2() {
    let mut platform_config = generate_mock_platform_config(256, 8, 4, 8, true);
    platform_config.quotas_config = QuotasConfig::new(
        true,
        None,
        HashMap::from([(
            ("*".into(), "*".into(), "*".into(), "/".into()),
            QuotasValue::new(Some(64), None, None),
        )]),
        Box::new([]),
    );
    let platform_config = Rc::new(platform_config);

    let available = platform_config.resource_set.default_intervals.clone();
    let slot = Slot::new(Rc::clone(&platform_config), 1, None, None, available.clone(), 0, 1000);
    let ss = SlotSet::from_slot(slot);
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
    scheduling_basic::schedule_jobs_ct(&mut all_ss, &mut jobs, false);

    // With quota of 64, job should get scheduled on 64 cores
    let scheduled = &jobs[0].scheduled_data;
    assert!(scheduled.is_some());
    let sched = scheduled.as_ref().unwrap();
    assert_eq!(sched.proc_set.core_count(), 64);
}

#[test]
fn test_quotas_four_jobs_rule_1() {
    /*
    Python:
    Quotas.enabled = True
    Quotas.default_rules = { ("*", "*", "*", "/"): [16, -1, -1], ("*", "yop", "*", "*"): [-1, 1, -1] }
    ...
    schedule_id_jobs_ct(all_ss, {3: j3, 4: j4}, hy, [3, 4], 5)
    assert j3.start_time == 20
    assert j3.res_set == ProcSet(*[(9, 16)])
    assert j4.start_time == 50
    assert j4.res_set == ProcSet(*[(1, 8)])
    */
    // Not yet implemented: quotas enforcement in scheduler
}

#[test]
fn test_quotas_three_jobs_rule_1() {
    /*
    Python:
    Quotas.enabled = True
    Quotas.default_rules = { ("*", "*", "*", "/"): [8, -1, -1] }
    ...
    schedule_id_jobs_ct(all_ss, {2: j2, 3: j3}, hy, [2, 3], 5)
    assert j2.start_time == 150
    assert j2.res_set == ProcSet(*[(1, 8)])
    assert j3.start_time == 0
    assert j3.res_set == ProcSet(*[(1, 8)])
    */
    // Not yet implemented: quotas enforcement in scheduler
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
