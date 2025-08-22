use crate::models::{JobAssignment, JobBuilder, Moldable, ProcSet, ProcSetCoresOp};
use crate::platform::PlatformConfig;
use crate::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use crate::scheduler::quotas::*;
use crate::scheduler::scheduling;
use crate::scheduler::slot::{Slot, SlotSet};
use crate::scheduler::tests::platform_mock::generate_mock_platform_config;
use indexmap::indexmap;
use std::collections::HashMap;
use std::rc::Rc;
use crate::scheduler::calendar::QuotasConfig;

fn quotas_platform_config() -> Rc<PlatformConfig> {
    let platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    Rc::new(platform_config)
}

#[test]
fn test_quotas_rules_from_json() {
    let quotas_rules_json = r#"{
            "quotas": {
                "*,*,*,john": [100, "ALL", "0.5*ALL"],
                "*,projA,*,*": ["34", "ALL", "2*ALL"]
            }
        }"#.to_string();

    let quotas = QuotasConfig::load_from_json(quotas_rules_json, true, 100).default_rules;

    assert_eq!(quotas.len(), 2);
    assert!(quotas.contains_key(&("*".into(), "*".into(), "*".into(), "john".into())));
    assert!(quotas.contains_key(&("*".into(), "projA".into(), "*".into(), "*".into())));
    assert_eq!(
        quotas[&("*".into(), "*".into(), "*".into(), "john".into())],
        QuotasValue::new(Some(100), Some(100), Some(50*3600))
    );
    assert_eq!(
        quotas[&("*".into(), "projA".into(), "*".into(), "*".into())],
        QuotasValue::new(Some(34), Some(100), Some(200*3600))
    );
}

#[test]
fn test_quotas_one_job_no_rules() {
    let platform_config = quotas_platform_config();

    let available = platform_config.resource_set.default_intervals.clone();
    let slot = Slot::new(Rc::clone(&platform_config), 1, None, None, 0, 1000, available.clone(), None);
    let ss = SlotSet::from_slot(slot);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    let moldable = Moldable::new(
        0,
        60,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]),
    );

    let job = JobBuilder::new(1)
        .user("user".into())
        .project("project".into())
        .queue("queue".into())
        .add_type_key("type1".into())
        .moldable(moldable)
        .build();

    let jobs = &mut indexmap![1 => job];
    scheduling::schedule_jobs(&mut all_ss, jobs);

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
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    let moldable = Moldable::new(
        1,
        60,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]),
    );

    let job = JobBuilder::new(1)
        .user("user".into())
        .project("project".into())
        .queue("queue".into())
        .add_type_key("type1".into())
        .moldable(moldable)
        .build();

    let mut jobs = indexmap![1 => job];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs);

    println!("jobs: {:?}", jobs);

    // With quota of 1, job should not get any resources
    assert!(jobs[0].assignment.is_none());
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
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    let moldable = Moldable::new(
        2,
        60,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("nodes".into(), 2)])]),
    );

    let job = JobBuilder::new(2)
        .user("user".into())
        .project("project".into())
        .queue("queue".into())
        .add_type_key("type1".into())
        .moldable(moldable)
        .build();

    let mut jobs = indexmap![2 => job];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs);

    // With a quota of 64, the job should get scheduled on 64 cores
    let scheduled = &jobs[0].assignment;
    assert!(scheduled.is_some());
    let sched = scheduled.as_ref().unwrap();
    assert_eq!(sched.proc_set.core_count(), 64);
}

#[test]
fn test_quotas_four_jobs_rule_1() {
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
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 10000);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    // The first two jobs are already scheduled
    let job1 = JobBuilder::new(1)
        .user("toto".into())
        .queue("default".into())
        .assign(JobAssignment::new(0, 19, ProcSet::from_iter(9..=24), 0))
        .build();
    let job2 = JobBuilder::new(2)
        .user("lulu".into())
        .project("yop".into())
        .queue("default".into())
        .assign(JobAssignment::new(0, 49, ProcSet::from_iter(1..=8), 0))
        .build();
    let jobs = vec![&job1, &job2];
    // Insert scheduled jobs into slots
    let ss = all_ss.get_mut("default").unwrap();
    ss.split_slots_for_jobs_and_update_resources(&jobs, true, true, None);

    // Now schedule two more jobs
    let moldable_j3 = Moldable::new(
        3,
        10,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("cpus".into(), 1)])]),
    );
    let j3 = JobBuilder::new(3).user("toto".into()).queue("default".into()).moldable(moldable_j3).build();
    let moldable_j4 = Moldable::new(
        4,
        60,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("cpus".into(), 1)])]),
    );
    let j4 = JobBuilder::new(4)
        .user("lulu".into())
        .project("yop".into())
        .queue("default".into())
        .moldable(moldable_j4)
        .build();
    let mut jobs_new = indexmap![3 => j3, 4 => j4];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs_new);
    let j3 = &jobs_new[0];
    let j4 = &jobs_new[1];
    // Check results
    assert!(j3.assignment.is_some());
    assert!(j4.assignment.is_some());
    let sched3 = j3.assignment.as_ref().unwrap();
    let sched4 = j4.assignment.as_ref().unwrap();
    assert_eq!(sched3.begin, 20);
    assert_eq!(sched3.proc_set, ProcSet::from_iter(9..=16));
    assert_eq!(sched4.begin, 50);
    assert_eq!(sched4.proc_set, ProcSet::from_iter(1..=8));
}

#[test]
fn test_quotas_three_jobs_rule_1() {
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
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 10000);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    // First job is already scheduled
    let job = JobBuilder::new(1)
        .user("toto".into())
        .queue("default".into())
        .assign(JobAssignment::new(50, 149, ProcSet::from_iter(17..=24), 0))
        .build();
    let jobs = vec![&job];
    let ss = all_ss.get_mut("default").unwrap();
    ss.split_slots_for_jobs_and_update_resources(&jobs, true, true, None);

    // Now schedule two more jobs
    let moldable_j2 = Moldable::new(
        5,
        200,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("cpus".into(), 1)])]),
    );
    let j2 = JobBuilder::new(2).user("toto".into()).queue("default".into()).moldable(moldable_j2).build();
    let moldable_j3 = Moldable::new(
        6,
        100,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(available.clone(), vec![("cpus".into(), 1)])]),
    );
    let j3 = JobBuilder::new(3)
        .user("lulu".into())
        .project("yop".into())
        .queue("default".into())
        .moldable(moldable_j3)
        .build();
    let mut jobs_new = indexmap![2 => j2, 3 => j3];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs_new);
    let j2 = &jobs_new[0];
    let j3 = &jobs_new[1];
    // Check results
    assert!(j2.assignment.is_some());
    assert!(j3.assignment.is_some());
    let sched2 = j2.assignment.as_ref().unwrap();
    let sched3 = j3.assignment.as_ref().unwrap();
    assert_eq!(sched2.begin, 150);
    assert_eq!(sched2.proc_set, ProcSet::from_iter(1..=8));
    assert_eq!(sched3.begin, 0);
    assert_eq!(sched3.proc_set, ProcSet::from_iter(1..=8));
}

#[test]
fn test_quotas_two_job_rules_nb_res_quotas_file() {
    // Match python: quotas for toto (1 proc), john (150 procs), others unlimited
    let quotas_config = QuotasConfig::new(
        true,
        None,
        HashMap::from([
            (("*".into(), "*".into(), "*".into(), "toto".into()), QuotasValue::new(Some(1), None, None)),
            (("*".into(), "*".into(), "*".into(), "john".into()), QuotasValue::new(Some(150), None, None)),
        ]),
        Box::new(["*".into()]),
    );
    let mut platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    platform_config.quotas_config = quotas_config;
    let res = platform_config.resource_set.default_intervals.clone();
    let platform_config = Rc::new(platform_config);

    // SlotSet with a single slot [0,100] with all procs
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 100);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    // Job 1: user toto, requests 2 nodes (should be denied, only 1 proc allowed)
    let moldable_j1 = Moldable::new(7, 60, HierarchyRequests::from_requests(vec![HierarchyRequest::new(res.clone(), vec![("cpus".into(), 2)])]));
    let j1 = JobBuilder::new(1).user("toto".into()).queue("default".into()).moldable(moldable_j1).build();
    // Job 2: user tutu, requests 2 nodes (should succeed, unlimited for others)
    let moldable_j2 = Moldable::new(8, 60, HierarchyRequests::from_requests(vec![HierarchyRequest::new(res.clone(), vec![("cpus".into(), 2)])]));
    let j2 = JobBuilder::new(2).user("tutu".into()).queue("default".into()).moldable(moldable_j2).build();
    let mut jobs = indexmap![1 => j1, 2 => j2];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs);
    let j1 = &jobs[0];
    let j2 = &jobs[1];
    // Check results
    assert!(j1.assignment.is_none(), "j1 should not be scheduled due to quotas");
    assert!(j2.assignment.is_some(), "j2 should be scheduled");
    let sched2 = j2.assignment.as_ref().unwrap();
    assert_eq!(sched2.proc_set, ProcSet::from_iter(1..=16));
}

#[test]
fn test_quotas_two_jobs_job_type_proc() {
    // Match python: quotas for job_type yop (max 1 running job), tracked job_types ["yop"]
    let quotas_config = QuotasConfig::new(
        true,
        None,
        HashMap::from([
            (("*".into(), "*".into(), "yop".into(), "*".into()), QuotasValue::new(None, Some(1), None)),
        ]),
        Box::new(["yop".into()]),
    );
    let mut platform_config = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    platform_config.quotas_config = quotas_config;
    let res = platform_config.resource_set.default_intervals.clone();
    let platform_config = Rc::new(platform_config);
    // SlotSet with a single slot [0,100] with all procs
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 100);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    // Both jobs have job_type "yop", request 1 node each, walltime 50
    let moldable_j1 = Moldable::new(9, 50, HierarchyRequests::from_requests(vec![HierarchyRequest::new(res.clone(), vec![("nodes".into(), 1)])]));
    let j1 = JobBuilder::new(1)
        .user("toto".into())
        .queue("default".into())
        .add_type_key("yop".into())
        .moldable(moldable_j1)
        .build();
    let moldable_j2 = Moldable::new(10, 50, HierarchyRequests::from_requests(vec![HierarchyRequest::new(res.clone(), vec![("nodes".into(), 1)])]));
    let j2 = JobBuilder::new(2)
        .user("toto".into())
        .queue("default".into())
        .add_type_key("yop".into())
        .moldable(moldable_j2)
        .build();
    let mut jobs = indexmap![1 => j1, 2 => j2];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs);
    let j1 = &jobs[0];
    let j2 = &jobs[1];
    // Check results
    assert!(j1.assignment.is_some(), "j1 should be scheduled");
    assert!(j2.assignment.is_some(), "j2 should be scheduled");
    let sched1 = j1.assignment.as_ref().unwrap();
    let sched2 = j2.assignment.as_ref().unwrap();
    assert_eq!(sched1.begin, 0);
    assert_eq!(sched2.begin, 50);
}
