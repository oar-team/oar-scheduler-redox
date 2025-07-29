use crate::models::{JobBuilder, Moldable, ProcSet, TimeSharingType};
use crate::platform::PlatformConfig;
use crate::scheduler::hierarchy::HierarchyRequests;
use crate::scheduler::scheduling;
use crate::scheduler::slot::SlotSet;
use crate::scheduler::tests::platform_mock::generate_mock_platform_config;
use std::collections::HashMap;
use std::rc::Rc;

fn platform_config() -> Rc<PlatformConfig> {
    let platform_config = generate_mock_platform_config(false, 64, 8, 4, 8, false);
    Rc::new(platform_config)
}

#[test]
fn test_quotas_two_job_rules_nb_res_quotas_file() {
    let platform_config = platform_config();
    let res = platform_config.as_ref().resource_set.default_intervals.clone();
    // SlotSet with a single slot [0,1000] with all 64 procs
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), 0, 1000);
    let mut all_ss = HashMap::from([("default".to_string(), ss)]);

    let moldable1 = Moldable::new(1, 60, HierarchyRequests::new_single(res.clone(), vec![("cpus".into(), 2)]));
    let moldable2 = Moldable::new(2, 60, HierarchyRequests::new_single(res.clone(), vec![("cpus".into(), 7)]));

    let job_1 = JobBuilder::new(1).moldable(moldable1.clone()).build();
    let job_2 = JobBuilder::new(2)
        .user("toto".into())
        .time_sharing(TimeSharingType::UserAll)
        .moldable(moldable1.clone())
        .build();
    let job_3 = JobBuilder::new(3)
        .user("toto".into())
        .time_sharing(TimeSharingType::UserAll)
        .moldable(moldable1.clone())
        .build();

    let job_4 = JobBuilder::new(3)
        .user("toto".into())
        .name("tata".into())
        .time_sharing(TimeSharingType::AllName)
        .moldable(moldable2.clone())
        .build();
    let job_5 = JobBuilder::new(3)
        .user("toto".into())
        .name("tata2".into())
        .time_sharing(TimeSharingType::AllName)
        .moldable(moldable2.clone())
        .build();

    let mut jobs = vec![job_1, job_2, job_3, job_4, job_5];
    scheduling::schedule_jobs(&mut all_ss, &mut jobs);
    let j1 = jobs[0].clone().scheduled_data.unwrap();
    let j2 = jobs[1].clone().scheduled_data.unwrap();
    let j3 = jobs[2].clone().scheduled_data.unwrap();
    let j4 = jobs[3].clone().scheduled_data.unwrap();
    let j5 = jobs[4].clone().scheduled_data.unwrap();

    assert_eq!(j1.proc_set, ProcSet::from_iter(1..=16));
    assert_eq!(j1.begin, 0);
    assert_eq!(j2.proc_set, ProcSet::from_iter(17..=32));
    assert_eq!(j2.begin, 0);
    assert_eq!(j3.proc_set, ProcSet::from_iter(17..=32));
    assert_eq!(j3.begin, 0);
    assert_eq!(j4.proc_set, ProcSet::from_iter(1..=56));
    assert_eq!(j4.begin, 60);
    assert_eq!(j5.proc_set, ProcSet::from_iter(1..=56));
    assert_eq!(j5.begin, 120);
}
