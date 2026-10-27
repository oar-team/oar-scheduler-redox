use crate::model::job::{Job, JobBuilder, Moldable, ProcSet};
use crate::platform::PlatformConfig;
use crate::scheduler::calendar::QuotasConfig;
use crate::scheduler::hierarchy::{HierarchyRequest, HierarchyRequests};
use crate::scheduler::quotas;
use crate::scheduler::scheduling;
use crate::scheduler::slotset::SlotSet;
use crate::scheduler::tests::platform_mock::generate_mock_platform_config;
use chrono::{Datelike, Local, TimeZone};
use indexmap::indexmap;
use std::collections::HashMap;
use std::rc::Rc;

fn period_weekstart(now_epoch: i64) -> i64 {
    let dt = match Local.timestamp_opt(now_epoch, 0) {
        chrono::LocalResult::Single(dt) => dt,
        _ => panic!("invalid time"),
    };
    let week_start = dt - chrono::Duration::days(dt.weekday().num_days_from_monday() as i64);
    week_start
        .date_naive()
        .and_time(chrono::NaiveTime::MIN)
        .and_local_timezone(Local)
        .unwrap()
        .timestamp()
}

fn local_to_sql_minutes(epoch: i64) -> String {
    // Format as "%Y-%m-%d %H:%M" in local time
    let dt = match Local.timestamp_opt(epoch, 0) {
        chrono::LocalResult::Single(dt) => dt,
        _ => panic!("invalid time"),
    };
    dt.format("%Y-%m-%d %H:%M").to_string()
}

fn rules_example_full() -> String {
    r#"{
        "periodical": [
            ["08:00-19:00 mon-fri * *", "quotas_workday", "workdays"],
            ["19:00-00:00 mon-thu * *", "quotas_night", "nights of workdays"],
            ["00:00-08:00 tue-fri * *", "quotas_night", "nights of workdays"],
            ["19:00-00:00 fri * *", "quotas_weekend", "weekend"],
            ["* sat-sun * *", "quotas_weekend", "weekend"],
            ["00:00-08:00 mon * *", "quotas_weekend", "weekend"]
        ],
        "oneshot": [
            ["2020-07-23 19:30", "2020-08-29 08:30", "quotas_holiday", "summer holiday"],
            ["2020-03-16 19:30", "2020-05-10 08:30", "quotas_holiday", "confinement"]
        ],
        "quotas_workday": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]},
        "quotas_night": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]},
        "quotas_weekend": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]},
        "quotas_holiday": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]}
    }"#
        .to_string()
}

fn rules_example_simple_json() -> String {
    // Equivalent to the Python rules_example_simple
    // quotas_1 => 16 resources for everyone (/), quotas_2 => 24 resources for everyone (/)
    r#"{
        "periodical": [
            ["* mon-wed * *", "quotas_1", "test1"],
            ["* thu-sun * *", "quotas_2", "test2"]
        ],
        "quotas_1": {"*,*,*,/": [16, -1, -1], "*,projA,*,*": [20, -1, -1]},
        "quotas_2": {"*,*,*,/": [24, -1, -1], "*,projB,*,*": [15, -1, -1]}
    }"#
        .to_string()
}

fn rules_default_example_json() -> String {
    r#"{
        "periodical": [
            ["* * * *", "quotas_night_weekend", "workdays"],
            ["08:00-19:00 mon-fri * *", "quotas_workday", "workdays"]
        ],
        "quotas_workday": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]},
        "quotas_night_weekend": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]}
    }"#
        .to_string()
}
fn rules_only_default_example_json() -> String {
    r#"{
        "periodical": [
            ["* * * *", "quotas_workday", "workdays"]
        ],
        "quotas_workday": {
            "*,*,*,john": [100, -1, -1]
        }
    }"#
        .to_string()
}
fn rules_example_with_oneshot_json(tw: i64) -> String {
    // Start from the simple example and add a oneshot "holiday" window overlapping Mon-Wed
    // Periodical rules (simple):
    //  - Mon-Wed => quotas_1 (resource limit 16)
    //  - Thu-Sun => quotas_2 (resource limit 24)
    // Oneshoot window:
    //  - from Monday 12:00 to Thursday 00:00 => 2.5 days where quotas_holiday applies (limit 32)
    let mut base = rules_example_simple_json();
    let start = tw + 12 * 3600; // Monday 12:00
    let end = tw + 3 * 86400; // Thursday 00:00
    let oneshot = format!(
        "\n\"oneshot\": [[\"{}\", \"{}\", \"quotas_holiday\", \"summer holiday\"]],",
        local_to_sql_minutes(start),
        local_to_sql_minutes(end)
    );
    // Inject the oneshot array right after the opening brace
    base.insert_str(1, &oneshot);
    // Add the quotas_holiday rule before the closing brace
    if let Some(pos) = base.rfind('}') {
        let add = "\n  ,\n  \"quotas_holiday\": {\"*,*,*,*\": [32, -1, -1]}\n";
        base.insert_str(pos, add);
    }
    base
}

fn add_oneshots_to_rules(rules: &mut String, _oneshots: &[&str]) {
    // For this test we only need a syntactically valid oneshot section; use an empty list
    rules.insert_str(1, "\n\"oneshot\": [],");
}


#[test]
fn test_quota_limits_periodical_segments() {
    let json = rules_example_simple_json();
    let mut pc: PlatformConfig = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    pc.quotas_config = QuotasConfig::load_from_json(json, true, 100, 3 * 7 * 24 * 3600);
    let pc = Rc::new(pc);

    let t0 = period_weekstart(Local::now().timestamp());
    let t1 = t0 + 7 * 86400 - 1;
    let ss = SlotSet::from_platform_config(Rc::clone(&pc), t0, t1);

    // Helper job (only user/queue matter for key matching)
    let job = JobBuilder::new(1)
        .user("john".into())
        .queue("default".into())
        .moldable(Moldable::new(0, 3600, HierarchyRequests::from_requests(Vec::new())))
        .build();

    // Monday: resource limit is 20: 16 allowed, 24 rejected
    let b_mon = t0 + 1; // avoid boundary
    let sid_mon = ss.slot_at(b_mon, None).unwrap().id();
    assert!(quotas::check_slots_quotas(ss.iter().between(sid_mon, sid_mon), &job, b_mon, b_mon + 3600, 16).is_none());
    assert!(quotas::check_slots_quotas(ss.iter().between(sid_mon, sid_mon), &job, b_mon, b_mon + 3600, 24).is_some());

    // Thursday: resource limit is 24: 24 allowed, 30 rejected
    let b_thu = t0 + 3 * 86400 + 1;
    let sid_thu = ss.slot_at(b_thu, None).unwrap().id();
    assert!(quotas::check_slots_quotas(ss.iter().between(sid_thu, sid_thu), &job, b_thu, b_thu + 3600, 24).is_none());
    assert!(quotas::check_slots_quotas(ss.iter().between(sid_thu, sid_thu), &job, b_thu, b_thu + 3600, 30).is_some());
}

#[test]
fn test_quota_limits_in_oneshot_window() {
    let now = Local::now().timestamp();
    let tw = period_weekstart(now);
    let json = rules_example_with_oneshot_json(tw);

    let mut pc: PlatformConfig = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    pc.quotas_config = QuotasConfig::load_from_json(json, true, 100, 3 * 7 * 24 * 3600);
    let pc = Rc::new(pc);

    let t0 = tw;
    let t1 = t0 + 7 * 86400 - 1;
    let ss = SlotSet::from_platform_config(Rc::clone(&pc), t0, t1);
    // Helper job
    let job = JobBuilder::new(10)
        .user("john".into())
        .queue("default".into())
        .project("projB".into())
        .moldable(Moldable::new(0, 3600, HierarchyRequests::from_requests(Vec::new())))
        .build();

    // Monday 13:00 within oneshot window: 32 allowed
    let b = tw + 13 * 3600;
    let sid = ss.slot_at(b, None).unwrap().id();
    assert!(quotas::check_slots_quotas(ss.iter().between(sid, sid), &job, b, b + 3600, 32).is_none());
    assert!(quotas::check_slots_quotas(ss.iter().between(sid, sid), &job, b, b + 3600, 33).is_some());

    // Thursday 23:00: periodical quotas_1 (15) 14 allowed, 16 rejected (for projB)
    let b = tw + 5 * 86400 - 3600;
    let sid = ss.slot_at(b, None).unwrap().id();
    assert!(quotas::check_slots_quotas(ss.iter().between(sid, sid), &job, b, b + 3600, 14).is_none());
    assert!(quotas::check_slots_quotas(ss.iter().between(sid, sid), &job, b, b + 3600, 16).is_some());
}

#[test]
fn test_calendar_periodical_from_json() {
    let qc = QuotasConfig::load_from_json(rules_example_full(), true, 100, 3 * 7 * 24 * 3600);
    assert!(qc.calendar.is_some());
    let cal = qc.calendar.unwrap();
    assert!(!cal.ordered_periodicals().is_empty());

    let qc = QuotasConfig::load_from_json(rules_default_example_json(), true, 100, 3 * 7 * 24 * 3600);
    assert!(qc.calendar.is_some());
    let cal = qc.calendar.unwrap();
    assert!(!cal.ordered_periodicals().is_empty());

    let qc = QuotasConfig::load_from_json(rules_only_default_example_json(), true, 100, 3 * 7 * 24 * 3600);
    assert!(qc.calendar.is_some());
    let cal = qc.calendar.unwrap();
    assert!(!cal.ordered_periodicals().is_empty());

    let mut json = rules_example_simple_json();
    add_oneshots_to_rules(&mut json, &["''"]);
    let qc = QuotasConfig::load_from_json(json, true, 100, 3 * 7 * 24 * 3600);
    assert!(qc.calendar.is_some());
    let cal = qc.calendar.unwrap();
    assert!(!cal.ordered_periodicals().is_empty());
}

#[test]
fn test_rules_at_periodical_segment() {
    let mut pc: PlatformConfig = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    pc.quotas_config = QuotasConfig::load_from_json(rules_example_simple_json(), true, 100, 3 * 7 * 24 * 3600);

    let cal = pc.quotas_config.calendar.unwrap();
    let t0 = period_weekstart(Local::now().timestamp());

    let (res, _i1, _i2) = cal.rules_at(t0, 0, 0);
    assert!(res.is_some());
    let (_rid, end) = res.unwrap();

    // Periodical Mon-Wed should give a 3-day window from week start
    assert_eq!(end - t0 + 1, 3 * 86400);
}

#[test]
fn test_rules_at_oneshot_priority() {
    let mut pc: PlatformConfig = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    let now = Local::now().timestamp();
    let tw = period_weekstart(now);
    let json = rules_example_with_oneshot_json(tw);
    let t = tw + (1 * 86400) + 12 * 3600; // Tuesday 12:00

    pc.quotas_config = QuotasConfig::load_from_json(json, true, 100, 3 * 7 * 24 * 3600);
    let cal = pc.quotas_config.calendar.unwrap();

    let (res, _i1, _i2) = cal.rules_at(t, 0, 0);
    assert!(res.is_some());

    let (_rules_id, end) = res.unwrap();
    assert!(end > t);

    // The oneshot should end at Thursday 00:00 (end is Wednesday 23:59:59)
    assert_eq!(end, tw + 3 * 86400 - 1);
}

#[test]
fn test_calendar_simple_slotset_ids_and_lengths() {
    let json = rules_example_simple_json();
    let mut pc: PlatformConfig = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    pc.quotas_config = QuotasConfig::load_from_json(json, true, 100, 3 * 7 * 24 * 3600);
    let pc = Rc::new(pc);

    let t0 = period_weekstart(Local::now().timestamp());
    let t1 = t0 + 3 * 7 * 86400 - 1;
    let ss = SlotSet::from_platform_config(Rc::clone(&pc), t0, t1);

    let mut v: Vec<(i32, i64, i32)> = vec![];
    let mut cur = ss.first_slot().cloned();
    while let Some(s) = cur {
        v.push((s.id(), s.end() - s.begin() + 1, s.quotas().rules_id()));
        cur = s.next().and_then(|nid| ss.get_slot(nid)).cloned();
    }
    assert!(v.len() >= 6);

    // Assert durations
    assert_eq!((v[0].1, v[1].1, v[2].1, v[3].1, v[4].1, v[5].1), (3 * 86400, 4 * 86400, 3 * 86400, 4 * 86400, 3 * 86400, 4 * 86400));
    // rule IDs are implementation details but are checked to be identical for same-rule slots
    assert_eq!((v[0].2, v[1].2), (v[2].2, v[3].2));

    // Extend by 1s to force trailing slot of next period
    let ss2 = SlotSet::from_platform_config(Rc::clone(&pc), t0, t1 + 1);
    let mut v2: Vec<(i32, i64, i32)> = vec![];
    let mut cur2 = ss2.first_slot().cloned();
    while let Some(s) = cur2 {
        v2.push((s.id(), s.end() - s.begin() + 1, s.quotas().rules_id()));
        cur2 = s.next().and_then(|nid| ss2.get_slot(nid)).cloned();
    }
    // The 5th slot should be a 1-second spillover with the default rule (id = -1)
    assert_eq!(v2[6].1, 1);
    assert_eq!(v2[6].2, -1);
}


#[test]
fn test_calendar_simple_slotset_splitting() {
    // Build a SlotSet over 2 weeks and ensure splitting alternates quotas_1 and quotas_2 as expected
    let json = rules_example_simple_json();
    let mut platform_config: PlatformConfig = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    platform_config.quotas_config = QuotasConfig::load_from_json(json, true, 100, 3 * 7 * 24 * 3600);
    let platform_config = Rc::new(platform_config);

    let now = Local::now().timestamp();
    let tw = period_weekstart(now);
    let t0 = tw;
    let t1 = t0 + 2 * 7 * 86400; // two weeks

    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), t0, t1);

    // Helper job used for quota probing
    let mold = Moldable::new(1, 3600, HierarchyRequests::from_requests(Vec::new()));
    let job = JobBuilder::new(999).user("john".into()).queue("default".into()).moldable(mold).build();

    // Collect (duration, inferred_limit_resources) for each slot by probing quotas
    let mut v: Vec<(i64, i64)> = vec![];
    let mut slot = ss.first_slot().cloned();
    while let Some(s) = slot {
        let b = s.begin();
        let e = (b + 3600).min(s.end());
        let id = s.id();
        // try 30 -> 32, else 24, else 16
        let lim = if quotas::check_slots_quotas(ss.iter().between(id, id), &job, b, e, 30).is_none() {
            32
        } else if quotas::check_slots_quotas(ss.iter().between(id, id), &job, b, e, 24).is_none() {
            24
        } else {
            16
        };
        v.push((s.end() - s.begin() + 1, lim));
        slot = s.next().and_then(|nid| ss.get_slot(nid)).cloned();
    }

    // Expect alternating [3 days of 16], [4 days of 24], [3 days of 16], [4 days of 24], ... starting Monday
    // first two entries (over 2 weeks)
    assert!(v.len() >= 4);
    assert_eq!(v[0], (3 * 86400, 16));
    assert_eq!(v[1], (4 * 86400, 24));
    assert_eq!(v[2], (3 * 86400, 16));
    assert_eq!(v[3], (4 * 86400, 24));
}

#[test]
fn test_temporal_slotset_oneshot() {
    let now = Local::now().timestamp();
    let tw = period_weekstart(now);
    let json = rules_example_with_oneshot_json(tw);

    let mut platform_config: PlatformConfig = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    platform_config.quotas_config = QuotasConfig::load_from_json(json, true, 100, 3 * 7 * 24 * 3600);
    let platform_config = Rc::new(platform_config);

    let t0 = tw;
    let t1 = t0 + 14 * 86400 - 1; // two full week

    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), t0, t1);

    // Helper job for probing
    let mold = Moldable::new(1, 3600, HierarchyRequests::from_requests(Vec::new()));
    let job = JobBuilder::new(999).user("john".into()).queue("default".into()).moldable(mold).build();

    // Collect (duration, inferred_limit_resources)
    let mut v: Vec<(i64, i64)> = vec![];
    let mut slot = ss.first_slot().cloned();
    while let Some(s) = slot {
        let b = s.begin();
        let e = (b + 3600).min(s.end());
        let id = s.id();
        let lim = if quotas::check_slots_quotas(ss.iter().between(id, id), &job, b, e, 30).is_none() {
            32
        } else if quotas::check_slots_quotas(ss.iter().between(id, id), &job, b, e, 24).is_none() {
            24
        } else {
            16
        };
        v.push((s.end() - s.begin() + 1, lim));
        slot = s.next().and_then(|nid| ss.get_slot(nid)).cloned();
    }

    // Expect: [12h of 16], [2.5 days of 32], [4 days of 24], [3 days of 16], [4 days of 24]
    assert_eq!(v.len(), 5);
    assert_eq!(v, vec![(12 * 3600, 16), (2 * 86400 + 12 * 3600, 32), (4 * 86400, 24), (3 * 86400, 16), (4 * 86400, 24)]);
}

#[test]
fn test_check_slots_quotas_against_limits() {
    // Build 2 weeks SlotSet with the simple rules and check quotas limits for a job
    let json = rules_example_simple_json();
    let mut platform_config: PlatformConfig = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    platform_config.quotas_config = QuotasConfig::load_from_json(json, true, 100, 3 * 7 * 24 * 3600);
    let platform_config = Rc::new(platform_config);

    let now = Local::now().timestamp();
    let t0 = period_weekstart(now);
    let t1 = t0 + 2 * 7 * 86400 - 1;

    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), t0, t1);

    // Create a simple job (user john, queue default) – only used for rule selection
    let mold = Moldable::new(1, 86400, HierarchyRequests::from_requests(Vec::new()));
    let job = JobBuilder::new(2).user("john".into()).queue("default".into()).moldable(mold).build();

    // Take the full first 7 days window
    let left = ss.slot_at(t0, None).unwrap().id();
    let right = ss.slot_at(t0 + 7 * 86400 - 1, None).unwrap().id();

    // 10 resources within first period (limit 16) -> Ok
    let res = quotas::check_slots_quotas(ss.iter().between(left, right), &job, t0, t0 + 86400 - 1, 10);
    assert!(res.is_none());

    // 20 resources entirely inside first 3 days period (limit 16) -> should fail
    let res = quotas::check_slots_quotas(ss.iter().between(left, left), &job, t0, t0 + 3600, 20);
    assert!(res.is_some());
}

// ---------------------------------------------------------------------------
// Ports of `oar3/tests/kao/test_temporal_quotas.py` (calendar / slot splitting)
// ---------------------------------------------------------------------------

/// Builds a platform config whose quotas calendar is the "simple" example
/// (Mon-Wed: `quotas_1`, Thu-Sun: `quotas_2`) with a 3 weeks period.
fn simple_calendar_platform_config() -> Rc<PlatformConfig> {
    let mut platform_config: PlatformConfig = generate_mock_platform_config(false, 256, 8, 4, 8, true);
    platform_config.quotas_config = QuotasConfig::load_from_json(rules_example_simple_json(), true, 100, 3 * 7 * 24 * 3600);
    Rc::new(platform_config)
}

/// Returns `(slot id, duration, quotas rules id)` for each slot of the set.
fn slots_description(ss: &SlotSet) -> Vec<(i32, i64, i32)> {
    let mut description = Vec::new();
    let mut slot = ss.first_slot().cloned();
    while let Some(s) = slot {
        description.push((s.id(), s.end() - s.begin() + 1, s.quotas().rules_id()));
        slot = s.next().and_then(|next_id| ss.get_slot(next_id)).cloned();
    }
    description
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_calendar_periodical_default_fromJson`
#[test]
fn test_calendar_periodical_default_fromJson() {
    let platform_config = simple_calendar_platform_config();
    let calendar = platform_config.quotas_config.calendar.as_ref().expect("a calendar");
    // Every day of the week is covered by a periodical rule.
    assert_eq!(calendar.ordered_periodicals().len(), 2);
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_calendar_periodical_only_default_fromJson`
#[test]
fn test_calendar_periodical_only_default_fromJson() {
    let json = r#"{
        "periodical": [
            ["* * * *", "quotas_workday", "workdays"]
        ],
        "quotas_workday": {"*,*,*,john": [100, -1, -1]}
    }"#
    .to_string();
    let quotas_config = QuotasConfig::load_from_json(json, true, 100, 3 * 7 * 24 * 3600);
    let calendar = quotas_config.calendar.as_ref().expect("a calendar");
    assert_eq!(calendar.ordered_periodicals().len(), 1);
}

// The Python tests use the 0-based `quotas_rules_id` of the periodicals list; the Rust
// `Quotas::rules_id` numbering is 1-based (`0` is kept for "no calendar entry").
/// `oar3/tests/kao/test_temporal_quotas.py::test_calendar_simple_slotSet_2`
#[test]
fn test_calendar_simple_slotSet_2() {
    let platform_config = simple_calendar_platform_config();
    let t0 = period_weekstart(Local::now().timestamp());
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), t0, t0 + 4 * 86400);

    let first = ss.first_slot().expect("a first slot");
    assert_eq!(first.quotas().rules_id(), 1);
    let second = ss.get_slot(first.next().expect("a next slot")).expect("a second slot");
    assert_eq!(second.quotas().rules_id(), 2);
    assert_eq!(first.end() - first.begin(), 3 * 86400 - 1);
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_calendar_simple_slotSet_3`
#[test]
fn test_calendar_simple_slotSet_3() {
    let platform_config = simple_calendar_platform_config();
    let t0 = period_weekstart(Local::now().timestamp());
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), t0, t0 + 5 * 86400);

    let first = ss.first_slot().expect("a first slot");
    assert_eq!(first.quotas().rules_id(), 1);
    let second = ss.get_slot(first.next().expect("a next slot")).expect("a second slot");
    assert_eq!(second.quotas().rules_id(), 2);
    assert_eq!(first.end() - first.begin(), 3 * 86400 - 1);
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_calendar_simple_slotSet_4`
#[test]
fn test_calendar_simple_slotSet_4() {
    let platform_config = simple_calendar_platform_config();
    let t0 = period_weekstart(Local::now().timestamp());
    let t1 = t0 + 2 * 7 * 86400 - 1;
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), t0, t1);

    assert_eq!(
        slots_description(&ss),
        vec![(1, 259200, 1), (2, 345600, 2), (3, 259200, 1), (4, 345600, 2)]
    );
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_calendar_simple_slotSet_5`
#[test]
fn test_calendar_simple_slotSet_5() {
    let platform_config = simple_calendar_platform_config();
    let t0 = period_weekstart(Local::now().timestamp());
    let t1 = t0 + 2 * 7 * 86400;
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), t0, t1);

    assert_eq!(
        slots_description(&ss),
        vec![(1, 259200, 1), (2, 345600, 2), (3, 259200, 1), (4, 345600, 2), (5, 1, 1)]
    );
}

// ---------------------------------------------------------------------------
// Ports of `oar3/tests/kao/test_temporal_quotas.py` (jobs against the calendar)
// ---------------------------------------------------------------------------

/// Builds a 32 resources platform (4 nodes of 8 cores) whose quotas calendar is
/// the "simple" example, and a [`SlotSet`] covering `[begin, end]`.
fn simple_calendar_slot_set(begin: i64, end: i64) -> (Rc<PlatformConfig>, SlotSet) {
    let mut platform_config: PlatformConfig = generate_mock_platform_config(false, 32, 4, 8, 1, true);
    platform_config.quotas_config = QuotasConfig::load_from_json(rules_example_simple_json(), true, 100, 3 * 7 * 24 * 3600);
    let platform_config = Rc::new(platform_config);
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), begin, end);
    (platform_config, ss)
}

/// Builds a job requesting `nodes` nodes (each of 8 cores) with the given
/// walltime.
fn node_job(platform_config: &Rc<PlatformConfig>, id: i64, nodes: u32, walltime: i64) -> Job {
    let res = platform_config.resource_set.default_resources.clone();
    let moldable = Moldable::new(
        0,
        walltime,
        HierarchyRequests::from_requests(vec![HierarchyRequest::new(res, vec![("nodes".into(), nodes)])]),
    );
    JobBuilder::new(id).user("toto".into()).queue("default".into()).moldable(moldable).build()
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_test`
///
/// A job requesting 3 nodes (24 cores) does not fit in the Mon-Wed period (16
/// cores quota): it is scheduled at the beginning of the Thu-Sun period.
/// Divergence: an unscheduled job has no assignment in Rust (`assignment == None`)
/// where Python uses `start_time == -1`.
#[test]
fn test_test() {
    let t0 = period_weekstart(Local::now().timestamp());
    let (platform_config, ss) = simple_calendar_slot_set(t0, t0 + 7 * 86400 - 1);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    let j1 = node_job(&platform_config, 1, 3, 60);
    let j2 = node_job(&platform_config, 2, 4, 60);
    let mut jobs = indexmap![1 => j1, 2 => j2];

    scheduling::schedule_jobs(&mut all_ss, &mut jobs);

    let j1 = &jobs[0];
    let j2 = &jobs[1];
    let assignment1 = j1.assignment.as_ref().expect("j1 should be scheduled");
    assert_eq!(assignment1.begin - t0, 259200);
    assert!(j2.assignment.is_none(), "j2 should not be scheduled");
    assert_eq!(assignment1.resources, ProcSet::from_iter(1..=24));
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_temporal_quotas_4_jobs_rule_nb_res_1`
#[test]
fn test_temporal_quotas_4_jobs_rule_nb_res_1() {
    let t0 = period_weekstart(Local::now().timestamp());
    let (platform_config, ss) = simple_calendar_slot_set(t0, t0 + 7 * 86400 - 1);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    let j1 = node_job(&platform_config, 1, 3, 60);
    let j2 = node_job(&platform_config, 2, 4, 60);
    let j3 = node_job(&platform_config, 3, 1, (3.5 * 86400.0) as i64);
    let j4 = node_job(&platform_config, 4, 1, 60);
    let mut jobs = indexmap![1 => j1, 2 => j2, 3 => j3, 4 => j4];

    scheduling::schedule_jobs(&mut all_ss, &mut jobs);

    let j1 = &jobs[0];
    let j2 = &jobs[1];
    let j3 = &jobs[2];
    let j4 = &jobs[3];
    let assignment1 = j1.assignment.as_ref().expect("j1 should be scheduled");
    assert_eq!(assignment1.begin - t0, 259200);
    assert!(j2.assignment.is_none(), "j2 should not be scheduled");
    let assignment3 = j3.assignment.as_ref().expect("j3 should be scheduled");
    assert_eq!(assignment3.begin - t0, 259260);
    let assignment4 = j4.assignment.as_ref().expect("j4 should be scheduled");
    assert_eq!(assignment4.begin - t0, 0);

    assert_eq!(assignment1.resources, ProcSet::from_iter(1..=24));
    assert_eq!(assignment3.resources, ProcSet::from_iter(1..=8));
    assert_eq!(assignment4.resources, ProcSet::from_iter(1..=8));
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_temporal_quotas_oneshot_1_job_rule_nb_res_1`
///
/// The oneshot window (Monday 12:00 for 2.5 days) lifts the quota to 32 cores: the
/// job requesting 4 nodes (32 cores) waits for it.
#[test]
fn test_temporal_quotas_oneshot_1_job_rule_nb_res_1() {
    let mut platform_config: PlatformConfig = generate_mock_platform_config(false, 32, 4, 8, 1, true);
    let tw = period_weekstart(Local::now().timestamp());
    let oneshot_start = tw + 86400 / 2;
    let oneshot_stop = oneshot_start + (2.5 * 86400.0) as i64;
    let rules = rules_example_simple_json().replacen(
        "{",
        &format!(
            "{{\n\"oneshot\": [[\"{}\", \"{}\", \"quotas_holiday\", \"summer holiday\"]],\n\"quotas_holiday\": {{\"*,*,*,*\": [32, -1, -1]}},",
            local_to_sql_minutes(oneshot_start),
            local_to_sql_minutes(oneshot_stop)
        ),
        1,
    );
    platform_config.quotas_config = QuotasConfig::load_from_json(rules, true, 100, 3 * 7 * 24 * 3600);
    let platform_config = Rc::new(platform_config);
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), tw, tw + 7 * 86400 - 1);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    let job = node_job(&platform_config, 1, 4, 60);
    let mut jobs = indexmap![1 => job];

    scheduling::schedule_jobs(&mut all_ss, &mut jobs);

    let assignment = jobs[0].assignment.as_ref().expect("the job should be scheduled");
    assert_eq!(assignment.begin - tw, 43200);
    assert_eq!(assignment.resources, ProcSet::from_iter(1..=32));
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_temporal_quotas_job_no_quotas`
///
/// A job with `no_quotas` ignores the calendar limits: it starts immediately even
/// though it requests more resources than the current period allows.
#[test]
fn test_temporal_quotas_job_no_quotas() {
    let t0 = period_weekstart(Local::now().timestamp());
    let (platform_config, ss) = simple_calendar_slot_set(t0, t0 + 7 * 86400 - 1);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    let mut job = node_job(&platform_config, 1, 4, 60);
    job.no_quotas = true;
    let mut jobs = indexmap![1 => job];

    scheduling::schedule_jobs(&mut all_ss, &mut jobs);

    let assignment = jobs[0].assignment.as_ref().expect("the job should be scheduled");
    assert_eq!(assignment.begin - t0, 0);
    assert_eq!(assignment.resources, ProcSet::from_iter(1..=32));
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_temporal_quotas_window_time_limit_reached`
///
/// The first job (3 nodes for 3 days) exhausts the resources time quota of the
/// window: the second job (5 nodes for 10 days) cannot be scheduled at all.
#[test]
fn test_temporal_quotas_window_time_limit_reached() {
    let t0 = period_weekstart(Local::now().timestamp());
    let (platform_config, ss) = simple_calendar_slot_set(t0, t0 + 14 * 86400);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    let j1 = node_job(&platform_config, 1, 3, 3 * 86400);
    let j2 = node_job(&platform_config, 2, 5, 10 * 86400);
    let mut jobs = indexmap![1 => j1, 2 => j2];

    scheduling::schedule_jobs(&mut all_ss, &mut jobs);

    let assignment1 = jobs[0].assignment.as_ref().expect("j1 should be scheduled");
    assert_eq!(assignment1.begin - t0, 259200);
    assert!(jobs[1].assignment.is_none(), "j2 should not be scheduled");
    assert_eq!(assignment1.resources, ProcSet::from_iter(1..=24));
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_temporal_quots_multi_periods_nb_resources`
///
/// Divergence: the Python test builds the oneshot window from `datetime.utcnow()`
/// while the calendar parses the windows in local time (the test is therefore
/// timezone dependent); the Rust port uses the local time.
#[test]
fn test_temporal_quots_multi_periods_nb_resources() {
    let json = format!(
        r#"{{
            "periodical": [["* * * *", "quotas_1", "test1"]],
            "oneshot": [["{}", "{}", "quotas_2", "not important"]],
            "quotas_1": {{"*,*,*,/": [24, -1, -1]}},
            "quotas_2": {{"*,*,*,/": [8, -1, -1]}}
        }}"#,
        local_to_sql_minutes(Local::now().timestamp() + 5 * 60),
        local_to_sql_minutes(Local::now().timestamp() + 10 * 60)
    );
    let mut platform_config: PlatformConfig = generate_mock_platform_config(false, 32, 4, 8, 1, true);
    platform_config.quotas_config = QuotasConfig::load_from_json(json, true, 100, 3 * 7 * 24 * 3600);
    let platform_config = Rc::new(platform_config);

    let t0 = Local::now().timestamp();
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), t0, t0 + 14 * 86400);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    // A single node: it fits both in the periodical (24 cores) and in the oneshot
    // window (8 cores) quotas.
    let j = node_job(&platform_config, 2, 1, 300);
    // Two nodes (16 cores): it does not fit in the oneshot window and must be
    // delayed until after it.
    let j1 = node_job(&platform_config, 3, 2, 300);
    let mut jobs = indexmap![2 => j, 3 => j1];

    scheduling::schedule_jobs(&mut all_ss, &mut jobs);

    let assignment = jobs[&2].assignment.as_ref().expect("the first job should be scheduled");
    assert_eq!(assignment.begin - t0, 0);
    let delayed = jobs[&3].assignment.as_ref().expect("the second job should be scheduled");
    assert!(delayed.begin - t0 > 0, "the second job should be delayed");
}

/// `oar3/tests/kao/test_temporal_quotas.py::test_temporal_quots_multi_periods_nb_jobs`
///
/// Same scenario as [`test_temporal_quots_multi_periods_nb_resources`] but the
/// oneshot window limits the number of running jobs (1) instead of the resources.
#[test]
fn test_temporal_quots_multi_periods_nb_jobs() {
    let json = format!(
        r#"{{
            "periodical": [["* * * *", "quotas_1", "test1"]],
            "oneshot": [["{}", "{}", "quotas_2", "not important"]],
            "quotas_1": {{"*,*,*,/": [-1, 2, -1]}},
            "quotas_2": {{"*,*,*,/": [-1, 1, -1]}}
        }}"#,
        local_to_sql_minutes(Local::now().timestamp() + 5 * 60),
        local_to_sql_minutes(Local::now().timestamp() + 10 * 60)
    );
    let mut platform_config: PlatformConfig = generate_mock_platform_config(false, 32, 4, 8, 1, true);
    platform_config.quotas_config = QuotasConfig::load_from_json(json, true, 100, 3 * 7 * 24 * 3600);
    let platform_config = Rc::new(platform_config);

    let t0 = Local::now().timestamp();
    let ss = SlotSet::from_platform_config(Rc::clone(&platform_config), t0, t0 + 14 * 86400);
    let mut all_ss = HashMap::from([("default".into(), ss)]);

    let j = node_job(&platform_config, 2, 1, 300);
    let j1 = node_job(&platform_config, 3, 2, 300);
    let mut jobs = indexmap![2 => j, 3 => j1];

    scheduling::schedule_jobs(&mut all_ss, &mut jobs);

    let assignment = jobs[&2].assignment.as_ref().expect("the first job should be scheduled");
    assert_eq!(assignment.begin - t0, 0);
    let delayed = jobs[&3].assignment.as_ref().expect("the second job should be scheduled");
    assert!(delayed.begin - t0 > 0, "the second job should be delayed");
}
