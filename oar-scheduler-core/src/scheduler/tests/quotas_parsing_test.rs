use crate::scheduler::quotas_parsing::{PeriodicalEntry, PeriodicalJsonEntry, QuotasConfigEntries};
use serde_json::Value;
use std::collections::HashMap;

#[test]
fn test_parse_periodical_entry() {
    let entry = PeriodicalJsonEntry {
        period: "08:00-19:00 mon-fri * *".into(),
        rule: "workday_quota".into(),
        description: "Work hours".into(),
    };

    let rules_json = r#"{
            "workday_quota": {
                "*,*,*,john": [100, "ALL", "0.5*ALL"],
                "*,projA,*,*": ["34", "ALL", "2*ALL"]
            }
        }"#.to_string();
    let entries = serde_json::from_str::<HashMap<Box<str>, Value>>(&rules_json).expect("Failed to parse quotas config base JSON");
    let mut config_entries = QuotasConfigEntries::new(entries, 100);

    let result = PeriodicalEntry::from_json_entry(&entry, &mut config_entries);
    assert_eq!(result.len(), 5); // 5 weekdays

    // Verify first entry (Monday)
    let monday_entry = &result[0];
    assert_eq!(monday_entry.begin_time, 0 * 86400 + 8 * 3600);
    assert_eq!(monday_entry.duration, 11 * 3600); // 08:00-19:00 = 11 hours
}

#[test]
fn test_overnight_period() {
    let entry = PeriodicalJsonEntry {
        period: "22:00-02:00 * * *".into(),
        rule: "overnight".into(),
        description: "Overnight period".into(),
    };

    let rules_json = r#"{
            "overnight": {
                "*,*,*,john": [100, "ALL", "0.5*ALL"],
                "*,projA,*,*": ["34", "ALL", "2*ALL"]
            }
        }"#.to_string();
    let entries = serde_json::from_str::<HashMap<Box<str>, Value>>(&rules_json).expect("Failed to parse quotas config base JSON");
    let mut config_entries = QuotasConfigEntries::new(entries, 100);

    let result = PeriodicalEntry::from_json_entry(&entry, &mut config_entries);

    // Should have entries for each day, with proper overflow handling
    assert_eq!(result.len(), 7 * 2); // 7 days * 2 entries per day (split at midnight)
}
