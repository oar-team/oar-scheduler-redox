use crate::scheduler::quotas;
use crate::scheduler::quotas::QuotasMap;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;

pub type PeriodicalsJson = Box<[(Box<str>, Box<str>, Box<str>)]>;
pub type OneshotsJson = Box<[(Box<str>, Box<str>, Box<str>, Box<str>)]>;

// Map day names to their corresponding weekday numbers (0=Monday, 6=Sunday)
const DAYS_TO_NUM_ARRAY: [(&str, i32); 7] = [("mon", 0), ("tue", 1), ("wed", 2), ("thu", 3), ("fri", 4), ("sat", 5), ("sun", 6)];
lazy_static::lazy_static! {
    static ref DAYS_TO_NUM: HashMap<&'static str, i32> = HashMap::from_iter(DAYS_TO_NUM_ARRAY);
}

/// Holds the raw and parsed quota root configuration entries.
pub struct QuotasConfigEntries {
    all_value: i64,
    id_counter: i32,
    entries: HashMap<Box<str>, Value>,       // name -> Serde value representing the rules (or other data)
    parsed_entries: HashMap<Box<str>, (i32, QuotasMap)>, // name -> (id, QuotasKey -> QuotasValue)
}
/// Represents a periodical entry parsed from JSON into Box<str>.
pub struct PeriodicalJsonEntry {
    pub(crate) period: Box<str>,
    pub(crate) rule: Box<str>,
    pub(crate) description: Box<str>,
}
/// Represents a oneshot entry parsed from JSON into Box<str>.
pub struct OneshotJsonEntry {
    begin: Box<str>,
    end: Box<str>,
    rule: Box<str>,
    description: Box<str>,
}
/// Represents a fully parsed periodical entry.
pub struct PeriodicalEntry {
    pub(crate) begin_time: i64, // Begin time in seconds from week start (0-604800)
    pub(crate) duration: i64,
    rules_id: i32,
    period_string: Box<str>,
    description: Box<str>,
}
/// Represents a fully parsed oneshot entry.
pub struct OneshotEntry {
    begin_time: i64, // Epoch time in seconds
    end_time: i64,   // Epoch time in seconds
    rules_id: i32,
    begin_string: Box<str>,
    end_string: Box<str>,
    description: Box<str>,
}

impl QuotasConfigEntries {
    pub fn new(entries: HashMap<Box<str>, Value>, all_value: i64) -> Self {
        QuotasConfigEntries {
            all_value,
            id_counter: 0,
            entries,
            parsed_entries: HashMap::new(),
        }
    }
    /// Get the ID for a given rule name, parsing and storing it if not already done.
    fn get_rules_id(&mut self, rule_name: &str) -> i32 {
        if let Some((id, _quotas_map)) = self.parsed_entries.get(rule_name) {
            return *id;
        }
        if let Some(value) = self.entries.get(rule_name) {
            // Here we would parse the value into a QuotasMap
            // For simplicity, we just assign an ID
            self.id_counter += 1;
            let parsed_value = serde_json::from_value::<HashMap<String, Vec<Value>>>(value.clone()).expect("Failed to parse quotas");
            let quotas_map = quotas::build_quotas_map(&parsed_value, self.all_value);
            self.parsed_entries.insert(rule_name.into(), (self.id_counter, quotas_map));
            return self.id_counter;
        }
        panic!("Rule name '{}' not found in quotas configuration entries", rule_name);
    }
    pub fn to_parsed_entries(self) -> HashMap<Box<str>, (i32, QuotasMap)> {
        self.parsed_entries
    }
}

impl PeriodicalJsonEntry {
    pub(crate) fn from_tuple(t: &(Box<str>, Box<str>, Box<str>)) -> Self {
        PeriodicalJsonEntry {
            period: t.0.clone(),
            rule: t.1.clone(),
            description: t.2.clone(),
        }
    }
}
impl OneshotJsonEntry {
    pub(crate) fn from_tuple(t: &(Box<str>, Box<str>, Box<str>, Box<str>)) -> Self {
        OneshotJsonEntry {
            begin: t.0.clone(),
            end: t.1.clone(),
            rule: t.2.clone(),
            description: t.3.clone(),
        }
    }
}

impl PeriodicalEntry {
    pub(crate) fn from_json_entry(periodical: &PeriodicalJsonEntry, config_entries: &mut QuotasConfigEntries) -> Vec<Self> {
        let parts: Vec<&str> = periodical.period.split_whitespace().collect();
        if parts.len() != 4 {
            panic!("Unable to parse periodical quotas period format. Expected 4 parts: time_range days month day");
        }

        let time_range = parts[0];
        let days = parts[1];
        let _month = parts[2];
        let _day = parts[3];

        // Handle month and day wildcards (not fully implemented as in Python)
        if _month != "*" || _day != "*" {
            panic!("Can’t parse periodical quotas. Month and day specifications are not yet implemented, please leave them as '*'");
        }

        // Parse time range
        let (begin_time, end_time) = if time_range == "*" {
            (0, 24 * 3600)
        } else {
            let time_parts: Vec<&str> = time_range.split('-').collect();
            if time_parts.len() != 2 {
                panic!("Invalid time range format in periodical quotas configuration. Expected 'HH:MM-HH:MM'");
            }
            let begin = parse_time_to_seconds(time_parts[0]);
            let end = parse_time_to_seconds(time_parts[1]);
            (begin, if end == 0 { 24 * 3600 } else { end })
        };
        // Duration might be negative if it wraps past midnight
        let mut duration = end_time - begin_time;
        if duration == 0 {
            panic!("Invalid time range in periodical quotas configuration. Begin and end times cannot be the same.");
        }

        // Parse days
        let day_numbers = parse_day_range(days);
        if day_numbers.is_empty() {
            panic!(
                "Invalid days configuration in periodical quotas configuration. No valid days found.\
                Use '*' for all days or specify days in an array like 'mon,tue,wed,thu,fri,sat,sun', or as ranges like 'mon-fri'."
            );
        }

        // Create entries for each day
        let mut entries = Vec::new();
        let rules_id = config_entries.get_rules_id(&periodical.rule);
        for day in day_numbers {
            let day_begin = day as i64 * 24 * 3600;

            if end_time < begin_time {
                // Adds the entry for midnight to end_time on this same day
                entries.push(PeriodicalEntry {
                    begin_time: day_begin,
                    duration: end_time,
                    rules_id, // This would be set based on periodical.rule
                    period_string: periodical.period.clone(),
                    description: periodical.description.clone(),
                });
                // Set the duration so the next entry goes from begin_time to midnight
                duration = 24 * 3600 - begin_time;
            }

            entries.push(PeriodicalEntry {
                begin_time: day_begin + begin_time,
                duration,
                rules_id, // This would be set based on periodical.rule
                period_string: periodical.period.clone(),
                description: periodical.description.clone(),
            });
        }

        // Sort entries by begin_time
        entries.sort_by(|a, b| a.begin_time.cmp(&b.begin_time));
        entries
    }
}

impl OneshotEntry {
    pub(crate) fn from_json_entry(entry: &OneshotJsonEntry, config_entries: &mut QuotasConfigEntries) -> Self {
        let begin_time = parse_datetime(&entry.begin).unwrap_or_else(|e| {
            panic!(
                "Invalid begin time format '{}' in oneshot entry. Expected format: YYYY-MM-DD hh:mm. Error: {}",
                entry.begin, e
            )
        });
        let end_time = parse_datetime(&entry.end).unwrap_or_else(|e| {
            panic!(
                "Invalid end time format '{}' in oneshot entry. Expected format: YYYY-MM-DD hh:mm. Error: {}",
                entry.end, e
            )
        });
        if end_time <= begin_time {
            panic!(
                "Invalid time range in oneshot entry: end time '{}' must be after begin time '{}'",
                entry.end, entry.begin
            );
        }

        Self {
            begin_time: begin_time.timestamp(),
            end_time: end_time.timestamp(),
            rules_id: config_entries.get_rules_id(&entry.rule),
            begin_string: entry.begin.clone(),
            end_string: entry.end.clone(),
            description: entry.description.clone(),
        }
    }
}

// Helper function to parse time string in "HH:MM" format to seconds since midnight
fn parse_time_to_seconds(time_str: &str) -> i64 {
    if time_str == "*" {
        return 0;
    }

    let time_parts: Vec<&str> = time_str.split(':').collect();
    if time_parts.len() != 2 {
        return 0;
    }

    let hours = time_parts[0].parse::<i64>().unwrap_or(0);
    let minutes = time_parts[1].parse::<i64>().unwrap_or(0);
    hours * 3600 + minutes * 60
}

// Parse day range like "mon-fri" to a vector of day numbers
fn parse_day_range(day_range: &str) -> Vec<i32> {
    if day_range == "*" {
        return (0..7).collect();
    }

    let mut result = Vec::new();
    for part in day_range.split(',') {
        if part.contains('-') {
            let range_parts: Vec<&str> = part.split('-').collect();
            if range_parts.len() == 2 {
                if let (Some(&start), Some(&end)) = (DAYS_TO_NUM.get(range_parts[0]), DAYS_TO_NUM.get(range_parts[1])) {
                    if start <= end {
                        result.extend(start..=end);
                    } else {
                        // Handle wrap-around (e.g., sun-mon)
                        result.extend(start..7);
                        result.extend(0..=end);
                    }
                }
            }
        } else if let Some(&day_num) = DAYS_TO_NUM.get(part) {
            result.push(day_num);
        }
    }

    result.sort_unstable();
    result.dedup();
    result
}

/// Parse a datetime string in the format "YYYY-MM-DD hh:mm" to a DateTime<Utc>
fn parse_datetime(datetime_str: &str) -> Result<DateTime<Utc>, String> {
    // Add seconds if not present
    let datetime_with_seconds = if datetime_str.len() == 16 {
        // Format: YYYY-MM-DD hh:mm
        format!("{}:00", datetime_str)
    } else {
        datetime_str.to_string()
    };

    // Parse with timezone support
    NaiveDateTime::parse_from_str(&datetime_with_seconds, "%Y-%m-%d %H:%M:%S")
        .map(|datetime| datetime.and_utc())
        .map_err(|e| e.to_string())
}
