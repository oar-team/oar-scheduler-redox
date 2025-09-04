/*
 * Copyright (c) 2025 Clément GRENNERAT
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, version 3.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see https://www.gnu.org/licenses/.
 *
 */

//! Module handling the temporal quotas

use crate::scheduler::calendar::parsing::{
    OneshotEntry, OneshotJsonEntry, OneshotsJson, PeriodicalEntry, PeriodicalJsonEntry, PeriodicalsJson, QuotasConfigEntries,
};
use crate::scheduler::quotas;
use crate::scheduler::quotas::{Quotas, QuotasMap, QuotasTree};
use crate::scheduler::slotset::SlotSet;
use chrono::{Datelike, Local, TimeZone, Timelike};
use log::warn;
#[cfg(feature = "pyo3")]
use pyo3::{prelude::PyDictMethods, types::PyDict, Bound, IntoPyObject, PyErr, Python};
use serde_json::Value;
use std::collections::HashMap;
use std::rc::Rc;

/// Configuration of quotas stored in PlatformConfig.
#[allow(dead_code)]
#[derive(Debug)]
pub struct QuotasConfig {
    pub enabled: bool,
    pub calendar: Option<Calendar>,
    pub default_rules_id: i32, // should be negative as periodicals and oneshots have positive ids
    pub default_rules: Rc<QuotasMap>,
    pub default_rules_tree: Rc<QuotasTree>,
    pub tracked_job_types: Box<[Box<str>]>, // called job_types in python
}
impl Default for QuotasConfig {
    fn default() -> Self {
        QuotasConfig::new(true, None, Default::default(), Box::new(["*".into()]))
    }
}
#[cfg(feature = "pyo3")]
impl<'a> IntoPyObject<'a> for &QuotasConfig {
    type Target = PyDict;
    type Output = Bound<'a, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'a>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);

        dict.set_item("enabled", self.enabled)?;
        // Quotas rust-to-python conversion is not supported

        Ok(dict)
    }
}

impl QuotasConfig {
    /// Creates a new QuotasConfig with the given parameters.
    pub fn new(enabled: bool, calendar: Option<Calendar>, default_rules: QuotasMap, tracked_job_types: Box<[Box<str>]>) -> Self {
        let default_rules_tree = Rc::new(QuotasTree::from(default_rules.clone()));
        QuotasConfig {
            enabled,
            calendar,
            default_rules_id: -1,
            default_rules: Rc::new(default_rules),
            default_rules_tree,
            tracked_job_types,
        }
    }
    pub fn load_from_file(path: &str, enabled: bool, all_value: i64, quotas_window_time_limit: i64) -> Self {
        let json = std::fs::read_to_string(path).expect("Failed to read quotas config file");
        Self::load_from_json(json, enabled, all_value, quotas_window_time_limit)
    }
    pub fn load_from_json(json: String, enabled: bool, all_value: i64, quotas_window_time_limit: i64) -> Self {
        let entries = serde_json::from_str::<HashMap<Box<str>, Value>>(&json).expect("Failed to parse quotas config base JSON");

        let job_types = entries
            .get("job_types")
            .and_then(|v| serde_json::from_value::<Box<[Box<str>]>>(v.clone()).ok())
            .unwrap_or_else(|| Box::new(["*".into()]));
        let quotas = entries
            .get("quotas")
            .map(|v| serde_json::from_value::<HashMap<String, Vec<Value>>>(v.clone()).expect("Failed to parse quotas"))
            .map(|hm| quotas::build_quotas_map(&hm, all_value));
        let periodical = entries
            .get("periodical")
            .map(|v| serde_json::from_value::<PeriodicalsJson>(v.clone()).expect("Failed to parse periodical quotas"));
        let oneshot = entries
            .get("oneshot")
            .map(|v| serde_json::from_value::<OneshotsJson>(v.clone()).expect("Failed to parse periodical quotas"));

        let calendar = if periodical.is_some() || oneshot.is_some() {
            Some(Calendar::from_config(
                entries,
                periodical,
                oneshot,
                all_value,
                quotas_window_time_limit,
            ))
        } else {
            None
        };
        QuotasConfig::new(enabled, calendar, quotas.unwrap_or_default(), job_types)
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct Calendar {
    /// Periodicals are applied until the end of the week containing the instant (now + quotas_window_time_limit).
    quotas_window_time_limit: i64,

    pub rules_map: HashMap<i32, (Rc<QuotasMap>, Rc<QuotasTree>)>,
    ordered_periodicals: Vec<PeriodicalEntry>,
    ordered_oneshot: Vec<OneshotEntry>,
}

impl Calendar {
    fn from_config(
        json_entries: HashMap<Box<str>, Value>,
        periodicals: Option<PeriodicalsJson>,
        oneshots: Option<OneshotsJson>,
        all_values: i64,
        quotas_window_time_limit: i64,
    ) -> Self {
        let mut config_entries = QuotasConfigEntries::new(json_entries, all_values);

        let ordered_periodicals = if let Some(periodicals) = periodicals {
            let mut entries = periodicals
                .into_iter()
                .map(|periodical| PeriodicalJsonEntry::from_tuple(&periodical))
                .map(|periodical| PeriodicalEntry::from_json_entry(&periodical, &mut config_entries))
                .flatten()
                .collect::<Vec<PeriodicalEntry>>();

            // Sort and merge periodicals
            entries.sort_by(|a, b| a.week_begin_time.cmp(&b.week_begin_time));

            entries.dedup_by(|a, b| {
                // If a and b have the same rules_id and are following each other, edit b and return true (to remove a)
                if a.rules_id != b.rules_id || a.week_begin_time != b.week_end_time + 1 {
                    // If a and b overlap, throw a warning
                    if a.week_begin_time <= b.week_end_time {
                        warn!(
                            "Overlapping periodical entries detected: {} and {} with ({}) starting before ({}) ends (interval [{}, {}] and [{}, {}] overlap)",
                            a.description, b.description, a.period_string, b.period_string, a.week_begin_time, a.week_end_time, b.week_begin_time, b.week_end_time
                        );
                    }
                    return false;
                }
                b.week_end_time = a.week_end_time;
                b.period_string = format!("{} + {}", b.period_string, a.period_string).into_boxed_str();
                true
            });
            entries
        } else {
            vec![]
        };
        let ordered_oneshot = if let Some(oneshots) = oneshots {
            let mut entries = oneshots
                .into_iter()
                .map(|oneshot| OneshotJsonEntry::from_tuple(&oneshot))
                .map(|oneshot| OneshotEntry::from_json_entry(&oneshot, &mut config_entries))
                .collect::<Vec<OneshotEntry>>();

            // Sort and merge oneshots
            entries.sort_by(|oneshot_a, oneshot_b| oneshot_a.begin_time.cmp(&oneshot_b.begin_time));
            entries.dedup_by(|oneshot_a, oneshot_b| {
                // If a and b have the same rules_id and are following each other, edit b and return true (to remove a)
                if oneshot_a.rules_id != oneshot_b.rules_id || oneshot_a.begin_time != oneshot_b.end_time {
                    // If a and b overlap, throw a warning
                    if oneshot_a.begin_time < oneshot_b.end_time {
                        warn!(
                            "Overlapping oneshot entries detected: {} and {} with ({}) < ({})",
                            oneshot_a.description, oneshot_b.description, oneshot_a.begin_string, oneshot_b.begin_string
                        );
                    }
                    return false;
                }
                oneshot_b.end_time = oneshot_a.end_time;
                oneshot_b.end_string = format!("{} merged to {}", oneshot_b.end_string, oneshot_a.end_string).into_boxed_str();
                true
            });
            entries
        } else {
            vec![]
        };
        let rules_map = config_entries.to_rules_map();

        Self {
            quotas_window_time_limit,
            rules_map,
            ordered_periodicals,
            ordered_oneshot,
        }
    }

    /// Returns the active rules_id at a given time with the end time of these rules.
    /// Also returns the indices of the oneshot and periodical rules used.
    /// The indices can be used to optimize future lookups by starting the search from these indices since rules are ordered by time.
    /// At the first call, call with oneshots_start_index = 0 and periodicals_start_index = 0.
    /// If no rule is active at the given time, returns None and the same indices as provided.
    pub fn rules_at(&self, time: i64, oneshots_start_index: usize, periodicals_start_index: usize) -> (Option<(i32, i64)>, usize, usize) {
        // Check oneshots first, as they have priority over periodicals
        for (i, oneshot) in self.ordered_oneshot[oneshots_start_index..].iter().enumerate() {
            if time >= oneshot.begin_time && time < oneshot.end_time {
                return (Some((oneshot.rules_id, oneshot.end_time)), i, periodicals_start_index);
            }
        }
        // Find the time in the week (0 = Monday 00:00:00, 604800 = Sunday 23:59:59)
        let week_datetime = match Local.timestamp_opt(time, 0) {
            chrono::LocalResult::Single(dt) => dt,
            _ => panic!("Failed to convert time to DateTime"),
        };

        let week_time = (week_datetime.weekday().num_days_from_monday() as i64) * 24 * 3600
            + (week_datetime.hour() as i64) * 3600
            + (week_datetime.minute() as i64) * 60
            + (week_datetime.second() as i64);

        // Iterating over ordered_periodicals, looping back to the start if needed
        for (i, periodical) in self.ordered_periodicals[periodicals_start_index..]
            .iter()
            .chain(self.ordered_periodicals[..periodicals_start_index].iter())
            .enumerate()
        {
            if week_time >= periodical.week_begin_time && week_time < periodical.week_end_time {
                let periodical_end_time = time + (periodical.week_end_time - week_time);
                return (Some((periodical.rules_id, periodical_end_time)), oneshots_start_index, i);
            }
        }
        (None, oneshots_start_index, periodicals_start_index)
    }

    pub fn split_slotset_for_temporal_quotas(&self, slot_set: &mut SlotSet) {
        if self.ordered_oneshot.is_empty() && self.ordered_periodicals.is_empty() {
            return;
        }
        self.split_slotset_for_oneshots(slot_set);
        self.split_slotset_for_periodicals(slot_set);
    }

    /// Splits the slotset according to the oneshot entries in the calendar.
    /// Sets the correct [`Quotas`] structs to the slots.
    fn split_slotset_for_oneshots(&self, slot_set: &mut SlotSet) {
        let slotset_begin = slot_set.begin();
        let mut starting_id = None;
        for oneshot in &self.ordered_oneshot {
            let (begin_slot_id, end_slot_id) = if let Some(slots) = slot_set.split_slots_for_range(oneshot.begin_time, oneshot.end_time, starting_id)
            {
                slots
            } else {
                // [oneshot.begin_time, oneshot.end_time] is completely before or after the slotset (disjoint ranges)
                if oneshot.begin_time < slotset_begin {
                    continue; // Before the slotset
                }
                // After the slotset, we are done
                break;
            };
            starting_id = Some(begin_slot_id);
            let rules = self.rules_map.get(&oneshot.rules_id).unwrap();
            let quotas = Quotas::new(
                Rc::clone(slot_set.get_platform_config()),
                oneshot.rules_id,
                Rc::clone(&rules.0),
                Rc::clone(&rules.1),
            );
            for slot_id in slot_set.iter().between(begin_slot_id, end_slot_id).map(|s| s.id).collect::<Vec<i32>>() {
                slot_set.get_slot_mut(slot_id).unwrap().quotas = quotas.clone();
            }
        }
    }

    /// Splits the slotset according to the periodical entries in the calendar.
    /// Sets the correct [`Quotas`] structs to the slots.
    fn split_slotset_for_periodicals(&self, slot_set: &mut SlotSet) {
        let max_time = slot_set.begin() + self.quotas_window_time_limit;

        let slotset_begin = slot_set.begin();
        let slotset_begin_datetime = match Local.timestamp_opt(slotset_begin, 0) {
            chrono::LocalResult::Single(dt) => dt,
            _ => panic!("Failed to convert time to DateTime"),
        };
        let mut week_begin = slotset_begin
            - (slotset_begin_datetime.weekday().num_days_from_monday() as i64) * 24 * 3600
            - (slotset_begin_datetime.hour() as i64) * 3600
            - (slotset_begin_datetime.minute() as i64) * 60
            - (slotset_begin_datetime.second() as i64);

        let mut start_slot_id = None;
        while week_begin < max_time {
            for periodical in &self.ordered_periodicals {
                let periodical_begin = periodical.week_begin_time + week_begin;
                let periodical_end = periodical.week_end_time + week_begin;

                let (begin_slot_id, end_slot_id) =
                    if let Some(slots) = slot_set.split_slots_for_range(periodical_begin, periodical_end, start_slot_id) {
                        slots
                    } else {
                        // [periodical_begin, periodical_end] is completely before or after the slotset (disjoint ranges)
                        if periodical_end < slotset_begin {
                            continue; // Before the slotset
                        }
                        // After the slotset, we are done
                        break;
                    };
                start_slot_id = Some(begin_slot_id);

                let rules = self.rules_map.get(&periodical.rules_id).unwrap();
                let quotas = Quotas::new(
                    Rc::clone(slot_set.get_platform_config()),
                    periodical.rules_id,
                    Rc::clone(&rules.0),
                    Rc::clone(&rules.1),
                );
                for slot_id in slot_set.iter().between(begin_slot_id, end_slot_id).map(|s| s.id).collect::<Vec<i32>>() {
                    if slot_set.get_slot(slot_id).unwrap().quotas.rules_id() == slot_set.get_platform_config().quotas_config.default_rules_id {
                        slot_set.get_slot_mut(slot_id).unwrap().quotas = quotas.clone();
                    }
                }
            }
            week_begin += 7 * 24 * 3600;
        }
    }

    pub fn get_rules_by_id(&self, rules_id: i32) -> Option<&(Rc<QuotasMap>, Rc<QuotasTree>)> {
        self.rules_map.get(&rules_id)
    }

    pub fn ordered_periodicals(&self) -> &Vec<PeriodicalEntry> {
        &self.ordered_periodicals
    }
    pub fn ordered_oneshots(&self) -> &Vec<OneshotEntry> {
        &self.ordered_oneshot
    }
    pub fn rules_map(&self) -> HashMap<i32, Rc<QuotasMap>> {
        self.rules_map.iter().map(|(k, v)| (*k, Rc::clone(&v.0))).collect()
    }
    pub fn quotas_window_time_limit(&self) -> i64 {
        self.quotas_window_time_limit
    }
}

/// Module handling the parsing of temporal quotas from JSON configuration.
pub mod parsing {
    use crate::scheduler::quotas;
    use crate::scheduler::quotas::{QuotasMap, QuotasTree};
    use chrono::{DateTime, Local, NaiveDateTime};
    use serde_json::Value;
    use std::collections::HashMap;
    use std::rc::Rc;

    pub type PeriodicalsJson = Box<[(Box<str>, Box<str>, Box<str>)]>;
    pub type OneshotsJson = Box<[(Box<str>, Box<str>, Box<str>, Box<str>)]>;

    // Map day names to their corresponding weekday numbers (0=Monday, 6=Sunday)
    const DAYS_TO_NUM_ARRAY: [(&str, i32); 7] = [("mon", 0), ("tue", 1), ("wed", 2), ("thu", 3), ("fri", 4), ("sat", 5), ("sun", 6)];
    lazy_static::lazy_static! {
        static ref DAYS_TO_NUM: HashMap<&'static str, i32> = HashMap::from_iter(DAYS_TO_NUM_ARRAY);
    }

    /// Holds the raw and parsed quota root configuration entries,
    /// and allows to parse entries on demand and to reuse already parsed entries.
    pub struct QuotasConfigEntries {
        all_value: i64,
        id_counter: i32,
        json_entries: HashMap<Box<str>, Value>, // name -> Serde value representing the rules (or other data)
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
    #[derive(Debug)]
    pub struct PeriodicalEntry {
        pub(crate) week_begin_time: i64, // Begin time in seconds from week start (0-604800)
        pub(crate) week_end_time: i64,
        pub(crate) rules_id: i32,
        pub(crate) period_string: Box<str>,
        pub(crate) description: Box<str>,
    }
    /// Represents a fully parsed oneshot entry.
    #[derive(Debug)]
    pub struct OneshotEntry {
        pub(crate) begin_time: i64, // Epoch time in seconds
        pub(crate) end_time: i64,   // Epoch time in seconds
        pub(crate) rules_id: i32,
        pub(crate) begin_string: Box<str>,
        pub(crate) end_string: Box<str>,
        pub(crate) description: Box<str>,
    }

    impl QuotasConfigEntries {
        pub fn new(json_entries: HashMap<Box<str>, Value>, all_value: i64) -> Self {
            QuotasConfigEntries {
                all_value,
                id_counter: 0,
                json_entries,
                parsed_entries: HashMap::new(),
            }
        }
        /// Get the ID for a given rule name, parsing and storing it if not already done.
        fn get_rules_id(&mut self, rule_name: &str) -> i32 {
            if let Some((id, _quotas_map)) = self.parsed_entries.get(rule_name) {
                return *id;
            }
            if let Some(value) = self.json_entries.get(rule_name) {
                self.id_counter += 1;
                let parsed_value = serde_json::from_value::<HashMap<String, Vec<Value>>>(value.clone()).expect("Failed to parse quotas");
                let quotas_map = quotas::build_quotas_map(&parsed_value, self.all_value);
                self.parsed_entries.insert(rule_name.into(), (self.id_counter, quotas_map));
                return self.id_counter;
            }
            panic!("Rule name '{}' not found in quotas configuration entries", rule_name);
        }
        /// Consumes self and returns a map of rule IDs to their corresponding QuotasMap (the rules).
        pub fn to_rules_map(self) -> HashMap<i32, (Rc<QuotasMap>, Rc<QuotasTree>)> {
            self.parsed_entries
                .into_iter()
                .map(|(_, v)| (v.0, (Rc::new(v.1.clone()), Rc::new(QuotasTree::from(v.1)))))
                .collect()
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
                let mut end_time = end_time;
                if end_time < begin_time {
                    // Adds the entry for midnight to end_time on this same day
                    entries.push(PeriodicalEntry {
                        week_begin_time: day_begin,
                        week_end_time: end_time - 1,
                        rules_id,
                        period_string: periodical.period.clone(),
                        description: periodical.description.clone(),
                    });
                    // Set the end_time so the next entry goes from begin_time to midnight
                    end_time = 24 * 3600;
                }
                entries.push(PeriodicalEntry {
                    week_begin_time: day_begin + begin_time,
                    week_end_time: day_begin + end_time - 1,
                    rules_id,
                    period_string: periodical.period.clone(),
                    description: periodical.description.clone(),
                });
            }

            // Sort entries by begin_time
            entries.sort_by(|a, b| a.week_begin_time.cmp(&b.week_begin_time));
            entries
        }
    }

    impl OneshotEntry {
        pub(crate) fn from_json_entry(entry: &OneshotJsonEntry, config_entries: &mut QuotasConfigEntries) -> Self {
            let begin_time = parse_datetime(format!("{}:00", &entry.begin).as_str()).unwrap_or_else(|e| {
                panic!(
                    "Invalid begin time format '{}' in oneshot entry. Expected format: YYYY-MM-DD hh:mm. Error: {}",
                    entry.begin, e
                )
            });
            let end_time = parse_datetime(format!("{}:00", &entry.end).as_str()).unwrap_or_else(|e| {
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
                end_time: end_time.timestamp() - 1,
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
    fn parse_datetime(datetime_str: &str) -> Result<DateTime<Local>, String> {
        // Add seconds if not present
        let datetime_with_seconds = if datetime_str.len() == 16 {
            // Format: YYYY-MM-DD hh:mm
            format!("{}:00", datetime_str)
        } else {
            datetime_str.to_string()
        };

        // Parse with timezone support
        NaiveDateTime::parse_from_str(&datetime_with_seconds, "%Y-%m-%d %H:%M:%S")
            .map(|datetime| datetime.and_local_timezone(Local).unwrap())
            .map_err(|e| e.to_string())
    }
}
