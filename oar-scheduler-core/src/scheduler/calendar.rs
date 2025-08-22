use crate::scheduler::quotas;
use crate::scheduler::quotas::{QuotasMap, QuotasTree};
use crate::scheduler::quotas_parsing::{OneshotEntry, OneshotJsonEntry, OneshotsJson, PeriodicalEntry, PeriodicalJsonEntry, PeriodicalsJson, QuotasConfigEntries};
#[cfg(feature = "pyo3")]
use pyo3::{prelude::PyDictMethods, types::PyDict, Bound, IntoPyObject, PyErr, Python};
use serde_json::Value;
use std::collections::HashMap;
use std::rc::Rc;

/// Configuration of quotas stored in PlatformConfig.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct QuotasConfig {
    pub enabled: bool,
    pub calendar: Option<Calendar>,
    pub default_rules_id: i32,
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
    pub fn load_from_file(path: &str, enabled: bool, all_value: i64) -> Self {
        let json = std::fs::read_to_string(path).expect("Failed to read quotas config file");
        Self::load_from_json(json, enabled, all_value)
    }
    pub fn load_from_json(json: String, enabled: bool, all_value: i64) -> Self {
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
            Some(Calendar::from_config(entries, periodical, oneshot, all_value))
        } else {
            None
        };
        QuotasConfig::new(enabled, calendar, quotas.unwrap_or_default(), job_types)
    }
}


#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct Calendar {
    config: String,
    quotas_period: String,

    period_end: i64,
    quotas_window_time_limit: String,

    ordered_periodical_ids: Box<[u32]>,

    op_index: u32,
    periodicals: Vec<String>,

    ordered_oneshot_ids: Box<[u32]>,
    oneshots: Vec<String>,
    oneshots_begin: Option<i64>,
    oneshots_end: Option<i64>,

    quotas_rules_list: Vec<String>,
    quotas_rules2id: HashMap<String, u32>,
    quotas_ids2rules: HashMap<u32, String>,
}

impl Calendar {
    fn from_config(entries: HashMap<Box<str>, Value>, periodicals: Option<PeriodicalsJson>, oneshots: Option<OneshotsJson>, all_values: i64) -> Self {
        let mut config_entries = QuotasConfigEntries::new(entries, all_values);

        if let Some(periodicals) = periodicals {
            let entries = periodicals
                .into_iter()
                .map(|periodical| PeriodicalJsonEntry::from_tuple(&periodical))
                .map(|periodical| PeriodicalEntry::from_json_entry(&periodical, &mut config_entries))
                .flatten()
                .collect::<Vec<PeriodicalEntry>>();
            // TODO: build Calendar structure based on the PeriodicalEntry items
        }
        if let Some(oneshots) = oneshots {
            let entries = oneshots
                .into_iter()
                .map(|oneshot| OneshotJsonEntry::from_tuple(&oneshot))
                .map(|oneshot| OneshotEntry::from_json_entry(&oneshot, &mut config_entries))
                .collect::<Vec<OneshotEntry>>();
            // TODO: build Calendar structure based on the OneshotEntry items
        }

        let quotas_rules = config_entries.to_parsed_entries();

        Self {
            config: "".to_string(),
            quotas_period: "".to_string(),
            period_end: 0,
            quotas_window_time_limit: "".to_string(),
            ordered_periodical_ids: Box::new([]),
            op_index: 0,
            periodicals: vec![],
            ordered_oneshot_ids: Box::new([]),
            oneshots: vec![],
            oneshots_begin: None,
            oneshots_end: None,
            quotas_rules_list: vec![],
            quotas_rules2id: Default::default(),
            quotas_ids2rules: Default::default(),
        }
    }
}
