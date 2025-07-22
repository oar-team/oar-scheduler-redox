use crate::models::models::{proc_set_to_python, Job};
use crate::platform::{PlatformConfig, ResourceSet};
use crate::scheduler::slot::Slot;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;
use pyo3::{Bound, IntoPyObject, PyErr, Python};
use pyo3::prelude::PyDictMethods;
use pyo3::types::{PyDict, PyList};
use oar3_rust_macros::{benchmark, benchmark_hierarchy};

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
    nb_periodicals: u32,

    ordered_oneshot_ids: Box<[u32]>,
    oneshots: Vec<String>,
    oneshots_begin: Option<i64>,
    oneshots_end: Option<i64>,
    nb_oneshots: u32,

    quotas_rules_list: Vec<String>,
    quotas_rules2id: HashMap<String, u32>,
    quotas_ids2rules: HashMap<u32, String>,
    nb_quotas_rules: u32,
}
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
impl<'a> IntoPyObject<'a> for &QuotasConfig {
    type Target = PyDict;
    type Output = Bound<'a, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'a>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);

        dict.set_item("enabled", self.enabled).unwrap();
        // Other fields not in use for now.

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
}

/// key: (queue, project, job_type, user)
pub type QuotasKey = (Box<str>, Box<str>, Box<str>, Box<str>);

/// Used to store the quotas maximum values for a certain rule, and to track a slot current quota usage
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct QuotasValue {
    resources: Option<u32>,       // Number of busy resources
    running_jobs: Option<u32>,    // Number of running jobs
    resources_times: Option<i64>, // Resource time in use (nb_resources * walltime)
}
impl QuotasValue {
    pub fn new(resources: Option<u32>, running_jobs: Option<u32>, resources_times: Option<i64>) -> Self {
        QuotasValue {
            resources,
            running_jobs,
            resources_times,
        }
    }
    /// Increments the values of `self` by the given amounts.
    /// Used by the counters to track the current usage of quotas.
    pub fn increment(&mut self, resources: u32, running_jobs: u32, resources_times: i64) {
        if let Some(r) = &mut self.resources {
            *r += resources;
        }
        if let Some(rj) = &mut self.running_jobs {
            *rj += running_jobs;
        }
        if let Some(rt) = &mut self.resources_times {
            *rt += resources_times;
        }
    }
    /// Combines the values of `self` and `other` by taking the maximum for resources and running_jobs,
    /// and summing resources_times (as resources_times depend on the time).
    /// Used to combine slot quotas and make checks against larger time windows.
    pub fn combine(&mut self, other: &QuotasValue) {
        if let Some(r) = &mut self.resources {
            if let Some(other_r) = other.resources {
                *r = (*r).max(other_r);
            }
        }
        if let Some(rj) = &mut self.running_jobs {
            if let Some(other_rj) = other.running_jobs {
                *rj = (*rj).max(other_rj);
            }
        }
        if let Some(rt) = &mut self.resources_times {
            if let Some(other_rt) = other.resources_times {
                *rt += other_rt;
            }
        }
    }
    /// Checks if the values in `counts` exceed the limits from `self`.
    /// If any limit is exceeded, it returns a tuple with a description and the limit value.
    pub fn check(&self, counts: &QuotasValue) -> Option<(Box<str>, i64)> {
        if let Some(resources) = self.resources {
            if let Some(counted_resources) = counts.resources {
                if counted_resources > resources {
                    return Some(("Resources exceeded".into(), resources as i64));
                }
            }
        }
        if let Some(running_jobs) = self.running_jobs {
            if let Some(counted_running_jobs) = counts.running_jobs {
                if counted_running_jobs > running_jobs {
                    return Some(("Running jobs exceeded".into(), running_jobs as i64));
                }
            }
        }
        if let Some(resources_times) = self.resources_times {
            if let Some(counted_resources_times) = counts.resources_times {
                if counted_resources_times > resources_times {
                    return Some(("Resources times exceeded".into(), resources_times));
                }
            }
        }
        None
    }
    /// Converts an array of serde values integer Number or String to a QuotasValue.
    /// Values "ALL" will be replaced by the `all_value` parameter, and values "x*ALL" will multiply the `all_value` by the float `x`.
    /// Examples: `[100, "ALL", "0.5*ALL"]`, `["34.5", "ALL", "2*ALL"]` are valid inputs.
    #[allow(dead_code)]
    pub fn from_serde_values(values: &[Value], all_value: i64) -> QuotasValue {
        let parsed = values
            .iter()
            .map(|v| match v {
                Value::Number(n) => n
                    .as_i64()
                    .expect(format!("Invalid quotas value number: expected i64, got {}", n).as_str()),
                Value::String(s) => {
                    if s == "ALL" {
                        all_value
                    } else if s.ends_with("*ALL") {
                        (s[..s.len() - 4].parse::<f64>().expect(
                            format!(
                                "Invalid quotas value number: excepted f64 multiplicator, got {}",
                                s[..s.len() - 4].to_string()
                            )
                            .as_str(),
                        ) * all_value as f64) as i64
                    } else {
                        s.parse::<i64>()
                            .expect(format!("Invalid quotas value number: excepted i64, got {}", s).as_str())
                    }
                }
                _ => 0,
            })
            .collect::<Vec<i64>>();

        QuotasValue {
            resources: Some(parsed[0] as u32),
            running_jobs: Some(parsed[1] as u32),
            resources_times: Some(parsed[2]),
        }
    }
}
impl Default for QuotasValue {
    fn default() -> Self {
        QuotasValue {
            resources: Some(0),
            running_jobs: Some(0),
            resources_times: Some(0),
        }
    }
}

/// Represent a set of Quotas limits or counters.
/// Keys are tuples of (queue, project, job_type, user).
pub type QuotasMap = HashMap<QuotasKey, QuotasValue>;

/// Parses a JSON string representing quotas into a QuotasMap.
/// The JSON must be a mapping between a string key (formatted as `queue,project,job_type,user` with names or `*` or `/`)
///     and an array of values (see `QuotasValue::from_serde_values`).
#[allow(dead_code)]
pub fn quotas_map_from_json(json: &str, all_value: i64) -> QuotasMap {
    let quotas = serde_json::from_str::<HashMap<String, Vec<Value>>>(json).expect("Invalid quotas JSON format");
    quotas
        .iter()
        .map(|(key, value)| {
            let key_parts: Vec<&str> = key.split(',').collect();
            if key_parts.len() != 4 {
                panic!(
                    "Invalid quotas key format: expected 4 parts, got {} parts in {}",
                    key_parts.len(),
                    key.as_str()
                );
            }
            let queue = key_parts[0].into();
            let project = key_parts[1].into();
            let job_type = key_parts[2].into();
            let user = key_parts[3].into();

            let quotas_value = QuotasValue::from_serde_values(value, all_value);
            ((queue, project, job_type, user), quotas_value)
        })
        .collect()
}

/// Represent a set of Quotas limits or counters organized in a tree structure.
/// The tree is a nested HashMap where each level corresponds to a Quota dimension (queue, project, job_type, user).
type QuotasTreeMap = HashMap<Box<str>, HashMap<Box<str>, HashMap<Box<str>, HashMap<Box<str>, QuotasValue>>>>;
#[derive(Debug, Clone)]
pub struct QuotasTree(QuotasTreeMap);
impl From<QuotasMap> for QuotasTree {
    fn from(rules: QuotasMap) -> Self {
        let mut tree_map = QuotasTreeMap::new();
        for ((queue, project, job_type, user), value) in rules {
            tree_map
                .entry(queue)
                .or_default()
                .entry(project)
                .or_default()
                .entry(job_type)
                .or_default()
                .entry(user)
                .insert_entry(value);
        }
        QuotasTree(tree_map)
    }
}

trait QuotasTreeNodeTrait {
    fn first_valid_key(&self, key: &str) -> Option<Box<str>>;
}
impl<T> QuotasTreeNodeTrait for HashMap<Box<str>, T> {
    /// Finds the first valid key in a QuotasTreeMap HashMap of a specific level.
    /// It checks in order for a key named `key`, then for '*', and at last for '/'.
    /// Returns the first valid key found as `Some(Box<str>)`.
    fn first_valid_key(&self, key: &str) -> Option<Box<str>> {
        let mut has_all = false;
        let mut has_any = false;
        let has_key = self
            .keys()
            .into_iter()
            .find(|k| {
                if k.as_ref() == key {
                    return true;
                }
                if !has_all && k.as_ref() == "*" {
                    has_all = true;
                }
                if !has_any && k.as_ref() == "/" {
                    has_any = true;
                }
                false
            })
            .map(|k| k.clone());
        has_key.or_else(|| {
            if has_any {
                Some("/".into())
            } else if has_all {
                Some("*".into())
            } else {
                None
            }
        })
    }
}

/// Tracks quotas for a single slot
#[derive(Clone)]
pub struct Quotas {
    counters: QuotasMap,
    rules_id: i32, // Used to differentiate Quotas instances with different rules.
    rules: Rc<QuotasMap>,
    rules_tree: Rc<QuotasTree>,
    platform_config: Rc<PlatformConfig>,
}
impl Debug for Quotas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Quotas")
            .field("counters", &self.counters)
            .field("rules_id", &self.rules_id)
            .field("rules", &self.rules)
            .field("rules_tree", &self.rules_tree)
            .finish()
    }
}
/// Quotas count the resources, running jobs, and resource times for jobs with a certain queue, project, job_type and user.
/// Counters are stored in a HashMap with keys being tuples of (queue, project, job_type, user) which might be the match all character "\*", and values being `QuotasValue`.
/// When a job is scheduled, all relevant counters are incremented, i.e., counters (*, *, *, *), (queue, *, *, *), (queue, project, *, *), ...
/// Quotas also stores a set of rules with the same data structure (and a copy of it in a tree data structure), but with limits instead of counters, and the support for a "for each" rule with a "/" key.
/// The "/" key allows defining a rule applicable to all queues, projects, job types, or users, but with separate counters for each one.
/// One can check if the counters for a job exceed the limits defined in the rules by calling `Quotas::check`.
/// Checking if a job’s counters exceed the limits finds the rule that is the most specific for the job, i.e., the one with the least number of wildcards, and checks the counters of that rule key.
impl Quotas {
    /// Creates a new Quotas instance with the given configuration and rules.
    /// As rules are also mostly common in Quotas instances, it is also a Rc.
    pub fn new(platform_config: Rc<PlatformConfig>) -> Quotas {
        Quotas {
            counters: QuotasMap::default(),
            rules_id: platform_config.quotas_config.default_rules_id,
            rules: Rc::clone(&platform_config.quotas_config.default_rules),
            rules_tree: Rc::clone(&platform_config.quotas_config.default_rules_tree),
            platform_config,
        }
    }

    /// Increment the Quotas counters for a job.
    /// The job does not need to be scheduled yet, hence the slot width (end - begin + 1) and resource_count are provided.
    pub fn increment_for_job(&mut self, job: &Job, slot_width: i64, resource_count: u32) {
        let resources = resource_count;
        let running_jobs = 1;
        let resources_times = slot_width * resources as i64;

        let matched_queues = ["*", &job.queue];
        let matched_projects = ["*", &job.project];
        // Tracking only the types configured in QuotasConfig::job_types.
        let matched_job_types = self
            .platform_config
            .quotas_config
            .tracked_job_types
            .iter()
            .filter(|t| &(***t) == "*" || job.types.contains(&t.to_string()))
            .collect::<Box<[&Box<str>]>>();
        let matched_users = ["*", &job.user];

        matched_queues.iter().for_each(|queue| {
            matched_projects.iter().for_each(|project| {
                matched_job_types.iter().for_each(|job_type| {
                    matched_users.iter().for_each(|user| {
                        let value = self
                            .counters
                            .entry(((*queue).into(), (*project).into(), (*job_type).clone(), (*user).into()))
                            .or_insert(QuotasValue::new(Some(0), Some(0), Some(0)));
                        value.increment(resources, running_jobs, resources_times);
                    });
                });
            });
        });
    }

    /// Combines the counters of `self` and `quotas` by taking the maximum for resources and running_jobs,
    /// and summing resources_times as it depends on the time.
    /// Used to combine slot quotas and make checks against larger time windows.
    pub fn combine(&mut self, quotas: &Quotas) {
        for (key, value) in &quotas.counters {
            self.counters.entry(key.clone()).and_modify(|v| v.combine(value)).or_insert(value.clone());
        }
    }

    /// Finds the rule key that should be applied to `job` (i.e., the QuotasMapKey).
    /// The rule is found by looking at `Quotas::rules_tree` with the following key priority: named > '/' > '*'
    /// It returns two keys, the first one being the same as the second one, but with the "/" replaced by the actual name, and the value QuotasValue (the limits).
    pub fn find_applicable_rule(&self, job: &Job) -> Option<(QuotasKey, QuotasKey, &QuotasValue)> {
        let key_queue = job.queue.as_str();
        let key_project = job.project.as_str();
        let key_job_type = job.types.get(0).map(|s| s.clone().into_boxed_str()).unwrap_or("*".into()); // TODO: document that only the first job type is used for quotas
        let key_user = job.user.as_str();

        let mut rule_key = None;
        let mut rule_value = None;

        if let Some(key_queue) = self.rules_tree.0.first_valid_key(key_queue) {
            let map = self.rules_tree.0.get(&key_queue).unwrap();
            if let Some(key_project) = map.first_valid_key(key_project) {
                let map = map.get(&key_project).unwrap();
                if let Some(key_job_type) = map.first_valid_key(key_job_type.as_ref()) {
                    let map = map.get(&key_job_type).unwrap();
                    if let Some(key_user) = map.first_valid_key(key_user) {
                        rule_value = map.get(&key_user);
                        rule_key = Some((key_queue, key_project, key_job_type, key_user));
                    }
                }
            }
        }

        let mut rule_key_counter = rule_key.clone();
        // If the key is "/", replace by the queue name, project name, job type, or user.
        if let Some((key_queue, key_project, _key_job_type, key_user)) = &mut rule_key_counter {
            if key_queue.as_ref() == "/" {
                *key_queue = job.queue.clone().into();
            }
            if key_project.as_ref() == "/" {
                *key_project = job.project.clone().into();
            }
            // "/" is not available for projects
            if key_user.as_ref() == "/" {
                *key_user = job.user.clone().into();
            }

            return Some((rule_key_counter.unwrap(), rule_key.unwrap(), rule_value.unwrap()));
        }
        None
    }
    /// Checks if the quotas counters for the job `job` exceeds the limits.
    /// WARNING: This function assumes that the counters have already been updated with `Quotas::update_for_job`.
    ///     This function does not update the counters and only checks if the counters for the job exceed the limits defined in the rules.
    /// If not, return Some with a description, the exceeded rule key, and the exceeded limit value.
    pub fn check(&self, job: &Job) -> Option<(Box<str>, QuotasKey, i64)> {
        let (rule_key_counter, rule_key, rule_value) = self.find_applicable_rule(job)?;
        let counts = self.counters.get(&rule_key_counter)?;
        rule_value.check(counts).map(|(description, limit)| (description, rule_key, limit))
    }
}

/// The job does not need to be scheduled yet, hence the walltime and resource_count are provided.
pub fn check_slots_quotas<'s>(slots: Vec<&Slot>, job: &Job, walltime: i64, resource_count: u32) -> Option<(Box<str>, QuotasKey, i64)> {
    let mut slots_quotas: HashMap<i32, (Quotas, i64)> = HashMap::new();
    // Combine in slot_quotas all quotas with the total duration they cover, grouped by rules_id.
    for slot in slots {
        let quotas = slot.quotas();
        slots_quotas
            .entry(quotas.rules_id)
            .and_modify(|(q, duration)| {
                q.combine(&quotas);
                *duration += (slot.end() - slot.begin() + 1);
            })
            .or_insert((quotas.clone(), slot.end() - slot.begin() + 1));
    }
    check_quotas(slots_quotas, job, resource_count)

}
/// The job does not need to be scheduled yet, hence the resource_count is provided.
#[benchmark]
pub fn check_quotas<'s>(mut slots_quotas: HashMap<i32, (Quotas, i64)>, job: &Job, resource_count: u32) -> Option<(Box<str>, QuotasKey, i64)> {
    // Check each combined quotas against the job.
    for (_, (quotas, duration)) in slots_quotas.iter_mut() {
        // Checking if after updating, it exceeds the rules.
        quotas.increment_for_job(job, *duration, resource_count); // Doing it on a clone of quotas to avoid modifying the original.
        let res = quotas.check(job);
        if res.is_some() {
            return res;
        }
    }
    None
}
