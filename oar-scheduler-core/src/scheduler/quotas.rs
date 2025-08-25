use crate::models::Job;
use crate::platform::PlatformConfig;
use crate::scheduler::slotset::SlotIterator;
use auto_bench_fct::auto_bench_fct_hy;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;

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
                Value::Number(n) => {
                    let n = n
                        .as_i64()
                        .expect(format!("Invalid quotas value number: expected i64, got {}", n).as_str());
                    if n < 0 { None } else { Some(n) }
                }
                Value::String(s) => {
                    if s == "ALL" {
                        Some(all_value)
                    } else if s.ends_with("*ALL") {
                        Some(
                            (s[..s.len() - 4].parse::<f64>().expect(
                                format!(
                                    "Invalid quotas value number: excepted f64 multiplicator, got {}",
                                    s[..s.len() - 4].to_string()
                                )
                                .as_str(),
                            ) * all_value as f64) as i64,
                        )
                    } else {
                        let n = s
                            .parse::<i64>()
                            .expect(format!("Invalid quotas value number: excepted i64, got {}", s).as_str());
                        if n < 0 { None } else { Some(n) }
                    }
                }
                _ => None,
            })
            .collect::<Vec<Option<i64>>>();

        QuotasValue {
            resources: parsed[0].map(|i| i as u32),
            running_jobs: parsed[1].map(|i| i as u32),
            resources_times: parsed[2].map(|i| i * 3600), // Converting hours to seconds
        }
    }
}
impl Default for QuotasValue {
    fn default() -> Self {
        QuotasValue {
            resources: None,
            running_jobs: None,
            resources_times: None,
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
pub fn build_quotas_map(quotas_map: &HashMap<String, Vec<Value>>, all_value: i64) -> QuotasMap {
    quotas_map
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
    fn first_valid_key(&self, key: Option<&str>) -> Option<Box<str>>;
    fn first_valid_key_multiple(&self, keys: &[&Box<str>]) -> Option<Box<str>>;
}
impl<T> QuotasTreeNodeTrait for HashMap<Box<str>, T> {
    /// Finds the first valid key in a QuotasTreeMap HashMap of a specific level.
    /// It checks in order for a key named `key`, then for '*', and at last for '/'.
    /// Returns the first valid key found as `Some(Box<str>)`.
    fn first_valid_key(&self, key: Option<&str>) -> Option<Box<str>> {
        let mut has_all = false;
        let mut has_any = false;
        let has_key = self
            .keys()
            .into_iter()
            .find(|k| {
                if let Some(key) = key {
                    if k.as_ref() == key {
                        return true;
                    }
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
    fn first_valid_key_multiple(&self, keys: &[&Box<str>]) -> Option<Box<str>> {
        let mut has_all = false;
        let mut has_any = false;
        let valid_key = self.keys().into_iter().find(|k| {
            if keys.contains(k) {
                return true;
            }
            if !has_all && k.as_ref() == "*" {
                has_all = true;
            }
            if !has_any && k.as_ref() == "/" {
                has_any = true;
            }
            false
        });
        if let Some(key) = valid_key {
            return Some(key.clone());
        }
        if has_any {
            Some("/".into())
        } else if has_all {
            Some("*".into())
        } else {
            None
        }
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
    pub fn new(platform_config: Rc<PlatformConfig>, rules_id: i32, rules: Rc<QuotasMap>, rules_tree: Rc<QuotasTree>) -> Quotas {
        Quotas {
            counters: QuotasMap::default(),
            rules_id,
            rules,
            rules_tree,
            platform_config,
        }
    }
    /// Creates a new Quotas instance with the given configuration and rules.
    /// As rules are also mostly common in Quotas instances, it is also a Rc.
    pub fn from_platform_config(platform_config: Rc<PlatformConfig>) -> Quotas {
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
        if job.types.contains_key("container") {
            // Job container does not increment quotas counters but do are subject to quotas limits.
            return;
        }
        let resources = resource_count;
        let running_jobs = 1;
        let resources_times = slot_width * resources as i64;

        let matched_queues = ["*", &job.queue];
        let mut matched_projects = vec!["*"];
        if let Some(project) = job.project.as_ref() {
            matched_projects.push(project);
        }
        // Tracking only the types configured in QuotasConfig::job_types.
        let matched_job_types = self
            .platform_config
            .quotas_config
            .tracked_job_types
            .iter()
            .filter(|t| &(***t) == "*" || job.types.contains_key(*t))
            .collect::<Box<[&Box<str>]>>();

        let mut matched_users = vec!["*"];
        if let Some(user) = job.user.as_ref() {
            matched_users.push(user);
        }

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
    /// Quotas rules are checked against all `job.types` keys, but as the map keys are not ordered,
    /// it will lead to undefined behavior if there are quotas rules for different job types and jobs that have these job types at the same time.
    /// It returns two keys, the first one being the same as the second one, but with the "/" replaced by the actual name, and the value QuotasValue (the limits).
    pub fn find_applicable_rule(&self, job: &Job) -> Option<(QuotasKey, QuotasKey, &QuotasValue)> {
        let key_queue = Some(job.queue.as_ref());
        let key_project = job.project.as_ref().map(|s| s.as_ref());
        let key_job_types = job.types.iter().map(|(k, _v)| k).collect::<Box<[&Box<str>]>>();
        let key_user = job.user.as_ref().map(|s| s.as_ref());

        let mut rule_key = None;
        let mut rule_value = None;

        if let Some(key_queue) = self.rules_tree.0.first_valid_key(key_queue) {
            let map = self.rules_tree.0.get(&key_queue).unwrap();
            if let Some(key_project) = map.first_valid_key(key_project) {
                let map = map.get(&key_project).unwrap();
                if let Some(key_job_type) = map.first_valid_key_multiple(key_job_types.as_ref()) {
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
            if let Some(project) = &job.project {
                if key_project.as_ref() == "/" {
                    *key_project = project.clone().into();
                }
            }
            // "/" is not available for job types
            if let Some(user) = &job.user {
                if key_user.as_ref() == "/" {
                    *key_user = user.clone().into();
                }
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

    pub fn rules_id(&self) -> i32 {
        self.rules_id
    }
}

/// The job does not need to be scheduled yet; hence the start time, end time and resource_count are provided.
/// `slots` are the encompassing slots for the job.
/// Returns Some if quotas are exceeded, with a description, the rule key, and the limit value.
pub fn check_slots_quotas<'s>(slots: SlotIterator, job: &Job, start: i64, end: i64, resource_count: u32) -> Option<(Box<str>, QuotasKey, i64)> {
    let mut slots_quotas: HashMap<i32, (Quotas, i64)> = HashMap::new();

    // Combine in slot_quotas all quotas with the total duration they cover, grouped by rules_id.
    for slot in slots {
        let quotas = slot.quotas();
        let used_width = slot.end().min(end) - slot.begin().max(start) + 1;
        slots_quotas
            .entry(quotas.rules_id)
            .and_modify(|(q, duration)| {
                q.combine(&quotas);
                *duration += used_width;
            })
            .or_insert((quotas.clone(), used_width));
    }
    check_quotas(slots_quotas, job, resource_count)
}
/// The job does not need to be scheduled yet, hence the resource_count is provided.
/// Returns Some if quotas are exceeded, with a description, the rule key, and the limit value.
#[auto_bench_fct_hy]
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
