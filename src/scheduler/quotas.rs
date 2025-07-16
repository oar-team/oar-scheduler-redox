use crate::models::models::Job;
use crate::platform::PlatformConfig;
use std::collections::HashMap;
use std::rc::Rc;

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
#[derive(Debug, Clone)]
pub struct QuotasConfig {
    enabled: bool,
    calendar: Option<Calendar>,
    default_rules: Rc<QuotasMap>,
    default_rules_tree: Rc<QuotasTree>,
    tracked_job_types: Box<[Box<str>]>, // called job_types in python
}
impl Default for QuotasConfig {
    fn default() -> Self {
        // TODO : real default value.
        QuotasConfig {
            enabled: false,
            calendar: None,
            default_rules: Rc::new(QuotasMap::default()),
            default_rules_tree: Rc::new(QuotasTree::from(QuotasMap::default())),
            tracked_job_types: Box::new([]),
        }
    }
}

impl QuotasConfig {
    /// Creates a new QuotasConfig with the given parameters.
    pub fn new(enabled: bool, calendar: Option<Calendar>, default_rules: QuotasMap, tracked_job_types: Box<[Box<str>]>) -> Self {
        let default_rules_tree = Rc::new(QuotasTree::from(default_rules.clone()));
        QuotasConfig {
            enabled,
            calendar,
            default_rules: Rc::new(default_rules),
            default_rules_tree,
            tracked_job_types,
        }
    }
}

/// key: (queue, project, job_type, user)
type QuotasKey = (Box<str>, Box<str>, Box<str>, Box<str>);

/// Used to store the quotas maximum values for a certain rule, and to track a slot current quota usage
#[derive(Debug, Clone)]
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
    pub fn check(&self, counts: &QuotasValue) -> Option<(&str, i64)> {
        if let Some(resources) = self.resources {
            if resources >= 0 {
                if let Some(counted_resources) = counts.resources {
                    if counted_resources > resources {
                        return Some(("resources exceeded", resources as i64));
                    }
                }
            }
        }
        if let Some(running_jobs) = self.running_jobs {
            if running_jobs >= 0 {
                if let Some(counted_running_jobs) = counts.running_jobs {
                    if counted_running_jobs > running_jobs {
                        return Some(("running jobs exceeded", running_jobs as i64));
                    }
                }
            }
        }
        if let Some(resources_times) = self.resources_times {
            if resources_times >= 0 {
                if let Some(counted_resources_times) = counts.resources_times {
                    if counted_resources_times > resources_times {
                        return Some(("resources times exceeded", resources_times));
                    }
                }
            }
        }
        None
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
type QuotasMap = HashMap<QuotasKey, QuotasValue>;

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
    rules: Rc<QuotasMap>,
    rules_tree: Rc<QuotasTree>,
    platform_config: Rc<PlatformConfig>,
}
impl Quotas {
    /// Creates a new Quotas instance with the given configuration and rules.
    /// As rules are also mostly common in Quotas instances, it is also a Rc.
    pub fn new(platform_config: Rc<PlatformConfig>, rules: Option<Rc<QuotasMap>>) -> Quotas {
        let rules = match rules {
            Some(r) => (Rc::new(QuotasTree::from((*r).clone())), r),
            None => (
                Rc::clone(&platform_config.quotas_config.default_rules_tree),
                Rc::clone(&platform_config.quotas_config.default_rules),
            ),
        };
        Quotas {
            counters: QuotasMap::default(),
            rules: rules.1,
            rules_tree: rules.0,
            platform_config,
        }
    }

    /// Increment the Quotas counters for a scheduled job.
    pub fn update_for_job(&mut self, job: &Job) {
        if let Some(scheduled_data) = &job.scheduled_data {
            let resources = scheduled_data.count_resources();
            let running_jobs = 1;
            let resources_times = (scheduled_data.end - scheduled_data.begin) * resources as i64;

            // Tracking only the types configured in QuotasConfig::job_types.
            let matched_queues = ["*", &job.queue];
            let matched_projects = ["*", &job.project];
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
                                .entry((queue.clone().into(), project.clone().into(), (*job_type).clone(), user.clone().into()))
                                .or_default();
                            value.increment(resources, running_jobs, resources_times);
                        });
                    });
                });
            });
        } else {
            panic!("Job must have scheduled data to update slot quotas counters.");
        }
    }

    /// Combines the counters of `self` and `quotas` by taking the maximum for resources and running_jobs,
    /// and summing resources_times as it depends on the time.
    /// Used to combine slot quotas and make checks against larger time windows.
    pub fn combine(&mut self, quotas: &Quotas) {
        for (key, value) in &quotas.counters {
            self.counters.entry(key.clone()).and_modify(|v| v.combine(value));
        }
    }

    /// Finds the rule key that should be applied to `job` (i.e., the QuotasMapKey).
    /// The rule is found by looking at `Quotas::rules_tree` with the following key priority: named > '/' > '*'
    /// It returns the QuotasValue (the limits) and two keys, the first one being the same as the second one, but with the "/" replaced by the actual name.
    pub fn find_applicable_rule(&self, job: &Job) -> Option<(QuotasKey, QuotasKey, &QuotasValue)> {
        let key_queue = job.queue.as_str();
        let key_project = job.project.as_str();
        let key_job_types = job
            .types
            .iter()
            .cloned()
            .map(|t| t.into_boxed_str())
            .chain(self.platform_config.quotas_config.tracked_job_types.iter().cloned())
            .collect::<Box<[Box<str>]>>();
        let key_user = job.user.as_str();

        let mut rule_key = None;
        let mut rule_value = None;

        if let Some(key_queue) = self.rules_tree.0.first_valid_key(key_queue) {
            let map = self.rules_tree.0.get(&key_queue).unwrap();
            if let Some(key_project) = map.first_valid_key(key_project) {
                let map = map.get(&key_project).unwrap();
                for key_job_type in key_job_types {
                    if let Some(key_job_type) = map.first_valid_key(key_job_type.as_ref()) {
                        let map = map.get(&key_job_type).unwrap();
                        if let Some(key_user) = map.first_valid_key(key_user) {
                            rule_value = map.get(&key_user);
                            rule_key = Some((key_queue, key_project, key_job_type, key_user));
                        }
                        break;
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
    /// Checks if the job is within the quotas limits.
    /// If not, return Some with a description, the exceeded rule key, and the exceeded limit value.
    pub fn check(&self, job: &Job) -> Option<(&str, QuotasKey, i64)> {
        let (rule_key_counter, rule_key, rule_value) = self.find_applicable_rule(job)?;
        let counts = self.counters.get(&rule_key_counter)?;
        rule_value.check(counts).map(|(description, limit)| (description, rule_key, limit))
    }
}
