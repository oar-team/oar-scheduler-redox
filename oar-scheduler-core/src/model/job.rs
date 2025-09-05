use crate::scheduler::hierarchy::HierarchyRequests;
use auto_bench_fct::auto_bench_fct_hy;
use log::warn;
use range_set_blaze::RangeSetBlaze;
use std::collections::HashMap;

pub type ProcSet = RangeSetBlaze<u32>;

#[derive(Debug, Clone)]
pub struct Job {
    pub id: i64,
    pub name: Option<Box<str>>,
    pub user: Option<Box<str>>,
    pub project: Option<Box<str>>,
    pub queue: Box<str>,
    pub types: HashMap<Box<str>, Option<Box<str>>>,
    pub moldables: Vec<Moldable>,
    /// This attribute is set to true if job has the type key "no_quotas", which means the job is not limited by quotas.
    pub no_quotas: bool,
    /// The time interval and resources assigned to the job.
    pub assignment: Option<JobAssignment>,
    /// Used for benchmarking the quotas hit count
    pub quotas_hit_count: u32,
    pub time_sharing: Option<TimeSharingType>,
    pub placeholder: PlaceholderType,
    /// List of job dependencies, tuples of (job_id, state, exit_code)
    pub dependencies: Vec<(i64, Box<str>, Option<i32>)>,
    /// Attribute used to store the start time of advance reservation jobs before they get an assignment.
    pub advance_reservation_begin: Option<i64>,
    /// Job submission epoch seconds (used for multifactor age)
    pub submission_time: i64,
    /// Job QoS score in [0.0, 1.0] (used for multifactor qos)
    pub qos: f64,
    /// Job nice value (>=1.0) (used for multifactor nice)
    pub nice: f64,
    pub karma: f64,
    pub message: String,
    pub state: String,
}

#[derive(Debug, Clone)]
pub struct JobAssignment {
    pub begin: i64,
    pub end: i64,
    pub resources: ProcSet,
    /// Index of the moldable used for this assignment in the job's moldables vector. In Python, this was the id of the moldable.
    pub moldable_index: usize,
}

#[derive(Debug, Clone)]
pub struct Moldable {
    pub id: i64,
    pub walltime: i64,
    pub requests: HierarchyRequests,
    /// Moldable’s cache key is only calculated at initialization. If fields are changed, the cache key must be recalculated.
    pub cache_key: Box<str>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TimeSharingType {
    /// timesharing=\*,\*
    AllAll,
    /// timesharing=user,* or timesharing=*,user
    UserAll,
    /// timesharing=*,name or timesharing=name,*
    AllName,
    /// timesharing=user,name
    UserName,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlaceholderType {
    /// Mark the job as a placeholder and name it by the String parameter,
    /// meaning it is not a real job but a placeholder for other jobs to be scheduled on its resources.
    Placeholder(Box<str>),
    /// Allow the job to use the resources of the placeholder referenced by the String parameter.
    Allow(Box<str>),
    None,
}

impl TimeSharingType {
    pub fn from_str(user: &str, job: &str) -> Self {
        match (user, job) {
            ("*", "*") => TimeSharingType::AllAll,
            ("*", "name") => TimeSharingType::AllName,
            ("name", "*") => TimeSharingType::AllName,
            ("user", "*") => TimeSharingType::UserAll,
            ("*", "user") => TimeSharingType::UserAll,
            ("user", "name") => TimeSharingType::UserName,
            _ => {
                warn!("Invalid time sharing type: user={}, job={}", user, job);
                TimeSharingType::AllAll // Default to AllAll if invalid
            }
        }
    }
    pub fn from_types(types: &HashMap<Box<str>, Option<Box<str>>>) -> Option<Self> {
        if let Some(value) = types.get(&Box::from("timesharing")) {
            if let Some(value) = value {
                let parts: Vec<&str> = value.split(',').collect();
                if parts.len() == 2 {
                    return Some(TimeSharingType::from_str(parts[0], parts[1]));
                } else {
                    warn!("Invalid time sharing type: {}", value);
                }
            } else {
                warn!("Invalid time sharing type: missing value");
            }
        }
        None
    }
}

impl PlaceholderType {
    pub fn from_types(types: &HashMap<Box<str>, Option<Box<str>>>) -> Self {
        if let Some(value) = types.get(&Box::from("placeholder")) {
            if let Some(name) = value {
                return PlaceholderType::Placeholder(name.clone());
            } else {
                warn!("Invalid placeholder type: missing name");
            }
        }
        if let Some(value) = types.get(&Box::from("allow")) {
            if let Some(name) = value {
                return PlaceholderType::Allow(name.clone());
            } else {
                warn!("Invalid allow type: missing name");
            }
        }
        PlaceholderType::None
    }
    pub fn is_placeholder(&self) -> bool {
        matches!(self, PlaceholderType::Placeholder(_))
    }
    pub fn is_allow(&self) -> bool {
        matches!(self, PlaceholderType::Allow(_))
    }
    pub fn is_none(&self) -> bool {
        matches!(self, PlaceholderType::None)
    }
}

impl Job {
    pub fn is_scheduled(&self) -> bool {
        self.assignment.is_some()
    }
    pub fn begin(&self) -> Option<i64> {
        if let Some(data) = &self.assignment { Some(data.begin) } else { None }
    }
    pub fn end(&self) -> Option<i64> {
        if let Some(data) = &self.assignment { Some(data.end) } else { None }
    }
    pub fn walltime(&self) -> Option<i64> {
        if let Some(data) = &self.assignment {
            Some(data.end - data.begin + 1)
        } else {
            None
        }
    }
    pub fn resource_count(&self) -> Option<u32> {
        if let Some(data) = &self.assignment {
            Some(data.resources.core_count())
        } else {
            None
        }
    }
    pub fn slot_set_name(&self) -> Box<str> {
        let mut slot_set_name: Box<str> = "default".into();
        // Manage inner jobs
        if self.types.contains_key::<Box<str>>(&"inner".into()) {
            slot_set_name = self.types[&Box::from("inner")].clone().unwrap();
        }
        slot_set_name
    }

    /// Returns true if the job can be scheduled using the cache.
    pub fn can_use_cache(&self) -> bool {
        self.time_sharing.is_none() && self.placeholder.is_none() && !self.no_quotas
    }
    /// Returns true if the job assignment can be used to insert a cache entry.
    pub fn can_set_cache(&self) -> bool {
        self.can_use_cache() && self.dependencies.is_empty()
    }
}

pub struct JobBuilder {
    id: i64,
    name: Option<Box<str>>,
    user: Option<Box<str>>,
    project: Option<Box<str>>,
    queue: Option<Box<str>>,
    types: HashMap<Box<str>, Option<Box<str>>>,
    moldables: Vec<Moldable>,
    assignment: Option<JobAssignment>,
    time_sharing: Option<TimeSharingType>,
    placeholder: Option<PlaceholderType>,
    dependencies: Vec<(i64, Box<str>, Option<i32>)>,
    advance_reservation_start_time: Option<i64>,
    submission_time: i64,
    message: String,
    state: String,
}

impl JobBuilder {
    pub fn new(id: i64) -> Self {
        JobBuilder {
            id,
            name: None,
            user: None,
            project: None,
            queue: None,
            types: HashMap::new(),
            moldables: vec![],
            assignment: None,
            time_sharing: None,
            placeholder: None,
            dependencies: Vec::new(),
            advance_reservation_start_time: None,
            submission_time: 0,
            message: String::new(),
            state: "Waiting".into(),
        }
    }
    pub fn moldable_auto(mut self, id: i64, walltime: i64, requests: HierarchyRequests) -> Self {
        self.moldables.push(Moldable::new(id, walltime, requests));
        self
    }
    pub fn moldable(mut self, moldable: Moldable) -> Self {
        self.moldables.push(moldable);
        self
    }
    pub fn moldables(mut self, moldables: Vec<Moldable>) -> Self {
        self.moldables = moldables;
        self
    }
    pub fn time_sharing(mut self, ts_type: TimeSharingType) -> Self {
        self.time_sharing = Some(ts_type);
        self
    }
    pub fn time_sharing_opt(mut self, time_sharing: Option<TimeSharingType>) -> Self {
        self.time_sharing = time_sharing;
        self
    }
    pub fn placeholder(mut self, placeholder: PlaceholderType) -> Self {
        self.placeholder = Some(placeholder);
        self
    }
    pub fn name(mut self, name: Box<str>) -> Self {
        self.name = Some(name);
        self
    }
    pub fn name_opt(mut self, name: Option<Box<str>>) -> Self {
        self.name = name;
        self
    }
    pub fn user(mut self, user: Box<str>) -> Self {
        self.user = Some(user);
        self
    }
    pub fn user_opt(mut self, user: Option<Box<str>>) -> Self {
        self.user = user;
        self
    }
    pub fn project(mut self, project: Box<str>) -> Self {
        self.project = Some(project);
        self
    }
    pub fn project_opt(mut self, project: Option<Box<str>>) -> Self {
        self.project = project;
        self
    }
    pub fn queue(mut self, queue: Box<str>) -> Self {
        self.queue = Some(queue);
        self
    }
    pub fn types(mut self, types: HashMap<Box<str>, Option<Box<str>>>) -> Self {
        self.types = types;
        self
    }
    pub fn add_type(mut self, key: Box<str>, value: Box<str>) -> Self {
        self.types.insert(key, Some(value));
        self
    }
    pub fn add_type_key(mut self, key: Box<str>) -> Self {
        self.types.insert(key, None);
        self
    }
    pub fn assign(mut self, assignment: JobAssignment) -> Self {
        self.assignment = Some(assignment);
        self
    }
    pub fn assign_opt(mut self, assignment: Option<JobAssignment>) -> Self {
        self.assignment = assignment;
        self
    }
    pub fn dependencies(mut self, dependencies: Vec<(i64, Box<str>, Option<i32>)>) -> Self {
        self.dependencies = dependencies;
        self
    }
    pub fn add_dependency(mut self, dep_job_id: i64, dep_job_state: Box<str>, dep_job_exit_code: Option<i32>) -> Self {
        self.dependencies.push((dep_job_id, dep_job_state, dep_job_exit_code));
        self
    }
    pub fn add_valid_dependency(self, dep_job_id: i64) -> Self {
        self.add_dependency(dep_job_id, "Waiting".into(), None)
    }
    pub fn set_advance_reservation_start_time(mut self, start_time: i64) -> Self {
        self.advance_reservation_start_time = Some(start_time);
        self
    }
    pub fn submission_time(mut self, submission_time: i64) -> Self {
        self.submission_time = submission_time;
        self
    }
    pub fn message(mut self, message: String) -> Self {
        self.message = message;
        self
    }
    pub fn state(mut self, state: String) -> Self {
        self.state = state;
        self
    }
    // Computes automatically the no_quotas from the types and TimeSharing and Placeholder if None.
    pub fn build(self) -> Job {
        Job {
            id: self.id,
            name: self.name,
            user: self.user,
            project: self.project,
            queue: self.queue.unwrap_or_else(|| Box::from("default")),
            no_quotas: self.types.contains_key(&Box::from("no_quotas")),
            time_sharing: self.time_sharing.or(TimeSharingType::from_types(&self.types)),
            placeholder: self.placeholder.unwrap_or(PlaceholderType::from_types(&self.types)),
            types: self.types,
            moldables: self.moldables,
            assignment: self.assignment,
            quotas_hit_count: 0,
            dependencies: self.dependencies,
            advance_reservation_begin: self.advance_reservation_start_time,
            submission_time: self.submission_time,
            qos: 0.0,
            nice: 1.0,
            karma: 0.0,
            message: self.message,
            state: self.state,
        }
    }
}

impl JobAssignment {
    pub fn new(begin: i64, end: i64, proc_set: ProcSet, moldable_index: usize) -> JobAssignment {
        JobAssignment {
            begin,
            end,
            resources: proc_set,
            moldable_index,
        }
    }
    pub fn count_resources(&self) -> u32 {
        self.resources.len() as u32
    }
}

impl Moldable {
    pub fn new(id: i64, walltime: i64, requests: HierarchyRequests) -> Moldable {
        Moldable {
            cache_key: format!("{}-{}", walltime, requests.get_cache_key()).into(),
            id,
            walltime,
            requests,
        }
    }
}

pub trait ProcSetCoresOp {
    fn sub_proc_set_with_cores(&self, core_count: u32) -> Option<ProcSet>;
    fn core_count(&self) -> u32;
}

impl ProcSetCoresOp for ProcSet {
    /// Tries to claim a subset of the `ProcSet` with the specified number of cores.
    /// Will not substract cores to the slots. This function will only try to find a fitting subset of cores
    /// If successful, return a new `ProcSet` that represents the selected available cores.
    /// Returns `None` if there are not enough cores available.
    #[auto_bench_fct_hy]
    fn sub_proc_set_with_cores(&self, core_count: u32) -> Option<ProcSet> {
        let available_cores = self.core_count();
        if available_cores < core_count {
            return None;
        }
        let mut selected_proc_set = ProcSet::new();
        let mut remaining_core_count = core_count;
        for range in self.ranges() {
            let core_count = range.end() - range.start() + 1;
            if remaining_core_count >= core_count {
                selected_proc_set |= &ProcSet::from_iter(range);
                if remaining_core_count == core_count {
                    break;
                }
                remaining_core_count -= core_count;
            } else {
                // Split and break
                let sub_range = *range.start()..=(range.start() + remaining_core_count - 1);
                selected_proc_set |= &ProcSet::from_iter(sub_range);
                break;
            }
        }
        Some(selected_proc_set)
    }
    #[inline]
    fn core_count(&self) -> u32 {
        self.len() as u32
    }
}
