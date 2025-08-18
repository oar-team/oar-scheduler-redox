use crate::scheduler::hierarchy::HierarchyRequests;
use auto_bench_fct::auto_bench_fct_hy;
use log::warn;
#[cfg(feature = "pyo3")]
use pyo3::prelude::{PyAnyMethods, PyListMethods, PyModule};
#[cfg(feature = "pyo3")]
use pyo3::types::{PyDict, PyList, PyTuple};
#[cfg(feature = "pyo3")]
use pyo3::{Bound, IntoPyObject, PyAny, PyErr, Python};
use range_set_blaze::RangeSetBlaze;
use std::collections::HashMap;

pub type ProcSet = RangeSetBlaze<u32>;
#[cfg(feature = "pyo3")]
pub fn proc_set_to_python<'a>(py: Python<'a>, proc_set: &ProcSet) -> Bound<'a, PyAny> {
    let procset_module = PyModule::import(py, "procset").unwrap();
    let procset_class = procset_module.getattr("ProcSet").unwrap();
    let procint_class = procset_module.getattr("ProcInt").unwrap();

    let list = PyList::empty(py);
    for range in proc_set.ranges() {
        list.append(procint_class.call1((range.start(), range.end())).unwrap()).unwrap();
    }

    let procset_instance = procset_class.call1(PyTuple::new(py, list).unwrap()).unwrap();
    procset_instance
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: u32,
    pub name: Option<Box<str>>,
    pub user: Option<Box<str>>,
    pub project: Option<Box<str>>,
    pub queue: Box<str>,
    pub types: HashMap<Box<str>, Option<Box<str>>>,
    pub moldables: Vec<Moldable>,
    /// The time interval and resources assigned to the job.
    pub assignment: Option<JobAssignment>,
    /// Used for benchmarking the quotas hit count
    pub quotas_hit_count: u32,
    pub time_sharing: Option<TimeSharingType>,
    /// List of job dependencies, tuples of (job_id, state, exit_code)
    pub dependencies: Vec<(u32, Box<str>, Option<i32>)>,
}

#[derive(Debug, Clone)]
pub enum TimeSharingType {
    /// timesharing=\*,\*
    AllAll,
    /// timesharing=user,*
    UserAll,
    /// timesharing=*,name
    AllName,
    /// timesharing=user,name
    UserName,
}
impl TimeSharingType {
    pub fn from_str(user: &str, job: &str) -> Self {
        match (user, job) {
            ("*", "*") => TimeSharingType::AllAll,
            ("*", "name") => TimeSharingType::AllName,
            ("user", "*") => TimeSharingType::UserAll,
            ("user", "name") => TimeSharingType::UserName,
            _ => {
                warn!("Invalid time sharing type: user={}, job={}", user, job);
                TimeSharingType::AllAll // Default to AllAll if invalid
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobAssignment {
    pub begin: i64,
    pub end: i64,
    pub proc_set: ProcSet,
    pub moldable_index: usize,
}

#[derive(Debug, Clone)]
pub struct Moldable {
    pub id: u32,
    pub walltime: i64,
    pub requests: HierarchyRequests,
    /// Cache key is only calculated at initialization. If fields are changed, the cache key must be recalculated.
    pub cache_key: Box<str>,
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
            Some(data.proc_set.core_count())
        } else {
            None
        }
    }
    /// Returns true if the job can be scheduled using the cache.
    pub fn can_use_cache(&self) -> bool {
        self.time_sharing.is_none()
    }
    /// Returns true if the job assignment can be used to insert a cache entry.
    pub fn can_set_cache(&self) -> bool {
        self.can_use_cache() && self.dependencies.is_empty()
    }
}

pub struct JobBuilder {
    id: u32,
    name: Option<Box<str>>,
    user: Option<Box<str>>,
    project: Option<Box<str>>,
    queue: Option<Box<str>>,
    types: HashMap<Box<str>, Option<Box<str>>>,
    moldables: Vec<Moldable>,
    assignment: Option<JobAssignment>,
    time_sharing: Option<TimeSharingType>,
}
impl JobBuilder {
    pub fn new(id: u32) -> Self {
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
        }
    }
    pub fn moldable_auto(mut self, id: u32, walltime: i64, requests: HierarchyRequests) -> Self {
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
    pub fn queue(mut self, queue: Box<str>) -> Self {
        self.queue = Some(queue);
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
    pub fn build(self) -> Job {
        Job {
            id: self.id,
            name: self.name,
            user: self.user,
            project: self.project,
            queue: self.queue.unwrap_or_else(|| "default".into()),
            types: self.types,
            moldables: self.moldables,
            assignment: self.assignment,
            quotas_hit_count: 0,
            time_sharing: self.time_sharing,
            dependencies: vec![],
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'a> IntoPyObject<'a> for &Job {
    type Target = PyDict;
    type Output = Bound<'a, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'a>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        dict.set_item("id", self.id)?;

        if let Some(name) = &self.name {
            dict.set_item("name", name.as_ref())?;
        } else {
            dict.set_item("name", py.None())?;
        }
        if let Some(user) = &self.user {
            dict.set_item("user", user.as_ref())?;
        } else {
            dict.set_item("user", py.None())?;
        }
        if let Some(project) = &self.project {
            dict.set_item("project", project.as_ref())?;
        } else {
            dict.set_item("project", py.None())?;
        }

        dict.set_item("queue", self.queue.clone().as_ref())?;
        dict.set_item(
            "types",
            self.types
                .iter()
                .map(|(k, v)| (k.as_ref(), v.clone().map(|v| v.to_string())))
                .collect::<HashMap<&str, Option<String>>>(),
        )?;
        dict.set_item("moldables", self.moldables.iter().enumerate().collect::<Vec<(usize, &Moldable)>>())?;
        if let Some(assignment) = &self.assignment {
            let assignment_dict = PyDict::new(py);
            assignment_dict.set_item("begin", assignment.begin)?;
            assignment_dict.set_item("end", assignment.end)?;
            assignment_dict.set_item("proc_set", proc_set_to_python(py, &assignment.proc_set))?;
            assignment_dict.set_item("moldable_index", assignment.moldable_index)?;
            dict.set_item("assignment", assignment_dict)?;
        } else {
            dict.set_item("assignment", py.None())?;
        }
        Ok(dict)
    }
}
#[cfg(feature = "pyo3")]
impl<'a> IntoPyObject<'a> for &Moldable {
    type Target = PyDict;
    type Output = Bound<'a, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'a>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        dict.set_item("id", &self.id)?;
        dict.set_item("walltime", &self.walltime)?;
        dict.set_item("requests", &self.requests)?;
        dict.set_item("cache_key", &self.cache_key.to_string())?;
        Ok(dict)
    }
}

impl JobAssignment {
    pub fn new(begin: i64, end: i64, proc_set: ProcSet, moldable_index: usize) -> JobAssignment {
        JobAssignment {
            begin,
            end,
            proc_set,
            moldable_index,
        }
    }
    pub fn count_resources(&self) -> u32 {
        self.proc_set.len() as u32
    }
}

impl Moldable {
    pub fn new(id: u32, walltime: i64, requests: HierarchyRequests) -> Moldable {
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
