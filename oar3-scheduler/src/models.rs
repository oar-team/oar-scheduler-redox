use crate::scheduler::hierarchy::HierarchyRequests;
use auto_bench_fct::auto_bench_fct_hy;
#[cfg(feature = "pyo3")]
use pyo3::prelude::{PyAnyMethods, PyListMethods, PyModule};
#[cfg(feature = "pyo3")]
use pyo3::types::{PyDict, PyList, PyTuple};
#[cfg(feature = "pyo3")]
use pyo3::{Bound, IntoPyObject, IntoPyObjectRef, PyAny, PyErr, Python};
use range_set_blaze::RangeSetBlaze;

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
    pub name: Box<str>,
    pub user: Box<str>,
    pub project: Box<str>,
    pub queue: Box<str>,
    pub types: Vec<Box<str>>,
    pub moldables: Vec<Moldable>,
    pub scheduled_data: Option<ScheduledJobData>,
    /// Used for benchmarking the quotas hit count
    pub quotas_hit_count: u32,
    pub time_sharing: Option<TimeSharingType>,
}

#[derive(Debug, Clone)]
pub enum TimeSharingType {
    /// timesharing=*,*
    AllAll,
    /// timesharing=user,*
    UserAll,
    /// timesharing=*,name
    AllName,
    /// timesharing=user,name
    UserName,
}

#[derive(Debug, Clone)]
pub struct ScheduledJobData {
    pub begin: i64,
    pub end: i64,
    pub proc_set: ProcSet,
    pub moldable_index: usize,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(IntoPyObjectRef))]
pub struct Moldable {
    pub walltime: i64,
    pub requests: HierarchyRequests,
}

impl Job {
    pub fn is_scheduled(&self) -> bool {
        self.scheduled_data.is_some()
    }
    pub fn begin(&self) -> Option<i64> {
        if let Some(data) = &self.scheduled_data { Some(data.begin) } else { None }
    }
    pub fn end(&self) -> Option<i64> {
        if let Some(data) = &self.scheduled_data { Some(data.end) } else { None }
    }
    pub fn walltime(&self) -> Option<i64> {
        if let Some(data) = &self.scheduled_data {
            Some(data.end - data.begin + 1)
        } else {
            None
        }
    }
    pub fn resource_count(&self) -> Option<u32> {
        if let Some(data) = &self.scheduled_data {
            Some(data.proc_set.core_count())
        } else {
            None
        }
    }
}

pub struct JobBuilder {
    id: u32,
    name: Option<Box<str>>,
    user: Option<Box<str>>,
    project: Option<Box<str>>,
    queue: Option<Box<str>>,
    types: Option<Vec<Box<str>>>,
    moldables: Vec<Moldable>,
    scheduled_data: Option<ScheduledJobData>,
    time_sharing: Option<TimeSharingType>
}
impl JobBuilder {
    pub fn new(id: u32) -> Self {
        JobBuilder {
            id,
            name: None,
            user: None,
            project: None,
            queue: None,
            types: None,
            moldables: vec![],
            scheduled_data: None,
            time_sharing: None,
        }
    }
    pub fn moldable_auto(mut self, walltime: i64, requests: HierarchyRequests) -> Self {
        self.moldables.push(Moldable::new(walltime, requests));
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
    pub fn name(mut self, name: Box<str>) -> Self {
        self.name = Some(name);
        self
    }
    pub fn user(mut self, user: Box<str>) -> Self {
        self.user = Some(user);
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
    pub fn single_type(mut self, job_type: Box<str>) -> Self {
        self.types = Some(vec![job_type]);
        self
    }
    pub fn types(mut self, types: Vec<Box<str>>) -> Self {
        self.types = Some(types);
        self
    }
    pub fn scheduled(mut self, scheduled_data: ScheduledJobData) -> Self {
        self.scheduled_data = Some(scheduled_data);
        self
    }
    pub fn build(self) -> Job {
        Job {
            id: self.id,
            name: self.name.unwrap_or_else(|| "Unnamed Job".into()),
            user: self.user.unwrap_or_else(|| "unknown_user".into()),
            project: self.project.unwrap_or_else(|| "unknown_project".into()),
            queue: self.queue.unwrap_or_else(|| "default".into()),
            types: self.types.unwrap_or_default(),
            moldables: self.moldables,
            scheduled_data: self.scheduled_data,
            quotas_hit_count: 0,
            time_sharing: self.time_sharing,
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
        dict.set_item("id", self.id).unwrap();
        dict.set_item("user", self.user.as_ref()).unwrap();
        dict.set_item("project", self.project.as_ref()).unwrap();
        dict.set_item("queue", self.queue.as_ref()).unwrap();
        dict.set_item("types", self.types.iter().map(|s| s.as_ref()).collect::<Vec<_>>()).unwrap();
        dict.set_item("moldables", self.moldables.iter().enumerate().collect::<Vec<(usize, &Moldable)>>())
            .unwrap();
        if let Some(scheduled_data) = &self.scheduled_data {
            let scheduled_dict = PyDict::new(py);
            scheduled_dict.set_item("begin", scheduled_data.begin).unwrap();
            scheduled_dict.set_item("end", scheduled_data.end).unwrap();
            scheduled_dict
                .set_item("proc_set", proc_set_to_python(py, &scheduled_data.proc_set))
                .unwrap();
            scheduled_dict.set_item("moldable_index", scheduled_data.moldable_index).unwrap();
            dict.set_item("scheduled_data", scheduled_dict).unwrap();
        } else {
            dict.set_item("scheduled_data", py.None()).unwrap();
        }
        Ok(dict)
    }
}

impl ScheduledJobData {
    pub fn new(begin: i64, end: i64, proc_set: ProcSet, moldable_index: usize) -> ScheduledJobData {
        ScheduledJobData {
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
    pub fn new(walltime: i64, requests: HierarchyRequests) -> Moldable {
        Moldable { walltime, requests }
    }
    pub fn get_cache_key(&self) -> String {
        format!("{}-{}", self.walltime, self.requests.get_cache_key())
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
