use crate::scheduler::hierarchy::HierarchyRequests;
use range_set_blaze::RangeSetBlaze;

pub type ProcSet = RangeSetBlaze<u32>;

#[derive(Debug, Clone)]
pub struct Job {
    pub id: u32,
    pub user: String,
    pub project: String,
    pub queue: String,
    pub types: Vec<String>,
    pub moldables: Vec<Moldable>,
    pub scheduled_data: Option<ScheduledJobData>,
}

#[derive(Debug, Clone)]
pub struct ScheduledJobData {
    pub begin: i64,
    pub end: i64,
    pub proc_set: ProcSet,
    #[allow(dead_code)]
    pub moldable_index: usize,
}

#[derive(Debug, Clone)]
pub struct Moldable {
    pub walltime: i64,
    pub requests: HierarchyRequests,
}

impl Job {
    pub fn new(id: u32, user: String, project: String, queue: String, types: Vec<String>, moldable: Vec<Moldable>) -> Job {
        Job {
            id,
            user,
            project,
            queue,
            types,
            moldables: moldable,
            scheduled_data: None,
        }
    }
    #[allow(dead_code)]
    pub fn new_scheduled(id: u32, user: String, project: String, queue: String, types: Vec<String>, moldable: Vec<Moldable>, scheduled_data: ScheduledJobData) -> Job {
        Job {
            id,
            user,
            project,
            queue,
            types,
            moldables: moldable,
            scheduled_data: Some(scheduled_data),
        }
    }
    pub fn is_scheduled(&self) -> bool {
        self.scheduled_data.is_some()
    }
    pub fn begin(&self) -> Option<i64> {
        if let Some(data) = &self.scheduled_data {
            Some(data.begin)
        } else {
            None
        }
    }
    pub fn end(&self) -> Option<i64> {
        if let Some(data) = &self.scheduled_data {
            Some(data.end)
        } else {
            None
        }
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
