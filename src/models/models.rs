use range_set_blaze::RangeSetBlaze;

pub type ProcSet = RangeSetBlaze<u32>;

#[derive(Debug, Clone)]
pub struct Job {
    pub id: u32,
    pub moldables: Vec<Moldable>,
    pub scheduled_data: Option<ScheduledJobData>,
}

#[derive(Debug, Clone)]
pub struct ScheduledJobData {
    pub begin: i64,
    pub end: i64,
    pub proc_set: ProcSet,
    pub moldable_index: usize,
}

#[derive(Debug, Clone)]
pub struct Moldable {
    pub walltime: i64,
    pub core_count: u32,
    pub filter_proc_set: ProcSet,
}

impl Job {
    pub fn new(id: u32, moldable: Vec<Moldable>) -> Job {
        Job {
            id,
            moldables: moldable,
            scheduled_data: None,
        }
    }

    pub fn new_scheduled(id: u32, moldable: Vec<Moldable>, scheduled_data: ScheduledJobData) -> Job {
        Job {
            id,
            moldables: moldable,
            scheduled_data: Some(scheduled_data),
        }
    }
    pub fn is_scheduled(&self) -> bool {
        self.scheduled_data.is_some()
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
}

impl Moldable {
    pub fn new(walltime: i64, core_count: u32 , filter_proc_set: ProcSet) -> Moldable {
        Moldable {
            walltime,
            core_count,
            filter_proc_set,
        }
    }
    pub fn get_cache_key(&self) -> String {
        format!("{}-{}-{}", self.walltime, self.core_count , self.filter_proc_set.to_string())
    }
}


pub trait ProcSetCoresOp {
    fn sub_proc_set_with_cores(&self, core_count: u32) -> Option<ProcSet>;
    fn core_count(&self) -> u32;
}
impl ProcSetCoresOp for ProcSet {
    /// Tries to claim a subset of the `ProcSet` with the specified number of cores.
    /// If successful, returns a new `ProcSet` that represents the claimed cores.
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
