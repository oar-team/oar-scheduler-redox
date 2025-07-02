use crate::scheduler::slot::ProcSet;

#[derive(Debug, Clone)]
pub struct Job {
    pub id: u32,
    pub moldable: Vec<Moldable>,
    pub begin: Option<i64>,      // Defined by the scheduler
    pub end: Option<i64>,        // Defined by the scheduler
    pub chosen_moldable_id: Option<u32>, // Defined by the scheduler
}

#[derive(Debug, Clone)]
pub struct Moldable {
    pub id: u32,
    pub walltime: i64,
    pub assigned_resources: Vec<Resource>,
}

#[derive(Debug, Clone)]
pub struct Resource {
    pub id: u32,
    pub scheduler_priority: u32,
    pub cpu_count: u32,
    pub available_upto: i64,
}

impl Job {
    pub fn new(id: u32, moldable: Vec<Moldable>) -> Job {
        Job {
            id,
            moldable,
            begin: None,
            end: None,
            chosen_moldable_id: None,
        }
    }
    pub fn new_from_resources(id: u32, walltime: i64, resources: Vec<Resource>) -> Job {
        Job::new(id, vec![Moldable::new(0, walltime, resources)])
    }
    pub fn new_from_proc_set(id: u32, walltime: i64, proc_set: ProcSet) -> Job {
        let resources = proc_set.iter().map(|r| Resource::new(r)).collect();
        Self::new_from_resources(id, walltime, resources)
    }

    pub fn new_scheduled(id: u32, begin: i64, end: i64, moldable: Vec<Moldable>, chosen_moldable_id: u32) -> Job {
        Job {
            id,
            moldable,
            begin: Some(begin),
            end: Some(end),
            chosen_moldable_id: Some(chosen_moldable_id),
        }
    }
    pub fn new_scheduled_from_resources(id: u32, start_time: i64, stop_time: i64, gantt_resources: Vec<Resource>) -> Job {
        let moldable = vec![Moldable::new(0, stop_time - start_time + 1, gantt_resources.clone())];
        Self::new_scheduled(id, start_time, stop_time, moldable, 0)
    }
    pub fn new_scheduled_from_proc_set(id: u32, start_time: i64, stop_time: i64, proc_set: ProcSet) -> Job {
        let resources = proc_set.iter().map(|r| Resource::new(r)).collect();
        Self::new_scheduled_from_resources(id, start_time, stop_time, resources)
    }
    pub fn is_scheduled(&self) -> bool {
        self.begin.is_some() && self.end.is_some() && self.chosen_moldable_id.is_some()
    }
    pub fn get_proc_set(&self) -> ProcSet {
        self.moldable.get(self.chosen_moldable_id.unwrap() as usize).unwrap().get_proc_set()
    }
}

impl Moldable {
    pub fn new(id: u32, walltime: i64, assigned_resources: Vec<Resource>) -> Moldable {
        Moldable {
            id,
            walltime,
            assigned_resources,
        }
    }
    pub fn get_proc_set(&self) -> ProcSet {
        ProcSet::from_iter(self.assigned_resources.iter().map(|r| r.id))
    }
}

impl Resource {
    pub fn new(id: u32) -> Resource {
        Resource {
            id,
            scheduler_priority: 0,
            cpu_count: 1,
            available_upto: 0,
        }
    }
    pub fn to_proc_set(&self) -> ProcSet {
        ProcSet::from([self.id])
    }
}
