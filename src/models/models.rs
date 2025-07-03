use crate::scheduler::slot::ProcSet;

#[derive(Debug, Clone)]
pub struct Job {
    pub id: u32,
    pub moldables: Vec<Moldable>,
    pub begin: Option<i64>,      // Defined by the scheduler
    pub end: Option<i64>,        // Defined by the scheduler
    pub chosen_moldable_index: Option<usize>, // Defined by the scheduler
}

#[derive(Debug, Clone)]
pub struct Moldable {
    pub walltime: i64,
    pub proc_set: ProcSet,
}

impl Job {
    pub fn new(id: u32, moldable: Vec<Moldable>) -> Job {
        Job {
            id,
            moldables: moldable,
            begin: None,
            end: None,
            chosen_moldable_index: None,
        }
    }
    pub fn new_from_proc_set(id: u32, walltime: i64, proc_set: ProcSet) -> Job {
        let moldable = Moldable::new(walltime, proc_set);
        Job::new(id, vec![moldable])
    }

    pub fn new_scheduled(id: u32, begin: i64, end: i64, moldable: Vec<Moldable>, chosen_moldable_index: usize) -> Job {
        Job {
            id,
            moldables: moldable,
            begin: Some(begin),
            end: Some(end),
            chosen_moldable_index: Some(chosen_moldable_index),
        }
    }
    pub fn new_scheduled_from_proc_set(id: u32, begin: i64, end: i64, proc_set: ProcSet) -> Job {
        let moldable = Moldable::new(end - begin + 1, proc_set);
        Self::new_scheduled(id, begin, end, vec![moldable], 0)
    }
    pub fn is_scheduled(&self) -> bool {
        self.begin.is_some() && self.end.is_some() && self.chosen_moldable_index.is_some()
    }
    pub fn get_proc_set(&self) -> &ProcSet {
        &self.moldables.get(self.chosen_moldable_index.unwrap()).unwrap().proc_set
    }
}

impl Moldable {
    pub fn new(walltime: i64, proc_set: ProcSet) -> Moldable {
        Moldable {
            walltime,
            proc_set,
        }
    }
    pub fn get_cache_key(&self) -> String {
        format!("{}-{}", self.walltime, self.proc_set.to_string())
    }
}
