use crate::model::job::ProcSet;
use crate::platform::PlatformConfig;
use crate::scheduler::quotas::Quotas;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::rc::Rc;

/// A slot is a time interval storing the available resources described as a ProcSet.
/// The time interval is [b, e] (b and e included, in epoch seconds).
/// A slot can have a previous and a next slot, allowing to build a doubly linked list.
#[derive(Clone)]
pub struct Slot {
    pub id: i32,
    pub prev: Option<i32>,
    pub next: Option<i32>,
    pub proc_set: ProcSet,
    pub begin: i64,
    pub end: i64,
    pub quotas: Quotas,
    pub platform_config: Rc<PlatformConfig>,
    /// Stores taken intervals that can be shared with the user and the job.
    /// It is the complementary of the original `ts_itvs` of the python scheduler, which lists the occupied intervals without the shareable ones.
    /// Mapping: (user_name or *) -> (job name or *) -> ProcSet
    pub time_shared_proc_sets: HashMap<Box<str>, HashMap<Box<str>, ProcSet>>,
    /// Stores intervals reserved by [`PlaceholderType::Placeholder`] jobs not yet used by [`PlaceholderType::Allow`] jobs
    pub placeholder_proc_sets: HashMap<Box<str>, ProcSet>,
}
impl Debug for Slot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Slot {{ id: {}, prev: {:?}, next: {:?}, begin: {}, end: {}, proc_set: {} }}",
            self.id, self.prev, self.next, self.begin, self.end, self.proc_set
        )
    }
}

impl Slot {
    /// Creates a new slot with the attributes specified as parameters.
    /// If `quotas` is None, the quotas are initialized from the platform configuration default quotas.
    pub fn new(
        platform_config: Rc<PlatformConfig>,
        id: i32,
        prev: Option<i32>,
        next: Option<i32>,
        begin: i64,
        end: i64,
        proc_set: ProcSet,
        quotas: Option<Quotas>,
    ) -> Slot {
        Slot {
            id,
            prev,
            next,
            proc_set,
            begin,
            end,
            quotas: quotas.unwrap_or(Quotas::from_platform_config(platform_config.clone())),
            platform_config,
            time_shared_proc_sets: HashMap::new(),
            placeholder_proc_sets: HashMap::new(),
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }
    #[allow(dead_code)]
    pub fn prev(&self) -> Option<i32> {
        self.prev
    }
    #[allow(dead_code)]
    pub fn next(&self) -> Option<i32> {
        self.next
    }
    pub fn proc_set(&self) -> &ProcSet {
        &self.proc_set
    }
    pub fn begin(&self) -> i64 {
        self.begin
    }
    #[allow(dead_code)]
    pub fn end(&self) -> i64 {
        self.end
    }
    pub fn quotas(&self) -> &Quotas {
        &self.quotas
    }

    pub fn sub_proc_set(&mut self, proc_set: &ProcSet) {
        self.proc_set = self.proc_set.clone() - proc_set;
    }
    pub fn add_proc_set(&mut self, proc_set: &ProcSet) {
        self.proc_set = self.proc_set.clone() | proc_set;
    }

    /// Creates a new slot with the attributes specified as parameters,
    /// and with the same proc_set and quotas as the slot `self`.
    pub fn duplicate(&self, id: i32, prev: Option<i32>, next: Option<i32>, begin: i64, end: i64) -> Slot {
        Slot::new(
            Rc::clone(&self.platform_config),
            id,
            prev,
            next,
            begin,
            end,
            self.proc_set.clone(),
            Some(self.quotas.clone()),
        )
    }

    /// Returns the time-shareable procset for this slot for the given user and job names.
    pub fn get_time_sharing_proc_set(&self, user_name: &Box<str>, job_name: &Box<str>) -> ProcSet {
        if let Some(map) = self
            .time_shared_proc_sets
            .get(&Box::from("*"))
            .or_else(|| self.time_shared_proc_sets.get(user_name))
        {
            if let Some(proc_set) = map.get(&Box::from("*")).or_else(|| map.get(job_name)) {
                return proc_set.clone();
            }
        }
        ProcSet::new()
    }

    /// Updates the `time_shared_proc_set` adding an entry for the user and job names.
    /// user_name and job_name can either be a user and job name, or be `*`.
    /// This will declare that jobs with the given user and job names can use the proc_set resources in this slot even if they are not in `self.proc_set`.
    pub fn add_time_sharing_entry(&mut self, user_name: &Box<str>, job_name: &Box<str>, proc_set: &ProcSet) {
        self.time_shared_proc_sets
            .entry(user_name.clone())
            .or_insert(HashMap::new())
            .entry(job_name.clone())
            .and_modify(|p| *p |= proc_set)
            .or_insert(proc_set.clone());
    }

    /// Updates the `placeholder_proc_sets` adding an entry for the given name.
    /// This will declare that jobs with `placeholder` set to [`PlaceholderType::Placeholder(name)`] can use the `proc_set` resources in this slot,
    /// even if they are not in `self.proc_set`.
    pub fn add_placeholder_entry(&mut self, name: &Box<str>, proc_set: &ProcSet) {
        self.placeholder_proc_sets
            .entry(name.clone())
            .and_modify(|p| *p |= proc_set)
            .or_insert(proc_set.clone());
    }
    /// Updates the `placeholder_proc_sets` removing the `proc_set` from the entry for the given name.
    /// This will declare that jobs with `placeholder` set to [`PlaceholderType::Placeholder(name)`] can no longer use the `proc_set` resources in
    /// this slot as they are no longer available (used by a scheduled job with `placeholder` set to [`PlaceholderType::Allow(name)`]).
    pub fn sub_placeholder_entry(&mut self, name: &Box<str>, proc_set: &ProcSet) {
        self.placeholder_proc_sets.entry(name.clone()).and_modify(|p| {
            *p = p.clone() - proc_set;
        });
    }
}

