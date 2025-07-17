use crate::models::models::Moldable;
use crate::models::models::{Job, ProcSet, ProcSetCoresOp};
use crate::platform::PlatformConfig;
use crate::scheduler::quotas::Quotas;
use prettytable::{format, row, Table};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::iter::Iterator;
use std::rc::Rc;

/// A slot is a time interval storing the available resources described as a ProcSet.
/// The time interval is [b, e] (b and e included, in epoch seconds).
/// A slot can have a previous and a next slot, allowing to build a doubly linked list.
#[derive(Clone)]
pub struct Slot {
    id: i32,
    prev: Option<i32>,
    next: Option<i32>,
    proc_set: ProcSet,
    begin: i64,
    end: i64,
    quotas: Quotas,
    platform_config: Rc<PlatformConfig>,
    // Stores the intervals that might be taken, but available to be shared with the user and the job.
    // HashMap<user_name or *, HashMap<job name or *, ProcSet>>
    // time_shared_proc_set: HashMap<String, HashMap<String, ProcSet>>,
    // placeholder_proc_set: HashMap<String, ProcSet>,
}
impl Debug for Slot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Including all fields except quotas and platform_config for brevity
        write!(
            f,
            "Slot {{ id: {}, prev: {:?}, next: {:?}, begin: {}, end: {}, proc_set: {} }}",
            self.id, self.prev, self.next, self.begin, self.end, self.proc_set
        )
    }
}

impl Slot {
    pub fn new(platform_config: Rc<PlatformConfig>, id: i32, prev: Option<i32>, next: Option<i32>, begin: i64, end: i64, proc_set: ProcSet, quotas: Option<Quotas>) -> Slot {
        Slot {
            id,
            prev,
            next,
            proc_set,
            begin,
            end,
            quotas: quotas.unwrap_or(Quotas::new(platform_config.clone())),
            platform_config,
            //time_shared_proc_set: HashMap::new(),
            //placeholder_proc_set: HashMap::new(),
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
}

/// A SlotSet is a collection of Slots ordered by time.
/// It is a doubly linked list of Slots with O(1) access by id through a HashMap.
/// A SlotSet cannot be empty.
#[derive(Clone)]
pub struct SlotSet {
    #[allow(dead_code)]
    begin: i64, // beginning of the SlotSet (begin of the first slot)
    #[allow(dead_code)]
    end: i64, // end of the SlotSet (end of the last slot)
    first_id: i32, // id of the first slot in the list
    last_id: i32,  // id of the last slot in the list
    next_id: i32,  // next available id
    slots: HashMap<i32, Slot>,
    cache: HashMap<String, i32>, // Stores a slot id for a given moldable cache key, allowing to start again at this slot if multiple moldable have the same cache key, i.e., are identical.
    platform_config: Rc<PlatformConfig>,
}
impl Debug for SlotSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SlotSet {{ begin: {}, end: {}, first_id: {}, last_id: {}, next_id: {}, slots_count: {} }}",
            self.begin,
            self.end,
            self.first_id,
            self.last_id,
            self.next_id,
            self.slots.len()
        )
    }
}

impl SlotSet {
    /// Create a SlotSet from a HashMap of Slots. Slots must form a doubly linked list.
    #[allow(dead_code)]
    pub fn from_map(platform_config: Rc<PlatformConfig>, slots: HashMap<i32, Slot>, first_slot_id: i32) -> SlotSet {
        // Find the first slot
        let first_slot = slots
            .get(&first_slot_id)
            .expect(format!("SlotSet::from_slots: first slot not found, no slot with the id {} found", first_slot_id).as_str());
        // Find the last slot and the biggest id
        let mut last_slot = first_slot;
        let mut next_id = first_slot.id + 1;
        while let Some(next_slot_id) = last_slot.next {
            let next_slot = slots
                .get(&next_slot_id)
                .expect(format!("SlotSet::from_slots: next slot of id {} not found.", next_slot_id).as_str());
            // Sanity checks
            assert_eq!(
                next_slot.id, next_slot_id,
                "SlotSet::from_slots: inconsistent map: the key {} is associated with the slot of id {}.",
                next_slot_id, last_slot.id
            );
            if next_slot.prev.is_none() || next_slot.prev.unwrap() != last_slot.id {
                panic!(
                    "SlotSet::from_slots: doubly linked list broken: slot of id {} has a next slot with id {:?}, but this next slot has a prev slot with id {:?}.",
                    last_slot.id, next_slot_id, next_slot.prev
                );
            }
            if next_slot.id >= next_id {
                next_id = next_slot.id + 1;
            }
            last_slot = next_slot;
        }
        SlotSet {
            begin: first_slot.begin,
            end: last_slot.end,
            first_id: first_slot.id,
            last_id: last_slot.id,
            next_id,
            slots,
            cache: HashMap::new(),
            platform_config,
        }
    }
    /// Create a `SlotSet` with a single slot.
    pub fn from_slot(slot: Slot) -> SlotSet {
        SlotSet {
            platform_config: Rc::clone(&slot.platform_config),
            begin: slot.begin,
            end: slot.end,
            first_id: slot.id,
            last_id: slot.id,
            next_id: slot.id + 1,
            slots: HashMap::from([(slot.id, slot)]),
            cache: HashMap::new(),
        }
    }
    /// Create a `SlotSet` with a single slot that covers the entire range from `begin` to `end` with a `ProcSet = platform_config.resource_set.default_intervals`.
    pub fn from_platform(platform_config: Rc<PlatformConfig>, begin: i64, end: i64) -> SlotSet {
        let proc_set = platform_config.resource_set.default_intervals.clone();
        let slot = Slot::new(platform_config, 1, None, None, begin, end, proc_set, None);
        SlotSet::from_slot(slot)
    }

    pub fn get_platform_config(&self) -> &Rc<PlatformConfig> {
        &self.platform_config
    }

    /// Builds a `Table` for displaying the slots in a human-readable format.
    pub fn to_table(&self) -> Table {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_CLEAN);
        table.add_row(row![
            buFc->"Id",
            buFc->"Prev",
            buFc->"Next",
            buFc->"Begin (epoch)",
            buFc->"End (epoch)",
            buFc->"Size (days)",
            buFc->"ProcSet",
            //buFc->"Placeholders ProcSets"
        ]);
        let mut slot = self.first_slot();
        while let Some(s) = slot {
            table.add_row(row![
                s.id,
                s.prev.map(|prev| format!("Some({})", prev)).unwrap_or("None".to_string()),
                s.next.map(|next| format!("Some({})", next)).unwrap_or("None".to_string()),
                s.begin,
                s.end,
                format!("{:.2}", (s.end - s.begin) as f32 / 3600f32 / 24f32),
                s.proc_set,
                //s.placeholder_proc_set,
            ]);

            slot = if let Some(next_id) = s.next { self.slots.get(&next_id) } else { None };
        }
        table
    }

    pub fn increment_next_id(&mut self) {
        self.next_id += 1;
    }

    pub fn first_slot(&self) -> Option<&Slot> {
        self.slots.get(&self.first_id)
    }
    #[allow(dead_code)]
    pub fn last_slot(&self) -> Option<&Slot> {
        self.slots.get(&self.last_id)
    }

    pub fn get_slot(&self, slot_id: i32) -> Option<&Slot> {
        self.slots.get(&slot_id)
    }

    /// If there is a cache hit with this moldable, returns the slot id of the last slot iterated over for this cache key.
    /// If there is no cache hit, returns None.
    pub fn get_cache_first_slot(&self, moldable: &Moldable) -> Option<i32> {
        self.cache.get(&moldable.get_cache_key()).cloned()
    }
    pub fn insert_cache_entry(&mut self, key: String, slot_id: i32) {
        self.cache.insert(key, slot_id);
    }

    /// Returns the id of the slot from [`Self::slot_at`].
    #[allow(dead_code)]
    pub fn slot_id_at(&self, time: i64, starting_id: Option<i32>) -> Option<i32> {
        self.slot_at(time, starting_id).map(|slot| slot.id)
    }
    /// Returns the slot containing the given time, or None if no such slot exists.
    pub fn slot_at(&self, time: i64, starting_id: Option<i32>) -> Option<&Slot> {
        let mut slot = if let Some(starting_id) = starting_id {
            self.slots.get(&starting_id)
        } else {
            self.first_slot()
        };
        while let Some(s) = slot {
            if time < s.begin {
                return None;
            }
            if time <= s.end {
                return Some(s);
            }
            slot = if let Some(next_id) = s.next { self.slots.get(&next_id) } else { None };
        }
        None
    }
    /// Returns an iterator over the slots in the SlotSet, starting from the first slot and going to the last slot.
    /// It is a double-ended iterator, so you can also iterate backwards.
    /// You can change the start and end slot id of the iterator using [`SlotIterator::start_at`], [`SlotIterator::end_at`], or [`SlotIterator::between`],
    /// and create an iterator with width using [`SlotIterator::with_width`].
    pub fn iter(&self) -> SlotIterator {
        SlotIterator {
            slots: &self.slots,
            begin: Some(self.first_id),
            end: Some(self.last_id),
        }
    }
    /// Helper function to set the previous slot of a slot by its id.
    fn set_slot_prev_id(&mut self, slot_id: i32, prev_id: Option<i32>) {
        self.slots.get_mut(&slot_id).map(|slot| slot.prev = prev_id);
    }
    /// Helper function to set the next slot of a slot by its id.
    fn set_slot_next_id(&mut self, slot_id: i32, next_id: Option<i32>) {
        self.slots.get_mut(&slot_id).map(|slot| slot.next = next_id);
    }
    /// Updates the prev_id of the slot following `slot` to make sure the linked list is correct.
    /// If `slot` is the last slot, updates `self.last_id`
    fn set_next_slot_correct_prev_id(&mut self, slot: &Slot) {
        if let Some(next_id) = slot.next {
            self.set_slot_prev_id(next_id, Some(slot.id));
        } else {
            self.last_id = slot.id;
        }
    }
    /// Updates the next_id of the slot preceding `slot` to make sure the linked list is correct.
    /// If `slot` is the first slot, updates `self.first_id`
    fn set_prev_slot_correct_next_id(&mut self, slot: &Slot) {
        if let Some(prev_id) = slot.prev {
            self.set_slot_next_id(prev_id, Some(slot.id));
        } else {
            self.first_id = slot.id;
        }
    }

    /// Split a given slot just before the given time. Splits between time-1 and time.
    /// The new slot is created and inserted before or after the original slot depending on `before`.
    /// ```
    ///           |                     |       |          |          |
    ///           |       Slot 1        |  -->  |  Slot 2  |  Slot 1  |
    ///           |                     |       |          |          |
    ///         --|---------------------|--   --|----------|----------|--
    ///           |0                  10|       |0   time-1|time    10|
    /// ```
    /// If trying to split with `time-1` and `time` already in two different slots, it will panic (i.e., splitting with time = the beginning of a slot).
    /// Returns the two slots, starting with the new one.
    pub(crate) fn split_at(&mut self, slot_id: i32, time: i64, before: bool) -> (i32, i32) {
        // Sanity checks
        let slot = self
            .slots
            .get_mut(&slot_id)
            .expect(format!("SlotSet::split_at_before: slot of id {} not found", slot_id).as_str());
        assert!(
            time > slot.begin && time <= slot.end,
            "SlotSet::split_at_before: split time {} not in the slot time range: must be > {} and <={}",
            time,
            slot.begin,
            slot.end
        );
        let new_begin = time;

        // Create new slot
        let new_slot_id = self.next_id;
        let new_slot = if before {
            let new_slot = slot.duplicate(new_slot_id, slot.prev, Some(slot.id), slot.begin, new_begin - 1);
            // Update original slot
            slot.begin = new_begin;
            slot.prev = Some(new_slot_id);
            // Update before slot or first_slot_id
            self.set_prev_slot_correct_next_id(&new_slot);
            new_slot
        } else {
            let new_slot = slot.duplicate(new_slot_id, Some(slot.id), slot.next, new_begin, slot.end);
            // Update original slot
            slot.end = new_begin - 1;
            slot.next = Some(new_slot_id);
            // Update after slot or last_slot_id
            self.set_next_slot_correct_prev_id(&new_slot);
            new_slot
        };

        self.slots.insert(new_slot_id, new_slot);
        self.increment_next_id();
        (new_slot_id, slot_id)
    }
    /// Find the slot containing the given time and split it before or after the time. See `Self::split_at`.
    #[allow(dead_code)]
    pub fn find_and_split_at(&mut self, time: i64, before: bool) -> (i32, i32) {
        let slot = self.slot_at(time, None);
        if let Some(slot) = slot {
            self.split_at(slot.id, time, before)
        } else {
            panic!("SlotSet::find_and_split_at_before: no slot found at time {}", time);
        }
    }

    /// Finds the slot containing `begin`, and the slot containing `end`. Returns their ids.
    ///     /// If start_slot_id is not None, it will be used to find faster the slot of begin and end by not looping through all the slots.
    /// Equivalent to calling two times `Self::slot_id_at`.
    #[allow(dead_code)]
    pub fn get_encompassing_range(&self, begin: i64, end: i64, start_slot_id: Option<i32>) -> Option<(&Slot, &Slot)> {
        if let Some(begin_slot) = self.slot_at(begin, start_slot_id) {
            if let Some(end_slot) = self.slot_at(end, Some(begin_slot.id)) {
                return Some((begin_slot, end_slot));
            }
        }
        None
    }

    /// Find the slot right before begin, and the slot right after end. Returns their ids.
    /// If start_slot_id is not None, it will be used to find faster the slot of `begin` and end by not looping through all the slots.
    /// Equivalent to calling two times `Self::slot_id_at`, and getting the previous/next ids.
    #[allow(dead_code)]
    pub fn get_encompassing_range_strict(&self, begin: i64, end: i64, start_slot_id: Option<i32>) -> Option<(&Slot, &Slot)> {
        match self.get_encompassing_range(begin, end, start_slot_id).map(|(s1, s2)| (s1.prev, s2.next)) {
            Some((Some(begin_id), Some(end_id))) => match (self.slots.get(&begin_id), self.slots.get(&end_id)) {
                (Some(begin_slot), Some(end_slot)) => Some((begin_slot, end_slot)),
                _ => None,
            },
            _ => None,
        }
    }

    /// Splits the slots to make them fit a job at time `begin..=end`. Create new slots on the outside of the range.
    /// If start_slot_id is not None, it will be used to find faster the slots of the range by not looping through all the slots.
    /// Returns the first and last slot ids in which the range can fit, and then in which the job can be scheduled.
    pub fn split_slots_for_range(&mut self, begin: i64, end: i64, start_slot_id: Option<i32>) -> (i32, i32) {
        let (begin_slot, end_slot) = if let Some(slots) = self.get_encompassing_range(begin, end, start_slot_id) {
            slots
        } else {
            panic!(
                "SlotSet::split_slots_for_job: no encompassing range found: no slot found at time {} or {}",
                begin, end
            );
        };
        let begin_slot_id = begin_slot.id;
        let end_slot_id = end_slot.id;
        let end_slot_end = end_slot.end;

        if begin_slot.begin < begin {
            self.split_at(begin_slot_id, begin, true);
        }
        if end_slot_end > end {
            self.split_at(end_slot_id, end + 1, false);
        }
        (begin_slot_id, end_slot_id)
    }
    /// See `SlotSet::split_slots_for_jobs_and_update_resources`.
    pub fn split_slots_for_job_and_update_resources(
        &mut self,
        job: &Job,
        sub_resources: bool,
        do_update_quotas: bool,
        start_slot_id: Option<i32>,
    ) -> (i32, i32) {
        let scheduled_data = job
            .scheduled_data
            .as_ref()
            .expect("Job must be scheduled to split slots and update resources for it");

        let (begin_slot_id, end_slot_id) = self.split_slots_for_range(scheduled_data.begin, scheduled_data.end, start_slot_id);
        self.iter()
            .between(begin_slot_id, end_slot_id)
            .map(|slot| slot.id)
            .collect::<Vec<i32>>()
            .iter()
            .for_each(|slot_id| {
                let slot = self.slots.get_mut(&slot_id).unwrap();
                if sub_resources {
                    slot.sub_proc_set(&scheduled_data.proc_set);
                    if self.platform_config.quotas_config.enabled && do_update_quotas {
                        slot.quotas
                            .update_for_job(job, scheduled_data.end - scheduled_data.begin + 1, scheduled_data.proc_set.core_count());
                    }
                } else {
                    self.slots.get_mut(&slot_id).unwrap().add_proc_set(&scheduled_data.proc_set);
                    // TODO: update quotas for the job. Anyway, adding is never used?
                }
            });
        (begin_slot_id, end_slot_id)
    }

    /// Splits the slots to make them fit the jobs. `jobs` must be sorted by start time.
    /// Also subtracts slot resources, and increment quotas counters for the jobs.
    /// If `sub_resources` is true, the resources are subtracted from the slots. Otherwise, they are added.
    /// If `do_update_quotas` is true, the quotas are also updated for the jobs.
    /// Pseudo jobs (for proc_set availability) should sub resources with `do_update_quotas` set to `false`.
    pub fn split_slots_for_jobs_and_update_resources(
        &mut self,
        jobs: &Vec<&Job>,
        sub_resources: bool,
        do_update_quotas: bool,
        mut start_slot_id: Option<i32>,
    ) {
        for job in jobs {
            let (begin_slot_id, _end_slot_id) = self.split_slots_for_job_and_update_resources(job, sub_resources, do_update_quotas, start_slot_id);
            start_slot_id = Some(begin_slot_id);
        }
    }

    /// Returns the intersection of all the slots’ intervals between begin_slot_id and end_slot_id (inclusive)
    pub fn intersect_slots_intervals(&self, begin_slot_id: i32, end_slot_id: i32) -> ProcSet {
        self.iter()
            .between(begin_slot_id, end_slot_id)
            .fold(ProcSet::from_iter([u32::MIN..=u32::MAX]), |acc, slot| acc & slot.proc_set())
    }
    #[allow(dead_code)]
    pub fn begin(&self) -> i64 {
        self.begin
    }
    #[allow(dead_code)]
    pub fn end(&self) -> i64 {
        self.end
    }
    /// Returns the number of slots in the SlotSet.
    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }
}

/// double-ended iterator over Slots in a SlotSet, with the ability to iterate within a beginning and end slot id.
#[derive(Clone)]
pub struct SlotIterator<'a> {
    slots: &'a HashMap<i32, Slot>,
    begin: Option<i32>, // Must always be Some unless the iterator reached its end
    end: Option<i32>,   // Must always be Some unless the iterator is reversed and reached its end
}
impl<'a> DoubleEndedIterator for SlotIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let slot = self.slots.get(&self.end?)?;
        // Move to the previous slot
        self.end = if Some(slot.id) == self.begin {
            None // Reached the end
        } else {
            slot.prev
        };
        Some(slot)
    }
}
impl<'a> Iterator for SlotIterator<'a> {
    type Item = &'a Slot;
    fn next(&mut self) -> Option<Self::Item> {
        let slot = self.slots.get(&self.begin?)?;
        // Move to the next slot
        self.begin = if Some(slot.id) == self.end {
            None // Reached the end
        } else {
            slot.next
        };
        Some(slot)
    }
}
impl<'a> SlotIterator<'a> {
    /// Iterate between two slots. If applying .between(x, y).rev(), it will iterate from y to x.
    /// If start is after end in the linked list, the iterator will go to start till the end of the list,
    /// or from the end till the start of the list if using .rev().
    pub fn between(self, start: i32, end: i32) -> SlotIterator<'a> {
        SlotIterator {
            slots: self.slots,
            begin: Some(start),
            end: Some(end),
        }
    }
    /// Start the iterator at a specific slot id. Works like [`SlotIterator::between`], but only sets the start slot id.
    pub fn start_at(mut self, start_id: i32) -> SlotIterator<'a> {
        self.begin = Some(start_id);
        self
    }
    /// End the iterator at a specific slot id. Works like [`SlotIterator::between`], but only sets the end slot id.
    #[allow(dead_code)]
    pub fn end_at(mut self, end_id: i32) -> SlotIterator<'a> {
        self.end = Some(end_id);
        self
    }
    /// Create an iterator that iterates with a minimum slot width.
    /// See [`SlotWidthIterator`].
    pub fn with_width(self, min_width: i64) -> SlotWidthIterator<'a> {
        SlotWidthIterator::from_iterator(self, min_width)
    }
}

/// Iterates over Slots, finding each time a following slot with a width `slot2.end - slot1.begin >= width`.
/// It is possible to iterate over a specific range in the linked list by using the [`SlotIterator`] methods like
/// [`SlotIterator::between`], [`SlotIterator::start_at`], and [`SlotIterator::end_at`] before calling [`SlotIterator::with_width`] or [`SlotWidthIterator::from_iterator`].
pub struct SlotWidthIterator<'a> {
    begin_iterator: SlotIterator<'a>,
    end_iterator: SlotIterator<'a>,
    end_slot: Option<&'a Slot>,
    min_width: i64,
}
impl<'a> SlotWidthIterator<'a> {
    /// Builds a new SlotWidthIterator from a SlotIterator and a minimum width.
    pub fn from_iterator(iter: SlotIterator<'a>, min_width: i64) -> SlotWidthIterator<'a> {
        SlotWidthIterator {
            begin_iterator: iter.clone(),
            end_iterator: iter,
            end_slot: None,
            min_width,
        }
    }
}
impl<'a> Iterator for SlotWidthIterator<'a> {
    type Item = (&'a Slot, &'a Slot);

    fn next(&mut self) -> Option<Self::Item> {
        let start_slot = match self.begin_iterator.next() {
            Some(slot) => slot,
            None => return None,
        };

        // Continue until we reach a width of at least min_width
        let mut end_slot = match self.end_slot {
            Some(slot) => slot,
            None => self.end_iterator.next()?,
        };
        while end_slot.end - start_slot.begin + 1 < self.min_width {
            end_slot = self.end_iterator.next()?;
        }
        self.end_slot = Some(end_slot);
        Some((start_slot, end_slot))
    }
}
