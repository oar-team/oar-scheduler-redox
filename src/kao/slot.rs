use crate::lib::models::Job;
use prettytable::{format, row, Table};
use range_set_blaze::RangeSetBlaze;
use std::cmp::max;
use std::collections::HashMap;

pub type ProcSet = RangeSetBlaze<u32>;

/// A slot is a time interval storing the available resources described as a ProcSet.
/// The time interval is [b, e] (b and e included, in epoch seconds).
/// A slot can have a previous and a next slot to build an ordered, doubly linked list.
#[derive(Clone, Debug)]
pub struct Slot {
    id: i32,
    prev: Option<i32>,
    next: Option<i32>,
    intervals: ProcSet,
    begin: i64,
    end: i64,
    // ts_itvs: HashMap<String, HashMap<String, ProcSet>>,
    // ph_itvs: HashMap<String, ProcSet>,
}

const MAX_TIME: i64 = i64::MAX;

impl Slot {
    pub fn new(id: i32, prev: Option<i32>, next: Option<i32>, itvs: ProcSet, b: i64, e: i64) -> Slot {
        Slot {
            id,
            prev,
            next,
            intervals: itvs,
            begin: b,
            end: e,
            //ts_itvs: HashMap::new(),
            //ph_itvs: HashMap::new(),
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn prev(&self) -> Option<i32> {
        self.prev
    }

    pub fn next(&self) -> Option<i32> {
        self.next
    }

    pub fn intervals(&self) -> &ProcSet {
        &self.intervals
    }

    pub fn begin(&self) -> i64 {
        self.begin
    }

    pub fn end(&self) -> i64 {
        self.end
    }

    pub fn sub_resources(&mut self, job: &Job) {
        self.intervals = self.intervals.clone() - job.res_set.clone();
    }
    pub fn add_resources(&mut self, job: &Job) {
        self.intervals = self.intervals.clone() | job.res_set.clone();
    }
}

/// A SlotSet is a collection of Slots ordered by time.
/// It is a doubly linked list of Slots with O(1) access by id through a HashMap.
/// A SlotSet cannot be empty.
#[derive(Clone, Debug)]
pub struct SlotSet {
    begin: i64,    // beginning of the SlotSet (begin of the first slot)
    end: i64,      // end of the SlotSet (end of the last slot)
    first_id: i32, // id of the first slot in the list
    last_id: i32,  // id of the last slot in the list
    next_id: i32,  // next available id
    slots: HashMap<i32, Slot>,
    // cache: HashMap<i32, i32>,
}

impl SlotSet {
    /// Create a SlotSet from a HashMap of Slots. Slots must form a doubly linked list.
    pub fn from_map(slots: HashMap<i32, Slot>, first_slot_id: i32) -> SlotSet {
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
            // cache: HashMap::new(),
        }
    }

    pub fn from_slot(slot: Slot) -> SlotSet {
        SlotSet {
            begin: slot.begin,
            end: slot.end,
            first_id: slot.id,
            last_id: slot.id,
            next_id: slot.id + 1,
            slots: HashMap::from([(slot.id, slot)]),
            // cache: HashMap::new(),
        }
    }

    pub fn from_itvs(itvs: ProcSet, begin: i64) -> SlotSet {
        let slot = Slot::new(1, None, None, itvs, begin, MAX_TIME);
        SlotSet::from_slot(slot)
    }

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
            buFc->"Itvs",
            buFc->"Ph_itvs"
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
                s.intervals,
                //s.ph_itvs,
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

    pub fn last_slot(&self) -> Option<&Slot> {
        self.slots.get(&self.last_id)
    }

    pub fn slot_id_at(&self, time: i64, starting_id: Option<i32>) -> Option<i32> {
        self.slot_at(time, starting_id).map(|slot| slot.id)
    }
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

    pub fn iter(&self) -> SlotIterator {
        SlotIterator {
            slots: &self.slots,
            current: Some(self.first_id),
            end: None,
            forward: true,
        }
    }
    pub fn iter_rev(&self) -> SlotIterator {
        SlotIterator {
            slots: &self.slots,
            current: Some(self.last_id),
            end: None,
            forward: false,
        }
    }
    /// Create an iterator that iterates from `start_id` to `end_id` (inclusive)
    /// If `end_id` is None or before `start_id` in the doubly linked list, iterates until the end of the list.
    pub fn iter_between(&self, start_id: i32, end_id: Option<i32>) -> SlotIterator {
        SlotIterator {
            slots: &self.slots,
            current: Some(start_id),
            end: end_id,
            forward: true,
        }
    }
    /// Create a reverse iterator that iterates from `start_id` backwards to `end_id` (inclusive)
    /// If `end_id` is None or after `start_id` in the doubly linked list, iterates until the start of the list.
    pub fn iter_between_rev(&self, start_id: i32, end_id: Option<i32>) -> SlotIterator {
        SlotIterator {
            slots: &self.slots,
            current: Some(start_id),
            end: end_id,
            forward: false,
        }
    }
    /// Create an iterator that iterates from `start_id` to `end_id` (inclusive)
    /// If `end_id` is None or before `start_id` in the doubly linked list, iterates until the end of the list.
    /// The iterator look each time for a following slot making sure that slot.b - following_slot.e + 1 >= min_width. If no such slot is found, the iterator returns None
    pub fn iter_between_with_width(&self, start_id: i32, end_id: Option<i32>, min_width: i64) -> SlotWidthIterator {
        SlotWidthIterator {
            slot_iterator: SlotIterator {
                slots: &self.slots,
                current: Some(start_id),
                end: end_id,
                forward: true,
            },
            min_width,
        }
    }

    fn set_slot_prev_id(&mut self, slot_id: i32, prev_id: Option<i32>) {
        self.slots.get_mut(&slot_id).map(|slot| slot.prev = prev_id);
    }
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
    /// If trying to split with time-1 and time already in two different slots, it will panic (i.e. splitting with time = the beginning of a slot).
    /// Returns the two slots, starting with the new one.
    /// [Removed Behavior] If the time at which to split is equal to the beginning of the slot, the slot will be split between time and time+1.
    /// ```
    ///           |                     |       |            |                |
    ///           |       Slot 1        |  -->  |   Slot 2   |     Slot 1     |
    ///           |                     |       |            |                |
    ///         --|---------------------|--   --|------------|----------------|--
    ///           |0 = time           10|       |0   time = 0|time+1 = 1    10|
    /// ```
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
        //assert_ne!(slot.b, slot.e, "SlotSet::split_at_before: slot of id {} is of size one", slot_id); // Already checked via time > slot.b
        // if slot.b == slot.e {
        //     return (slot_id, slot_id);
        // }
        // Prepare to create a new slot
        let new_begin = /*if slot.b == time {
            // Special case if splitting at the beginning: checked via an assertion.
            time + 1
        } else {*/
            time
            /*}*/;

        // Create new slot
        let new_slot_id = self.next_id;
        let new_slot = if before {
            let new_slot = Slot::new(new_slot_id, slot.prev, Some(slot.id), slot.intervals.clone(), slot.begin, new_begin - 1);
            // Update original slot
            slot.begin = new_begin;
            slot.prev = Some(new_slot_id);
            // Update before slot or first_slot_id
            self.set_prev_slot_correct_next_id(&new_slot);
            new_slot
        } else {
            let new_slot = Slot::new(new_slot_id, Some(slot.id), slot.next, slot.intervals.clone(), new_begin, slot.end);
            // Update original slot
            slot.end = new_begin - 1;
            slot.next = Some(new_slot_id);
            // Update after slot or last_slot_id
            self.set_next_slot_correct_prev_id(&new_slot);
            new_slot
        };

        println!("Splitting slot {} at time {}, begin_time={}", slot_id, time, new_begin);
        println!("Inserting id {} before id {}", new_slot_id, slot_id);
        self.slots.insert(new_slot_id, new_slot);
        self.increment_next_id();
        (new_slot_id, slot_id)
    }
    /// Find the slot containing the given time and split it before or after the time. See `Self::split_at`.
    pub fn find_and_split_at(&mut self, time: i64, before: bool) -> (i32, i32) {
        let slot = self.slot_at(time, None);
        if let Some(slot) = slot {
            self.split_at(slot.id, time, before)
        } else {
            panic!("Slot::find_and_split_at_before: no slot found at time {}", time);
        }
    }

    /// Finds the slot containing begin, and the slot containing end. Returns their ids.
    ///     /// If start_slot_id is not None, it will be used to find faster the slot of begin and end by not looping through all the slots.
    /// Equivalent to calling two times `Self::slot_id_at`.
    pub fn get_encompassing_range(&self, begin: i64, end: i64, start_slot_id: Option<i32>) -> Option<(&Slot, &Slot)> {
        if let Some(begin_slot) = self.slot_at(begin, start_slot_id) {
            if let Some(end_slot) = self.slot_at(end, Some(begin_slot.id)) {
                return Some((begin_slot, end_slot));
            }
        }
        None
    }

    /// Find the slot right before begin, and the slot right after end. Returns their ids.
    /// If start_slot_id is not None, it will be used to find faster the slot of begin and end by not looping through all the slots.
    /// Equivalent to calling two times `Self::slot_id_at`, and getting the previous/next ids.
    pub fn get_encompassing_range_strict(&self, begin: i64, end: i64, start_slot_id: Option<i32>) -> Option<(&Slot, &Slot)> {
        match self.get_encompassing_range(begin, end, start_slot_id).map(|(s1, s2)| (s1.prev, s2.next)) {
            Some((Some(begin_id), Some(end_id))) => match (self.slots.get(&begin_id), self.slots.get(&end_id)) {
                (Some(begin_slot), Some(end_slot)) => Some((begin_slot, end_slot)),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn split_slots_and_update_resources(&mut self, job: &Job, sub_resources: bool, start_slot_id: Option<i32>) {
        let (begin_slot_id, end_slot_id) = self.split_slots_for_job(job, start_slot_id);
        self.iter_between(begin_slot_id, Some(end_slot_id))
            .map(|slot| slot.id)
            .collect::<Vec<i32>>()
            .iter()
            .for_each(|slot_id| {
                if sub_resources {
                    self.slots.get_mut(&slot_id).unwrap().sub_resources(&job);
                } else {
                    self.slots.get_mut(&slot_id).unwrap().add_resources(&job);
                }
            });
    }
    /// Splits the slots to make them fit the job.
    /// If start_slot_id is not None, it will be used to find faster the slots of the job by not looping through all the slots.
    /// Returns the first and last slot ids in which the job can be scheduled.
    pub fn split_slots_for_job(&mut self, job: &Job, start_slot_id: Option<i32>) -> (i32, i32) {
        let begin = job.start_time;
        let end = job.start_time + max(job.walltime - 1, 0);
        let (begin_slot, end_slot) = if let Some(slots) = self.get_encompassing_range(begin, end, start_slot_id) {
            slots
        } else {
            panic!(
                "Slot::split_slots_for_job: no encompassing range found: no slot found at time {} or {}",
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
    /// Splits the slots to make them fit the jobs. `jobs` must be sorted by start time.
    /// Used to insert the previously scheduled jobs in the slots or container jobs.
    /// If start_slot_id is not None, it will be used to find faster the slots of the job by not looping through all the slots.
    /// Returns the first and last slot ids in which the job can be scheduled.
    pub fn split_slots_for_jobs(&mut self, jobs: &Vec<Job>, mut start_slot_id: Option<i32>) {
        for job in jobs {
            let (begin_slot_id, _end_slot_id) = self.split_slots_for_job(job, start_slot_id);
            start_slot_id = Some(begin_slot_id);
        }
    }
    
    /// Returns the intersection of all the slots’ intervals between begin_slot_id and end_slot_id (inclusive)
    pub fn intersect_slots_intervals(&self, begin_slot_id: i32, end_slot_id: i32) -> ProcSet {
        self.iter_between(begin_slot_id, Some(end_slot_id))
            .fold(ProcSet::from_iter([u32::MIN..=u32::MAX]), |acc, slot| acc & slot.intervals())
    }
    
    
}

#[derive(Clone)]
pub struct SlotIterator<'a> {
    slots: &'a HashMap<i32, Slot>,
    current: Option<i32>,
    end: Option<i32>,
    forward: bool, // true for next direction, false for prev direction
}
impl<'a> Iterator for SlotIterator<'a> {
    type Item = &'a Slot;

    fn next(&mut self) -> Option<Self::Item> {
        let current_id = self.current?;
        // Get the current slot
        let slot = match self.slots.get(&current_id) {
            Some(slot) => slot,
            None => return None,
        };
        // Move to the next slot based on direction
        self.current = if Some(current_id) == self.end {
            None // Reached the end
        } else if self.forward {
            slot.next
        } else {
            slot.prev
        };
        Some(slot)
    }
}

/// Iterates over Slots, finding each time a following slot with a width of at least min_width
pub struct SlotWidthIterator<'a> {
    slot_iterator: SlotIterator<'a>,
    min_width: i64,
}
impl<'a> Iterator for SlotWidthIterator<'a> {
    type Item = (&'a Slot, &'a Slot);

    fn next(&mut self) -> Option<Self::Item> {
        let start_slot = match self.slot_iterator.next() {
            Some(slot) => slot,
            None => return None,
        };
        let mut inner_iter = self.slot_iterator.clone();
        let mut end_slot = start_slot;
        // Continue until we reach a width of at least min_width
        while end_slot.end - start_slot.begin + 1 < self.min_width {
            end_slot = match inner_iter.next() {
                Some(slot) => slot,
                None => return None,
            };
        }

        Some((start_slot, end_slot))
    }
}
