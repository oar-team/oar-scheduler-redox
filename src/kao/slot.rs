use prettytable::{format, row, Table};
use range_set_blaze::RangeSetBlaze;
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
    itvs: ProcSet,
    b: i64,
    e: i64,
    // ts_itvs: HashMap<String, HashMap<String, ProcSet>>,
    // ph_itvs: HashMap<String, ProcSet>,
}

const MAX_TIME: i64 = i64::MAX;

impl Slot {
    pub fn new(
        id: i32,
        prev: Option<i32>,
        next: Option<i32>,
        itvs: ProcSet,
        b: i64,
        e: i64,
    ) -> Slot {
        Slot {
            id,
            prev,
            next,
            itvs,
            b,
            e,
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

    pub fn itvs(&self) -> &ProcSet {
        &self.itvs
    }

    pub fn b(&self) -> i64 {
        self.b
    }

    pub fn e(&self) -> i64 {
        self.e
    }
}

#[derive(Clone, Debug)]
pub struct SlotSet {
    begin: i64,
    end: i64,
    last_id: i32,
    slots: HashMap<i32, Slot>,
    // cache: HashMap<i32, i32>,
}

impl SlotSet {
    pub fn new() -> SlotSet {
        SlotSet {
            begin: 0,
            end: 0,
            last_id: 0,
            slots: HashMap::new(),
            // cache: HashMap::new(),
        }
    }

    /// Create a SlotSet from a HashMap of Slots
    /// The first Slot must have an id of 1. Slots must form a doubly linked list.
    pub fn from_map(slots: HashMap<i32, Slot>) -> SlotSet {
        // Find the first slot
        let first_slot = slots.get(&1).expect(
            "SlotSet::from_slots: first slot must have the id of 1, no slot with the id 1 found",
        );
        // Find the last slot
        let mut last_slot = first_slot;
        while let Some(next_slot_id) = last_slot.next {
            let next_slot = slots.get(&next_slot_id).expect(
                format!(
                    "SlotSet::from_slots: next slot of id {} not found.",
                    next_slot_id
                )
                .as_str(),
            );
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
            last_slot = next_slot;
        }
        SlotSet {
            begin: first_slot.b,
            end: last_slot.e,
            last_id: last_slot.id,
            slots,
            // cache: HashMap::new(),
        }
    }

    pub fn from_slot(slot: Slot) -> SlotSet {
        assert_eq!(
            slot.id, 1,
            "SlotSet::from_slot: first slot must have the id of 1. It has id {}",
            slot.id
        );
        SlotSet {
            begin: slot.b,
            end: slot.e,
            last_id: slot.id,
            slots: HashMap::from([(1, slot)]),
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
                s.prev
                    .map(|prev| format!("Some({})", prev))
                    .unwrap_or("None".to_string()),
                s.next
                    .map(|next| format!("Some({})", next))
                    .unwrap_or("None".to_string()),
                s.b,
                s.e,
                format!("{:.2}", (s.e - s.b) as f32 / 3600f32 / 24f32),
                s.itvs,
                //s.ph_itvs,
            ]);

            slot = if let Some(next_id) = s.next {
                self.slots.get(&next_id)
            } else {
                None
            };
        }
        table
    }

    pub fn new_id(&mut self) -> i32 {
        self.last_id += 1;
        self.last_id
    }

    pub fn first_slot(&self) -> Option<&Slot> {
        self.slots.get(&1)
    }

    pub fn last_slot(&self) -> Option<&Slot> {
        self.slots.get(&self.last_id)
    }

    pub fn slot_id_at(&self, time: i64, starting_id: Option<i32>) -> Option<i32> {
        let mut slot = if let Some(starting_id) = starting_id {
            self.slots.get(&starting_id)
        } else {
            self.first_slot()
        };
        while let Some(s) = slot {
            if time < s.b {
                return None;
            }
            if time <= s.e {
                return Some(s.id);
            }
            slot = if let Some(next_id) = s.next {
                self.slots.get(&next_id)
            } else {
                None
            };
        }
        None
    }

    pub fn iter(&self) -> SlotIterator {
        SlotIterator {
            slots: &self.slots,
            current: Some(1),
            end: None,
            forward: true,
        }
    }
    pub fn iter_rev(&self) -> SlotIterator {
        SlotIterator {
            slots: &self.slots,
            current: Some(1),
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
    pub fn iter_between_with_width(
        &self,
        start_id: i32,
        end_id: Option<i32>,
        min_width: i64,
    ) -> SlotWidthIterator {
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

    /// Split a given slot just before the given time. Splits between time-1 and time.
    /// The new slot is created and inserted before the original slot.
    /// ```
    ///           |                     |       |          |          |
    ///           |       Slot 1        |  -->  |  Slot 2  |  Slot 1  |
    ///           |                     |       |          |          |
    ///         --|---------------------|--   --|----------|----------|--
    ///           |0                  10|       |0   time-1|time    10|
    /// ```
    /// If the time at which to split is equal to the beginning of the slot, the slot will be split between time and time+1.
    /// ```
    ///           |                     |       |            |                |
    ///           |       Slot 1        |  -->  |   Slot 2   |     Slot 1     |
    ///           |                     |       |            |                |
    ///         --|---------------------|--   --|------------|----------------|--
    ///           |0 = time           10|       |0   time = 0|time+1 = 1    10|
    /// ```
    /// If the slot is of size one (i.e. begin equals end), the slot is not split and the function returns the id of the given slot.
    /// Returns the two slots, starting with the new one.
    pub fn split_at_before(&mut self, slot_id: i32, time: i64) -> (i32, i32) {
        // Sanity checks
        let slot = self
            .slots
            .get(&slot_id)
            .expect(format!("SlotSet::split_at_before: slot of id {} not found", slot_id).as_str());
        assert!(
            time >= slot.b && time <= slot.e,
            "SlotSet::split_at_before: split time {} must be between {} and {}",
            time,
            slot.b,
            slot.e
        );
        if slot.b == slot.e {
            return (slot_id, slot_id);
        }
        // Prepare to create a new slot
        let new_id = self.new_id();
        let slot = self.slots.get_mut(&slot_id).unwrap();
        let new_begin = if slot.b == time {
            // Special case if splitting at the beginning
            time + 1
        } else {
            time
        };
        // Create new slot
        let new_slot = Slot::new(
            new_id,
            slot.prev,
            Some(slot.id),
            slot.itvs.clone(),
            slot.b,
            new_begin - 1,
        );
        // Update original slot
        slot.b = new_begin;
        slot.prev = Some(new_id);
        // Update before slot
        if let Some(before_slot_id) = new_slot.prev {
            if let Some(before_slot) = self.slots.get_mut(&before_slot_id) {
                before_slot.next = Some(new_id);
            }
        }

        println!(
            "Splitting slot {} at time {}, begin_time={}",
            slot_id, time, new_begin
        );
        println!("Inserting id {} before id {}", new_id, slot_id);
        self.slots.insert(new_id, new_slot);
        (new_id, slot_id)
    }

    /// Split a given slot just before the given time. Splits between time-1 and time.
    /// Works like split_at_before but the new slot is created and inserted after the original slot.
    /// ```
    ///           |                     |       |          |          |
    ///           |       Slot 1        |  -->  |  Slot 1  |  Slot 2  |
    ///           |                     |       |          |          |
    ///         --|---------------------|--   --|----------|----------|--
    ///           |0                  10|       |0   time-1|time    10|
    /// ```
    /// Return the two slots, starting with the new one.
    pub fn split_at_after(&mut self, slot_id: i32, time: i64) -> (i32, i32) {
        // Sanity checks
        let slot = self
            .slots
            .get(&slot_id)
            .expect(format!("SlotSet::split_at_before: slot of id {} not found", slot_id).as_str());
        assert!(
            time >= slot.b && time <= slot.e,
            "SlotSet::split_at_before: split time {} must be between {} and {}",
            time,
            slot.b,
            slot.e
        );
        if slot.b == slot.e {
            return (slot_id, slot_id);
        }
        // Prepare to create a new slot
        let new_id = self.new_id();
        let slot = self.slots.get_mut(&slot_id).unwrap();
        let new_begin = if slot.b == time {
            // Special case if splitting at the beginning
            time + 1
        } else {
            time
        };
        // Create new slot
        let new_slot = Slot::new(
            new_id,
            Some(slot.id),
            slot.next,
            slot.itvs.clone(),
            new_begin,
            slot.e,
        );
        // Update original slot
        slot.e = new_begin - 1;
        slot.next = Some(new_id);
        // Update after slot
        if let Some(after_slot_id) = new_slot.next {
            if let Some(after_slot) = self.slots.get_mut(&after_slot_id) {
                after_slot.prev = Some(new_id);
            }
        }

        self.slots.insert(new_id, new_slot);
        (new_id, slot_id)
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
        while end_slot.e - start_slot.b + 1 < self.min_width {
            end_slot = match inner_iter.next() {
                Some(slot) => slot,
                None => return None,
            };
        }

        Some((start_slot, end_slot))
    }
}
