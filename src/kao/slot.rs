use range_set_blaze::RangeSetBlaze;
use std::collections::HashMap;

pub type ProcSet = RangeSetBlaze<u32>;

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
        assert_eq!(slot.id, 1, "SlotSet::from_slot: first slot must have the id of 1. It has id {}", slot.id);
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
}
