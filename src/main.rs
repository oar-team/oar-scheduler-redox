use std::collections::HashMap;
use crate::kao::slot::{ProcSet, Slot, SlotSet};
use crate::lib::models::Job;

mod kao;
mod lib;

fn main() {
    let s1: Slot = Slot::new(1, None, Some(2), ProcSet::from_iter([1..=32]), 1, 10);
    let s2: Slot = Slot::new(2, Some(1), Some(3), ProcSet::from_iter([1..=16, 28..=32]), 11, 20);
    let s3: Slot = Slot::new(3, Some(2), None, ProcSet::from_iter([1..=8, 30..=32]), 21, 30);

    println!("s1: {:?}", s1);
    println!("s2: {:?}", s2);
    println!("s3: {:?}", s3);

    let slots = HashMap::from([(1, s1), (2, s2), (3, s3)]);
    println!("slots: {:?}", slots);

    let mut ss: SlotSet = SlotSet::from_map(slots, 1);
    println!("ss: {:?}", ss);
    ss.to_table().printstd();

    println!("Slot at time 5:");
    let slot_id = ss.slot_id_at(5, None).unwrap();
    println!("{:?}", slot_id);

    println!("Split SlotSet at time 5:");
    println!("{:?}", ss.split_at(slot_id, 5, true));
    ss.to_table().printstd();

    println!("Slot at time 16:");
    let slot_id = ss.slot_id_at(15, None).unwrap();
    println!("{:?}", slot_id);

    println!("Split SlotSet at time 16:");
    println!("{:?}", ss.split_at(slot_id, 16, true));
    ss.to_table().printstd();

    println!("Split SlotSet at time 30:");
    println!("{:?}", ss.split_at(ss.slot_id_at(30, None).unwrap(), 30, false));
    ss.to_table().printstd();


    println!("Iterating SlotSet:");
    for s in ss.iter() {
        println!("Slot of id {} from {:0width$} to {:0width$}:", s.id(), s.begin(), s.end(), width = 2);
    }
    println!("Iterating SlotSet between 5 and 6:");
    for s in ss.iter_between(5, Some(6)) {
        println!("Slot of id {} from {:0width$} to {:0width$}:", s.id(), s.begin(), s.end(), width = 2);
    }
    println!("Iterating SlotSet between 2 and end:");
    for s in ss.iter_between(2, None) {
        println!("Slot of id {} from {:0width$} to {:0width$}", s.id(), s.begin(), s.end(), width = 2);
    }
    println!();
    println!("Iterating SlotSet between 5 and end with width of 10:");
    for s in ss.iter_between_with_width(5, None, 10) {
        println!("id {} from {:0width$} to {:0width$} to id {} from {:0width$} to {:0width$}", s.0.id(), s.0.begin(), s.0.end(), s.1.id(), s.1.begin(), s.1.end(), width = 2);
    }

    let job1 = Job::new(1, 18, 8, ProcSet::from_iter([4..=6]));
    ss.split_slots_and_update_resources(&job1, true);

    ss.to_table().printstd();
}
