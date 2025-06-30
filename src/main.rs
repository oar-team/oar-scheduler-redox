use std::collections::HashMap;
use crate::kao::slot::{ProcSet, Slot, SlotSet};

mod kao;

fn main() {
    let s1: Slot = Slot::new(1, None, Some(2), ProcSet::from_iter([1..=32]), 1, 10);
    let s2: Slot = Slot::new(2, Some(1), Some(3), ProcSet::from_iter([1..=16, 28..=32]), 11, 20);
    let s3: Slot = Slot::new(3, Some(2), None, ProcSet::from_iter([1..=8, 30..=32]), 21, 30);
    
    println!("s1: {:?}", s1);
    println!("s2: {:?}", s2);
    println!("s3: {:?}", s3);
    
    let slots = HashMap::from([(1, s1), (2, s2), (3, s3)]);
    println!("slots: {:?}", slots);
    
    let ss: SlotSet = SlotSet::from_map(slots);
    println!("ss: {:?}", ss);
    
}
