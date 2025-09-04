# Crate oar-scheduler-core

## Overview

This crate is a Rust library that implements the core scheduling algorithms of OAR3. It also contains the main data models.

This crate corresponds to all python code called after the `kamelot.py` file in OAR3.

The `Platform` trait and a mutable reference to a struct implementing this trait is the data access layer for the scheduler.
The Platform struct has the same role as the Python `Platform` class, but it centralizes even more data. It should provide access to jobs, resource
set, global configuration, quotas configuration and more.

## Usage

Crates such as [`oar-scheduler-redox`](/oar-scheduler-redox) or [`oar-scheduler-meta`](/oar-scheduler-meta) can use this library to implement a full
scheduler.

A struct implementing the `Platform` trait must be initialized first. Then, the scheduling cycle can be started with:

```rust
let mut platform =...; // Initialize a struct implementing the Platform trait
let queues = vec!["default".to_string(), "default2".to_string()]; // Define the queues to consider (mainly used for job sorting, hooks, and best-effort jobs consideration if queues = ['besteffort']
let slot_count = kamelot::schedule_cycle( & mut platform, queues);
// Slot count is normally only used for benchmarking
```

When scheduling multiple queues with different priorities, the scheduling can be done with fineer granularity using:

```rust
let mut platform =...; // Initialize a struct implementing the Platform trait

let queues_grouped_by_priority = vec![vec!["admin".to_string()], vec!["default".to_string(), "default2".to_string()], vec!["besteffort".to_string()]];

// Init slotset without including the already scheduled besteffort jobs
let mut slot_sets = kamelot::init_slot_sets( & mut platform, false);

for queues in queues_grouped_by_priority {
// Insert scheduled besteffort jobs if queues = ['besteffort'].
if active_queues.len() == 1 & & active_queues[0] == "besteffort" {
kamelot::add_already_scheduled_jobs_to_slot_set( & mut slot_sets, & mut platform, true, false);
}
kamelot::internal_schedule_cycle( & mut platform, & mut slot_sets, & active_queues);
}
```

## Edge cases and important implementation details

- The scheduler is single-threaded and synchronous. No async code should be used in this crate.
- The job’s `JobAssignment` struct stores the index of the assigned moldable as `moldable_index`. This index corresponds to the index of the moldable
  in the `job.moldables` vector, and not to the moldable’s id as it is in Python.
