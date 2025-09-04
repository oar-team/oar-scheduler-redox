# Crate oar-scheduler-redox

## Overview

This crate is a Maturin Python library exposing the [`oar-scheduler-core`](/oar-scheduler-core) crate to Python.
It can be used by the OAR3 Python meta-scheduler instead of the legacy Python scheduler. Though the meta-scheduler implementation and database
requests stay in Python.

You can use the library in internal mode, or in external mode.
A full integration into Python is available on the OAR3 [redox](https://github.com/oar-team/oar3/tree/redox) branch.

In OAR3, the mixed mode is implemented to be used in replacement of the internal scheduler.
It delegates some parts of the meta-scheduling to this crate. Though all the database requests are done in Python.

## Usage

### Building the library and installing it in a virtual environment

First, enable the virtual environment of OAR:

```bash
source /path/to/oar3/venv/bin/activate
```

Then, install the `maturin` tool if not already installed:

```bash
pip install maturin
```

Finally, build and install the library:

```bash
cd ./oar-scheduler-redox
maturin develop
```

Or, to build the library in release mode:

```bash
cd ./oar-scheduler-redox
maturin develop --release
```

### Usage in Python

See the full integration on the OAR3 [redox](https://github.com/oar-team/oar3/tree/redox) branch.

External mode:

```python
import oar_scheduler_redox

oar_scheduler_redox.schedule_cycle_external(session, config, platform, now, queues)
```

Internal mode (mixed mode):

```python
import oar_scheduler_redox

redox_platform = oar_scheduler_redox.build_redox_platform(
  session, config, platform, now, scheduled_jobs
)
redox_slot_sets = oar_scheduler_redox.build_redox_slot_sets(redox_platform)
for active_queues in grouped_active_queues:
  oar_scheduler_redox.schedule_cycle_internal(
    redox_platform, redox_slot_sets, active_queues
  )
  for queue in active_queues:
    oar_scheduler_redox.check_reservation_jobs(
      redox_platform, redox_slot_sets, queue
    )
```

## Edge cases and important implementation details

- This crate is able to run the Python tests on the Rust scheduler.
  For that, you must build and install the library in the OAR3 virtual environment, and configure the OAR3 location in a [`.env`](.env.example) file. 
