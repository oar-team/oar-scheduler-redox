Rust scheduler implementation for OAR3




# Roadmap
### Scheduler (`oar3-scheduler`)
- [x] Basic scheduler, advance reservation & platform setup
- [x] Hierarchies request support
- [x] Quotas support
- [x] Timesharing support
- [x] Job dependencies support
- [x] Job container support
- [ ] Placeholders support
- [ ] Temporal quotas support
- [ ] Envelopes support
### Benchmarking (`oar3-scheduler-bench`)
- [x] Benchmarking framework
- [x] Calling the Python scheduler from the benchmarking framework
- [x] Macros for function time measurement ([auto_bench_fct](https://crates.io/crates/auto_bench_fct))
### Python integration (`oar3-scheduler-lib`)
- [x] Expose the Rust scheduler as a Python library
- [x] Support external mode (convert platform: jobs, config, resources set, etc.)
- [ ] Support mixed mode (implement some parts of the meta-scheduler into Rust, and edit the Python meta-scheduler to add the integration)
- [ ] Support internal mode (convert slotset objects from Python to Rust and from Rust to Python)


# Crates & How to build/run
## Crate oar3-scheduler

This crate is a Rust library that implements the core scheduler of OAR3 in Rust.

## Crate oar3-scheduler-bench

This crate is used to benchmark the Rust and Python scheduler performance.
It provides sample workloads, mocking, python adapters, and a graphing
system to plot results.

### Running benchmarks
Configure the benchmarks in `main.rs` editing the initialization of the `BenchmarkConfig` struct.
Then, run with:

```bash
cargo run -p oar3-scheduler-bench
```
or in release mode (not available for Python and RustFromPython targets)
```bash
cargo run -p oar3-scheduler-bench --release
```

## Crate oar3-scheduler-lib

This crate is a Maturin Python library exposing the oar3-scheduler crate to Python.
It can be used by the OAR3 meta-scheduler instead of the legacy Python scheduler.

### Building the library and installing it in a virtual environment
First enable the virtual environment of OAR:
```bash
source /path/to/oar3/venv/bin/activate
```
Then, install the `maturin` tool if not already installed:
```bash
pip install maturin
```
Finally, build and install the library:
```bash
cd ./oar3-scheduler-lib
maturin develop
```
Or, to build the library in release mode:
```bash
cd ./oar3-scheduler-lib
maturin develop --release
```

### Usage in Python
You can use the library in Python as follows:
```python
import oar3_scheduler_lib
oar3_scheduler_lib.schedule_cycle(session, config, platform, queues)
```

# License
This project is licensed under the GNU General Public License v3.0 (GPL-3.0).
See the [LICENSE.md](LICENSE.md) file for details.
