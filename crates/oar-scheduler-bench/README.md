# Crate oar-scheduler-bench

## Overview

This crate is used to benchmark the Rust and Python scheduler performance.
It provides sample workloads, mocking, python adapters, and a graphing system to plot results.

Two benchmark programs are available:

- `oar-scheduler-bench` (the default binary): the scheduler-level benchmarks below, comparing the
  Rust scheduler, the Python scheduler and the Python scheduler calling the Rust one.
- `bench_quotas_metasched` (enabled by the `meta-bench` feature): the meta-scheduler end-to-end
  benchmark, the Rust equivalent of `oar3/bench/bench_quotas_metasched.py`. It is kept behind a
  feature because it links [`oar-scheduler-meta`](../oar-scheduler-meta), whose dependency on
  `oar-scheduler-core` enables the core default features (`benchmark_disable*`) and would disable the
  instrumentation the scheduler-level benchmarks rely on.

Three benchmarks targets are available:

- `Rust`: Runs the Rust scheduler directly using the [`oar-scheduler-core`](../oar-scheduler-core) crate.
- `Python`: Runs the original OAR3 Python scheduler. The `python-config` file should be properly configured to point to the OAR3 installation and
  virtual environment. Release mode is not supported.
- `RustFromPython`: Runs the original OAR3 Python scheduler, but configures it to use the Rust [`oar-scheduler-redox`](../oar-scheduler-redox) crate as
  a drop-in replacement of
  the Python scheduler. The `python-config` file should be properly configured to point to the OAR3 installation and virtual environment. Release mode
  is not supported, but anyway, it is the release mode of the [`oar-scheduler-redox`](../oar-scheduler-redox) maturin build that matters.

Various workloads are available and configured through the enum `WaitingJobsSampleType`. You can configure other samples yourself by adding new
variants.

## Usage

Configure the benchmarks in `main.rs` editing the initialization of the `BenchmarkConfig` struct.
Then, run with:

```bash
cargo run -p oar-scheduler-bench
```

or in release mode (not available for `Python` and `RustFromPython` targets)

```bash
cargo run -p oar-scheduler-bench --release
```

## Meta-scheduler benchmark: quotas OFF vs ON

`bench_quotas_metasched` is the Rust equivalent of `oar3/bench/bench_quotas_metasched.py`. It times
`meta_schedule()`, the full scheduling round (platform construction: resource set loading and quotas
configuration; then waiting-job retrieval, sorting, slot-set construction, the scheduling loop and the
writing of assignments), against a self-contained SQLite database that the program creates, seeds with
resources and jobs, and tears down. No external database and no PostgreSQL are required.

```bash
cargo run -p oar-scheduler-bench --release --features meta-bench --bin bench_quotas_metasched
cargo run -p oar-scheduler-bench --release --features meta-bench --bin bench_quotas_metasched -- --max-k 10 --nb-res 128
cargo run -p oar-scheduler-bench --release --features meta-bench --bin bench_quotas_metasched -- --mode profile --profile-jobs 512 --quotas on
```

Two modes:

- `sweep` (default) measures `meta_schedule` for a range of job-batch sizes, once with quotas disabled
  and once enabled, and writes a results table, a CSV (`bench_quotas_metasched.csv`) and a plot
  (`bench_quotas_metasched.svg`).
- `profile` runs a single configuration and reports the time split between the platform construction
  (resource set + quotas configuration) and the scheduling round itself.

Options are the same as the Python script (`--jobs`, `--max-k`, `--nb-res`, `--cores-per-node`,
`--req-res`, `--nb-users`, `--nb-projects`, `--repeat`, `--quotas`, `--profile-jobs`, `--outdir`);
`--help` prints them.

### Differences with the Python script

- The plot is an SVG (like the other benchmark outputs of this crate) instead of a PNG: this crate's
  `plotters` build has no bitmap backend.
- There is no `cProfile`/`pstats` equivalent: `--mode profile` reports the setup/scheduling split
  instead, and a call-level profile is obtained with `perf record --call-graph dwarf` or
  `cargo flamegraph` on the binary.
- `--repeat` defaults to 5 instead of 1: a Rust round takes a few milliseconds, so a single sample is
  dominated by noise. The best time of the repetitions is kept, and the order of the two
  configurations alternates between repetitions so that the second one does not systematically
  benefit from a warmer page cache.
- The table prints seconds with 6 decimals instead of 4, for the same reason.
- The jobs carry a `project` (`proj{k % nb_projects}`) like the Python ones; `NewJob` does not expose
  the column, so it is set right after the insertion (`set_job_project`).
- The SQLite journal is kept in memory and the synchronous mode is off, mirroring the Python harness
  where every measurement runs inside a transaction that is rolled back: what is measured is the
  scheduler and the SQL work, not one `fsync` per statement. Without it, a run is ~10× slower and
  entirely dominated by the disk.
- Below ~32 jobs the measurement is dominated by the fixed cost of a scheduling round; use
  `--max-k 9` or `--max-k 10` for numbers that reflect the per-job cost.

### Measurement caveat

A round takes a few milliseconds, so the measurement is sensitive to the machine. On a hybrid CPU
(performance and efficiency cores) the thread migrates between cores of very different speeds and the
same workload measures between 1x and 5x longer from one process to the next. Pin the benchmark to one
core and keep the machine quiet for reproducible numbers:

```bash
taskset -c 2 cargo run -p oar-scheduler-bench --release --features meta-bench \
    --bin bench_quotas_metasched -- --repeat 5 --max-k 10 --nb-res 128
```

Example output (Intel Core Ultra 7, 64 resources, pinned):

```text
    jobs    quotas_off     quotas_on    overhead    ratio
---------------------------------------------------------
       2      0.001828      0.001822   -0.000005    1.00x
       8      0.002773      0.002877    0.000104    1.04x
      32      0.006545      0.006896    0.000351    1.05x
     128      0.011098      0.012313    0.001215    1.11x
     256      0.015722      0.018346    0.002624    1.17x
```
