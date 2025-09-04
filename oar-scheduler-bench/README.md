# Crate oar-scheduler-bench

## Overview

This crate is used to benchmark the Rust and Python scheduler performance.
It provides sample workloads, mocking, python adapters, and a graphing system to plot results.

Three benchmarks targets are available:

- `Rust`: Runs the Rust scheduler directly using the [`oar-scheduler-core`](/oar-scheduler-core) crate.
- `Python`: Runs the original OAR3 Python scheduler. The `python-config` file should be properly configured to point to the OAR3 installation and
  virtual environment. Release mode is not supported.
- `RustFromPython`: Runs the original OAR3 Python scheduler, but configures it to use the Rust [`oar-scheduler-redox`](/oar-scheduler-redox) crate as
  a drop-in replacement of
  the Python scheduler. The `python-config` file should be properly configured to point to the OAR3 installation and virtual environment. Release mode
  is not supported, but anyway, it is the release mode of the [`oar-scheduler-redox`](/oar-scheduler-redox) maturin build that matters.

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
