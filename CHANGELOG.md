# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

No release is tagged yet, so every change to date is listed under `[Unreleased]`. All crates are versioned `0.1.0`; the first tag
will therefore be `0.1.0`. Crate names refer to the workspace layout decided in
[ADR 0001](docs/adr/0001-crates-workspace-layout.md): `crates/<crate-name>`.

## [Unreleased]

### Added

**Scheduling engine — `oar-scheduler-core`**

- Port of the OAR3 `kamelot` scheduling algorithm to Rust: `Slot` / `SlotSet` data structures (split, iteration, `next_id`),
  per-core resource accounting, moldable handling and the scheduling cycle with advance reservations.
- Tree-based `SlotSet` with a shared cache, about ten times faster than the linked-list variant on the benchmark workload.
- Hierarchies request support.
- Quotas support, including temporal quotas (periodical and one-shot windows) and quota-rule selection that considers all the job
  types of a moldable.
- Timesharing support.
- Job dependencies, including a regular job depending on an inner job.
- Job containers (inner jobs) and placeholders.
- Moldable filtering: a moldable can restrict the procset its cores are taken from.
- Job sorting in Rust: FIFO, fairshare and multifactor.
- `Configuration` struct parsed from an OAR `oar.conf` file, including booleans written as `yes` / `no`.
- Limit of the number of jobs scheduled per round for a group of queues (`MAX_JOB_PER_QUEUES_GROUP_SCHEDULING_ROUND`), keeping the
  first jobs in scheduling order and leaving the others `Waiting` for the next round.

**Plugins — `oar-scheduler-hooks`**

- Hook system: sysadmins implement scheduler plugins in Rust; an entrypoint crate registers the hooks into the core, which keeps the
  dependency graph acyclic.

**Database — `oar-scheduler-db`**

- Asynchronous database layer (`tokio` + `sqlx` + `sea-query`) supporting PostgreSQL in production and SQLite for tests, with a
  dynamically built schema.
- CRUD operations for platform initialization: resources, queues, moldables, job types, dependencies and reservations.
- CRUD operations for job and moldable updates: assignments, gantt predictions and their resources, `job_state_logs`, job
  `state` / `job_type` / `info_type` / `checkpoint` / `reservation`, and resource `last_job_date`.

**Python bindings — `oar-scheduler-redox`**

- Maturin package exposing the Rust scheduler to Python (`oar_scheduler_redox`), built for abi3 Python ≥ 3.8.
- External mode: drop-in replacement of the Python scheduler, converting platform, jobs, configuration and resource sets from Python
  to Rust.
- Internal (job in / job out) mode: parts of the meta-scheduler run in Rust, driven by the Python meta-scheduler.
- Advance-reservation check exposed to the Python meta-scheduler, and placeholder support in the Python → Rust job converter.

**Meta-scheduler — `oar-scheduler-meta`**

- Standalone, Python-independent meta-scheduler: gantt initialization with the running jobs, scheduling cycle per queue priority
  group, best-effort job killing (time-sharing aware, checkpoint signals), advance reservations (ack, expiry, reduced resource set),
  job launching and walltime-change requests, start-prediction and `oarsub` client notifications, resume jobs, fairshare accounting
  sums (karma), and `job_state_logs` / resource `last_job_date` maintenance.
- Gantt tables update and job assignment queries (`get_gantt_job_assignment`).
- Notifications to the `bipbip` commander through a native ZMQ `PUSH` socket (`omq-tokio`), sending the same `OARRUN` JSON
  payload as the Python meta-scheduler; `OAR_SCHEDULER_NOTIFY_TO_RUN_JOB_CMD` keeps the legacy `oar-notify-to-run-job` delegation.
- Greta client for node energy management: sleep / wake up over ZMQ, with external commands as a fallback;
  `ENERGY_SAVING_INTERNAL` selects the in-process implementation.

**Benchmarking — `oar-scheduler-bench`**

- Benchmark framework with precisely controlled workloads: cache, hyper-threading, hierarchies, quotas and timesharing sample types.
- Three targets: `Rust`, `Python` (the original OAR3 scheduler) and `RustFromPython` (the Python scheduler using the Rust library).
- SVG plot generation and per-function CPU-time measurement through the `auto_bench_fct` crate.

**Tooling and packaging**

- Nix flake with a development shell (Rust toolchain, PostgreSQL, maturin, `just`, OAR3 Python package) and two packages: the Python
  wheel (`oar-scheduler-redox`) and the standalone binary (`oar-scheduler-meta-redox`).
- `justfile` helpers for the PostgreSQL test instance (init, start, stop, reset, drop) and for the builds and tests.
- GitHub Actions and GitLab CI pipelines: build, tests and coverage (`tarpaulin`).
- Architecture decision records in `docs/adr/`, starting with ADR 0001 on the `crates/` workspace layout, and generated architecture
  diagrams in `docs/architecture/`.
- Per-crate READMEs, a roadmap, and GNU GPL v3.0 licensing.

### Changed

- Crates moved from the repository root to `crates/` ([ADR 0001](docs/adr/0001-crates-workspace-layout.md)): explicit workspace
  member list, CI and `justfile` invoking crates with `-p` from the workspace root, benchmark plots written under
  `CARGO_MANIFEST_DIR`, and relative links between crate READMEs.
- Crates renamed to `oar-scheduler-core`, `oar-scheduler-bench` and `oar-scheduler-redox`, with the Python module name fixed
  accordingly.
- Python integration refactored (kamelot split, richer `Platform` trait interaction, pre-sorted jobs passed to
  `build_redox_platform`) to support job in / job out mode.
- Configuration moved from TOML to the OAR-style `.env` / `oar.conf` format.
- Job identifiers widened to `i64`; job states and reservation states are enums instead of raw strings; `tokio` confined to the
  database crate.
- Toolchain and lints: rustc 1.86.0, edition 2024, workspace resolver 3.

### Fixed

- PostgreSQL `ColumnDecode` panic when reading enum columns: every query now casts enum columns to text (`CAST("state" AS text)`).
- Meta-scheduler panic when a job cannot be scheduled (indexing an empty `assigned_jobs` vector).
- Advance-reservation jobs fetched by the wrong query, and an incorrect resource saving query.
- Quotas: resource-time computation, timezone handling for temporal quotas, and rule lookup that ignored job types other than the
  requested one.
- Scheduling of jobs partially or entirely outside the slot set (split and resource decrement).
- External mode inconsistencies between the Python and Rust sides, and `Platform::get_waiting_jobs` (missing `queues` parameter).
- Resource queries ordered by id, making platform initialization deterministic.
- Job events were always inserted with `to_check = 'YES'`; the new `add_new_event_with_to_check` lets the caller decide.
- Test suite refactored to run against PostgreSQL and SQLite with a common setup and teardown.

### Removed

- Stale per-crate `Cargo.lock` files of `oar-scheduler-bench` and `oar-scheduler-core`: leftovers from standalone builds, now covered
  by the workspace lockfile.
- Unused SQL `OnConflict` plumbing in the database crate.

### Notes

- Internal (job in / job out) mode requires the OAR3 Python branch
  [`redox`](https://github.com/oar-team/oar3/tree/redox), which adapts the Python meta-scheduler to the Rust library.
- External mode replaces the scheduling functions of `oar.kao.kamelot` without modifying the Python meta-scheduler.
