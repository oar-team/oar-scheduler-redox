# Porting the OAR3 `kao` tests to Rust

This document tracks the port of the OAR3 Python test suite (`oar3/tests/kao/`, 26 files,
174 `test_*` functions) to the Rust workspace. It is the reference to know, for each
Python test, whether it has a Rust counterpart and where it lives.

## Conventions

- A Rust test is named after the Python function it ports. When a Python test is
  parametrized, one Rust test is written per case, with a suffix describing the
  parameter (e.g. `test_traverse_default`, `test_traverse_2_7`).
- Python `@pytest.fixture` based setups are replaced by small helper functions local to
  the Rust test module (mock platform, job builders, database setup).
- When the Rust implementation behaves differently from Python, the Rust test asserts the
  *Rust* behaviour and a comment documents the divergence (they are listed in
  [Divergences](#divergences) below). This is deliberate: a test suite that fails on
  every divergence is not usable, and a silently weakened assertion is worse.
- Tests that cannot exist because a feature is not implemented in Rust are marked **N/A**
  with the reason, not deleted.

## Status

Test counts are the number of `def test_*` functions in the Python file.

| Python file | Tests | Status | Rust location |
| --- | ---: | --- | --- |
| `test_slot.py` | 24 | **Ported** (46 Rust tests) | `crates/oar-scheduler-core/src/scheduler/tests/slot_test.rs` |
| `test_scheduling.py` | 22 | **Ported** (22 Rust tests) | `crates/oar-scheduler-core/src/scheduler/tests/oar3_scheduling_test.rs` |
| `test_scheduling_basic.py` | 2 | **Covered** by `test_assign_resources_mld_job_split_slots_1` and `test_schedule_id_jobs_ct_1` (the Python file duplicates two tests of `test_scheduling.py`) | `oar3_scheduling_test.rs` |
| `test_db_kamelot_basic.py` | 1 | **Ported** | `crates/oar-scheduler-meta/src/test/oar3_db_test.rs` |
| `test_db_kamelot.py` | 4 | **Ported** (3 Rust tests: `_1`, `_2` and `_3` only differ by command line arguments, see [Divergences](#divergences)) | `oar3_db_test.rs` |
| `test_db_kao.py` | 2 | **Ported** | `oar3_db_test.rs` |
| `test_max_job_per_queues_group_per_round.py` | 4 | **Ported** (3 Rust tests; `test_no_db_metasched_max_job_internal_cycle` exercises the Python internal cycle and has no Rust equivalent) | `crates/oar-scheduler-core/src/model/configuration.rs`, `crates/oar-scheduler-meta/src/test/meta_schedule_test.rs` |
| `test_quotas.py` | 12 | **Ported** (11 Rust tests, 1 N/A: rule tree cache) | `crates/oar-scheduler-core/src/scheduler/tests/quotas_test.rs` |
| `test_temporal_quotas.py` | 22 | **Ported** (22 Rust tests, 2 N/A: empty `assert True` tests) | `crates/oar-scheduler-core/src/scheduler/tests/temporal_quotas_test.rs` |
| `test_db_kamelot_fifo.py` | 2 | **N/A**: the FIFO scheduler (`oar.kao.kamelot_fifo`) is not ported | — |
| `test_db_metasched.py` | 10 | **Ported** (9 Rust tests, 1 N/A: external scheduler mode) | `crates/oar-scheduler-meta/src/test/oar3_metasched_test.rs` |
| `test_db_walltime_change.py` | 8 | **Ported** (5 Rust tests, 3 N/A: `oarwalltime` request validation is not ported) | `crates/oar-scheduler-meta/src/test/oar3_walltime_test.rs` |
| `test_db_all_in_one.py` | 26 | **Ported** (23 Rust tests, 3 N/A: skipped in OAR3) | `crates/oar-scheduler-meta/src/test/oar3_all_in_one_test.rs`, `oar3_all_in_one_quotas_test.rs`, `oar3_energy_saving_test.rs` |
| `test_db_all_in_one_others.py` | 9 | **Ported** (6 Rust tests, 3 N/A: resource properties are not implemented) | `crates/oar-scheduler-meta/src/test/oar3_all_in_one_others_test.rs` |
| `test_db_fairshare.py` | 1 | **Ported** | `crates/oar-scheduler-meta/src/test/oar3_fairshare_test.rs` |
| `test_db_fairshare_running.py` | 2 | **N/A**: `SCHEDULER_FAIRSHARING_INCLUDE_RUNNING_JOBS` (merging the consumption of running jobs into the karma) is not implemented in Rust | — |
| `test_db_multifactor.py` | 1 | **Ported** (`test_db_multifactor_fairshare`) | `crates/oar-scheduler-meta/src/test/oar3_fairshare_test.rs` |
| `test_karma_running.py` | 2 | **N/A**: tests `oar.kao.karma.get_running_consumption`, which does not exist in Rust (see `SCHEDULER_FAIRSHARING_INCLUDE_RUNNING_JOBS` above) | — |
| `test_karma_evaluate.py` | 2 | **N/A**: unit tests of the running-consumption merge of `evaluate_jobs_karma` (the feature does not exist in Rust); they also rely on a fake platform exposing `get_running_consumption` | — |
| `test_repro_overlap.py` | 1 | **N/A**: drives the OAR3 `oar.modules.bipbip` lifecycle (`BipBip.run`, `check_end_of_job`, `set_job_state`, `manage_remote_commands`/`pingchecker` monkeypatched) to check that a job keeps its resources between its launch by BipBip and its termination; the Rust workspace has no bipbip module (the meta-scheduler only notifies it over ZMQ) | — |
| `test_db_suspend_resume.py` | 2 | **N/A**: the Rust meta-scheduler runs the suspend/resume scripts but does not implement the suspend/resume scheduling policy of OAR3 | — |
| `test_parallel_windows.py` | 4 | **N/A**: parallel windows (`PARALLEL_WINDOWS`) are not implemented in the Rust core | — |
| `test_helpers.py` | 1 | **N/A**: `extract_find_assign_args` (the Python extension point for remote find/assign) is not ported | — |
| `test_bataar.py` | 8 | **N/A**: bataar is an external energy-aware simulator binary | — |
| `test_batsim_sched_proxy.py` | 1 | **N/A**: batsim is an external simulator binary | — |
| `test_simsim.py` | 1 | **N/A**: simsim is an external job simulator | — |

Totals: **135 Python tests ported or covered, 39 N/A, none remaining**. The workspace test
suite contains 238 tests (`cargo test --workspace`: 134 in `oar-scheduler-core`, 90 in the
`oar-scheduler-meta` binary, 13 in the `oar-scheduler-meta` library, 1 in
`oar-scheduler-redox`).

The N/A tests fall into four families: features that the Rust workspace does not implement
(resource properties, parallel windows, bipbip lifecycle, fairsharing with running jobs,
the FIFO scheduler, the external scheduler mode, the `oarwalltime` request API), external
simulator binaries (bataar, batsim, simsim), Python implementation details (the rule-tree
cache), and tests that OAR3 itself skips or that are empty (`assert True`).

### Tests present in OAR3 but empty

- `test_temporal_quotas.py::test_calendar_periodical_fromJson_bad` and
  `::test_calendar_simple_slotSet_multi_slot_1` contain a bare `assert True`: there is
  nothing to port.

### Not portable tests (N/A)

- `test_db_metasched.py::test_call_external_scheduler_fails`: the Rust meta-scheduler has
  no external scheduler mode (`meta_schedule` takes no `mode` argument).
- `test_db_walltime_change.py::test_walltime_change_enabled_case_insensitive`,
  `test_walltime_change_second_request_keeps_granted`,
  `test_walltime_change_max_increase_cannot_be_bypassed`: these test the user facing
  `oarwalltime` request API (`oar.lib.walltime.request`), which is not ported; only the
  processing side (`process_walltime_change_requests`) exists.
- `test_db_all_in_one.py::test_db_all_in_one_BE_to_kill`,
  `test_db_all_in_one_BE_to_checkpoint`, `test_db_all_in_one_BE_2`: skipped in OAR3 too
  (`@pytest.mark.skip(reason="Bug occurs only in travis-CI upto now")`).
- `test_db_all_in_one_others.py::test_db_properties_1`, `test_db_properties_2`,
  `test_db_properties_3`: the Rust scheduler does not implement job resource properties
  (`properties` / `-p` SQL filters) nor `network_address` hierarchy constraints; job
  resource descriptions only carry `(name, count)` pairs.
- `test_db_kamelot_fifo.py`: the FIFO scheduler is not ported.
- `test_quotas.py::test_rule_tree_cache_matches_fresh_build_and_switches`: it asserts the
  identity of the Python content-keyed rule-tree cache (`Quotas._rule_tree_cache`), an
  implementation detail that the Rust `QuotasTree` does not have (it is built eagerly).
- `test_db_fairshare_running.py`, `test_karma_running.py`, `test_karma_evaluate.py`: the
  `SCHEDULER_FAIRSHARING_INCLUDE_RUNNING_JOBS` feature (`get_running_consumption`, merging
  the consumption of the currently running jobs into the karma) is not implemented in Rust;
  the Rust `evaluate_jobs_karma` only reads the `accounting` table.
- `test_repro_overlap.py`: needs the OAR3 `oar.modules.bipbip` lifecycle (job launch and
  termination handling); the Rust workspace only has the ZMQ notification side.

## Divergences

Behavioural differences found while porting. Each one is asserted as the Rust behaviour in
the corresponding test.

1. **Resource placement.** When several allocations satisfy a request, OAR3 keeps the
   highest resource ids, the Rust implementation keeps the lowest (first fit). A job
   requesting 2 nodes on a 32 cores / 4 nodes platform gets `1..=16` in Rust and
   `17..=32` in Python. The slot layouts are mirrored, not different.
   Tests: `test_assign_resources_mld_job_split_slots_1`, `test_schedule_id_jobs_ct_1`,
   `test_schedule_error_1`.
2. **`SlotSet::split_slots` boundary.** Python splits at `job.start_time + job.walltime`
   (not `- 1`) and therefore over-subtracts one time unit when the slot ends exactly at
   the job end; the Rust API splits at `assignment.end + 1`. Tests: `test_split_slots_ab`,
   `test_split_slots_b`.
3. **`split_at` at `slot.begin`.** Python creates a one-unit slot when splitting at the
   beginning of a slot, the Rust `SlotSet::split_at` treats it as a programming error and
   panics (the public `find_and_split_at` never does that). Tests: `test_split_at_before_1`,
   `test_split_at_after_1` (ported with `#[should_panic]`).
4. **Inconsistent slot links.** `test_bug_split_slots` uses a slot whose `prev` link is
   inconsistent with the chain; `SlotSet::from_map` validates the links, so the Rust port
   fixes the link (the traversal order, and therefore the assertion, is unchanged).
5. **Unknown traversal end id.** Python's `traverse_with_width` returns nothing when the
   end id is unknown; the Rust iterator does not validate it and traverses to the end of
   the chain. Test: `test_traverse_with_width_explicit_chain`.
6. **Unschedulable jobs.** Python sets `start_time = -1` and `res_set = ProcSet()`; Rust
   leaves `job.assignment == None`.
7. **Multiple hierarchy requests in one moldable.** `test_assign_resources_mld_job_split_slots_2`
   (2 nodes + 1 gpu) gets the allocation `17..=21` in Python, which does not satisfy the
   request; the Rust implementation finds no valid allocation and leaves the slots
   unchanged.
8. **Meta-scheduler command line.** `kamelot` and `kao` take the queue name and the current
   time on the command line; the Rust `oar-scheduler-meta` binary has no arguments and
   schedules the waiting jobs of every active queue. `test_db_kamelot_1`, `_2` and `_3`
   are therefore merged into `test_db_kamelot_1_2_3`.

9. **Advance reservation security window.** The Rust meta-scheduler computed the end of
   the security window as `start + walltime - 1 - security_time`, one time unit short of
   OAR3's `start + walltime - security_time`; a reservation whose walltime equals the
   security time (60 s by default) could not be scheduled at all. Fixed while porting:
   `check_reservation_jobs` now uses `end_time + 1 - job_security_time`.
   Tests: `test_db_metasched_ar_2`, `test_db_all_in_one_AR_*`.
10. **Besteffort jobs killed in advance.** OAR3 kills the besteffort jobs that overlap the
    resources of every job of the security window (`jobs_to_launch_with_security_time`),
    including the reservations that start up to
    `SCHEDULER_BESTEFFORT_KILL_DURATION_BEFORE_RESERVATION` seconds in the future. The
    Rust port only considered the jobs launched during the current round. Fixed while
    porting.
    Tests: `test_db_metasched_ar_check_kill_be_security_time`,
    `test_db_metasched_ar_check_no_be_security_time`.
11. **`get_conf` walltime values.** `oar.lib.walltime.get_conf` returns the raw
    configuration value when it is not numeric (`"*"`, `""`); the Rust `get_conf` is typed
    (`i64`) and falls back to the default. The Python dict syntax (`{'default': 7200}`) is
    accepted, as in Python. Test: `test_walltime_get_conf`.
12. **`suspended` jobs.** The Rust `NewJob` model has no `suspended` column, so
    `test_db_suspended_duration_1` inserts a plain job (the expected state is the same).
13. **Job security time and the scheduling walltime.** OAR3's `get_data_jobs` schedules
    every job for `walltime + SCHEDULER_JOB_SECURITY_TIME` (`oar/lib/job_handling.py`), so
    the jobs of a quota-limited round are packed with a 60 s gap; the Rust meta-scheduler
    schedules the raw walltime and packs the jobs back to back. Consequences: the Python
    `test_db_all_in_one_quotas_1` expects `[0, 160, 420]` where Rust gives `[0, 100, 300]`,
    and `quotas_2` expects `[100, 220, 380]` where Rust launches the first job before the
    reservation (`[0, 100, 160]`). Asserted as the Rust behaviour.
14. **Quotas rules ids.** The `quotas_rules_id` of a slot is 0-based in Python (index in the
    periodicals list) and 1-based in the Rust `Quotas::rules_id` (0 is kept for "no calendar
    entry"). Tests: `test_calendar_simple_slotSet_2` to `_5`.
15. **Accounting sums (fixed).** `get_sum_accounting_window`/`_by_project`/`_by_user` decoded
    the `SUM(consumption)` column as `f64`; both SQLite and PostgreSQL return an integer for
    a `BIGINT` column, so every sum silently fell back to `0.0`, the karma became `NaN` and
    the `FAIRSHARE`/`MULTIFACTOR` orderings degenerated to the insertion order. Fixed while
    porting (`decode_consumption` accepts both representations).
    Tests: `test_db_fairsharing`, `test_db_multifactor_fairshare`.
16. **Fairsharing configuration.** The fairsharing settings have no default value in the Rust
    `Configuration` (`scheduler_fairsharing_*` are `None`) and `evaluate_jobs_karma` asserts
    them: the tests set the OAR3 defaults explicitly (15 days window, unit coefficients).
17. **Oneshot windows and time zones.** `test_temporal_quots_multi_periods_*` build their
    oneshot window from `datetime.utcnow()` while the calendar parses the windows in local
    time, which makes the Python tests time-zone dependent; the Rust port uses the local
    time.
18. **Job `message` column.** OAR3 fills `jobs.message` with the scheduling summary
    (`R=...,W=...,J=I,Q=default (Karma=0.00)`); the Rust scheduler does not, so
    `test_db_fairsharing` only asserts the ordering.

## Running the ported tests

```sh
cargo test -p oar-scheduler-core            # slot, scheduling, quotas, hierarchies, ...
cargo test -p oar-scheduler-meta --bin oar-scheduler-meta   # meta-scheduler tests
cargo test --workspace                      # everything
```

The meta-scheduler tests use an in-memory SQLite database and a recording notifier, so no
PostgreSQL server is needed.

The OAR3 Python suite itself can still be run against the Rust library through the
`python_tests` test of `oar-scheduler-redox` (`OAR3_PYTHON_TESTS_ENABLED=true`, see
`crates/oar-scheduler-redox/src/test.rs`).
