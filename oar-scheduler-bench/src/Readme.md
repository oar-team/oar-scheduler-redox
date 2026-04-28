# README: JSON metrics for the OAR Rust scheduler benchmark

## Overview

This document describes the structure of the JSON files produced by the OAR benchmark, the meaning of every exported metric, the units used, and whether each metric should be interpreted as:

* **per benchmark run total**
* **per job**
* **per function call**
* **configuration / metadata**

The current JSON export is produced from the benchmark code in `oar-scheduler-bench/src/benchmarker.rs` and `main.rs`. The JSON contains:

* a top-level benchmark mode string,
* a `config` section,
* a `results` array,
* and inside each `results[i]`, both top-level summary metrics and detailed scheduler phase metrics under `perf`.

---

## JSON structure

A benchmark JSON file has the following shape:

```json
{
  "mode": "phase-bench",
  "config": { ... },
  "results": [
    {
      "perf": { ... },
      "jobs_count": ...,
      "scheduled_jobs_count": ...,
      "scheduling_time": ...,
      "cache_hits": ...,
      "slot_count": ...,
      "quotas_hit": ...,
      "gantt_width": ...,
      "optimal_gantt_width": ...,
      "resource_occupation": ...
    }
  ]
}
```

Notes:

* `config` stores the run configuration.
* `results` contains one entry per sampled benchmark point, typically one per jobs-count value in the sweep.
* each numeric field inside `results[*]` is usually exported as a `BenchmarkMeasurementStatistics` object:

  * `min`
  * `max`
  * `mean`
  * `q1`
  * `q2`
  * `q3`
  * `std_dev`

---

## Important note about aggregation

### 1. Statistics layer

Most exported values are **statistics over repeated runs** at the same benchmark point.

If `averaging = 1`, then:

* `min = max = mean = q1 = q2 = q3`

If `averaging > 1`, these fields summarize the distribution across those repeated runs. 

### 2. `std_dev` is currently misnamed

In the current implementation, `std_dev` is **not the square root of variance**. It is actually the variance-like quantity:

```text
sum((x - mean)^2) / n
```

stored as `u32`.

So `std_dev` should currently be interpreted as **variance-like spread**, not a mathematically correct standard deviation. 

### 3. Quartiles

The internal code computes quartiles, but the raw `quartiles` object is skipped from JSON serialization. Only `q1`, `q2`, and `q3` are exported. 

---

## `config` section

These fields describe how the benchmark run was configured.

### `target`

Type: string
Meaning:

* `Rust`
* `Python`
* `RustFromPython`

This tells which scheduler implementation was benchmarked.
Category: **configuration metadata**. 

### `sample_type`

Type: string
Meaning: workload family used for waiting jobs, e.g.

* `Normal`
* `HighCacheHit`
* `Besteffort`
* `NodeOnly`
* `CoreOnly`
* `Fragmented`
* `HierarchyHeavy`
* `CacheFriendly`

Category: **configuration metadata**. 

### `cache`

Type: boolean
Meaning: whether scheduler cache is enabled.
Category: **configuration metadata**. 

### `averaging`

Type: integer
Meaning: number of repeated runs per benchmark point.
Category: **configuration metadata**. 

### `res_count`

Type: integer
Meaning: stored in config, but in the current code this is legacy metadata and no longer drives the real graph size for topology-based runs. Real graph size is derived from `nnodes` and `topology`.
Category: **configuration metadata**.

### `start`

Type: integer
Meaning: first jobs-count value in the benchmark sweep.
Category: **configuration metadata**. 

### `end`

Type: integer
Meaning: last jobs-count value in the benchmark sweep.
Category: **configuration metadata**. 

### `step`

Type: integer
Meaning: increment in jobs-count between benchmark points.
Category: **configuration metadata**. 

### `seed`

Type: integer
Meaning: base RNG seed for graph/job generation.
Category: **configuration metadata**. 

### `single_thread`

Type: boolean
Meaning: whether the sampling loop is sequential instead of `join_all`.
Category: **configuration metadata**. 

### `prefill`

Type: string
Meaning:

* `Empty`
* `Light`
* `Medium`
* `Heavy`

Controls how many prefill jobs are inserted before the measured benchmark phase.
Category: **configuration metadata**. 

### `topology`

Type: string
Meaning:

* `Homogeneous`
* `Heterogeneous`

Controls which synthetic resource graph shape is generated.
Category: **configuration metadata**. 

### `nnodes`

Type: integer
Meaning: logical number of nodes in the synthetic graph.
Category: **configuration metadata**.

---

## Top-level metrics in `results[*]`

These metrics summarize the whole benchmark point.

Unless stated otherwise, they are **totals for the full measured run at that jobs-count**, then wrapped into `BenchmarkMeasurementStatistics`.

### `jobs_count`

Unit: jobs
Meaning: number of waiting/probe jobs used for this benchmark point.
Aggregation: **per benchmark point total**, not per-job.
Notes: this is not a statistics object in the current export; it is stored as a plain `u32`. 

### `scheduled_jobs_count`

Unit: jobs
Meaning: number of jobs successfully scheduled in that run.
Aggregation: **per benchmark point total**.
Exported as statistics across repeated runs. 

### `scheduling_time`

Unit: milliseconds
Meaning: wall-clock time of the full `schedule_cycle(...)` call for the measured run.
Aggregation: **per benchmark point total wall-clock runtime**, not per-job and not per-function-call.
Notes: this is measured externally with `measure_time(...)`. 

### `cache_hits`

Unit: percent (`0..100`)
Meaning: percentage of moldables in the generated waiting jobs whose `cache_key` was already seen earlier in the same waiting-job set.
Aggregation: **per benchmark point summary ratio**, not a raw count.
Formula in code:

```text
(cache_hits * 100 / jobs_count)
```

where `cache_hits` here is computed by scanning generated waiting jobs before scheduling. 

### `slot_count`

Unit: slots
Meaning: number of slots returned by the scheduling backend after the schedule cycle.
Aggregation: **per benchmark point total snapshot**, not per-job.
Source: second value returned by scheduling backend wrapper. 

### `quotas_hit`

Unit: percent (`0..100`)
Meaning: percentage of jobs that hit quotas, computed from the sum of `quotas_hit_count` across scheduled jobs and divided by `jobs_count`.
Aggregation: **per benchmark point summary ratio**.
Formula:

```text
quotas_hits * 100 / jobs_count
```



### `gantt_width`

Unit: hours
Meaning: maximum job end time among scheduled jobs, divided by 60.
Aggregation: **per benchmark point total summary**.
Notes:

* internally computed in minutes-like scheduler time units,
* then divided by 60 before export. 

### `optimal_gantt_width`

Unit: hours
Meaning: idealized lower-bound width derived from total allocated core-time divided by cluster size, then divided by 60 before export.
Aggregation: **per benchmark point total summary**.
Notes:

* this is not per-job,
* it is meant as a compactness/packing baseline,
* current implementation still uses `res_count` as denominator in this formula, which should be interpreted carefully if `res_count` is stale.

### `resource_occupation`

Unit: percent (`0..100`)
Meaning:

```text
optimal_gantt_width * 100 / gantt_width
```

Aggregation: **per benchmark point summary ratio**.
Interpretation: rough packing efficiency estimate.
Notes: because it depends on `optimal_gantt_width`, it inherits the same denominator caveat. 

---

## `perf` section

The `perf` object contains scheduler-internal timings and counters exported from `PerfStats`, converted into `PerfBenchmarkResult`, then aggregated into `PerfBenchmarkAverageResult`. Timings are converted from nanoseconds to **milliseconds**, and counters are raw **counts**. 

A crucial point:

* the timing fields below are **accumulated totals across the full measured scheduling run**
* they are **not per-job averages**
* they are **not per-call averages**

They are sums over all relevant calls executed during one measured benchmark run, then summarized across `averaging` repeats. 

### Timing metrics in `perf`

#### `total_schedule_cycle_ms`

Unit: milliseconds
Meaning: total internal measured time accumulated for the whole `schedule_cycle` function.
Aggregation: **total over the full measured run**. 

#### `init_slot_sets_ms`

Unit: milliseconds
Meaning: accumulated time spent initializing slot sets.
Aggregation: **total over the full measured run**. 

#### `get_waiting_jobs_ms`

Unit: milliseconds
Meaning: accumulated time spent fetching waiting jobs from the platform abstraction.
Aggregation: **total over the full measured run**. 

#### `sort_jobs_ms`

Unit: milliseconds
Meaning: accumulated time spent sorting jobs before scheduling.
Aggregation: **total over the full measured run**. 

#### `schedule_jobs_ms`

Unit: milliseconds
Meaning: accumulated time spent in the main scheduling loop over jobs.
Aggregation: **total over the full measured run**. 

#### `save_assignments_ms`

Unit: milliseconds
Meaning: accumulated time spent saving assignments after scheduling.
Aggregation: **total over the full measured run**. 

#### `schedule_job_ms`

Unit: milliseconds
Meaning: accumulated time spent in the per-job scheduling function.
Aggregation: **sum over all scheduled/attempted jobs in the measured run**.
Interpretation: whole-job total, but accumulated across many jobs. 

#### `find_slots_ms`

Unit: milliseconds
Meaning: accumulated time spent searching candidate slots/time windows for jobs.
Aggregation: **sum over all calls during the measured run**. 

#### `intersect_slots_ms`

Unit: milliseconds
Meaning: accumulated time spent intersecting slot intervals / slot resource sets.
Aggregation: **sum over all calls during the measured run**. 

#### `hierarchy_request_ms`

Unit: milliseconds
Meaning: accumulated time spent in hierarchy matching / hierarchy request resolution.
Aggregation: **sum over all calls during the measured run**. 

#### `quotas_ms`

Unit: milliseconds
Meaning: accumulated time spent in quotas checks.
Aggregation: **sum over all calls during the measured run**.
Notes: this field exists in JSON schema because it is part of `PerfBenchmarkResult`, even if many runs currently show zeros. 

#### `update_slots_ms`

Unit: milliseconds
Meaning: accumulated time spent updating slots/resources after assignments.
Aggregation: **sum over all calls during the measured run**. 

---

## Counter metrics in `perf`

All counters below are raw **counts accumulated over the whole measured run**, then wrapped into statistics across repeated runs. They are **not per-job** and **not per-call averages** unless you normalize them yourself later.

#### `jobs_seen`

Unit: jobs
Meaning: total number of jobs seen by the internal scheduler instrumentation.
Aggregation: **total count over the measured run**. 

#### `jobs_scheduled`

Unit: jobs
Meaning: total number of jobs successfully scheduled according to internal perf counters.
Aggregation: **total count over the measured run**. 

#### `moldables_seen`

Unit: moldables
Meaning: total number of moldable job variants inspected.
Aggregation: **total count over the measured run**. 

#### `slot_windows_scanned`

Unit: slot windows
Meaning: total number of slot/time windows scanned during search.
Aggregation: **total count over the measured run**. 

#### `slots_split`

Unit: slots
Meaning: total number of slot split operations performed.
Aggregation: **total count over the measured run**. 

#### `slots_intersected`

Unit: slot intersections
Meaning: total number of slot intersection operations / intersected slot elements processed.
Aggregation: **total count over the measured run**. 

#### `hierarchy_calls`

Unit: calls
Meaning: number of hierarchy request/matching calls.
Aggregation: **total count over the measured run**. 

#### `hierarchy_partitions_scanned`

Unit: partitions
Meaning: total number of hierarchy partitions scanned by the matcher.
Aggregation: **total count over the measured run**.
Interpretation: a key structural-search cost indicator. 

#### `quotas_checks`

Unit: checks
Meaning: total number of quotas checks performed.
Aggregation: **total count over the measured run**. 

#### `quotas_rejects`

Unit: rejects
Meaning: total number of quota-based rejections.
Aggregation: **total count over the measured run**. 

#### `update_calls`

Unit: calls
Meaning: total number of update calls performed after placements.
Aggregation: **total count over the measured run**. 

#### `updated_slots`

Unit: slots
Meaning: total number of slots touched/updated during update operations.
Aggregation: **total count over the measured run**. 

#### `cache_hits`

Unit: hits
Meaning: internal scheduler cache hits recorded by perf instrumentation.
Aggregation: **total count over the measured run**.
Important: this is different from the top-level `cache_hits`, which is a percentage of repeated request signatures in the generated workload. 

#### `cache_misses`

Unit: misses
Meaning: internal scheduler cache misses recorded by perf instrumentation.
Aggregation: **total count over the measured run**. 

---

## Distinguishing similarly named metrics

### Top-level `cache_hits` vs `perf.cache_hits`

These are **different** metrics.

#### Top-level `cache_hits`

* unit: percent
* meaning: percentage of repeated request signatures in the generated waiting-job set
* computed before scheduling from waiting jobs
* aggregation: **summary ratio per benchmark point** 

#### `perf.cache_hits`

* unit: raw hit count
* meaning: actual internal scheduler cache hits during execution
* aggregation: **total count over the measured run** 

The same distinction applies to `cache_misses`, which exists only in `perf`.

---

## Statistics object semantics

Any field exported as `BenchmarkMeasurementStatistics` has this structure:

* `min`
* `max`
* `mean`
* `q1`
* `q2`
* `q3`
* `std_dev`

### Units

The unit of the statistics object is the same as the unit of the underlying metric:

* milliseconds for timing metrics
* counts for counters
* percent for ratio metrics
* hours for `gantt_width` and `optimal_gantt_width` 

### Aggregation meaning

These statistics summarize **repeated benchmark runs at the same jobs-count point**.

They do **not** mean:

* per-job distribution
* per-function-call distribution

They mean:

* distribution across repeated executions of the whole benchmark point. 

---

## Practical interpretation guide

### Metrics that are totals over the whole measured run

Examples:

* `scheduling_time`
* `perf.total_schedule_cycle_ms`
* `perf.find_slots_ms`
* `perf.hierarchy_request_ms`
* `perf.slot_windows_scanned`
* `perf.hierarchy_partitions_scanned`

These should be interpreted as:

* “how much work the scheduler did in total for this benchmark point”

### Metrics that are ratios / derived summaries

Examples:

* top-level `cache_hits`
* `quotas_hit`
* `resource_occupation`

These summarize the benchmark point into a compact percentage.

### Metrics that are metadata only

Examples:

* `config.sample_type`
* `config.topology`
* `config.prefill`
* `config.nnodes`

These tell you how the experiment was configured, not how the scheduler performed.

---

## Current caveats

### 1. `std_dev` is not true standard deviation

It is currently variance-like, not sqrt(variance). 

### 2. `optimal_gantt_width` normalization currently uses `res_count`

In the current code, `optimal_gantt_width` is still normalized by `config.res_count`, not by the dynamically computed `effective_res_count`. This affects:

* `optimal_gantt_width`
* `resource_occupation`

and should be interpreted carefully until fixed.

### 3. No explicit per-job or per-call averages are exported yet

The current JSON exports:

* totals over the measured run
* statistics across repeated runs

If you want:

* per-job latency,
* per-call phase cost,
* normalized counters like `hierarchy_partitions_scanned / job`,

those must currently be derived in post-processing. 

---

## Suggested post-processing derived metrics

These are not in the JSON directly, but are useful to compute:

* `find_slots_ms / scheduled_jobs_count.mean`
* `hierarchy_request_ms / scheduled_jobs_count.mean`
* `slot_windows_scanned / scheduled_jobs_count.mean`
* `hierarchy_partitions_scanned / scheduled_jobs_count.mean`
* `updated_slots / scheduled_jobs_count.mean`
* `perf.cache_hits / (perf.cache_hits + perf.cache_misses)`

These are useful because most current `perf` fields are totals, not normalized quantities. 

