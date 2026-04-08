import argparse
import json
from pathlib import Path
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np


def nested_get(dct, path, default=None):
    cur = dct
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def stat_mean(v, default=0.0):
    if isinstance(v, dict):
        x = v.get("mean", default)
    else:
        x = v
    try:
        return float(x)
    except (TypeError, ValueError):
        return float(default)


def safe_div(a, b, default=0.0):
    return a / b if b else default


def load_phase_json(path: Path):
    with path.open() as f:
        payload = json.load(f)

    cfg = payload.get("config", {})
    rows = payload.get("results", [])

    out = []
    for row in rows:
        perf = row.get("perf", {}) or {}

        jobs_count = stat_mean(row.get("jobs_count", 0))
        scheduled_jobs_count = stat_mean(row.get("scheduled_jobs_count", 0))
        slot_count = stat_mean(row.get("slot_count", 0))
        scheduling_time = stat_mean(row.get("scheduling_time", 0))
        total_time_per_job_avg = safe_div(stat_mean(perf.get("total_schedule_cycle_ns", 0)), jobs_count)
        cache_hits_external = stat_mean(row.get("cache_hits", 0))
        quotas_hit = stat_mean(row.get("quotas_hit", 0))
        gantt_width = stat_mean(row.get("gantt_width", 0))
        optimal_gantt_width = stat_mean(row.get("optimal_gantt_width", 0))
        resource_occupation = stat_mean(row.get("resource_occupation", 0))

        total_schedule_cycle_ns = stat_mean(perf.get("total_schedule_cycle_ns", 0))
        init_slot_sets_ns = stat_mean(perf.get("init_slot_sets_ns", 0))
        get_waiting_jobs_ns = stat_mean(perf.get("get_waiting_jobs_ns", 0))
        sort_jobs_ns = stat_mean(perf.get("sort_jobs_ns", 0))
        schedule_jobs_ns = stat_mean(perf.get("schedule_jobs_ns", 0))
        save_assignments_ns = stat_mean(perf.get("save_assignments_ns", 0))
        schedule_job_ns = stat_mean(perf.get("schedule_job_ns", 0))
        find_slots_ns = stat_mean(perf.get("find_slots_ns", 0))
        intersect_slots_ns = stat_mean(perf.get("intersect_slots_ns", 0))
        hierarchy_request_ns = stat_mean(perf.get("hierarchy_request_ns", 0))
        quotas_ns = stat_mean(perf.get("quotas_ns", 0))
        update_slots_ns = stat_mean(perf.get("update_slots_ns", 0))

        jobs_seen = stat_mean(perf.get("jobs_seen", 0))
        jobs_scheduled_internal = stat_mean(perf.get("jobs_scheduled", 0))
        moldables_seen = stat_mean(perf.get("moldables_seen", 0))
        slot_windows_scanned = stat_mean(perf.get("slot_windows_scanned", 0))
        slots_split = stat_mean(perf.get("slots_split", 0))
        slots_intersected = stat_mean(perf.get("slots_intersected", 0))
        hierarchy_calls = stat_mean(perf.get("hierarchy_calls", 0))
        hierarchy_partitions_scanned = stat_mean(perf.get("hierarchy_partitions_scanned", 0))
        quotas_checks = stat_mean(perf.get("quotas_checks", 0))
        quotas_rejects = stat_mean(perf.get("quotas_rejects", 0))
        update_calls = stat_mean(perf.get("update_calls", 0))
        updated_slots = stat_mean(perf.get("updated_slots", 0))
        cache_hits_internal = stat_mean(perf.get("cache_hits", 0))
        cache_misses_internal = stat_mean(perf.get("cache_misses", 0))

        out_row = {
            "source_file": path.name,
            "sample_type": cfg.get("sample_type", "Unknown"),
            "topology": cfg.get("topology", "Unknown"),
            "prefill": cfg.get("prefill", "Unknown"),
            "cache_enabled": cfg.get("cache", True),
            "target": cfg.get("target", "Unknown"),
            "res_count": cfg.get("res_count", 0),
            "nnodes": cfg.get("nnodes", 0),
            "jobs_count": jobs_count,
            "scheduled_jobs_count": scheduled_jobs_count,
            "slot_count": slot_count,
            "scheduling_time": scheduling_time,
            "total_time_per_job_avg": total_time_per_job_avg,
            "cache_hits_external": cache_hits_external,
            "quotas_hit": quotas_hit,
            "gantt_width": gantt_width,
            "optimal_gantt_width": optimal_gantt_width,
            "resource_occupation": resource_occupation,
            "total_schedule_cycle_ns": total_schedule_cycle_ns,
            "init_slot_sets_ns": init_slot_sets_ns,
            "get_waiting_jobs_ns": get_waiting_jobs_ns,
            "sort_jobs_ns": sort_jobs_ns,
            "schedule_jobs_ns": schedule_jobs_ns,
            "save_assignments_ns": save_assignments_ns,
            "schedule_job_ns": schedule_job_ns,
            "find_slots_ns": find_slots_ns,
            "intersect_slots_ns": intersect_slots_ns,
            "hierarchy_request_ns": hierarchy_request_ns,
            "quotas_ns": quotas_ns,
            "update_slots_ns": update_slots_ns,
            "jobs_seen": jobs_seen,
            "jobs_scheduled_internal": jobs_scheduled_internal,
            "moldables_seen": moldables_seen,
            "slot_windows_scanned": slot_windows_scanned,
            "slots_split": slots_split,
            "slots_intersected": slots_intersected,
            "hierarchy_calls": hierarchy_calls,
            "hierarchy_partitions_scanned": hierarchy_partitions_scanned,
            "quotas_checks": quotas_checks,
            "quotas_rejects": quotas_rejects,
            "update_calls": update_calls,
            "updated_slots": updated_slots,
            "cache_hits_internal": cache_hits_internal,
            "cache_misses_internal": cache_misses_internal,
        }

        out_row["series_auto"] = (
            f"{out_row['sample_type']} | "
            f"nnodes={out_row['nnodes']} | "
            f"{out_row['topology']} | "
            f"{out_row['prefill']} | "
            f"cache={'on' if out_row['cache_enabled'] else 'off'}"
        )

        out_row["cache_hit_ratio_internal"] = safe_div(cache_hits_internal, cache_hits_internal + cache_misses_internal)
        out_row["find_slots_share"] = safe_div(find_slots_ns, total_schedule_cycle_ns)
        out_row["hierarchy_share_of_find_slots"] = safe_div(hierarchy_request_ns, find_slots_ns)
        out_row["intersect_share_of_find_slots"] = safe_div(intersect_slots_ns, find_slots_ns)
        out_row["update_share_of_total"] = safe_div(update_slots_ns, total_schedule_cycle_ns)
        out_row["slot_windows_per_job"] = safe_div(slot_windows_scanned, jobs_count)
        out_row["slots_intersected_per_job"] = safe_div(slots_intersected, jobs_count)
        out_row["hierarchy_partitions_per_job"] = safe_div(hierarchy_partitions_scanned, jobs_count)
        out_row["updated_slots_per_scheduled_job"] = safe_div(updated_slots, jobs_count)
        out_row["slots_split_per_scheduled_job"] = safe_div(slots_split, jobs_count)
        out_row["schedule_job_ns_per_job"] = safe_div(schedule_job_ns, jobs_count)
        out_row["find_slots_ns_per_job"] = safe_div(find_slots_ns, jobs_count)
        out_row["hierarchy_request_ns_per_job"] = safe_div(hierarchy_request_ns, jobs_count)
        out_row["intersect_slots_ns_per_job"] = safe_div(intersect_slots_ns, jobs_count)
        out_row["update_slots_ns_per_job"] = safe_div(update_slots_ns, jobs_count)
        out_row["init_slot_sets_ns_per_job"] = safe_div(init_slot_sets_ns, jobs_count)
        out_row["sort_jobs_ns_per_job"] = safe_div(sort_jobs_ns, jobs_count)
        out_row["save_assignments_ns_per_job"] = safe_div(save_assignments_ns, jobs_count)

        out.append(out_row)

    return out


def load_inputs(path_str: str):
    path = Path(path_str)
    rows = []

    if path.is_dir():
        for p in sorted(path.glob("*.json")):
            rows.extend(load_phase_json(p))
    else:
        rows.extend(load_phase_json(path))

    return rows


def filter_rows(rows, sample=None, topology=None, prefill=None, cache=None, nnodes=None):
    out = []
    for r in rows:
        if sample is not None and r["sample_type"] != sample:
            continue
        if topology is not None and r["topology"] != topology:
            continue
        if prefill is not None and r["prefill"] != prefill:
            continue
        if cache is not None and r["cache_enabled"] != cache:
            continue
        if nnodes is not None and int(r["nnodes"]) != int(nnodes):
            continue
        out.append(r)
    return out


def grouped_series(rows, x_key, series_key, metric):
    grouped = defaultdict(list)
    for r in rows:
        grouped[r[series_key]].append(r)

    result = {}
    for series_name, srows in sorted(grouped.items(), key=lambda kv: str(kv[0])):
        by_x = defaultdict(list)
        for r in srows:
            by_x[r[x_key]].append(float(r.get(metric, 0.0)))

        xs = sorted(by_x.keys())
        ys = [sum(by_x[x]) / len(by_x[x]) for x in xs]
        result[series_name] = (np.array(xs, dtype=float), np.array(ys, dtype=float))
    return result


def plot_lines(rows, outpath, x_key, series_key, metrics, title, ylabel):
    fig, ax = plt.subplots(figsize=(10, 6))
    anything = False

    for metric in metrics:
        data = grouped_series(rows, x_key, series_key, metric)
        for series_name, (xs, ys) in data.items():
            ax.plot(xs, ys, marker="o", label=f"{metric} | {series_name}")
            anything = True

    if not anything:
        plt.close(fig)
        return

    ax.set_title(title)
    ax.set_xlabel(x_key)
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(outpath, dpi=160)
    plt.close(fig)


def plot_stacked(rows, outdir, x_key, series_key):
    phase_metrics = [
        ("init_slot_sets_ns_per_job", "init_slot_sets"),
        ("sort_jobs_ns_per_job", "sort_jobs"),
        ("find_slots_ns_per_job", "find_slots"),
        ("intersect_slots_ns_per_job", "intersect_slots"),
        ("hierarchy_request_ns_per_job", "hierarchy_request"),
        ("update_slots_ns_per_job", "update_slots"),
        ("save_assignments_ns_per_job", "save_assignments"),
    ]

    series_names = sorted(set(r[series_key] for r in rows), key=str)
    for s in series_names:
        sub = [r for r in rows if r[series_key] == s]
        if not sub:
            continue

        x_vals = sorted(set(r[x_key] for r in sub))
        if len(x_vals) < 1:
            continue

        ys = []
        labels = []

        for metric, label in phase_metrics:
            by_x = defaultdict(list)
            for r in sub:
                by_x[r[x_key]].append(float(r.get(metric, 0.0)))
            y = [sum(by_x[x]) / len(by_x[x]) if x in by_x else 0.0 for x in x_vals]
            ys.append(y)
            labels.append(label)

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.stackplot(x_vals, *ys, labels=labels, alpha=0.85)
        ax.set_title(f"Stacked phases | {s}")
        ax.set_xlabel(x_key)
        ax.set_ylabel("ns")
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=8, loc="upper left")
        fig.tight_layout()
        fig.savefig(outdir / f"stacked_phases_{sanitize_filename(str(s))}.png", dpi=160)
        plt.close(fig)


def sanitize_filename(s: str):
    bad = ["/", "\\", ":", "*", "?", '"', "<", ">", "|", " "]
    for ch in bad:
        s = s.replace(ch, "_")
    return s


def dump_summary(rows, outdir):
    summary = []
    keys = [
        "source_file",
        "sample_type",
        "topology",
        "prefill",
        "cache_enabled",
        "nnodes",
        "jobs_count",
        "scheduling_time",
        "total_time_per_job_avg",
        "total_schedule_cycle_ns",
        "find_slots_ns",
        "hierarchy_request_ns",
        "intersect_slots_ns",
        "update_slots_ns",
        "slot_windows_scanned",
        "slots_intersected",
        "hierarchy_partitions_scanned",
        "cache_hit_ratio_internal",
        "find_slots_share",
        "hierarchy_share_of_find_slots",
        "intersect_share_of_find_slots",
        "slot_windows_per_job",
        "slots_intersected_per_job",
        "hierarchy_partitions_per_job",
    ]
    for r in rows:
        summary.append({k: r.get(k) for k in keys})

    with (outdir / "summary_rows.json").open("w") as f:
        json.dump(summary, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="JSON file or directory with JSON files")
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--sample", default=None)
    parser.add_argument("--topology", default=None)
    parser.add_argument("--prefill", default=None)
    parser.add_argument("--nnodes", type=int, default=None)
    parser.add_argument("--cache", choices=["on", "off"], default=None)
    parser.add_argument(
        "--group-by",
        default="auto",
        choices=["auto", "sample_type", "topology", "prefill", "nnodes", "source_file"],
    )
    parser.add_argument(
        "--x-key",
        default="jobs_count",
        choices=["jobs_count", "nnodes"],
    )
    args = parser.parse_args()

    rows = load_inputs(args.input)

    cache_filter = None
    if args.cache == "on":
        cache_filter = True
    elif args.cache == "off":
        cache_filter = False

    rows = filter_rows(
        rows,
        sample=args.sample,
        topology=args.topology,
        prefill=args.prefill,
        cache=cache_filter,
        nnodes=args.nnodes,
    )

    if not rows:
        print("No matching rows found.")
        return

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if args.group_by == "auto":
        series_key = "series_auto"
    else:
        series_key = args.group_by

    x_key = args.x_key

    plot_lines(
        rows,
        outdir / "total_time_per_job_avg.png",
        x_key,
        series_key,
        ["total_time_per_job_avg"],
        "total time per job average",
        "ns",
    )

    # plot_lines(
    #     rows,
    #     outdir / "runtime_total.png",
    #     x_key,
    #     series_key,
    #     ["total_schedule_cycle_ns"],
    #     "Total runtime",
    #     "ns",
    # )

    plot_lines(
        rows,
        outdir / "runtime_core_phases.png",
        x_key,
        series_key,
        [
            "find_slots_ns_per_job",
            "hierarchy_request_ns_per_job",
            "intersect_slots_ns_per_job",
            "update_slots_ns_per_job",
        ],
        "Core phase runtimes",
        "ns",
    )

    # plot_lines(
    #     rows,
    #     outdir / "runtime_shares.png",
    #     x_key,
    #     series_key,
    #     [
    #         "find_slots_share",
    #         "hierarchy_share_of_find_slots",
    #         "intersect_share_of_find_slots",
    #         "update_share_of_total",
    #     ],
    #     "Runtime shares",
    #     "ratio",
    # )

    # plot_lines(
    #     rows,
    #     outdir / "counters_raw.png",
    #     x_key,
    #     series_key,
    #     [
    #         "slot_windows_scanned",
    #         "slots_intersected",
    #         "hierarchy_partitions_scanned",
    #         "updated_slots",
    #     ],
    #     "Raw counters",
    #     "count",
    # )

    plot_lines(
        rows,
        outdir / "counters_normalized.png",
        x_key,
        series_key,
        [
            "slot_windows_per_job",
            "slots_intersected_per_job",
            "hierarchy_partitions_per_job",
            "updated_slots_per_scheduled_job",
        ],
        "Normalized counters",
        "count per job",
    )

    plot_lines(
        rows,
        outdir / "cache_hit_ratio.png",
        x_key,
        series_key,
        ["cache_hit_ratio_internal"],
        "Internal cache hit ratio",
        "ratio",
    )

    plot_lines(
        rows,
        outdir / "slot_count.png",
        x_key,
        series_key,
        ["slot_count"],
        "slot count",
        "count",
    )

    plot_lines(
        rows,
        outdir / "gantt_occupation.png",
        x_key,
        series_key,
        ["gantt_width", "optimal_gantt_width", "resource_occupation"],
        "Gantt / occupation metrics",
        "mixed units",
    )

    plot_stacked(rows, outdir, x_key=x_key, series_key=series_key)
    dump_summary(rows, outdir)

    print(f"Wrote plots to {outdir}")


if __name__ == "__main__":
    main()