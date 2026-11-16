"""Paired benefit/cost experiments using the team's CentralController.

Central authority uses the team implementation behind a test-only gRPC service.
Agents perform recovery and LOCAL handoff in independent processes.
"""
import importlib.util
import json
import math
import os
import random
import statistics
from pathlib import Path

from .autonomous import EVALUATION, LIMITS, run_case
from .comparison import MODELS, write_csv
from .controller import CONTROLLER_IMPLEMENTATION

SCENARIOS = ("normal", "controller-outage", "holder-failure")


def make_workload(seed, count, duration):
    rng = random.Random(seed)
    return [dict(request_id=f"request-{i + 1}", crane=rng.choice(["crane-2", "crane-3"]),
                 requested_at=(i + .2 + .6 * rng.random()) * duration / count) for i in range(count)]



def paired(results, scenario, baseline, metric):
    """Only compare exact seeded inputs and timing parameters; missing stays missing."""
    def key(row):
        docker = row.get("docker", {})
        return tuple(row.get(field) for field in ("seed", "requested", "workload_sha256", "duration_seconds",
                     "crossing_time_seconds", "detection_timeout_seconds", "evaluation")) + (row.get("measurement_policy","fixed_window"), row.get("controller_implementation"), row.get("controller_source_commit"), row.get("recovery_driver"), row.get("execution_mode"), docker.get("image_id"), docker.get("cpus_per_service"), docker.get("memory_bytes_per_service"), row.get("heartbeat_interval_seconds"), row.get("host_rpc_timeout_seconds"))
    results = [row for row in results if row.get("evaluation") in {"benefits-v1","benefits-v2","benefits-v3","benefits-v4","benefits-docker-v1"}
               and (row.get("evaluation") not in {"benefits-v3", "benefits-v4", "benefits-docker-v1"} or
                    (row.get("controller_implementation") == CONTROLLER_IMPLEMENTATION and
                     isinstance(row.get("controller_source_commit"), str)))
               and (row.get("evaluation") != EVALUATION or
                    (row.get("recovery_driver") == "agent_workers" and
                     row.get("execution_mode") == "independent_processes"))
               and (row.get("evaluation") != "benefits-docker-v1" or
                    (row.get("execution_mode") == "docker_containers" and
                     row.get("recovery_driver") == "agent_workers" and row.get("passed") is True and
                     all(row.get("docker", {}).get(field) is not None for field in
                         ("image_id", "cpus_per_service", "memory_bytes_per_service"))))
               and isinstance(row.get("workload_sha256"), str)
               and all(row.get(field) is not None for field in ("seed", "requested",
                   "crossing_time_seconds", "detection_timeout_seconds"))
               and (row.get("duration_seconds") is not None or row.get("measurement_policy")=="complete_all_requests")]
    reference = {}
    for row in results:
        if row["scenario"] == scenario and row["model"] == baseline:
            reference.setdefault(key(row), []).append(row)
    differences = []
    for row in results:
        matches = reference.get(key(row), [])
        other = matches[0] if len(matches) == 1 else None
        if row["scenario"] == scenario and row["model"] == "cranelock" and other is not None:
            a, b = row.get(metric), other.get(metric)
            if isinstance(a, (float, int)) and isinstance(b, (float, int)):
                differences.append(dict(seed=row["seed"], requested=row["requested"], delta=a-b))
    values = [row["delta"] for row in differences]
    return dict(pairs=differences, median=statistics.median(values) if values else None,
                minimum=min(values) if values else None, maximum=max(values) if values else None)


async def run_benefits(output, *, seeds=(7, 20, 3104), loads=(6, 18), scenario="all", duration=3,
                       crossing_time=.03, detection_timeout=.25, plots=True, progress=None):
    if not seeds or len(set(seeds)) != len(seeds) or any(type(s) is not int for s in seeds):
        raise ValueError("Use distinct integer seeds")
    if not loads or len(set(loads)) != len(loads) or any(type(n) is not int or n < 3 for n in loads):
        raise ValueError("Use distinct request counts of at least 3")
    if scenario not in (*SCENARIOS, "all"):
        raise ValueError("Unknown benefits scenario")
    if any(not math.isfinite(n) or n <= 0 for n in (duration, crossing_time, detection_timeout)) or detection_timeout <= .05:
        raise ValueError("Times must be positive and finite; detection timeout must exceed .05")
    if duration < detection_timeout * 6:
        raise ValueError("Window must allow at least six detection timeouts")
    if plots and importlib.util.find_spec("matplotlib") is None:
        raise ValueError('Plotting requires pip install -e ".[evaluation]", or use --no-plots')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    results = []
    for seed in seeds:
        for count in loads:
            workload = make_workload(seed, count, duration)
            order = [(model, case) for case in SCENARIOS if scenario in (case, "all") for model in MODELS]
            random.Random(seed + count).shuffle(order)
            for model, case in order:
                case_workload = [dict(row, requested_at=0.0) for row in workload] if case == "normal" else workload
                result = await run_case(output / f"load-{count}" / f"seed-{seed}" / case / model,
                    model, case, case_workload, seed=seed, duration=duration,
                    crossing_time=crossing_time, detection_timeout=detection_timeout)
                results.append(result)
                if progress:
                    progress(f"[PASS] load={count} seed={seed} {case} {model}: {result['completed']}/{count}; post-fault entries={result.get('post_fault_new_entries')}")
    return export_benefits(output, results, loads, plots=plots)


def export_benefits(output, results, loads, *, plots=False):
    """Export a verified matrix without re-running its already measured cases."""
    output = Path(output)
    benefits = {}
    for load in loads:
        selected = [row for row in results if row["requested"] == load]
        benefits[str(load)] = dict(controller_outage_extra_entries=paired(selected,"controller-outage","central-lock","post_fault_new_entries"),
            holder_failure_extra_completed=paired(selected,"holder-failure","basic-token","completed"),
            normal_wait_cost_seconds=paired(selected,"normal","central-lock","mean_wait_seconds"),
            normal_completion_time_cost_seconds=paired(selected,"normal","central-lock","total_completion_seconds"),
            normal_rpc_cost_per_completion=paired(selected,"normal","central-lock","rpc_per_completed"))
    summary = dict(evaluation=EVALUATION, recovery_driver="agent_workers", execution_mode="independent_processes", passed=True, runs=len(results), limitations=LIMITS, benefits=benefits, results=results)
    fields = list(dict.fromkeys(k for row in results for k in row if k != "rpc_by_method"))
    write_csv(output / "benefits.csv", [{k: row.get(k) for k in fields} for row in results])
    (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if plots:
        plot_benefits(output, results)
    return summary


def plot_benefits(output, results):
    os.environ.setdefault("MPLCONFIGDIR", str((output / ".matplotlib").resolve()))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    specs = [("controller-outage", "central-lock", "post_fault_new_entries", 1, "Extra new entries after controller outage"),
             ("holder-failure", "basic-token", "completed", 1, "Extra completed uses after holder failure"),
             ("normal", "central-lock", "mean_wait_seconds", 1000, "Normal wait change vs central (ms; positive = cost)"),
             ("normal", "central-lock", "total_completion_seconds", 1, "Batch completion time change vs central (seconds)")]
    loads = sorted({row["requested"] for row in results})
    fig, axes = plt.subplots(2, 2, figsize=(13, 8))
    for ax, (scenario, baseline, metric, scale, title) in zip(axes.flat, specs):
        for i, load in enumerate(loads):
            pairs = paired([r for r in results if r["requested"] == load], scenario, baseline, metric)
            values = [row["delta"] * scale for row in pairs["pairs"]]
            if values:
                ax.bar(i, statistics.median(values), color="#438b83", alpha=.65)
                ax.scatter([i]*len(values), values, color="#172b3a", zorder=3)
            else:
                ax.text(i, .1, "Not measured", ha="center", transform=ax.get_xaxis_transform())
        ax.axhline(0, color="#888", linewidth=.7)
        ax.set_xticks(range(len(loads)), [f"{load} requests" for load in loads])
        ax.set_title(title, fontsize=10)
    fig.suptitle("CraneLock: paired benefits and costs — independent local processes")
    fig.text(.5, .02, "Bars: median paired difference. Dots: seeded pairs. Three agents on one host; not production performance.", ha="center", fontsize=9)
    fig.tight_layout(rect=(0,.05,1,.95))
    for suffix in ("png", "svg"):
        fig.savefig(output / f"benefit_summary.{suffix}", dpi=160)
    plt.close(fig)
