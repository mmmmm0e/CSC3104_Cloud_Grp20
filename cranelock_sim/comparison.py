"""Paired, bounded comparisons of reference adapters through the same trusted gate.

Central scheduling is a test fixture. Both token variants use the existing Agent;
only CraneLock enables real recovery voting. No team controller is substituted
silently, and no logical-time metrics are mixed with these host-local RPC runs.
"""
import asyncio
import csv
import hashlib
import json
import importlib.util
import math
import random
import os
import statistics
import time
from dataclasses import asdict
from pathlib import Path

import grpc

from cranelock_gate.client import Client, certificate
from cranelock_gate.core import Gate
from cranelock_gate.server import RunningServer
from cranelock_gate.storage import initialize
from .agents import AgentCluster, wall_clock
from .experiment import new_config, percentile, require
from .failures import until
from .lane import Lane, SafetyViolation


MODELS = ("central-lock", "basic-token", "cranelock")
SCENARIOS = ("normal", "holder-failure")
LIMITS = ("Reference central scheduler; token handoffs paced by test adapter; common gate/fencing "
          "for all models; one host process; no controller outage or production performance claim.")


def make_workload(seed, count, duration):
    """Identical survivor-only demand avoids dead-crane jobs confounding recovery."""
    rng = random.Random(seed)
    arrivals = sorted(rng.uniform(0, duration / 4) for _ in range(count))
    return [dict(request_id=f"request-{index + 1}", crane=rng.choice(["crane-2", "crane-3"]),
                 requested_at=arrival) for index, arrival in enumerate(arrivals)]


def workload_hash(workload):
    return hashlib.sha256(json.dumps(workload, sort_keys=True).encode()).hexdigest()


async def run_case(output, model, scenario, workload, *, seed, duration, crossing_time, detection_timeout):
    output.mkdir(parents=True, exist_ok=False)
    origin = time.monotonic()
    events, detections = [], {}

    def emit(event, **fields):
        now = time.monotonic() - origin
        events.append(dict(time=now, event=event, **fields))
        raw = fields.get("agent_event", {})
        if raw.get("event") == "TOKEN_HOLDER_SUSPECTED" and raw.get("peer_id") == "crane-1":
            detections.setdefault(raw["agent_id"], now)

    settings = dict(model=model, scenario=scenario, seed=seed, duration_seconds=duration,
        crossing_time_seconds=crossing_time, detection_timeout_seconds=detection_timeout,
        workload_sha256=workload_hash(workload), time_basis="host_monotonic_seconds", limitations=LIMITS)
    (output / "experiment.json").write_text(json.dumps(settings, indent=2), encoding="utf-8")
    (output / "workload.json").write_text(json.dumps(workload, indent=2), encoding="utf-8")
    config, keys = new_config(3)
    (output / "gate-config.json").write_text(json.dumps(asdict(config), indent=2), encoding="utf-8")
    cluster = AgentCluster(list(config.members), output, emit, timeout=detection_timeout,
        recovery_config=config if model == "cranelock" else None,
        keys=keys if model == "cranelock" else None)
    gate, server, channel, producer = None, None, None, None
    rows = [dict(row, submitted_at=None, entered_at=None, exited_at=None, status="waiting") for row in workload]
    try:
        initialize(output / "gate.sqlite", config, wall_clock())
        gate = Gate(output / "gate.sqlite", config, wall_clock)
        server = RunningServer(gate, "127.0.0.1:0")
        channel = grpc.insecure_channel(f"127.0.0.1:{server.port}")
        await asyncio.to_thread(grpc.channel_ready_future(channel).result, timeout=5)
        client = Client(channel, config, keys)
        state = await asyncio.to_thread(client.state)
        central = model == "central-lock"
        cert = certificate(config, keys, state, "crane-1", 0,
            mode="CENTRAL" if central else "LOCAL", signers=["controller"] if central else None)
        require(await asyncio.to_thread(client.call, "controller" if central else "crane-1",
            "InstallAuthority", certificate=cert, expected_epoch=state["epoch"],
            expected_revision=state["revision"]))
        lane = Lane(client, wall_clock, emit)
        require(await asyncio.to_thread(lane.observe))
        await cluster.start()
        await until(lambda: all(any(event["event"] == "agent_event" and
            event["agent_event"]["event"] == "HEARTBEAT_RECEIVED" and
            event["agent_event"]["agent_id"] == observer and
            event["agent_event"].get("peer_id") == peer for event in events)
            for observer in config.members for peer in config.members if observer != peer), 5, "heartbeat warmup")
        if any(agent.suspected for agent in cluster.agents.values()):
            raise RuntimeError("Unexpected suspicion during comparison startup")
        # All exported request and fault times use one origin after service warmup.
        events.clear()
        detections.clear()
        origin = time.monotonic()
        queue = asyncio.Queue()

        async def submit():
            for row in rows:
                await asyncio.sleep(max(0, row["requested_at"] - (time.monotonic() - origin)))
                row["submitted_at"] = time.monotonic() - origin
                emit("request", request_id=row["request_id"], crane=row["crane"],
                     scheduled_at=row["requested_at"])
                queue.put_nowait(row)

        producer = asyncio.create_task(submit())
        fault_at, detected_at, authority_at = None, None, None
        available = True
        if scenario == "holder-failure":
            # Same initial A holder, actual service stop, detector and request
            # trace for all models. Central controller remains available.
            await cluster.stop("crane-1")
            fault_at = time.monotonic() - origin
            emit("holder_stopped", crane="crane-1")
            await until(lambda: all(identity in detections for identity in ("crane-2", "crane-3")),
                        max(5, detection_timeout * 4), "shared failure detection")
            detected_at = min(detections.values())
            if detected_at < fault_at:
                raise RuntimeError("Comparison detected the failure before it was injected")
            if model == "basic-token":
                available = False  # No replacement token is manufactured.
                emit("recovery_disabled")
            elif model == "cranelock":
                state = await asyncio.to_thread(client.state)
                cert = await cluster.agents["crane-2"].propose_recovery(state, "crane-2")
                if cert is None:
                    raise RuntimeError("CraneLock comparison could not obtain a real quorum")
                (output / "recovery-certificate.json").write_text(json.dumps(cert, indent=2), encoding="utf-8")
                require(await asyncio.to_thread(client.call, "crane-2", "InstallAuthority",
                    certificate=cert, expected_epoch=state["epoch"], expected_revision=state["revision"]))
                accepted = await asyncio.to_thread(client.state)
                authority_at = time.monotonic() - origin
                cluster.authority_epoch = accepted["epoch"]
                for identity in ("crane-2", "crane-3"):
                    await cluster.agents[identity].reconcile(accepted["epoch"], accepted["holder"])
        if available:
            for _ in rows:
                remaining = duration - (time.monotonic() - origin)
                if remaining <= 0:
                    break
                try:
                    row = await asyncio.wait_for(queue.get(), timeout=remaining)
                except TimeoutError:
                    break
                require(await asyncio.to_thread(lane.observe))
                identity = row["crane"]
                state = await asyncio.to_thread(client.state)
                if central:
                    if state["holder"] != identity:
                        cert = certificate(config, keys, state, identity, state["epoch"],
                                           mode="CENTRAL", signers=["controller"])
                        require(await asyncio.to_thread(client.call, "controller", "InstallAuthority",
                            certificate=cert, expected_epoch=state["epoch"], expected_revision=state["revision"]))
                    if detected_at is not None and authority_at is None:
                        authority_at = time.monotonic() - origin
                else:
                    owner, _ = await cluster.snapshot("comparison_before_request")
                    if owner != identity:
                        response = await cluster.agents[identity].request_token()
                        if not response.accepted:
                            raise RuntimeError(f"Token request rejected: {response.reason}")
                        await cluster.handoff(client, lane, identity)
                    owner, snapshots = await cluster.snapshot("comparison_before_entry")
                    if owner != identity or snapshots[identity].epoch != cluster.authority_epoch:
                        raise SafetyViolation("Comparison entry does not hold current token")
                state = await asyncio.to_thread(client.state)
                grant = await asyncio.to_thread(client.call, identity, "RequestGrant",
                    epoch=state["epoch"], revision=state["revision"])
                require(grant)
                emit("grant_issued", request_id=row["request_id"], crane=identity,
                     grant_id=grant["grant"]["id"], epoch=state["epoch"], revision=state["revision"])
                if not await asyncio.to_thread(lane.enter, identity, row["request_id"], grant["grant"], state):
                    raise RuntimeError("Comparison entry denied")
                row.update(entered_at=time.monotonic() - origin, status="occupied")
                await asyncio.sleep(crossing_time)
                await asyncio.to_thread(lane.exit, identity, row["request_id"])
                exited = time.monotonic() - origin
                row.update(exited_at=exited, status="completed" if exited <= duration else "completed-after-horizon")
        await producer
        await asyncio.sleep(max(0, duration - (time.monotonic() - origin)))
        # Do not cancel an in-flight entry RPC or physical movement at the
        # horizon. Drain it, but exclude any completion outside the fixed window.
        completed = [row for row in rows if row["exited_at"] is not None and row["exited_at"] <= duration]
        admitted = [row for row in rows if row["entered_at"] is not None and row["entered_at"] <= duration]
        waits = [row["entered_at"] - row["requested_at"] for row in admitted]
        first_entry = min((row["entered_at"] for row in admitted), default=None)
        summary = dict(**settings, requested=len(rows), completed=len(completed),
            accepted=len(admitted), unfinished_at_horizon=len(rows) - len(completed),
            throughput_per_minute=len(completed) * 60 / duration,
            mean_wait_seconds=statistics.mean(waits) if waits else None,
            p95_wait_seconds=percentile(waits, 0.95), safety_violations=0,
            failure_detection_seconds=detected_at - fault_at if detected_at is not None else None,
            recovery_latency_seconds=first_entry - detected_at if first_entry is not None and detected_at is not None else None,
            authority_recovery_seconds=authority_at - detected_at if authority_at is not None else None,
            resumed_within_window=first_entry is not None if fault_at is not None else None,
            drain_seconds=max(0, time.monotonic() - origin - duration))
        (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary
    except Exception as error:
        emit("experiment_failed", error=type(error).__name__, detail=str(error))
        (output / "failure.json").write_text(json.dumps(dict(error=type(error).__name__, detail=str(error)), indent=2), encoding="utf-8")
        raise
    finally:
        if producer and not producer.done():
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
        await cluster.close()
        if channel:
            channel.close()
        try:
            if gate:
                with (output / "gate-events.jsonl").open("w", encoding="utf-8") as stream:
                    for event in gate.events():
                        stream.write(json.dumps(event) + "\n")
        finally:
            if server:
                await asyncio.to_thread(server.close)
            elif gate:
                gate.close()
            with (output / "events.jsonl").open("w", encoding="utf-8") as stream:
                for event in events:
                    stream.write(json.dumps(event) + "\n")
            write_csv(output / "requests.csv", rows)


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]) if rows else ["request_id"])
        writer.writeheader()
        writer.writerows(rows)


async def run_comparison(output, *, seeds=(7, 20, 3104), scenario="all", requests=6,
                         duration=2.0, crossing_time=0.03, detection_timeout=0.25,
                         plots=True, progress=None):
    seeds = tuple(seeds)
    if not seeds or len(set(seeds)) != len(seeds) or any(type(seed) is not int for seed in seeds):
        raise ValueError("Use one or more distinct integer seeds")
    if scenario not in (*SCENARIOS, "all"):
        raise ValueError(f"Unknown comparison scenario: {scenario}")
    if type(requests) is not int or requests < 1:
        raise ValueError("requests must be a positive integer")
    for name, value, minimum in (("duration", duration, 0), ("crossing_time", crossing_time, 0),
                                 ("detection_timeout", detection_timeout, 0.05)):
        if not math.isfinite(value) or value <= minimum:
            raise ValueError(f"{name} must be finite and > {minimum}")
    if plots and importlib.util.find_spec("matplotlib") is None:
        raise ValueError('Plotting requires pip install -e ".[evaluation]", or use --no-plots')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    settings = dict(seeds=seeds, requests=requests, duration_seconds=duration,
        crossing_time_seconds=crossing_time, detection_timeout_seconds=detection_timeout,
        scenario=scenario, limitations=LIMITS)
    (output / "comparison.json").write_text(json.dumps(settings, indent=2), encoding="utf-8")
    results = []
    # Seeded execution order reduces systematic model-order bias, without claiming
    # to eliminate host noise or to supply statistically independent samples.
    for seed in seeds:
        workload = make_workload(seed, requests, duration)
        (output / f"workload-seed-{seed}.json").write_text(json.dumps(workload, indent=2), encoding="utf-8")
        order = [(model, case) for case in SCENARIOS if scenario in (case, "all") for model in MODELS]
        random.Random(seed).shuffle(order)
        for model, case in order:
            result = await run_case(output / f"seed-{seed}" / case / model, model, case, workload,
                seed=seed, duration=duration, crossing_time=crossing_time, detection_timeout=detection_timeout)
            results.append(result)
            if progress:
                progress(f"[PASS] seed={seed} {case} {model}: {result['completed']}/{requests} completed; "
                         f"unfinished={result['unfinished_at_horizon']}; safety violations=0")
    write_csv(output / "comparison.csv", results)
    summary = dict(passed=True, runs=len(results), models=MODELS, scenarios=sorted({r["scenario"] for r in results}),
                   seeds=seeds, limitations=LIMITS, results=results)
    if plots:
        plot_results(output, results)
    (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def plot_results(output, results):
    os.environ.setdefault("MPLCONFIGDIR", str((output / ".matplotlib").resolve()))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    metrics = [("completed", "Completed within fixed window"),
               ("p95_wait_seconds", "p95 request-to-entry wait (seconds)"),
               ("recovery_latency_seconds", "Detection to safe entry (seconds)")]
    for metric, title in metrics:
        scenarios = sorted({row["scenario"] for row in results
                            if metric != "recovery_latency_seconds" or row["scenario"] == "holder-failure"})
        if not scenarios:
            continue
        fig, axes = plt.subplots(1, len(scenarios), figsize=(6 * len(scenarios), 4.5), squeeze=False)
        for ax, scenario in zip(axes[0], scenarios):
            for index, model in enumerate(MODELS):
                selected = [row for row in results if row["scenario"] == scenario and row["model"] == model]
                values = [row[metric] for row in selected if row[metric] is not None]
                if values:
                    ax.bar(index, statistics.median(values), color=["#577590", "#e09f3e", "#299d8f"][index], alpha=0.65)
                    ax.scatter([index] * len(values), values, color="#202020", s=24, zorder=3)
                missing = len(selected) - len(values)
                if missing:
                    ax.text(index, 0.02, f"{missing}/{len(selected)} undefined",
                            transform=ax.get_xaxis_transform(), ha="center", fontsize=8)
            ax.set_xticks(range(len(MODELS)), MODELS)
            ax.set_title(scenario)
            ax.set_ylabel(title)
            ax.set_ylim(bottom=0)
        fig.suptitle("Reference adapters • bars: median; dots: individual seeded runs")
        fig.text(0.5, 0.01, "Undefined waits/recovery are not zero; read unfinished counts in comparison.csv.",
                 ha="center", fontsize=9)
        fig.tight_layout(rect=(0, 0.05, 1, 0.93))
        fig.savefig(output / f"{metric}.png", dpi=160)
        fig.savefig(output / f"{metric}.svg")
        plt.close(fig)
