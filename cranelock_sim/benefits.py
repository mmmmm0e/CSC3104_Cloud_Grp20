"""Paired benefit/cost experiments using the team's CentralController.

Central authority uses the team implementation behind a test-only gRPC service.
Workload pacing and the CENTRAL -> LOCAL orchestration belong to this adapter.
"""
import asyncio
import importlib.util
import json
import math
import os
import random
import statistics
import time
from dataclasses import asdict
from pathlib import Path

import grpc

from cranelock.agent import Mode
from cranelock_gate.client import certificate
from cranelock_gate.core import Gate
from cranelock_gate.server import RunningServer
from cranelock_gate.storage import initialize
from .agents import AgentCluster, wall_clock
from .comparison import MODELS, run_case as holder_case, workload_hash, write_csv
from .controller import (CONTROLLER_IMPLEMENTATION, CONTROLLER_SOURCE, ControllerConnection,
                         MeasuredGateClient, PeerCounter, RPCStats, TeamControllerService)
from .experiment import new_config, percentile, require
from .failures import until
from .lane import Lane

SCENARIOS = ("normal", "controller-outage", "holder-failure")
LIMITS = ("Team CentralController authority logic with test-only gRPC Heartbeat/Assign transport. "
          "Workload scheduling and fallback initiation are adapter-driven, not autonomous deployed orchestration. "
          "Three agents, one host, common trusted gate. Load varies request count, not crane count. "
          "RPC attempts count instrumented peer, controller and gate calls; excludes management snapshots "
          "and gate internal maintenance. Not packet/byte counts or production benchmarks. "
          "Holder-failure reuses the LOCAL recovery experiment; its RPC overhead is not measured.")


def make_workload(seed, count, duration):
    rng = random.Random(seed)
    return [dict(request_id=f"request-{i + 1}", crane=rng.choice(["crane-2", "crane-3"]),
                 requested_at=(i + .2 + .6 * rng.random()) * duration / count) for i in range(count)]


async def run_case(output, model, scenario, workload, *, seed, duration, crossing_time, detection_timeout):
    if scenario == "holder-failure":
        result = await holder_case(output, model, scenario, workload, seed=seed, duration=duration,
                                   crossing_time=crossing_time, detection_timeout=detection_timeout)
        result.update(evaluation="benefits-v3", limitations=LIMITS, rpc_attempts=None,
                      rpc_per_completed=None, post_fault_new_entries=result["accepted"])
        (output / "summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        return result
    output.mkdir(parents=True, exist_ok=False)
    complete_all = scenario == "normal"
    # A watchdog catches a broken experiment; it is not a completion-count cutoff.
    safety_timeout = max(30, duration + len(workload) * (crossing_time + 1))
    origin = time.monotonic()
    events, stats = [], RPCStats()
    def emit(event, **fields):
        events.append(dict(time=time.monotonic() - origin, event=event, **fields))
    settings = dict(model=model, scenario=scenario, seed=seed,
        evaluation="benefits-v3", controller_implementation=CONTROLLER_IMPLEMENTATION,
        controller_source_commit=CONTROLLER_SOURCE,
        measurement_policy="complete_all_requests" if complete_all else "fixed_window",
        arrival_pattern="batch" if all(row["requested_at"] == 0 for row in workload) else "scheduled",
        safety_timeout_seconds=safety_timeout if complete_all else None,
        duration_seconds=None if complete_all else duration, crossing_time_seconds=crossing_time,
        detection_timeout_seconds=detection_timeout, workload_sha256=workload_hash(workload),
        time_basis="host_monotonic_seconds", limitations=LIMITS)
    for name, value in (("experiment.json", settings), ("workload.json", workload)):
        (output / name).write_text(json.dumps(value, indent=2), encoding="utf-8")
    config, keys = new_config(3)
    (output / "gate-config.json").write_text(json.dumps(asdict(config), indent=2), encoding="utf-8")
    cluster = controller = channel = server = gate = None
    connections, tasks = [], []
    rows = [dict(row, entered_at=None, exited_at=None, status="waiting") for row in workload]
    fault_at = authority_at = detected_at = None
    mode = "LOCAL" if model == "basic-token" else "CENTRAL"
    available = asyncio.Event()
    available.set()
    authority_lock = asyncio.Lock()
    try:
        initialize(output / "gate.sqlite", config, wall_clock())
        gate = Gate(output / "gate.sqlite", config, wall_clock)
        server = RunningServer(gate, "127.0.0.1:0")
        channel = grpc.insecure_channel(f"127.0.0.1:{server.port}")
        await asyncio.to_thread(grpc.channel_ready_future(channel).result, timeout=5)
        client = MeasuredGateClient(channel, config, keys, stats)
        controller = TeamControllerService(config, keys[config.controller_id], channel, stats)
        connections = [ControllerConnection(controller.port, stats) for _ in range(4)]
        state = await asyncio.to_thread(client.state)
        bootstrap = certificate(config, keys, state, "crane-1", 0, mode=mode,
                                signers=["controller"] if mode == "CENTRAL" else None)
        if mode == "CENTRAL":
            require(await connections[-1].assign("crane-1"))
        else:
            require(await asyncio.to_thread(client.call, "crane-1", "InstallAuthority",
                certificate=bootstrap, expected_epoch=state["epoch"], expected_revision=state["revision"]))
        lane = Lane(client, wall_clock, emit)
        require(await asyncio.to_thread(lane.observe))
        cluster = AgentCluster(list(config.members), output, emit, timeout=detection_timeout,
            initial_mode=Mode.CENTRAL if mode == "CENTRAL" else Mode.LOCAL,
            recovery_config=config if model == "cranelock" else None,
            keys=keys if model == "cranelock" else None, peer_interceptor=PeerCounter(stats),
            controller_probes={identity: connections[i].probe for i, identity in enumerate(config.members)}
                if model == "cranelock" else None)
        await cluster.start()
        await asyncio.sleep(max(.15, detection_timeout))
        if model == "cranelock":
            if not all(await asyncio.gather(*(connection.probe() for connection in connections))):
                raise RuntimeError("Team controller service adapter did not warm up")
            # Healthy controller must prevent a locally manufactured replacement.
            if await cluster.agents["crane-2"].propose_recovery(await asyncio.to_thread(client.state), "crane-2") is not None:
                raise RuntimeError("Recovery accepted while controller healthy")
        events.clear()
        origin = time.monotonic()
        stats.reset(deadline=None if complete_all else origin + duration)
        emit("workload_started", mode=mode, requests=len(rows))
        queue = asyncio.Queue()

        async def submit():
            for row in rows:
                await asyncio.sleep(max(0, row["requested_at"] - (time.monotonic() - origin)))
                emit("request", request_id=row["request_id"], crane=row["crane"], scheduled_at=row["requested_at"])
                queue.put_nowait(row)

        async def fault():
            nonlocal fault_at, authority_at, detected_at, mode
            await asyncio.sleep(duration / 3)
            async with authority_lock:
                fault_at = time.monotonic() - origin
                if model != "basic-token":
                    available.clear()
                await asyncio.to_thread(controller.close)
                emit("controller_stopped", mode=mode, occupant=lane.occupant)
                if await connections[-1].probe():
                    raise RuntimeError("Stopped controller still answers heartbeat")
                emit("controller_rpc_unavailable")
            if model != "cranelock":
                return
            await until(lambda: all(agent.controller_suspected for agent in cluster.agents.values()),
                        max(3, detection_timeout * 5), "controller suspicion majority")
            detections = [event["time"] for event in events if event.get("agent_event", {}).get("event") == "CONTROLLER_SUSPECTED"]
            detected_at = min(detections)
            async with authority_lock:
                if time.monotonic() - origin >= duration:
                    return
                state = await asyncio.to_thread(client.state)
                cert = await cluster.agents["crane-2"].propose_recovery(state, state["holder"])
                if cert is None:
                    raise RuntimeError("No genuine majority approved controller fallback")
                (output / "recovery-certificate.json").write_text(json.dumps(cert, indent=2), encoding="utf-8")
                require(await asyncio.to_thread(client.call, "crane-2", "InstallAuthority", certificate=cert,
                    expected_epoch=state["epoch"], expected_revision=state["revision"]))
                accepted = await asyncio.to_thread(client.state)
                if accepted["mode"] != "LOCAL" or accepted["epoch"] != state["epoch"] + 1:
                    raise RuntimeError("Fallback did not advance accepted authority")
                cluster.authority_epoch = accepted["epoch"]
                for agent in cluster.agents.values():
                    await agent.reconcile(accepted["epoch"], accepted["holder"], Mode.LOCAL)
                mode = "LOCAL"
                authority_at = time.monotonic() - origin
                emit("controller_fallback_installed", epoch=accepted["epoch"], holder=accepted["holder"],
                     occupant=lane.occupant, mode=mode)
                stale = await asyncio.to_thread(client.call, "controller", "InstallAuthority", certificate=bootstrap,
                    expected_epoch=bootstrap["payload"]["expected_epoch"],
                    expected_revision=bootstrap["payload"]["expected_revision"])
                if stale["allowed"] or stale["reason"] != "STALE_EPOCH":
                    raise RuntimeError("Old valid central certificate was not fenced by epoch")
                emit("stale_central_authority_rejected", reason=stale["reason"])
                available.set()

        async def worker():
            for _ in rows:
                remaining = (safety_timeout if complete_all else duration) - (time.monotonic() - origin)
                if remaining <= 0:
                    if complete_all:
                        raise TimeoutError("Normal experiment watchdog expired before all requests completed")
                    break
                try:
                    row = await asyncio.wait_for(queue.get(), remaining)
                except TimeoutError:
                    if complete_all:
                        raise TimeoutError("Normal experiment watchdog expired waiting for a request")
                    break
                # Check availability while owning the same lock as failure and
                # authority installation. Never lose the pending request.
                while True:
                    try:
                        await asyncio.wait_for(available.wait(), max(.001, (safety_timeout if complete_all else duration) - (time.monotonic() - origin)))
                    except TimeoutError:
                        return
                    await authority_lock.acquire()
                    if available.is_set():
                        break
                    authority_lock.release()
                try:
                    if time.monotonic() - origin >= (safety_timeout if complete_all else duration):
                        if complete_all:
                            raise TimeoutError("Normal experiment watchdog expired before entry")
                        break
                    identity = row["crane"]
                    require(await asyncio.to_thread(lane.observe))
                    if mode == "CENTRAL":
                        result = await connections[-1].assign(identity)
                        if not result["allowed"]:
                            raise RuntimeError(f"Central assignment denied: {result['reason']}")
                        emit("central_assignment", crane=identity)
                    else:
                        owner, _ = await cluster.snapshot("benefit_before_request")
                        if owner != identity:
                            response = await cluster.agents[identity].request_token()
                            if not response.accepted:
                                raise RuntimeError(response.reason)
                            await cluster.handoff(client, lane, identity)
                    state = await asyncio.to_thread(client.state)
                    if state["lane_state"] == "UNKNOWN":
                        # A slow handoff or host pause can expire the earlier
                        # clearance. Only a new physical observation restores it.
                        require(await asyncio.to_thread(lane.observe))
                        state = await asyncio.to_thread(client.state)
                    if time.monotonic() - origin >= (safety_timeout if complete_all else duration):
                        if complete_all:
                            raise TimeoutError("Normal experiment watchdog expired before grant")
                        break
                    grant = await asyncio.to_thread(client.call, identity, "RequestGrant", epoch=state["epoch"], revision=state["revision"])
                    require(grant)
                    emit("grant_issued", request_id=row["request_id"], crane=identity,
                         epoch=state["epoch"], revision=state["revision"], grant_id=grant["grant"]["id"])
                    if not await asyncio.to_thread(lane.enter, identity, row["request_id"], grant["grant"], state):
                        raise RuntimeError("Benefit entry denied")
                    row.update(entered_at=time.monotonic() - origin, status="occupied")
                finally:
                    authority_lock.release()
                await asyncio.sleep(crossing_time)
                await asyncio.to_thread(lane.exit, identity, row["request_id"])
                exited = time.monotonic() - origin
                row.update(exited_at=exited, status="completed" if complete_all or exited <= duration else "completed-after-horizon")

        async def horizon():
            await asyncio.sleep(duration)
            return stats.snapshot()
        worker_task = asyncio.create_task(worker())
        tasks.extend([worker_task, asyncio.create_task(submit())])
        if scenario == "controller-outage":
            tasks.append(asyncio.create_task(fault()))
        horizon_task = None if complete_all else asyncio.create_task(horizon())
        if horizon_task:
            tasks.append(horizon_task)
        await asyncio.gather(*tasks)
        total_completion = time.monotonic() - origin
        rpc_counts = stats.snapshot() if complete_all else horizon_task.result()
        completed = [row for row in rows if row["exited_at"] is not None and (complete_all or row["exited_at"] <= duration)]
        admitted = [row for row in rows if row["entered_at"] is not None and (complete_all or row["entered_at"] <= duration)]
        if complete_all and len(completed) != len(rows):
            raise RuntimeError("Normal experiment did not finish every request")
        resumed = [row for row in admitted if fault_at is not None and row["entered_at"] > fault_at]
        waits = [row["entered_at"] - row["requested_at"] for row in admitted]
        if cluster.violation:
            raise cluster.violation
        summary = dict(**settings, requested=len(rows), completed=len(completed), accepted=len(admitted),
            unfinished_at_horizon=None if complete_all else len(rows)-len(completed),
            total_completion_seconds=total_completion if complete_all else None,
            completed_all_requests=len(completed)==len(rows) if complete_all else None,
            throughput_per_minute=len(completed)*60/(total_completion if complete_all else duration),
            p95_wait_seconds=percentile(waits, .95), mean_wait_seconds=statistics.mean(waits) if waits else None,
            safety_violations=0, fault_at_seconds=fault_at, authority_at_seconds=authority_at,
            initial_mode="LOCAL" if model == "basic-token" else "CENTRAL", final_mode=mode,
            failure_detection_seconds=detected_at-fault_at if detected_at is not None else None,
            recovery_latency_seconds=resumed[0]["entered_at"]-detected_at if resumed and detected_at is not None else None,
            outage_to_entry_seconds=resumed[0]["entered_at"]-fault_at if resumed else None,
            resumed_within_window=bool(resumed) if fault_at is not None else None,
            post_fault_new_entries=len(resumed) if fault_at is not None else None,
            post_fault_completed=sum(row["exited_at"] > fault_at for row in completed) if fault_at is not None else None,
            rpc_attempts=sum(rpc_counts.values()), rpc_by_method=rpc_counts,
            rpc_per_completed=sum(rpc_counts.values())/len(completed) if completed else None,
            drain_seconds=0 if complete_all else max(0, time.monotonic()-origin-duration))
        (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary
    except Exception as error:
        emit("experiment_failed", error=type(error).__name__, detail=str(error))
        (output / "failure.json").write_text(json.dumps(dict(error=type(error).__name__, detail=str(error))), encoding="utf-8")
        raise
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if cluster:
            await cluster.close()
        for connection in connections:
            await connection.close()
        if controller:
            await asyncio.to_thread(controller.close)
        if gate:
            (output / "gate-events.jsonl").write_text("".join(json.dumps(event)+"\n" for event in gate.events()), encoding="utf-8")
        if server:
            await asyncio.to_thread(server.close)
        if channel:
            channel.close()
        (output / "events.jsonl").write_text("".join(json.dumps(event)+"\n" for event in events), encoding="utf-8")
        write_csv(output / "requests.csv", rows)


def paired(results, scenario, baseline, metric):
    """Only compare exact seeded inputs and timing parameters; missing stays missing."""
    def key(row):
        return tuple(row.get(field) for field in ("seed", "requested", "workload_sha256", "duration_seconds",
                     "crossing_time_seconds", "detection_timeout_seconds", "evaluation")) + (row.get("measurement_policy","fixed_window"), row.get("controller_implementation"), row.get("controller_source_commit"))
    results = [row for row in results if row.get("evaluation") in {"benefits-v1","benefits-v2","benefits-v3"}
               and (row.get("evaluation") != "benefits-v3" or
                    (row.get("controller_implementation") == CONTROLLER_IMPLEMENTATION and
                     isinstance(row.get("controller_source_commit"), str)))
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
    benefits = {}
    for load in loads:
        selected = [row for row in results if row["requested"] == load]
        benefits[str(load)] = dict(controller_outage_extra_entries=paired(selected,"controller-outage","central-lock","post_fault_new_entries"),
            holder_failure_extra_completed=paired(selected,"holder-failure","basic-token","completed"),
            normal_wait_cost_seconds=paired(selected,"normal","central-lock","mean_wait_seconds"),
            normal_completion_time_cost_seconds=paired(selected,"normal","central-lock","total_completion_seconds"),
            normal_rpc_cost_per_completion=paired(selected,"normal","central-lock","rpc_per_completed"))
    summary = dict(passed=True, runs=len(results), limitations=LIMITS, benefits=benefits, results=results)
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
    fig.suptitle("CraneLock: paired benefits and costs — reference integration")
    fig.text(.5, .02, "Bars: median paired difference. Dots: seeded pairs. Three agents on one host; not production performance.", ha="center", fontsize=9)
    fig.tight_layout(rect=(0,.05,1,.95))
    for suffix in ("png", "svg"):
        fig.savefig(output / f"benefit_summary.{suffix}", dpi=160)
    plt.close(fig)
