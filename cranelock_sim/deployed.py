"""Workload against existing endpoints. Never starts or recovers protocol services."""
import json
import math
import random
import statistics
import time
from pathlib import Path

import grpc

from .comparison import workload_hash, write_csv
from .experiment import require
from .lane import Lane

SCENARIOS = ("normal", "controller-outage", "holder-failure")
SCOPE = ("External deployed CraneLock services; synthetic physical lane and host-driven demand. "
         "Token handoff and recovery belong to the deployed agents. Grant requests use each "
         "crane's isolated signing client on the test host, as in external_smoke.py. "
         "No baseline comparison or physical-port claim. Fault latency brackets Docker "
         "command execution; it is not a measurement of the exact failure instant.")


def run_workload(deployment, output, *, scenario="normal", seed=20, requests=18,
                 duration=30., crossing_time=.1, new_empty_simulation=False,
                 clock=time.monotonic, sleep=time.sleep, wall_clock=time.time):
    """The deployment supplies simulator/crane clients, agent RPCs and fault controls.

    The caller holds the shared platform validator lock. Cleanup starts only services
    this run intentionally stopped; it never resets storage, votes or lane authority.
    """
    if not new_empty_simulation:
        raise ValueError("Explicit --new-empty-simulation is required; no other physical runner may be active")
    if scenario not in SCENARIOS or type(requests) is not int or requests < 3:
        raise ValueError("Use a supported scenario and at least three requests")
    if any(not math.isfinite(v) or v <= 0 for v in (duration, crossing_time)):
        raise ValueError("Duration and crossing time must be finite and positive")
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    origin = clock()
    events, stopped, rows = [], [], []
    lane = None
    fault_start = fault_end = None
    result = None

    def emit(event, **fields):
        events.append(dict(time=clock()-origin, timestamp=wall_clock(), event=event, **fields))

    def stop(service):
        nonlocal fault_start, fault_end
        started = clock()-origin
        stopped.append(service)  # Restore even if the command response is ambiguous.
        emit("fault_command_started", service=service, action="kill")
        deployment.fault("kill", service)
        finished = clock()-origin
        emit("controller_stopped" if service == "controller" else "holder_stopped",
             crane=service if service != "controller" else None,
             command_started_at=started, command_finished_at=finished)
        return started, finished

    try:
        state = deployment.simulator.state()
        if state["mode"] != "CENTRAL" or state["lane_state"] != "UNKNOWN" or state["grant"] is not None:
            raise RuntimeError("Requires healthy CENTRAL authority, UNKNOWN occupancy and no grant; refusing to reset existing state")
        members = sorted(deployment.simulator.config.members)
        for identity in members:
            agent = deployment.agent(identity)
            if ((agent.epoch, agent.revision, agent.token_holder) !=
                    (state["epoch"], state["revision"], state["holder"]) or
                    agent.mode != 1 or agent.has_token != (identity == state["holder"]) or
                    agent.pending_transfer_id or agent.request_queue or
                    any(peer.status != "AVAILABLE" for peer in agent.peers)):
                raise RuntimeError(f"{identity} is not idle and converged; run integrated.py state/check")

        if scenario == "holder-failure":
            stop("controller")
            deadline = clock()+30
            while state["mode"] != "LOCAL":
                if clock() >= deadline:
                    raise TimeoutError("Deployed agents did not install LOCAL authority during setup")
                sleep(.05)
                state = deployment.simulator.state()
            target = state["holder"]  # The current holder, not an assumed crane-1.
            members.remove(target)
        else:
            target = None
        rng = random.Random(seed)
        rows = [dict(request_id=f"request-{i+1}", crane=rng.choice(members),
                     requested_at=0. if scenario == "normal" else (i+.5)*duration/requests,
                     entered_at=None, exited_at=None, status="waiting") for i in range(requests)]
        settings = dict(model="deployed-cranelock", evaluation="deployed-v1", scenario=scenario,
            seed=seed, execution_mode="external_containers", recovery_driver="agent_workers",
            membership_id=deployment.simulator.config.membership_id,
            lane_id=deployment.simulator.config.lane_id,
            requested=requests, duration_seconds=None if scenario == "normal" else duration,
            crossing_time_seconds=crossing_time, workload_sha256=workload_hash(rows),
            measurement_policy="complete_all_requests" if scenario == "normal" else "fixed_window",
            initial_epoch=state["epoch"], initial_holder=state["holder"], initial_mode=state["mode"],
            endpoints=deployment.endpoints, limitations=SCOPE,
            grant_request_driver="host_actor_specific_clients", preparation_events=list(events))
        (output/"experiment.json").write_text(json.dumps(settings, indent=2), encoding="utf-8")
        (output/"workload.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
        events.clear()
        origin = clock()
        emit("workload_started", mode=state["mode"], requests=requests)
        lane = Lane(deployment.simulator, wall_clock, emit)
        require(lane.observe())  # Explicitly new, independently empty synthetic model.
        if target:
            fault_start, fault_end = stop(target)
        index = arrivals = 0
        pending = None
        last_authority = None
        horizon = max(120., duration) if scenario == "normal" else duration

        def tick():
            nonlocal arrivals, fault_start, fault_end, last_authority
            elapsed = clock()-origin
            while arrivals < len(rows) and rows[arrivals]["requested_at"] <= elapsed:
                row = rows[arrivals]
                emit("request", crane=row["crane"], request_id=row["request_id"], scheduled_at=row["requested_at"])
                arrivals += 1
            if scenario == "controller-outage" and fault_start is None and elapsed >= duration/3:
                fault_start, fault_end = stop("controller")
            observation = lane.observe()
            if observation["reason"] != "OUTSTANDING_GRANT":
                require(observation)
            authority = deployment.simulator.state()
            current = tuple(authority[k] for k in ("epoch", "revision", "holder", "mode"))
            if current != last_authority:
                emit("external_authority_observed", epoch=authority["epoch"],
                     holder=authority["holder"], mode=authority["mode"], occupant=lane.occupant)
                last_authority = current
            return authority

        while clock()-origin < horizon:
            state = tick()
            if index == len(rows):
                if scenario == "normal":
                    break
                sleep(.02)
                continue
            row = rows[index]
            if row["requested_at"] > clock()-origin:
                sleep(.02)
                continue
            identity = row["crane"]
            if state["holder"] != identity:
                signature = (state["epoch"], state["revision"], state["holder"], identity, index)
                if signature != pending:
                    try:
                        reply = deployment.request(state["holder"], identity, state, row["request_id"])
                        if reply.accepted:
                            pending = signature
                            emit("external_token_request", crane=identity, holder=state["holder"])
                    except grpc.RpcError as error:
                        if error.code() not in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
                            raise
                sleep(.02)
                continue
            agent = deployment.agent(identity)
            if not agent.has_token or (agent.epoch, agent.revision) != (state["epoch"], state["revision"]):
                sleep(.02)
                continue
            grant = deployment.cranes[identity].call(identity, "RequestGrant",
                epoch=state["epoch"], revision=state["revision"])
            if not grant["allowed"]:
                if grant["reason"] not in {"LANE_UNKNOWN", "LANE_BUSY", "OUTSTANDING_GRANT", "STALE_EPOCH", "STALE_REVISION", "NOT_HOLDER"}:
                    raise RuntimeError("Grant rejected: "+grant["reason"])
                sleep(.02)
                continue
            if not lane.enter(identity, row["request_id"], grant["grant"], state):
                sleep(.02)
                continue
            row.update(entered_at=clock()-origin, status="occupied")
            exit_at = clock()+crossing_time
            while clock() < exit_at:
                tick()  # Failures during occupancy never clear the physical model.
                sleep(min(.02, max(0, exit_at-clock())))
            lane.exit(identity, row["request_id"])
            row.update(exited_at=clock()-origin, status="completed")
            index += 1
        tick()
        elapsed = clock()-origin
        window = elapsed if scenario == "normal" else duration
        completed = [r for r in rows if r["exited_at"] is not None and r["exited_at"] <= window]
        admitted = [r for r in rows if r["entered_at"] is not None and r["entered_at"] <= window]
        resumed = [r for r in admitted if fault_end is not None and r["entered_at"] >= fault_end]
        if scenario == "normal" and len(completed) != requests:
            raise TimeoutError("Healthy deployed batch did not complete before its watchdog")
        if scenario != "normal" and not resumed:
            raise TimeoutError("No new deployed lane entry after the fault command completed")
        state = deployment.simulator.state()
        if scenario == "normal" and (state["mode"] != "CENTRAL" or state["epoch"] != settings["initial_epoch"]):
            raise RuntimeError("Unexpected authority recovery during the healthy deployed batch")
        if scenario != "normal" and (state["epoch"] <= settings["initial_epoch"] or state["mode"] != "LOCAL"):
            raise RuntimeError("Deployed agents did not establish replacement LOCAL authority")
        waits = [r["entered_at"]-r["requested_at"] for r in admitted]
        result = dict(**settings, passed=True, completed=len(completed), accepted=len(admitted),
            completed_all_requests=len(completed)==requests if scenario == "normal" else None,
            total_completion_seconds=elapsed if scenario == "normal" else None,
            mean_wait_seconds=statistics.mean(waits) if waits else None, safety_violations=0,
            fault_at_seconds=fault_start, fault_completed_at_seconds=fault_end,
            fault_command_start_to_entry_seconds=resumed[0]["entered_at"]-fault_start if resumed else None,
            fault_command_end_to_entry_seconds=resumed[0]["entered_at"]-fault_end if resumed else None,
            final_epoch=state["epoch"], final_mode=state["mode"])
    except Exception as error:
        emit("experiment_failed", error=type(error).__name__, detail=str(error),
             occupant=lane.occupant if lane else None)
        (output/"failure.json").write_text(json.dumps(dict(error=type(error).__name__, detail=str(error))), encoding="utf-8")
        raise
    finally:
        cleanup = []
        for service in reversed(stopped):
            try:
                deployment.fault("start", service)
                cleanup.append(dict(service=service, started=True))
            except Exception as error:
                cleanup.append(dict(service=service, started=False, error=str(error)))
        (output/"restoration.json").write_text(json.dumps(cleanup, indent=2), encoding="utf-8")
        (output/"events.jsonl").write_text("".join(json.dumps(e)+"\n" for e in events), encoding="utf-8")
        write_csv(output/"requests.csv", rows)
        if result:
            result["restoration_succeeded"] = all(item["started"] for item in cleanup)
            if not result["restoration_succeeded"]:
                result["passed"] = False
            (output/"summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result
