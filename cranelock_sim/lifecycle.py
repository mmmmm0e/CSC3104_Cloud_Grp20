import argparse
import asyncio
import csv
from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path
import time
import uuid

import grpc

from cranelock.agent import Mode
from cranelock.controller import CentralController
from cranelock.gate_client import GateClient
from cranelock.reconciliation import ReconciliationClient
from cranelock_gate.client import Client
from cranelock_gate.core import Gate
from cranelock_gate.server import RunningServer
from cranelock_gate.storage import initialize
from .agents import AgentCluster, wall_clock
from .controller import ControllerConnection, RPCStats, TeamControllerService
from .experiment import new_config, require
from .failures import until
from .lane import Lane, SafetyViolation


# verifies the real controller outage and return cycle
async def run_lifecycle(output, *, progress=print):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    origin = time.monotonic()
    events, rows, checks = [], [], []
    requested_at = {}
    config, keys = new_config(3)
    settings = dict(model="controller-lifecycle", scenario="controller-outage-return",
                    controller_implementation="cranelock.controller.CentralController",
                    time_basis="host_monotonic_seconds",
                    limitations="Single-process gRPC services; adapter coordinates outage and return; no Kubernetes.")
    (output / "experiment.json").write_text(json.dumps(settings, indent=2), encoding="utf-8")
    (output / "gate-config.json").write_text(json.dumps(asdict(config), indent=2), encoding="utf-8")
    log = (output / "demo.log").open("x", encoding="utf-8")

    # records real component events
    def emit(event, **fields):
        events.append(dict(time=time.monotonic() - origin, event=event, **fields))

    # reports only verified results
    def passed(check, condition=True, **fields):
        if not condition:
            raise SafetyViolation(check)
        checks.append(check)
        emit("check_passed", check=check, **fields)
        line = f"[PASS] controller lifecycle: {check}"
        log.write(line + "\n")
        log.flush()
        progress(line)

    gate = server = channel = cluster = service = None
    connections = []
    try:
        initialize(output / "gate.sqlite", config, wall_clock())
        gate = Gate(output / "gate.sqlite", config, wall_clock)
        server = RunningServer(gate, "127.0.0.1:0")
        address = f"127.0.0.1:{server.port}"
        channel = grpc.insecure_channel(address)
        client = Client(channel, config, keys)
        controller = CentralController(config, keys["controller"], channel)
        require(await asyncio.to_thread(controller.install_central, "crane-1"))
        service = TeamControllerService(controller)
        controller_port = service.port
        stats = RPCStats()
        connections = [ControllerConnection(controller_port, stats) for _ in config.members]

        # gives each crane its own signed gate client
        def gate_factory(identity):
            return GateClient(grpc.insecure_channel(address), config,
                              {identity: keys[identity]}, identity)

        cluster = AgentCluster(list(config.members), output, emit, timeout=0.8,
            recovery_config=config, keys=keys, initial_mode=Mode.CENTRAL,
            gate_factory=gate_factory,
            controller_probes={identity: connection.probe
                               for identity, connection in zip(config.members, connections)})
        lane = Lane(client, wall_clock, emit)
        require(await asyncio.to_thread(lane.observe))
        await cluster.start()
        await until(lambda: all(any(event.get("agent_event", {}).get("event") ==
            "CONTROLLER_HEARTBEAT_RECEIVED" and event["agent_event"]["agent_id"] == identity
            for event in events) for identity in config.members), 5, "controller heartbeats")
        emit("workload_started", mode="CENTRAL", requests=3)
        assignment = await connections[0].assign("crane-1")
        if not assignment["allowed"]:
            raise RuntimeError("Controller assignment RPC was rejected")
        emit("central_assignment", crane="crane-1", implementation=settings["controller_implementation"])
        await cluster.snapshot("central_ready")

        # obtains a grant and moves through the simulator boundary
        async def enter(identity, request_id):
            state = await asyncio.to_thread(client.state)
            result = await asyncio.to_thread(client.call, identity, "RequestGrant",
                epoch=state["epoch"], revision=state["revision"])
            require(result)
            emit("grant_issued", crane=identity, request_id=request_id,
                 grant_id=result["grant"]["id"], epoch=state["epoch"], revision=state["revision"])
            if not await asyncio.to_thread(lane.enter, identity, request_id, result["grant"], state):
                raise SafetyViolation("Authorized crane could not enter")
            row = dict(request_id=request_id, crane=identity, requested_at=requested_at[identity],
                       entered_at=time.monotonic()-origin, exited_at=None, status="occupied")
            rows.append(row)
            return state, result["grant"], row

        # exits only the current physical occupant
        async def exit_lane(row):
            await asyncio.to_thread(lane.exit, row["crane"], row["request_id"])
            row.update(exited_at=time.monotonic()-origin, status="completed")

        requested_at["crane-1"] = time.monotonic() - origin
        emit("request", request_id="central-before-outage", crane="crane-1")
        for identity, request_id in (("crane-2", "local-during-outage"),
                                     ("crane-3", "central-after-return")):
            requested_at[identity] = time.monotonic() - origin
            emit("request", request_id=request_id, crane=identity)
            reply = await cluster.agents[identity].request_token()
            if not reply.accepted:
                raise RuntimeError(reply.reason)
        expected_queue = list(cluster.agents["crane-1"].state.request_queue)
        passed("central_queue_recorded", expected_queue == ["crane-2", "crane-3"])
        old, old_grant, first = await enter("crane-1", "central-before-outage")
        passed("central_entry_uses_gate_and_physical_lane", lane.occupant == "crane-1")
        await asyncio.to_thread(service.close)
        emit("controller_stopped", occupant=lane.occupant)
        await until(lambda: all(agent.controller_suspected for agent in cluster.agents.values()),
                    6, "all agents detecting controller outage")
        passed("actual_controller_outage_detected")
        cert = await cluster.agents["crane-2"].propose_recovery(old, "crane-1")
        passed("majority_certificate_formed", cert is not None)
        (output / "recovery-certificate.json").write_text(json.dumps(cert, indent=2), encoding="utf-8")
        result = await asyncio.to_thread(client.call, "crane-2", "InstallAuthority",
            certificate=cert, expected_epoch=old["epoch"], expected_revision=old["revision"])
        require(result)
        cluster.authority_epoch = result["state"]["epoch"]
        await ReconciliationClient(config, "crane-2", keys["crane-2"]).reconcile(
            cluster.addresses, result["state"])
        emit("controller_fallback_installed", epoch=result["state"]["epoch"],
             holder="crane-1", occupant=lane.occupant)
        passed("local_epoch_installed_without_clearing_occupancy",
               result["state"]["epoch"] == 1 and result["state"]["lane_state"] == "OCCUPIED")
        for identity in expected_queue:
            reply = await cluster.agents[identity].request_token()
            if not reply.accepted:
                raise RuntimeError(reply.reason)
        passed("pending_requests_requeued_after_epoch_change",
               list(cluster.agents["crane-1"].state.request_queue) == expected_queue)
        stale = await asyncio.to_thread(client.call, "simulator", "ValidateEntry",
            epoch=old["epoch"], revision=old["revision"], holder="crane-1",
            grant_id=old_grant["id"], session_id=old_grant["id"])
        passed("old_entry_permission_fenced", stale["reason"] == "STALE_EPOCH")
        current = await asyncio.to_thread(client.state)
        blocked = await asyncio.to_thread(client.call, "crane-1", "RequestGrant",
            epoch=current["epoch"], revision=current["revision"])
        passed("occupied_lane_blocks_local_grant", blocked["reason"] == "LANE_BUSY")
        await exit_lane(first)
        await cluster.handoff(client, lane, "crane-2")
        await cluster.snapshot("local_handoff_complete")
        local, local_grant, second = await enter("crane-2", "local-during-outage")
        passed("local_handoff_committed_by_agent_at_gate", local["holder"] == "crane-2")

        queue_before_return = list(cluster.agents["crane-2"].state.request_queue)
        service = TeamControllerService(controller, port=controller_port)
        await until(lambda: not any(agent.controller_suspected for agent in cluster.agents.values()),
                    8, "controller return heartbeat")
        emit("controller_restored", occupant=lane.occupant)
        require(await connections[0].assign(local["holder"]))
        central = await controller.reconcile_remote(cluster.addresses)
        emit("controller_return_installed", epoch=central["epoch"], holder=central["holder"],
             mode="CENTRAL", occupant=lane.occupant, pending_queue=queue_before_return)
        await cluster.snapshot("controller_return_reconciled")
        passed("return_preserves_epoch_holder_and_physical_occupancy",
               central["epoch"] == local["epoch"] and central["holder"] == local["holder"]
               and central["lane_state"] == "OCCUPIED" and lane.occupant == "crane-2")
        passed("return_preserves_waiting_queue", queue_before_return == ["crane-3"]
               and list(cluster.agents["crane-2"].state.request_queue) == queue_before_return)
        stale = await asyncio.to_thread(client.call, "simulator", "ValidateEntry",
            epoch=local["epoch"], revision=local["revision"], holder="crane-2",
            grant_id=local_grant["id"], session_id=local_grant["id"])
        passed("pre_return_permission_fenced_by_revision", stale["reason"] == "STALE_REVISION")
        blocked = await asyncio.to_thread(client.call, "crane-2", "RequestGrant",
            epoch=central["epoch"], revision=central["revision"])
        passed("controller_return_cannot_clear_the_lane", blocked["reason"] == "LANE_BUSY")
        await exit_lane(second)
        require(await connections[0].assign("crane-3"))
        await controller.reconcile_remote(cluster.addresses)
        emit("central_assignment", crane="crane-3", implementation=settings["controller_implementation"])
        _, _, third = await enter("crane-3", "central-after-return")
        await exit_lane(third)
        owner, states = await cluster.snapshot("finished")
        passed("queued_crane_completes_after_controller_return", owner == "crane-3"
               and all(state.mode == int(Mode.CENTRAL) for state in states.values()))
        summary = dict(**settings, passed=True, checks_passed=checks, completed=3, requested=3,
            safety_violations=0, final_holder=owner, final_epoch=central["epoch"],
            final_lane_state="CLEAR", final_mode="CENTRAL", queue_preserved=True)
        (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary
    except BaseException as error:
        emit("experiment_failed", error=type(error).__name__, detail=str(error))
        (output / "failure.json").write_text(json.dumps(dict(error=type(error).__name__,
            detail=str(error))), encoding="utf-8")
        raise
    finally:
        if cluster:
            await cluster.close()
        for connection in connections:
            await connection.close()
        if service:
            await asyncio.to_thread(service.close)
        if gate:
            (output / "gate-events.jsonl").write_text("".join(
                json.dumps(event)+"\n" for event in gate.events()), encoding="utf-8")
        if server:
            await asyncio.to_thread(server.close)
        elif gate:
            gate.close()
        if channel:
            channel.close()
        (output / "events.jsonl").write_text("".join(json.dumps(event)+"\n" for event in events),
                                            encoding="utf-8")
        with (output / "requests.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=["request_id", "crane", "requested_at",
                                                       "entered_at", "exited_at", "status"])
            writer.writeheader()
            writer.writerows(rows)
        log.close()


# runs the full local integration cycle
def main():
    parser = argparse.ArgumentParser(description="Central to local to central integration demo")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or Path("run/evaluation") / (
        "lifecycle-" + datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
    print(f"Evidence directory: {output.resolve()}", flush=True)
    asyncio.run(run_lifecycle(output, progress=lambda line: print(line, flush=True)))


if __name__ == "__main__":
    main()
