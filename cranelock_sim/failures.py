"""Holder service-stop experiments using actual heartbeat detection and quorum RPCs."""
import asyncio
import csv
import json
import math
import time
from dataclasses import asdict
from pathlib import Path

import grpc

from cranelock_gate.client import Client, certificate
from cranelock_gate.core import Gate
from cranelock_gate.server import RunningServer
from cranelock_gate.storage import initialize
from cranelock import recovery
from .agents import AgentCluster, wall_clock
from .experiment import new_config, require
from .lane import Lane, SafetyViolation
from .network import PeerNetwork


SCENARIOS = ("before-entry", "during-occupancy")


async def until(condition, timeout, description):
    try:
        async with asyncio.timeout(timeout):
            while not condition():
                await asyncio.sleep(0.01)
    except TimeoutError as error:
        raise TimeoutError(f"Timed out waiting for {description}") from error


async def run_recovery_scenario(output, scenario, *, detection_timeout=0.5,
                                clearance_delay=0.1, crossing_time=0.05, progress=None,
                                fault="service-stop", restart=False):
    if scenario not in SCENARIOS:
        raise ValueError(f"Unknown recovery scenario: {scenario}")
    if fault not in {"service-stop", "majority-partition", "no-majority"}:
        raise ValueError(f"Unknown fault: {fault}")
    if fault == "no-majority" and scenario != "before-entry":
        raise ValueError("no-majority uses the before-entry setup")
    if restart and fault != "service-stop":
        raise ValueError("Restart scenarios require a service stop")
    if not math.isfinite(detection_timeout) or detection_timeout <= 0.05:
        raise ValueError("detection_timeout must be finite and greater than 0.05 seconds")
    for name, value in (("clearance_delay", clearance_delay), ("crossing_time", crossing_time)):
        if not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError(f"{name} must be finite and between 0 and 1 seconds")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    label = f"restart/{scenario}" if restart else scenario if fault == "service-stop" else f"{fault}/{scenario}"
    settings = dict(model="holder-restart-rejoin" if restart else
        "holder-service-stop-recovery" if fault == "service-stop" else "peer-network-partition",
        scenario=scenario, fault=fault,
        detection_timeout_seconds=detection_timeout, clearance_delay_seconds=clearance_delay,
        crossing_time_seconds=crossing_time, time_basis="host_monotonic_seconds")
    (output / "experiment.json").write_text(json.dumps(settings, indent=2), encoding="utf-8")
    origin = time.monotonic()
    events, rows, detections, checks = [], [], {}, []

    def emit(event, **fields):
        now = time.monotonic() - origin
        events.append(dict(time=now, event=event, **fields))
        agent_event = fields.get("agent_event", {})
        if (agent_event.get("event") == "TOKEN_HOLDER_SUSPECTED"
                and agent_event.get("peer_id") == "crane-1"):
            detections.setdefault(agent_event["agent_id"], now)

    def passed(check, **fields):
        checks.append(check)
        emit("check_passed", check=check, **fields)
        if progress:
            progress(f"[PASS] {label}: {check}")

    config, keys = new_config(3)
    (output / "gate-config.json").write_text(json.dumps(asdict(config), indent=2), encoding="utf-8")
    network = PeerNetwork(config.members, emit) if fault != "service-stop" else None
    cluster = AgentCluster(list(config.members), output, emit, recovery_config=config,
                           keys=keys, timeout=detection_timeout, network=network)
    gate, server, channel = None, None, None
    try:
        initialize(output / "gate.sqlite", config, wall_clock())
        gate = Gate(output / "gate.sqlite", config, wall_clock)
        server = RunningServer(gate, "127.0.0.1:0")
        channel = grpc.insecure_channel(f"127.0.0.1:{server.port}")
        await asyncio.to_thread(grpc.channel_ready_future(channel).result, timeout=5)
        client = Client(channel, config, keys)
        state = await asyncio.to_thread(client.state)
        bootstrap = certificate(config, keys, state, "crane-1", 0)
        require(await asyncio.to_thread(client.call, "crane-1", "InstallAuthority",
            certificate=bootstrap, expected_epoch=state["epoch"], expected_revision=state["revision"]))
        lane = Lane(client, wall_clock, emit)
        require(await asyncio.to_thread(lane.observe))
        await cluster.start()
        # Wait for actual bidirectional heartbeats; never prepopulate suspicion.
        await until(lambda: all(any(event["event"] == "agent_event" and
            event["agent_event"]["event"] == "HEARTBEAT_RECEIVED" and
            event["agent_event"]["agent_id"] == observer and
            event["agent_event"].get("peer_id") == peer for event in events)
            for observer in config.members for peer in config.members if observer != peer),
            5, "heartbeat warmup")
        if any(agent.suspected for agent in cluster.agents.values()):
            raise RuntimeError("Unexpected suspicion before failure injection")
        old_state = await asyncio.to_thread(client.state)
        old_grant = await asyncio.to_thread(client.call, "crane-1", "RequestGrant",
            epoch=old_state["epoch"], revision=old_state["revision"])
        require(old_grant)
        old_row = dict(request_id="old-holder", crane="crane-1",
            requested_at=time.monotonic() - origin, entered_at=None, exited_at=None,
            status="crashed-before-entry" if fault == "service-stop" else "isolated-before-entry")
        rows.append(old_row)
        emit("grant_issued", request_id=old_row["request_id"], crane="crane-1",
             grant_id=old_grant["grant"]["id"], epoch=0, revision=old_state["revision"])
        if scenario == "during-occupancy":
            if not await asyncio.to_thread(lane.enter, "crane-1", old_row["request_id"],
                                           old_grant["grant"], old_state):
                raise RuntimeError("Initial holder could not enter")
            old_row.update(entered_at=time.monotonic() - origin,
                           status="crashed-occupied" if fault == "service-stop" else "isolated-occupied")
        _, initial = await cluster.snapshot("before_crash")
        if not initial["crane-1"].has_token:
            raise SafetyViolation("Failure target does not hold the token")
        from cranelock import protocol_pb2 as pb
        if network:
            network.partition([["crane-1"], ["crane-2", "crane-3"]] if fault == "majority-partition"
                              else [["crane-1"], ["crane-2"], ["crane-3"]])
        else:
            # Closing the actual service preserves durable token and vote files.
            await cluster.stop("crane-1")
        crashed_at = time.monotonic() - origin
        emit("holder_isolated" if network else "holder_stopped", crane="crane-1")
        if network:
            for identity in config.members:
                await cluster.stubs[identity].GetAgentState(pb.Empty(), timeout=1)
                if any(task.done() for task in cluster.agents[identity].tasks):
                    raise RuntimeError("Partition stopped an agent worker")
            passed("all_agents_still_running")
            for operation in ("TokenRequest", "TokenTransfer"):
                try:
                    if operation == "TokenRequest":
                        await cluster.agents["crane-2"].request_token()
                    else:
                        await cluster.agents["crane-2"].stubs["crane-1"].TokenTransfer(
                            pb.TokenTransferMessage(sender_id="crane-2", receiver_id="crane-1",
                                epoch=0, transfer_id="partition-probe"), timeout=1)
                except grpc.aio.AioRpcError as error:
                    if error.code() != grpc.StatusCode.UNAVAILABLE:
                        raise
                else:
                    raise RuntimeError(f"Partition allowed cross-group {operation}")
            passed("partition_blocks_token_requests_and_transfers")
        else:
            try:
                await cluster.stubs["crane-1"].GetAgentState(pb.Empty(), timeout=0.1)
            except grpc.aio.AioRpcError:
                passed("holder_rpc_unavailable")
            else:
                raise RuntimeError("Stopped holder still responds to RPC")
        await until(lambda: all(identity in detections and "crane-1" in
            cluster.agents[identity].suspected for identity in ("crane-2", "crane-3")),
            max(5, detection_timeout * 4), "holder failure detection")
        if min(detections.values()) < crashed_at:
            raise RuntimeError("Failure was suspected before the injected stop")
        detected_at = min(detections.values())
        passed("survivors_detected_holder_failure")
        if fault == "no-majority":
            for identity in ("crane-2", "crane-3"):
                state = await asyncio.to_thread(client.state)
                cert = await cluster.agents[identity].propose_recovery(state, identity)
                if cert is not None:
                    raise SafetyViolation("An isolated minority formed a recovery certificate")
            passed("isolated_members_cannot_form_quorum")
            state = await asyncio.to_thread(client.state)
            forged_minority = certificate(config, keys, state, "crane-2", 1, signers=["crane-2"])
            denial = await asyncio.to_thread(client.call, "crane-2", "InstallAuthority",
                certificate=forged_minority, expected_epoch=state["epoch"], expected_revision=state["revision"])
            if denial["allowed"] or denial["reason"] != "NO_QUORUM":
                raise SafetyViolation("Gate accepted a single-member recovery certificate")
            passed("gate_rejects_minority_certificate", reason=denial["reason"])
            for identity in ("crane-2", "crane-3"):
                denial = await asyncio.to_thread(client.call, identity, "RequestGrant",
                    epoch=state["epoch"], revision=state["revision"])
                if denial["allowed"] or denial["reason"] != "WRONG_HOLDER":
                    raise SafetyViolation("Minority requester received lane permission")
            passed("nonholders_denied_lane_access")
            owner, _ = await cluster.snapshot("no_quorum")
            final = await asyncio.to_thread(client.state)
            if final["epoch"] != 0 or final["holder"] != "crane-1" or owner != "crane-1" or lane.occupant:
                raise SafetyViolation("Authority or occupancy changed without a quorum")
            passed("authority_unchanged_without_quorum")
            summary = dict(**settings, passed=True, checks_passed=checks, recovered=False,
                recovered_epoch=None, final_epoch=final["epoch"], final_holder=final["holder"],
                certificate_signers=[], completed_replacement_crossings=0, safety_violations=0,
                failure_detection_seconds=detected_at - crashed_at, recovery_latency_seconds=None,
                blocked_peer_rpcs=network.blocked_calls, final_lane_state=final["lane_state"],
                limitations="Peer RPC partition in one process; existing original authority remains valid; no OS network rules.")
            (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            return summary
        replacement = dict(request_id="replacement-holder", crane="crane-2",
            requested_at=time.monotonic() - origin, entered_at=None, exited_at=None, status="waiting")
        rows.append(replacement)
        state = await asyncio.to_thread(client.state)
        cert = await cluster.agents["crane-2"].propose_recovery(state, "crane-2")
        if cert is None:
            raise RuntimeError("Reachable majority failed to form a recovery certificate")
        signers = {item["signer_id"] for item in cert["signatures"]}
        if signers != {"crane-2", "crane-3"}:
            raise SafetyViolation(f"Recovery signed by unexpected members: {signers}")
        (output / "recovery-certificate.json").write_text(json.dumps(cert, indent=2), encoding="utf-8")
        passed("two_survivors_signed_recovery", signers=sorted(signers))
        installed = await asyncio.to_thread(client.call, "crane-2", "InstallAuthority",
            certificate=cert, expected_epoch=state["epoch"], expected_revision=state["revision"])
        require(installed)
        authority_at = time.monotonic() - origin
        authoritative = await asyncio.to_thread(client.state)
        if authoritative["epoch"] != 1 or authoritative["holder"] != "crane-2":
            raise SafetyViolation("Gate did not install replacement authority")
        cluster.authority_epoch = authoritative["epoch"]
        # Only reconcile from authority accepted and reread from the actual gate.
        for identity in ("crane-2", "crane-3"):
            await cluster.agents[identity].reconcile(authoritative["epoch"], authoritative["holder"])
        owner, _ = await cluster.snapshot("after_recovery")
        if owner != "crane-2":
            raise SafetyViolation("Live agents did not reconcile replacement ownership")
        passed("replacement_authority_installed", epoch=authoritative["epoch"])
        stale = await asyncio.to_thread(client.call, "crane-1", "RequestGrant",
            epoch=old_state["epoch"], revision=old_state["revision"])
        if stale["allowed"] or stale["reason"] != "STALE_EPOCH":
            raise SafetyViolation("Old holder permission was not fenced")
        if await asyncio.to_thread(lane.enter, "crane-1", "stale-entry",
                                   old_grant["grant"], old_state):
            raise SafetyViolation("Old entry grant caused movement after recovery")
        passed("old_permissions_fenced", reason=stale["reason"])
        if network:
            # A remains alive with an old token claim. Its single signature cannot
            # replace epoch-one authority; it also cannot vote without suspecting
            # its own still-running holder. Do not force suspicion flags.
            isolated = await cluster.stubs["crane-1"].GetAgentState(pb.Empty(), timeout=1)
            if not isolated.has_token or isolated.epoch != 0:
                raise RuntimeError("Old holder did not retain the stale claim under isolation")
            minority = await cluster.agents["crane-1"].propose_recovery(authoritative, "crane-1")
            if minority is not None:
                raise SafetyViolation("Isolated old holder recovered without a majority")
            minority_cert = certificate(config, keys, authoritative, "crane-1", 2, signers=["crane-1"])
            denial = await asyncio.to_thread(client.call, "crane-1", "InstallAuthority",
                certificate=minority_cert, expected_epoch=authoritative["epoch"],
                expected_revision=authoritative["revision"])
            if denial["allowed"] or denial["reason"] != "NO_QUORUM":
                raise SafetyViolation("Gate accepted minority replacement authority")
            passed("isolated_live_holder_fenced_and_cannot_recover")
        if scenario == "during-occupancy":
            if lane.occupant != "crane-1":
                raise SafetyViolation("Failure/recovery silently cleared the physical occupant")
            blocked = await asyncio.to_thread(client.call, "crane-2", "RequestGrant",
                epoch=authoritative["epoch"], revision=authoritative["revision"])
            if blocked["allowed"] or blocked["reason"] not in {"LANE_BUSY", "LANE_UNKNOWN"}:
                raise SafetyViolation("Replacement admitted before physical clearance")
            blocked_at = time.monotonic() - origin
            passed("occupied_lane_blocks_replacement", reason=blocked["reason"])
            await asyncio.sleep(clearance_delay)
            emit("trusted_simulator_removal", crane="crane-1", reason="Explicit test fixture clearance")
            await asyncio.to_thread(lane.exit, "crane-1", old_row["request_id"])
            old_row.update(exited_at=time.monotonic() - origin,
                           status="removed-after-crash" if not network else "cleared-after-isolation")
            clearance_at = time.monotonic() - origin
        else:
            # Recovery revoked an unentered reservation. Actual physical model is
            # empty, so fresh sampling establishes clearance; silence never does.
            require(await asyncio.to_thread(lane.observe))
            blocked_at = None
            clearance_at = time.monotonic() - origin
        passed("physical_clearance_confirmed")
        authority = await asyncio.to_thread(client.state)
        new_grant = await asyncio.to_thread(client.call, "crane-2", "RequestGrant",
            epoch=authority["epoch"], revision=authority["revision"])
        require(new_grant)
        emit("grant_issued", request_id=replacement["request_id"], crane="crane-2",
             grant_id=new_grant["grant"]["id"], epoch=authority["epoch"], revision=authority["revision"])
        if not await asyncio.to_thread(lane.enter, "crane-2", replacement["request_id"],
                                       new_grant["grant"], authority):
            raise RuntimeError("Replacement failed to enter the clear lane")
        entered_at = time.monotonic() - origin
        replacement.update(entered_at=entered_at, status="occupied")
        await asyncio.sleep(crossing_time)
        await asyncio.to_thread(lane.exit, "crane-2", replacement["request_id"])
        replacement.update(exited_at=time.monotonic() - origin, status="completed")
        passed("replacement_completed_safe_crossing")
        rejoined_crossings = 0
        restart_to_reconcile, restart_to_entry = None, None
        if restart:
            restarted = await cluster.restart("crane-1")
            owner, states = await cluster.snapshot("restarted_before_reconciliation")
            a = states["crane-1"]
            if a.mode != pb.REJOINING or a.has_token or a.epoch != 0 or owner != "crane-2":
                raise SafetyViolation("Restarted old holder became active from stale disk state")
            passed("restart_loads_old_state_but_blocks_ownership")
            if (await restarted.request_token()).accepted or await restarted.transfer_next():
                raise SafetyViolation("REJOINING agent initiated normal token operations")
            peer_request = await cluster.stubs["crane-1"].TokenRequest(pb.TokenRequestMessage(
                agent_id="crane-3", epoch=0, request_id="restart-request-probe"), timeout=1)
            peer_transfer = await cluster.stubs["crane-1"].TokenTransfer(pb.TokenTransferMessage(
                sender_id="crane-2", receiver_id="crane-1", epoch=0, transfer_id="restart-transfer-probe"), timeout=1)
            proposal = recovery.proposal(config, await asyncio.to_thread(client.state), "crane-2")
            vote = await cluster.stubs["crane-1"].VoteRecovery(pb.RecoveryProposal(
                proposer_id="crane-2", proposal_json=recovery.canonical(proposal),
                proposer_signature=recovery.sign(keys["crane-2"], proposal)), timeout=1)
            if peer_request.accepted or peer_transfer.accepted or vote.accepted or vote.reason != "REJOINING":
                raise SafetyViolation("REJOINING agent accepted a request, transfer or recovery vote")
            passed("rejoining_blocks_requests_transfers_and_votes")
            # Observe real higher-epoch heartbeat reception without using it as
            # authority. Heartbeats must not unblock the restarted agent.
            restarted_at = next(event["time"] for event in reversed(events)
                                if event["event"] == "agent_restarted")
            await until(lambda: any(event["event"] == "agent_event" and event["time"] >= restarted_at
                and event["agent_event"]["event"] == "HEARTBEAT_RECEIVED"
                and event["agent_event"]["agent_id"] == "crane-1"
                and event["agent_event"].get("peer_id") == "crane-2" for event in events),
                5, "restarted agent heartbeat reception")
            _, states = await cluster.snapshot("restarted_after_heartbeat")
            if states["crane-1"].mode != pb.REJOINING or states["crane-1"].has_token:
                raise SafetyViolation("Heartbeat reactivated an unreconciled restarted agent")
            stale = await asyncio.to_thread(client.call, "crane-1", "RequestGrant",
                epoch=old_state["epoch"], revision=old_state["revision"])
            if stale["allowed"] or stale["reason"] != "STALE_EPOCH":
                raise SafetyViolation("Restarted agent reused old permission")
            if await asyncio.to_thread(lane.enter, "crane-1", "restarted-stale-entry",
                                       old_grant["grant"], old_state):
                raise SafetyViolation("Restarted agent moved with its old grant")
            passed("heartbeats_and_old_permissions_cannot_reactivate_agent")
            accepted = await asyncio.to_thread(client.state)
            await restarted.reconcile(accepted["epoch"], accepted["holder"])
            owner, states = await cluster.snapshot("restarted_after_reconciliation")
            if (owner != "crane-2" or states["crane-1"].mode != pb.LOCAL
                    or states["crane-1"].has_token or states["crane-1"].epoch != accepted["epoch"]):
                raise SafetyViolation("Trusted reconciliation granted unintended token ownership")
            passed("gate_reconciliation_rejoins_without_granting_token")
            restart_to_reconcile = time.monotonic() - origin - restarted_at
            row = dict(request_id="rejoined-holder", crane="crane-1",
                requested_at=time.monotonic() - origin, entered_at=None, exited_at=None, status="waiting")
            rows.append(row)
            if not (await restarted.request_token()).accepted:
                raise RuntimeError("Reconciled agent's token request was rejected")
            require(await asyncio.to_thread(lane.observe))
            await cluster.handoff(client, lane, "crane-1")
            current = await asyncio.to_thread(client.state)
            grant = await asyncio.to_thread(client.call, "crane-1", "RequestGrant",
                epoch=current["epoch"], revision=current["revision"])
            require(grant)
            emit("grant_issued", request_id=row["request_id"], crane="crane-1",
                 grant_id=grant["grant"]["id"], epoch=current["epoch"], revision=current["revision"])
            if not await asyncio.to_thread(lane.enter, "crane-1", row["request_id"], grant["grant"], current):
                raise RuntimeError("Rejoined agent could not enter with its new grant")
            row.update(entered_at=time.monotonic() - origin, status="occupied")
            restart_to_entry = row["entered_at"] - restarted_at
            await asyncio.sleep(crossing_time)
            await asyncio.to_thread(lane.exit, "crane-1", row["request_id"])
            row.update(exited_at=time.monotonic() - origin, status="completed")
            rejoined_crossings = 1
            passed("rejoined_agent_completes_new_authorized_crossing")
        owner, _ = await cluster.snapshot("finished")
        final = await asyncio.to_thread(client.state)
        if owner != ("crane-1" if restart else "crane-2") or lane.occupant is not None or final["lane_state"] != "CLEAR":
            raise SafetyViolation("Replacement did not finish safely")
        summary = dict(**settings, passed=True, recovered=True, checks_passed=checks,
            failed_holder="crane-1" if not network else None,
            isolated_holder="crane-1" if network else None,
            replacement_holder="crane-2", final_holder=owner, recovered_epoch=final["epoch"],
            restarted=restart, completed_rejoined_crossings=rejoined_crossings,
            restart_ready_to_reconciled_seconds=restart_to_reconcile,
            restart_ready_to_safe_entry_seconds=restart_to_entry,
            certificate_signers=sorted(signers), failure_detection_seconds=detected_at - crashed_at,
            detection_seconds_by_agent={identity: at - crashed_at for identity, at in detections.items()},
            authority_recovery_seconds=authority_at - detected_at,
            recovery_latency_seconds=entered_at - detected_at,
            failure_to_safe_entry_seconds=entered_at - crashed_at,
            blocked_until_clearance_seconds=clearance_at - blocked_at if blocked_at is not None else None,
            completed_replacement_crossings=1, safety_violations=0, final_lane_state=final["lane_state"],
            blocked_peer_rpcs=network.blocked_calls if network else None,
            limitations=("Single-process peer RPC partition; management/gate reachable; stale old token remains fenced."
                         if network else "Single-process service restart; trusted adapter reconciliation; no OS kill or baselines."
                         if restart else "Single-process service stop; explicit simulator removal during occupancy; no OS kill or baselines."))
        (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary
    except Exception as error:
        emit("experiment_failed", error=type(error).__name__, detail=str(error))
        (output / "failure.json").write_text(json.dumps(dict(
            error=type(error).__name__, detail=str(error)), indent=2), encoding="utf-8")
        if progress:
            progress(f"[FAIL] {label}: {type(error).__name__}: {error}")
        raise
    finally:
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
            with (output / "requests.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=["request_id", "crane", "requested_at",
                                                           "entered_at", "exited_at", "status"])
                writer.writeheader()
                writer.writerows(rows)


async def run_recovery_suite(output, *, scenario="all", **options):
    if scenario not in (*SCENARIOS, "all"):
        raise ValueError(f"Unknown recovery scenario: {scenario}")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    results = {}
    for name in SCENARIOS if scenario == "all" else (scenario,):
        results[name] = await run_recovery_scenario(output / name, name, **options)
    summary = dict(passed=True, scenarios=results)
    (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


async def run_partition_suite(output, *, scenario="all", **options):
    scenarios = {"majority-before-entry": ("before-entry", "majority-partition"),
                 "majority-during-occupancy": ("during-occupancy", "majority-partition"),
                 "no-majority": ("before-entry", "no-majority")}
    if scenario not in (*scenarios, "all"):
        raise ValueError(f"Unknown partition scenario: {scenario}")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    results = {}
    for name in scenarios if scenario == "all" else (scenario,):
        setup, fault = scenarios[name]
        results[name] = await run_recovery_scenario(output / name, setup, fault=fault, **options)
    summary = dict(passed=True, scenarios=results)
    (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


async def run_restart_suite(output, *, scenario="all", **options):
    return await run_recovery_suite(output, scenario=scenario, restart=True, **options)
