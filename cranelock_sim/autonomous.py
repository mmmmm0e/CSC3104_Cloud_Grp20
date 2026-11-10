"""Measured experiments against independent services; the runner never recovers agents.

The runner supplies demand and trusted physical observations. Agents own recovery
and LOCAL token handoff. CENTRAL scheduling uses a test-only assignment transport
around the team's controller, in the controller's own process. Loopback only.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import socket
import sqlite3
import statistics
import subprocess
import sys
import time
from dataclasses import asdict

import grpc

from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from cranelock_gate.client import Client, certificate
from .controller import (CONTROLLER_IMPLEMENTATION, CONTROLLER_SOURCE,
                         ControllerConnection, RPCStats, TeamControllerService)
from .experiment import new_config, percentile, require
from .lane import Lane, SafetyViolation

EVALUATION = "benefits-v4"
LIMITS = ("Independent crane, controller and gate processes on one host using loopback gRPC. "
          "Agents autonomously vote, install recovery authority and hand off the LOCAL token. "
          "The runner submits requests, injects process failure and models physical occupancy. "
          "CENTRAL assignment transport and workload scheduling are experimental adapters around "
          "the team's CentralController. No Docker, remote-network or port-hardware claim. "
          "RPC overhead is not measured across the child processes. Fixed three-agent membership.")


def wall_clock():
    return (time.time_ns() // 1_000_000) / 1000


class ProcessCluster:
    """Own only processes launched for this experiment and retain their raw logs."""
    def __init__(self, output, emit):
        self.output, self.emit = output, emit
        self.processes, self.streams, self.channels = {}, {}, []
        self.stopped, self.offsets = set(), {}

    def spawn(self, name, *arguments):
        stream = (self.output / f"{name}.log").open("wb")
        self.streams[name] = stream
        try:
            process = subprocess.Popen(
                [sys.executable, "-B", "-u", "-m", *map(str, arguments)],
                cwd=Path(__file__).resolve().parents[1], stdout=stream, stderr=stream,
                env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        except BaseException:
            stream.close()
            raise
        self.processes[name] = process
        return process

    def check(self):
        for name, process in self.processes.items():
            if name not in self.stopped and process.poll() is not None:
                raise RuntimeError(f"{name} exited unexpectedly; inspect {name}.log")

    def collect(self):
        for name in self.processes:
            if not name.startswith("crane-"):
                continue
            with (self.output / f"{name}.log").open("rb") as stream:
                stream.seek(self.offsets.get(name, 0))
                while True:
                    start = stream.tell()
                    line = stream.readline()
                    if not line or not line.endswith(b"\n"):
                        self.offsets[name] = start
                        break
                    try:
                        event = json.loads(line)
                    except (ValueError, UnicodeDecodeError):
                        continue  # Native gRPC diagnostics are retained in the raw log.
                    if isinstance(event, dict) and "event" in event:
                        self.emit("agent_event", agent_event=event,
                                  recorded_monotonic=event.get("monotonic"))

    async def stop(self, name):
        self.stopped.add(name)
        process = self.processes[name]
        if process.poll() is None:
            process.kill()  # Actual process failure, not a manufactured suspicion flag.
        await asyncio.to_thread(process.wait, timeout=10)

    async def close(self):
        try:
            await asyncio.gather(*(self.stop(name) for name in self.processes))
            self.collect()
        finally:
            for channel in self.channels:
                channel.close()
            for stream in self.streams.values():
                stream.close()


async def run_case(output, model, scenario, workload, *, seed, duration, crossing_time, detection_timeout):
    # Imported lazily: benefits imports this entry point and comparison supplies
    # only hashing/export helpers, never the legacy recovery experiment.
    from .comparison import workload_hash, write_csv

    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    complete_all = scenario == "normal"
    initial_mode = "LOCAL" if model == "basic-token" or scenario == "holder-failure" else "CENTRAL"
    config, keys = new_config(3)
    public = output / "gate-config.json"
    public.write_text(json.dumps(asdict(config), indent=2), encoding="utf-8")
    for identity, key in keys.items():
        (output / f"{identity}.key.json").write_text(
            json.dumps({identity: key.private_bytes_raw().hex()}), encoding="utf-8")
    settings = dict(model=model, scenario=scenario, seed=seed, evaluation=EVALUATION,
        controller_implementation=CONTROLLER_IMPLEMENTATION, controller_source_commit=CONTROLLER_SOURCE,
        recovery_driver="agent_workers", execution_mode="independent_processes",
        measurement_policy="complete_all_requests" if complete_all else "fixed_window",
        arrival_pattern="batch" if complete_all else "scheduled",
        duration_seconds=None if complete_all else duration, crossing_time_seconds=crossing_time,
        detection_timeout_seconds=detection_timeout, workload_sha256=workload_hash(workload),
        time_basis="host_monotonic_seconds", limitations=LIMITS)
    (output / "experiment.json").write_text(json.dumps(settings, indent=2), encoding="utf-8")
    (output / "workload.json").write_text(json.dumps(workload, indent=2), encoding="utf-8")
    events, tasks = [], []
    origin = time.monotonic()
    measuring = False
    def emit(event, recorded_monotonic=None, **fields):
        stamp = recorded_monotonic if isinstance(recorded_monotonic, (int, float)) else time.monotonic()
        if measuring and stamp >= origin:
            events.append(dict(time=stamp-origin, event=event, **fields))
    cluster = ProcessCluster(output, emit)
    connection = channel = None
    rows = [dict(row, entered_at=None, exited_at=None, status="waiting") for row in workload]
    fault_at = detected_at = authority_at = None
    mode = initial_mode
    shutdown = asyncio.Event()
    lane = None
    reservations = []

    async def poll(condition, timeout, description):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            cluster.check()
            cluster.collect()
            try:
                value = await condition()
                if value:
                    return value
            except grpc.RpcError as error:
                if error.code() not in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
                    raise
            await asyncio.sleep(.02)
        raise TimeoutError(f"Timed out waiting for {description}")

    try:
        # Reserve distinct endpoints until the relevant process is launched.
        addresses = {}
        for name in ("gate", *config.members):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            reservations.append(sock)
            addresses[name] = f"127.0.0.1:{sock.getsockname()[1]}"
        initialized = await asyncio.to_thread(subprocess.run,
            [sys.executable, "-B", "-m", "cranelock_gate.server", "init", "--config", str(public),
             "--database", str(output / "gate.sqlite")], capture_output=True, timeout=15,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        if initialized.returncode:
            raise RuntimeError("Gate initialization failed")
        reservations[0].close()
        cluster.spawn("gate", "cranelock_gate.server", "serve", "--config", public,
            "--database", output / "gate.sqlite", "--bind", addresses["gate"], "--metrics-port", "0")
        channel = grpc.insecure_channel(addresses["gate"])
        client = Client(channel, config, keys, timeout=.5)
        async def gate_state():
            return await asyncio.to_thread(client.state)
        await poll(gate_state, 15, "gate startup")
        ready = output / "controller-ready.json"
        cluster.spawn("controller", "cranelock_sim.autonomous", "--controller", "--config", public,
            "--key", output / "controller.key.json", "--gate", addresses["gate"], "--ready", ready)
        async def controller_ready():
            if ready.exists():
                try:
                    return json.loads(ready.read_text())
                except ValueError:
                    pass
        endpoint = await poll(controller_ready, 15, "controller startup")
        connection = ControllerConnection(endpoint["assignment_port"], RPCStats())
        before = await gate_state()
        bootstrap = certificate(config, keys, before, "crane-1", 0,
            mode=initial_mode, signers=[config.controller_id] if initial_mode == "CENTRAL" else None)
        if initial_mode == "CENTRAL" or model == "central-lock":
            require(await connection.assign("crane-1"))
            mode = initial_mode = "CENTRAL"
        else:
            require(await asyncio.to_thread(client.call, "crane-1", "InstallAuthority", certificate=bootstrap,
                expected_epoch=before["epoch"], expected_revision=before["revision"]))
        lane = Lane(client, wall_clock, emit)
        require(await asyncio.to_thread(lane.observe))
        async def sample_lane():
            observed = await asyncio.to_thread(lane.observe)
            # A pending entry grant deliberately prevents a clear observation
            # from clearing the reservation. Keep the real reservation intact.
            if observed["reason"] != "OUTSTANDING_GRANT":
                require(observed)
        stubs = {}
        for index, identity in enumerate(config.members, 1):
            reservations[index].close()
            args = ["cranelock", "run", "--id", identity, "--listen", addresses[identity],
                "--holder", "crane-1", "--epoch", "0", "--mode", initial_mode,
                "--state-file", str(output / f"{identity}.json"), "--interval", ".05",
                "--timeout", str(detection_timeout), "--gate-address", addresses["gate"],
                "--gate-config", str(public), "--gate-keys", str(output / f"{identity}.key.json")]
            if model == "cranelock":
                args += ["--enable-recovery"]
                if scenario != "holder-failure":
                    args += ["--controller-address", f"127.0.0.1:{endpoint['heartbeat_port']}"]
            for peer in config.members:
                if peer != identity:
                    args += ["--peer", f"{peer}={addresses[peer]}"]
            cluster.spawn(identity, *args)
            peer_channel = grpc.insecure_channel(addresses[identity])
            cluster.channels.append(peer_channel)
            stubs[identity] = rpc.CraneAgentStub(peer_channel)
        async def warm():
            states = await asyncio.gather(*(asyncio.to_thread(stub.GetAgentState, pb.Empty(), timeout=.5)
                                          for stub in stubs.values()))
            return all(s.mode == getattr(pb, initial_mode) for s in states)
        await poll(warm, 15, "agent reconciliation")
        await asyncio.sleep(max(.2, detection_timeout))
        cluster.collect()
        settings.update(initial_mode=initial_mode, process_ids={n:p.pid for n,p in cluster.processes.items()},
                        service_endpoints=dict(addresses, controller=endpoint))
        assert len(set(settings["process_ids"].values())) == 5
        (output / "experiment.json").write_text(json.dumps(settings, indent=2), encoding="utf-8")
        origin = time.monotonic()
        measuring = True
        emit("workload_started", mode=initial_mode, requests=len(rows), recovery_driver="agent_workers")
        if scenario == "holder-failure":
            fault_at = time.monotonic()-origin
            await cluster.stop("crane-1")
            emit("holder_stopped", crane="crane-1")

        async def observe():
            nonlocal mode, authority_at, detected_at
            last = None
            while not shutdown.is_set():
                cluster.check()
                cluster.collect()
                await sample_lane()
                state = await gate_state()
                current = (state["epoch"], state["revision"], state["holder"], state["mode"])
                if current != last:
                    if fault_at is not None and state["epoch"] > 0 and authority_at is None:
                        authority_at = time.monotonic()-origin
                        emit("controller_fallback_installed" if scenario=="controller-outage" else "recovery_installed",
                             epoch=state["epoch"], holder=state["holder"], occupant=lane.occupant,
                             recovery_driver="agent_workers")
                    mode = state["mode"]
                    snapshots = {}
                    for identity, stub in stubs.items():
                        if identity in cluster.stopped:
                            continue
                        snapshot = await asyncio.to_thread(stub.GetAgentState, pb.Empty(), timeout=.5)
                        snapshots[identity] = dict(epoch=snapshot.epoch, revision=snapshot.revision,
                            holder=snapshot.token_holder, has_token=snapshot.has_token,
                            mode=pb.Mode.Name(snapshot.mode))
                    emit("agent_snapshot", owner=state["holder"], authority_epoch=state["epoch"],
                         agents=snapshots, mode=mode)
                    last = current
                kinds = {"CONTROLLER_SUSPECTED"} if scenario=="controller-outage" else {"TOKEN_HOLDER_SUSPECTED"}
                detections = [e["time"] for e in events if e["event"]=="agent_event" and
                    e["agent_event"].get("event") in kinds and fault_at is not None and e["time"]>=fault_at]
                if detections:
                    detected_at = min(detections)
                await asyncio.sleep(.02)

        async def fault():
            nonlocal fault_at
            await asyncio.sleep(duration/3)
            fault_at = time.monotonic()-origin
            await cluster.stop("controller")
            emit("controller_stopped", occupant=lane.occupant, mode=mode)
            if await connection.probe():
                raise SafetyViolation("Stopped controller still answers")
            emit("controller_rpc_unavailable")

        horizon = max(30, duration + len(rows)*(crossing_time+1)) if complete_all else duration
        async def arrivals():
            for row in rows:
                await asyncio.sleep(max(0, row["requested_at"]-(time.monotonic()-origin)))
                emit("request", request_id=row["request_id"], crane=row["crane"], scheduled_at=row["requested_at"])

        async def worker():
            for row in rows:
                await asyncio.sleep(max(0,row["requested_at"]-(time.monotonic()-origin)))
                if time.monotonic()-origin >= horizon:
                    break
                requested = None
                while time.monotonic()-origin < horizon:
                    cluster.check()
                    state = await gate_state()
                    identity = row["crane"]
                    if state["mode"] == "CENTRAL":
                        if "controller" in cluster.stopped:
                            await asyncio.sleep(.02)
                            continue
                        try:
                            assigned = await connection.assign(identity)
                        except grpc.RpcError as error:
                            if "controller" in cluster.stopped:
                                continue
                            raise error
                        require(assigned)
                        emit("central_assignment", crane=identity)
                        state = await gate_state()
                    elif state["holder"] != identity:
                        token = (state["epoch"],state["revision"],state["holder"],identity)
                        if token != requested:
                            try:
                                answer = await asyncio.to_thread(stubs[state["holder"]].TokenRequest,
                                    pb.TokenRequestMessage(agent_id=identity,epoch=state["epoch"],
                                                           revision=state["revision"],
                                                           request_id=row["request_id"]),timeout=.2)
                                if answer.accepted:
                                    requested = token
                            except grpc.RpcError:
                                pass  # An unavailable holder must be recovered by agents.
                        await asyncio.sleep(.01)
                        continue
                    else:
                        own = await asyncio.to_thread(stubs[identity].GetAgentState,pb.Empty(),timeout=.5)
                        if not own.has_token or (own.epoch,own.revision)!=(state["epoch"],state["revision"]):
                            await asyncio.sleep(.01)
                            continue
                    await sample_lane()
                    state = await gate_state()
                    grant = await asyncio.to_thread(client.call,identity,"RequestGrant",
                        epoch=state["epoch"],revision=state["revision"])
                    if not grant["allowed"]:
                        await asyncio.sleep(.01)
                        continue
                    emit("grant_issued",request_id=row["request_id"],crane=identity,
                         epoch=state["epoch"],revision=state["revision"],grant_id=grant["grant"]["id"])
                    if not await asyncio.to_thread(lane.enter,identity,row["request_id"],grant["grant"],state):
                        await asyncio.sleep(.01)
                        continue
                    row.update(entered_at=time.monotonic()-origin,status="occupied")
                    await asyncio.sleep(crossing_time)
                    await asyncio.to_thread(lane.exit,identity,row["request_id"])
                    row.update(exited_at=time.monotonic()-origin,status="completed")
                    break
            if complete_all and any(r["exited_at"] is None for r in rows):
                raise TimeoutError("Batch did not finish before its watchdog")
            if not complete_all:
                await asyncio.sleep(max(0,duration-(time.monotonic()-origin)))

        observer = asyncio.create_task(observe())
        worker_task = asyncio.create_task(worker())
        tasks = [observer, worker_task, asyncio.create_task(arrivals())]
        if scenario == "controller-outage":
            tasks.append(asyncio.create_task(fault()))
        # Surface observer/process failures immediately, rather than waiting for
        # a blocked workload to exhaust its watchdog.
        async def work():
            await asyncio.gather(*tasks[1:])
            shutdown.set()
        finisher = asyncio.create_task(work())
        try:
            await asyncio.gather(observer, finisher)
        finally:
            if not finisher.done():
                finisher.cancel()
            await asyncio.gather(finisher, return_exceptions=True)
        shutdown.set()
        elapsed = time.monotonic()-origin
        cluster.collect()
        state = await gate_state()
        completed = [r for r in rows if r["exited_at"] is not None and (complete_all or r["exited_at"]<=duration)]
        admitted = [r for r in rows if r["entered_at"] is not None and (complete_all or r["entered_at"]<=duration)]
        waits = [r["entered_at"]-r["requested_at"] for r in admitted]
        resumed = [r for r in admitted if fault_at is not None and r["entered_at"]>fault_at]
        result = dict(**settings, requested=len(rows),completed=len(completed),accepted=len(admitted),
            completed_all_requests=len(completed)==len(rows) if complete_all else None,
            total_completion_seconds=elapsed if complete_all else None,
            unfinished_at_horizon=None if complete_all else len(rows)-len(completed),
            mean_wait_seconds=statistics.mean(waits) if waits else None,p95_wait_seconds=percentile(waits,.95),
            throughput_per_minute=len(completed)*60/(elapsed if complete_all else duration),
            fault_at_seconds=fault_at,authority_at_seconds=authority_at,
            final_mode=state["mode"],safety_violations=0,passed=True,
            failure_detection_seconds=detected_at-fault_at if detected_at is not None else None,
            recovery_latency_seconds=resumed[0]["entered_at"]-detected_at if resumed and detected_at is not None else None,
            outage_to_entry_seconds=resumed[0]["entered_at"]-fault_at if resumed else None,
            resumed_within_window=bool(resumed) if fault_at is not None else None,
            post_fault_new_entries=len(resumed) if fault_at is not None else None,
            post_fault_completed=sum(r["exited_at"]>fault_at for r in completed) if fault_at is not None else None,
            rpc_attempts=None,rpc_per_completed=None,rpc_by_method={},
            drain_seconds=0 if complete_all else max(0,elapsed-duration))
        (output/"summary.json").write_text(json.dumps(result,indent=2),encoding="utf-8")
        return result
    except BaseException as error:
        if isinstance(error, Exception):
            emit("experiment_failed",error=type(error).__name__,detail=str(error))
            (output/"failure.json").write_text(json.dumps(dict(error=type(error).__name__,detail=str(error))),encoding="utf-8")
        raise
    finally:
        shutdown.set()
        for task in tasks:
            if not task.done(): task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        cluster.collect()
        # Teardown disconnects are retained in raw logs, not presented as faults
        # injected during the measured workload.
        measuring = False
        await cluster.close()
        if connection: await connection.close()
        if channel: channel.close()
        for sock in reservations: sock.close()
        if (output/"gate.sqlite").exists():
            with sqlite3.connect((output/"gate.sqlite").as_uri()+"?mode=ro",uri=True) as db:
                audit=[json.loads(row[0]) for row in db.execute("SELECT value FROM events ORDER BY event_id")]
            (output/"gate-events.jsonl").write_text("".join(json.dumps(e)+"\n" for e in audit),encoding="utf-8")
        events.sort(key=lambda e:e["time"])
        (output/"events.jsonl").write_text("".join(json.dumps(e)+"\n" for e in events),encoding="utf-8")
        write_csv(output/"requests.csv",rows)


async def controller_process(args):
    from cranelock_gate.config import Config
    from cranelock_gate.demo import load_keys
    config=Config.load(args.config)
    channel=grpc.insecure_channel(args.gate)
    service=TeamControllerService(config,load_keys(args.key)[config.controller_id],channel,RPCStats())
    try:
        port=await service.controller.start_heartbeat_server("127.0.0.1:0")
        Path(args.ready).write_text(json.dumps(dict(assignment_port=service.port,heartbeat_port=port)),encoding="utf-8")
        await asyncio.Event().wait()
    finally:
        await service.controller.stop_heartbeat_server()
        service.close()
        channel.close()


def main():
    parser=argparse.ArgumentParser(description="Internal independent controller process for evaluation")
    parser.add_argument("--controller",action="store_true",required=True)
    for name in ("config","key","gate","ready"):
        parser.add_argument(f"--{name}",required=True)
    asyncio.run(controller_process(parser.parse_args()))


if __name__=="__main__":
    main()
