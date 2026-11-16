"""Measured experiments against independent services; the runner never recovers agents.

The runner supplies demand and trusted physical observations. Agents own recovery
and LOCAL token handoff. CENTRAL scheduling uses a test-only assignment transport
around the team's controller, in the controller's own process. Loopback only.
"""
import argparse
import asyncio
import base64
import json
import os
from pathlib import Path
import socket
import sqlite3
import statistics
import subprocess
import sys
import time
import shutil

import grpc

from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from cranelock_gate.client import Client
from cranelock_gate.config import Config
from cranelock_gate.demo import load_keys
from cranelock_gate.generated import gate_pb2
from .controller import (CONTROLLER_IMPLEMENTATION, CONTROLLER_SOURCE,
                         ControllerConnection, RPCStats, TeamControllerService)
from .experiment import percentile, require
from .lane import Lane, SafetyViolation
from .workload_client import GrantAttempt, channel as workload_channel
from .workload_tls import IDENTITY

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


async def run_case(output, model, scenario, workload, *, seed, duration, crossing_time, detection_timeout,
                   cluster_factory=ProcessCluster):
    # Imported lazily: benefits imports this entry point and comparison supplies
    # only hashing/export helpers, never the legacy recovery experiment.
    from .comparison import workload_hash, write_csv

    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    complete_all = scenario == "normal"
    initial_mode = "LOCAL" if model == "basic-token" or scenario == "holder-failure" else "CENTRAL"
    public = output / "gate-config.json"
    provisioned = subprocess.run([sys.executable, "-B", "-m", "cranelock_sim.evaluation_provision", str(output)],
        capture_output=True, timeout=30, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    if provisioned.returncode:
        raise RuntimeError("Evaluation credential provisioning failed")
    config = Config.load(public)
    private = output / ".credentials"
    database = output / "state" / "gate" / "gate.sqlite"
    simulator_keys = load_keys(private / "driver" / "simulator.key.json")
    if set(simulator_keys) != {config.simulator_id}:
        raise ValueError("Evaluation requires simulator-only signing credentials")
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
    cluster = cluster_factory(output, emit)
    docker = getattr(cluster, "containerized", False)
    if docker:
        settings.update(evaluation="benefits-docker-v1", execution_mode="docker_containers",
                        limitations=cluster.limitations, detection_time_basis="host_log_receipt",
                        heartbeat_interval_seconds=.5, host_rpc_timeout_seconds=2)
    startup_timeout = 60 if docker else 15
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
        for name in ("gate", *config.members, *(f"{member}-workload" for member in config.members)):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            reservations.append(sock)
            addresses[name] = f"127.0.0.1:{sock.getsockname()[1]}"
        if docker:
            cluster.addresses.update({address: f"{name}:{address.rsplit(':', 1)[1]}"
                                      for name, address in addresses.items()})
        init_arguments = ["cranelock_gate.server", "init", "--config", str(public),
                          "--database", str(database)]
        initialized = await asyncio.to_thread(cluster.initialize, init_arguments) if docker else await asyncio.to_thread(subprocess.run,
            [sys.executable, "-B", "-m", "cranelock_gate.server", "init", "--config", str(public),
             "--database", str(database)], capture_output=True, timeout=15,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        if initialized.returncode:
            raise RuntimeError("Gate initialization failed")
        reservations[0].close()
        cluster.spawn("gate", "cranelock_gate.server", "serve", "--config", public,
            "--database", database, "--bind", addresses["gate"], "--metrics-port", "0")
        channel = cluster.gate_channel(addresses["gate"]) if docker else grpc.insecure_channel(addresses["gate"])
        client = Client(channel, config, simulator_keys, timeout=2 if docker else .5)
        async def gate_state():
            return await asyncio.to_thread(client.state)
        await poll(gate_state, startup_timeout, "gate startup")
        ready = output / "state" / "controller" / "ready.json"
        cluster.spawn("controller", "cranelock_sim.autonomous", "--controller", "--config", public,
            "--key", private / "controller" / "controller.key.json", "--gate", addresses["gate"], "--ready", ready)
        async def controller_ready():
            if ready.exists():
                try:
                    return json.loads(ready.read_text())
                except ValueError:
                    pass
        endpoint = await poll(controller_ready, startup_timeout, "controller startup")
        connection = ControllerConnection(endpoint["assignment_port"], RPCStats())
        if initial_mode == "CENTRAL" or model == "central-lock":
            require(await connection.assign("crane-1"))
            mode = initial_mode = "CENTRAL"
        else:
            bootstrap = json.loads((private / "driver" / "bootstrap-command.json").read_text(encoding="utf-8"))
            signed = gate_pb2.SignedCommand(payload_json=base64.b64decode(bootstrap["payload"]),
                                           signature=base64.b64decode(bootstrap["signature"]))
            response = await asyncio.to_thread(client.stub.InstallAuthority, signed, timeout=client.timeout)
            require(dict(allowed=response.allowed, reason=response.reason,
                         replayed=response.replayed, action_applied=response.action_applied))
        lane = Lane(client, wall_clock, emit)
        require(await asyncio.to_thread(lane.observe))
        async def sample_lane():
            observed = await asyncio.to_thread(lane.observe)
            # A pending entry grant deliberately prevents a clear observation
            # from clearing the reservation. Keep the real reservation intact.
            if observed["reason"] != "OUTSTANDING_GRANT":
                require(observed)
        stubs, workloads = {}, {}
        for index, identity in enumerate(config.members, 1):
            reservations[index].close()
            reservations[index + len(config.members)].close()
            service_credentials = private / identity
            tls = service_credentials / "workload-tls"
            allowlist = service_credentials / "workload.json"
            allowlist.write_text(json.dumps(dict(crane_id=identity, lane_id=config.lane_id,
                allowed_workload_uri_sans=[IDENTITY])), encoding="utf-8")
            args = ["cranelock", "run", "--id", identity, "--listen", addresses[identity],
                "--holder", "crane-1", "--epoch", "0", "--mode", initial_mode,
                "--state-file", str(output / "state" / identity / "state.json"), "--interval", ".5" if docker else ".05",
                "--timeout", str(detection_timeout), "--gate-address", addresses["gate"],
                "--gate-config", str(public), "--gate-keys", str(service_credentials / f"{identity}.key.json"),
                "--workload-listen", addresses[f"{identity}-workload"],
                "--workload-tls-ca", str(tls/"ca.pem"), "--workload-tls-cert", str(tls/"server.pem"),
                "--workload-tls-key", str(tls/"server-key.pem"), "--workload-allowlist", str(allowlist)]
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
            driver_tls = private / "driver" / (identity + "-workload")
            workload_connection, workloads[identity] = workload_channel(addresses[f"{identity}-workload"],
                (driver_tls/"ca.pem").read_bytes(), (driver_tls/"client.pem").read_bytes(), (driver_tls/"client-key.pem").read_bytes())
            cluster.channels.append(workload_connection)
        async def warm():
            states = await asyncio.gather(*(asyncio.to_thread(stub.GetAgentState, pb.Empty(), timeout=.5)
                                          for stub in stubs.values()))
            return all(s.mode == getattr(pb, initial_mode) and
                (not docker or (all(peer.status == "AVAILABLE" for peer in s.peers)
                 and len(s.peers) == len(config.members)-1 and s.token_holder == "crane-1"
                 and s.has_token == (s.agent_id == "crane-1"))) for s in states)
        await poll(warm, startup_timeout, "agent reconciliation")
        await asyncio.sleep(max(.2, detection_timeout))
        cluster.collect()
        settings.update(initial_mode=initial_mode, process_ids={n:p.pid for n,p in cluster.processes.items()},
                        service_endpoints=dict(addresses, controller=endpoint))
        assert len(set(settings["process_ids"].values())) == 5
        if docker:
            settings["container_ids"] = settings.pop("process_ids")
            settings["docker"] = cluster.inventory()
        (output / "experiment.json").write_text(json.dumps(settings, indent=2), encoding="utf-8")
        origin = time.monotonic()
        measuring = True
        emit("workload_started", mode=initial_mode, requests=len(rows), recovery_driver="agent_workers")
        fault_completed_at = None
        if scenario == "holder-failure":
            fault_at = time.monotonic()-origin
            await cluster.stop("crane-1")
            fault_completed_at = time.monotonic()-origin
            emit("holder_stopped", crane="crane-1", command_started_at=fault_at,
                 command_finished_at=fault_completed_at)

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
            nonlocal fault_at, fault_completed_at
            await asyncio.sleep(duration/3)
            fault_at = time.monotonic()-origin
            await cluster.stop("controller")
            fault_completed_at = time.monotonic()-origin
            emit("controller_stopped", occupant=lane.occupant, mode=mode,
                 command_started_at=fault_at, command_finished_at=fault_completed_at)
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
                attempt = None
                central_assigned = False
                while time.monotonic()-origin < horizon:
                    cluster.check()
                    state = await gate_state()
                    identity = row["crane"]
                    if state["mode"] == "CENTRAL":
                        if "controller" in cluster.stopped:
                            await asyncio.sleep(.02)
                            continue
                        if not central_assigned or state["holder"] != identity:
                            try:
                                assigned = await connection.assign(identity)
                            except grpc.RpcError as error:
                                if "controller" in cluster.stopped:
                                    continue
                                raise error
                            require(assigned)
                            central_assigned = True
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
                    if attempt is None:
                        attempt = GrantAttempt(state["epoch"], state["revision"])
                    try:
                        grant = await asyncio.to_thread(attempt.call, workloads[identity])
                    except grpc.RpcError as error:
                        if error.code() not in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
                            raise
                        await asyncio.sleep(.01)
                        continue
                    if attempt.unresolved(grant):
                        await asyncio.sleep(.01)
                        continue
                    attempt = None
                    if not grant["allowed"]:
                        await asyncio.sleep(.01)
                        continue
                    if grant["replayed"] or not grant["action_applied"]:
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
            final_mode=state["mode"],final_epoch=state["epoch"],safety_violations=0,passed=True,
            failure_detection_seconds=detected_at-fault_at if detected_at is not None else None,
            recovery_latency_seconds=resumed[0]["entered_at"]-detected_at if resumed and detected_at is not None else None,
            outage_to_entry_seconds=resumed[0]["entered_at"]-fault_at if resumed else None,
            resumed_within_window=bool(resumed) if fault_at is not None else None,
            post_fault_new_entries=len(resumed) if fault_at is not None else None,
            post_fault_completed=sum(r["exited_at"]>fault_at for r in completed) if fault_at is not None else None,
            rpc_attempts=None,rpc_per_completed=None,rpc_by_method={},
            drain_seconds=0 if complete_all else max(0,elapsed-duration))
        if docker:
            after_command = [r for r in admitted if fault_completed_at is not None and r["entered_at"] >= fault_completed_at]
            result.update(fault_completed_at_seconds=fault_completed_at,
                fault_command_end_to_entry_seconds=after_command[0]["entered_at"]-fault_completed_at if after_command else None,
                failure_detection_seconds=None, recovery_latency_seconds=None)
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
        if database.exists():
            # The Gate is stopped; mode=rw allows SQLite to recover its WAL on
            # Windows after abrupt process termination without creating a database.
            with sqlite3.connect(database.as_uri()+"?mode=rw",uri=True) as db:
                audit=[json.loads(row[0]) for row in db.execute("SELECT value FROM events ORDER BY event_id")]
                with sqlite3.connect(output/"gate.sqlite") as snapshot:
                    db.backup(snapshot)
            (output/"gate-events.jsonl").write_text("".join(json.dumps(e)+"\n" for e in audit),encoding="utf-8")
        for identity in config.members:
            state_path = output/"state"/identity/"state.json"
            if state_path.exists():
                shutil.copyfile(state_path, output/f"{identity}.json")
        events.sort(key=lambda e:e["time"])
        (output/"events.jsonl").write_text("".join(json.dumps(e)+"\n" for e in events),encoding="utf-8")
        write_csv(output/"requests.csv",rows)


async def controller_process(args):
    from cranelock_gate.config import Config
    from cranelock_gate.demo import load_keys
    config=Config.load(args.config)
    if args.gate_tls_directory:
        tls = Path(args.gate_tls_directory)
        credentials = grpc.ssl_channel_credentials((tls/"ca.pem").read_bytes(),
            (tls/"client-key.pem").read_bytes(), (tls/"client.pem").read_bytes())
        channel = grpc.secure_channel(args.gate, credentials)
    else:
        channel=grpc.insecure_channel(args.gate)
    service=TeamControllerService(config,load_keys(args.key)[config.controller_id],channel,RPCStats(), args.assignment_bind)
    try:
        credentials = None
        if args.tls_directory:
            tls = Path(args.tls_directory)
            credentials = grpc.ssl_server_credentials(
                [((tls/"server-key.pem").read_bytes(), (tls/"server.pem").read_bytes())],
                root_certificates=(tls/"ca.pem").read_bytes(), require_client_auth=True)
        port=await service.controller.start_heartbeat_server(args.heartbeat_bind, credentials=credentials)
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
    parser.add_argument("--assignment-bind", default="127.0.0.1:0")
    parser.add_argument("--heartbeat-bind", default="127.0.0.1:0")
    parser.add_argument("--tls-directory")
    parser.add_argument("--gate-tls-directory")
    asyncio.run(controller_process(parser.parse_args()))


if __name__=="__main__":
    main()
