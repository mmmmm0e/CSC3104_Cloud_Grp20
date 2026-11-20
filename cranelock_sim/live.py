"""Operator-driven CraneLock demo with fresh, local gRPC services."""
import argparse
import asyncio
from collections import deque
import csv
from dataclasses import asdict, replace
from datetime import datetime
import json
from pathlib import Path
import secrets
import socket
import threading
import time
import uuid

import grpc

from cranelock.agent import Agent, Mode
from cranelock.controller import CentralController, ControllerHeartbeatClient
from cranelock.gate_client import GateClient
from cranelock_gate.client import Client, may_enter
from cranelock_gate.core import Gate
from cranelock_gate.server import RunningServer
from cranelock_gate.storage import initialize

from .agents import wall_clock
from .dashboard import make_server
from .experiment import new_config, require
from .lane import Lane, SafetyViolation
from .network import PeerNetwork


AGENT_EVENTS = {
    "AGENT_STARTED", "BACKGROUND_FAILED", "CONTROLLER_SUSPECTED",
    "CONTROLLER_RESTORED", "TOKEN_HOLDER_SUSPECTED", "TOKEN_QUEUED",
    "TOKEN_TRANSFER_RECEIVED", "RECOVERY_VOTE_CAST", "RECOVERY_NO_QUORUM",
    "RECOVERY_CERTIFICATE_FORMED", "RECOVERY_AUTHORITY_INSTALL_RESULT",
    "RECOVERY_RETRY_BLOCKED", "MODE_CHANGED",
}


class LiveYard:
    def __init__(self, root):
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.directory = Path(root) / f"live-{stamp}-{uuid.uuid4().hex[:8]}"
        self.session_token = secrets.token_urlsafe(32)
        self.lock = asyncio.Lock()
        self.event_lock = threading.Lock()
        self.events = deque(maxlen=160)
        self.rows = []
        self.waiting = []
        self.agents, self.probes, self.addresses = {}, {}, {}
        self.stopped = set()
        self.server = self.controller = self.channel = self.tick = self.stream = None
        self.reservation = self.earlier_grant = self.occupied_row = None
        self.controller_online = False
        self.observations = True
        self.safety_violations = 0
        self.decision = "Yard ready. Request a turn to begin."

    # records observed events and keeps a short tail for the ui
    def emit(self, event, **fields):
        record = dict(time=time.monotonic() - self.origin, event=event, **fields)
        with self.event_lock:
            self.events.append(record)
            if self.stream:
                self.stream.write(json.dumps(record, allow_nan=False) + "\n")
                self.stream.flush()

    # keeps the demo log focused on protocol decisions
    def agent_event(self, event):
        if event["event"] in AGENT_EVENTS:
            self.emit("agent_event", agent_event=event)

    # starts the native agent with its own gate and controller clients
    async def start_agent(self, identity):
        agent = Agent(
            identity, {peer: address for peer, address in self.addresses.items() if peer != identity},
            self.members[0], epoch=0, mode=Mode.CENTRAL, interval=.15, timeout=1.2,
            state_file=self.directory / f"{identity}.json", event_sink=self.agent_event,
            recovery_config=self.config, recovery_private_key=self.keys[identity],
            controller_probe=self.probes[identity].probe,
            gate=GateClient(grpc.insecure_channel(self.gate_address), self.config,
                            {identity: self.keys[identity]}, identity, timeout=1),
        )
        try:
            await agent.start(self.addresses[identity], interceptors=(self.network.interceptor(identity),))
        except BaseException:
            await agent.close()
            raise
        self.agents[identity] = agent
        self.stopped.discard(identity)

    # starts a fresh yard and waits for each local service
    async def start(self):
        self.directory.mkdir(parents=True, exist_ok=False)
        self.origin = time.monotonic()
        self.stream = (self.directory / "events.jsonl").open("x", encoding="utf-8")
        config, self.keys = new_config(3)
        self.config = replace(config, grant_ttl_seconds=30)
        self.members = tuple(self.config.members)
        self.settings = dict(
            model="interactive-yard", scenario="automatic-recovery",
            limitations="Operator-driven simulation with local gRPC services and simulated lane occupancy.",
        )
        (self.directory / "experiment.json").write_text(json.dumps(self.settings), encoding="utf-8")
        (self.directory / "gate-config.json").write_text(json.dumps(asdict(self.config)), encoding="utf-8")
        self.database = self.directory / "gate.sqlite"
        initialize(self.database, self.config, wall_clock())
        self.server = RunningServer(Gate(self.database, self.config, wall_clock), "127.0.0.1:0")
        self.gate_address = f"127.0.0.1:{self.server.port}"
        self.channel = grpc.insecure_channel(self.gate_address)
        self.client = Client(self.channel, self.config, {
            identity: self.keys[identity] for identity in (self.config.simulator_id, self.config.operator_id)
        }, timeout=1)
        self.controller = CentralController(self.config, self.keys[self.config.controller_id], self.channel)
        require(await asyncio.to_thread(self.controller.install_central, self.members[0]))
        port = await self.controller.start_heartbeat_server()
        self.controller_address = f"127.0.0.1:{port}"
        self.controller_online = True
        self.lane = Lane(self.client, wall_clock, self.emit)
        require(await asyncio.to_thread(self.lane.observe))
        self.network = PeerNetwork(self.members, self.record_network_event)
        sockets = {}
        try:
            for identity in self.members:
                sock = sockets[identity] = socket.socket()
                sock.bind(("127.0.0.1", 0))
                self.addresses[identity] = f"127.0.0.1:{sock.getsockname()[1]}"
                self.probes[identity] = ControllerHeartbeatClient(
                    self.controller_address, identity, self.config.controller_id, timeout=.5)
            for identity, sock in sockets.items():
                sock.close()
                await self.start_agent(identity)
        finally:
            for sock in sockets.values():
                sock.close()
        self.cached = await asyncio.to_thread(self.client.state)
        self.emit("workload_started", mode="CENTRAL")
        self.tick = asyncio.create_task(self.observe_loop())

    # keeps partition changes in the event trail
    def record_network_event(self, event, **fields):
        if event != "peer_rpc_blocked":
            self.emit(event, **fields)

    # refreshes gate state and requeues revoked reservations
    async def refresh_gate(self):
        state = await asyncio.to_thread(self.client.state)
        previous = self.cached
        self.cached = state
        if state["epoch"] > previous["epoch"]:
            event = "controller_fallback_installed" if previous["mode"] == "CENTRAL" else "recovery_installed"
            self.emit(event, epoch=state["epoch"], holder=state["holder"])
        elif state["mode"] == "CENTRAL" and previous["mode"] == "LOCAL":
            self.emit("central_authority_restored", epoch=state["epoch"], holder=state["holder"])
        if self.reservation and (state["grant"] or {}).get("id") != self.reservation["grant"]["id"]:
            self.earlier_grant = self.reservation
            row = self.reservation["row"]
            row["status"] = "waiting"
            self.waiting.insert(0, row)
            self.reservation = None
            self.decision = "The previous reservation expired or was revoked."
        return state

    # samples the physical lane and retries queued requests
    async def observe_loop(self):
        while True:
            await asyncio.sleep(.3)
            async with self.lock:
                if not self.server:
                    continue
                try:
                    if self.observations:
                        await asyncio.to_thread(self.lane.observe)
                    state = await self.refresh_gate()
                    await self.dispatch(state)
                except (grpc.RpcError, RuntimeError) as error:
                    self.decision = str(error)

    # uses native authority decisions to reserve access for the next request
    async def dispatch(self, state):
        if self.safety_violations or self.reservation or self.lane.occupant or state["lane_state"] != "CLEAR":
            return
        row = next((row for row in self.waiting if row["crane"] not in self.stopped), None)
        if row is None:
            return
        identity = row["crane"]
        agent = self.agents[identity]
        if agent.background_failure:
            return
        if state["mode"] == "CENTRAL":
            if not self.controller_online:
                self.decision = "Controller stopped. Queued requests wait for majority recovery."
                return
            if state["holder"] != identity:
                result = await asyncio.to_thread(self.controller.install_central, identity)
                if not result["allowed"]:
                    self.decision = result["reason"]
                    return
                state = result["state"]
        elif state["holder"] != identity:
            if state["holder"] not in self.stopped:
                reply = await agent.request_token()
                if not reply.accepted:
                    self.decision = reply.reason
            return
        result = await agent.gate.request_grant(str(uuid.uuid4()), state["epoch"], state["revision"])
        self.decision = result["reason"]
        if may_enter(result):
            self.waiting.remove(row)
            row["status"] = "reserved"
            self.reservation = dict(crane=identity, grant=result["grant"], authority=state, row=row)
            self.emit("grant_issued", crane=identity, request_id=row["request_id"],
                      epoch=state["epoch"], revision=state["revision"])
            self.decision = f"{identity} reserved the lane. Click Enter lane."

    # confirms deliberate removal of the simulated occupant through the gate
    def operator_clear(self):
        with self.lane.lock:
            state = self.client.state()
            require(self.client.call(self.config.operator_id, "ClearLane",
                session_id=state["session_id"], observation_revision=state["observation_revision"],
                sequence=state["observation_sequence"] + 1, observed_at_ms=round(wall_clock() * 1000),
                reason="Operator explicitly confirmed removal in the simulated yard"))
            if self.lane.occupant:
                self.emit("trusted_simulator_removal", crane=self.lane.occupant)
                self.lane.occupant = self.lane.request_id = None

    # applies one operator action before reporting the resulting state
    async def action(self, payload):
        async with self.lock:
            action = payload.get("action")
            identity = payload.get("crane", self.members[0])
            if identity not in self.members:
                raise ValueError("Unknown crane")
            try:
                if action == "request":
                    if identity in self.stopped:
                        raise ValueError("Restart this crane before requesting access.")
                    if (identity != self.lane.occupant
                            and (not self.reservation or self.reservation["crane"] != identity)
                            and not any(row["crane"] == identity for row in self.waiting)):
                        row = dict(request_id=uuid.uuid4().hex, crane=identity, status="waiting",
                                   requested_at=time.monotonic() - self.origin, entered_at=None, exited_at=None)
                        self.rows.append(row)
                        self.waiting.append(row)
                        self.emit("request", crane=identity, request_id=row["request_id"])
                    self.decision = f"{identity} is waiting for a reservation."
                elif action == "enter":
                    await self.enter(identity)
                elif action == "exit":
                    if identity != self.lane.occupant or identity in self.stopped:
                        raise ValueError("Only the running physical occupant can exit.")
                    row = self.occupied_row
                    if self.observations and self.server:
                        await asyncio.to_thread(self.lane.exit, identity, row["request_id"])
                    else:
                        self.lane.occupant = self.lane.request_id = None
                        self.emit("physical_exit", crane=identity, request_id=row["request_id"])
                    row.update(exited_at=time.monotonic() - self.origin, status="completed")
                    self.occupied_row = None
                    self.decision = f"{identity} exited the lane."
                elif action == "stop_agent":
                    if identity not in self.stopped:
                        self.stopped.add(identity)
                        await self.agents[identity].close()
                        self.emit("crane_stopped", crane=identity)
                elif action == "restart_agent":
                    if identity not in self.stopped:
                        raise ValueError("Stop this crane before restarting it.")
                    await self.start_agent(identity)
                    self.emit("agent_restarted", crane=identity)
                elif action == "stop_controller":
                    await self.controller.stop_heartbeat_server()
                    self.controller_online = False
                    self.emit("controller_stopped")
                elif action == "restore_controller":
                    if not self.controller_online:
                        await self.controller.start_heartbeat_server(self.controller_address)
                        self.controller_online = True
                        self.emit("controller_restarted")
                elif action in {"partition", "isolate", "heal"}:
                    await self.partition(action)
                elif action == "stale_entry":
                    await self.try_earlier_grant()
                elif action == "clear":
                    await asyncio.to_thread(self.operator_clear)
                    if self.occupied_row:
                        self.occupied_row["status"] = "removed"
                        self.occupied_row = None
                    self.decision = "Operator confirmed simulated clearance."
                elif action == "sensor":
                    self.observations = not self.observations
                    self.decision = "Position updates resumed." if self.observations else "Position updates paused."
                elif action == "gate":
                    if self.server:
                        await self.close_gate()
                    else:
                        self.server = RunningServer(Gate(self.database, self.config, wall_clock), self.gate_address)
                    self.emit("gate_availability", available=self.server is not None)
                else:
                    raise ValueError("Unknown action")
                if self.server and action in {"request", "exit", "clear"}:
                    await self.dispatch(await self.refresh_gate())
            except (grpc.RpcError, RuntimeError, ValueError) as error:
                self.decision = str(error)
                self.emit("action_denied", action=action, reason=self.decision)
            return await self.snapshot()

    # applies physical movement only through the shared lane entry validator
    async def enter(self, identity):
        if self.safety_violations or identity in self.stopped or self.agents[identity].background_failure:
            raise ValueError("This crane cannot move.")
        pending = self.reservation
        if not pending or pending["crane"] != identity:
            raise ValueError("This crane has no current reservation.")
        row = pending["row"]
        try:
            accepted = await asyncio.to_thread(self.lane.enter, identity, row["request_id"],
                                              pending["grant"], pending["authority"])
        except SafetyViolation:
            self.safety_violations += 1
            raise
        self.decision = "Entry accepted by the Gate." if accepted else "Entry rejected. No crane moved."
        if accepted:
            row.update(entered_at=time.monotonic() - self.origin, status="occupied")
            self.occupied_row = row
            self.earlier_grant, self.reservation = pending, None

    # asks the gate to recheck an earlier consumed or revoked grant
    async def try_earlier_grant(self):
        if not self.earlier_grant:
            raise ValueError("No earlier grant has been recorded.")
        old = self.earlier_grant
        result = await asyncio.to_thread(self.client.call, self.config.simulator_id, "ValidateEntry",
            epoch=old["authority"]["epoch"], revision=old["authority"]["revision"],
            holder=old["crane"], grant_id=old["grant"]["id"], session_id=old["grant"]["id"])
        self.decision = result["reason"]
        self.emit("earlier_permission_checked", allowed=result["allowed"], reason=result["reason"])
        if may_enter(result):
            self.safety_violations += 1
            raise SafetyViolation("The Gate accepted an earlier grant.")

    # isolates peer links without taking away access to the gate
    async def partition(self, action):
        if action == "partition":
            holder = (await self.refresh_gate())["holder"]
            groups = [[holder], [identity for identity in self.members if identity != holder]]
        elif action == "isolate":
            groups = [[identity] for identity in self.members]
        else:
            groups = [list(self.members)]
        self.network.partition(groups)

    # reports current services, gate authority and independent occupancy
    async def snapshot(self):
        fresh = False
        if self.server:
            try:
                await self.refresh_gate()
                fresh = True
            except (grpc.RpcError, RuntimeError):
                pass
        agents = []
        for identity, agent in self.agents.items():
            state = agent.state
            agents.append(dict(id=identity, running=identity not in self.stopped and not agent.background_failure,
                mode=state.mode.name, epoch=state.current_epoch, revision=state.revision,
                has_token=state.has_token, queue=list(state.request_queue),
                controller_suspected=agent.controller_suspected))
        with self.event_lock:
            events = list(self.events)[-70:]
        recovery = next((event["agent_event"] for event in reversed(events)
            if event.get("agent_event", {}).get("event") in {"RECOVERY_CERTIFICATE_FORMED", "RECOVERY_NO_QUORUM"}), None)
        vote_status = "Waiting for a recovery attempt"
        if recovery:
            if recovery["event"] == "RECOVERY_NO_QUORUM":
                vote_status = "No quorum"
            elif recovery["target_epoch"] == self.cached["epoch"] and recovery["holder"] == self.cached["holder"]:
                vote_status = "Recovery epoch installed at the Gate"
            else:
                vote_status = "Certificate collected"
        return dict(session_token=self.session_token, can_reset=True,
            gate=self.cached, gate_online=self.server is not None,
            gate_fresh=fresh, controller_online=self.controller_online, observations=self.observations,
            occupant=self.lane.occupant, agents=agents, waiting=[row["crane"] for row in self.waiting],
            reservation=self.reservation["crane"] if self.reservation else None,
            earlier_grant=self.earlier_grant is not None,
            recovery=dict(votes=recovery["votes"] if recovery else 0, required=self.config.majority, status=vote_status),
            decision=self.decision, events=events, groups=self.network.groups,
            completed=sum(row["status"] == "completed" for row in self.rows),
            evidence=str(self.directory.resolve()))

    # serializes snapshots with actions
    async def state(self):
        async with self.lock:
            return await self.snapshot()

    # exports the audit before closing the gate service
    async def close_gate(self):
        server, self.server = self.server, None
        try:
            events = await asyncio.to_thread(server.gate.events)
            (self.directory / "gate-events.jsonl").write_text(
                "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
        finally:
            await asyncio.to_thread(server.close)

    # stops local services and saves the run evidence
    async def close(self):
        if self.tick:
            self.tick.cancel()
            await asyncio.gather(self.tick, return_exceptions=True)
        await asyncio.gather(*(agent.close() for identity, agent in self.agents.items()
                              if identity not in self.stopped))
        await asyncio.gather(*(probe.close() for probe in self.probes.values()))
        if self.controller:
            await self.controller.stop_heartbeat_server()
        if self.server:
            await self.close_gate()
        if self.channel:
            self.channel.close()
        if self.stream:
            self.stream.close()
            self.stream = None
            if hasattr(self, "settings"):
                summary = dict(**self.settings, requested=len(self.rows), safety_violations=self.safety_violations,
                               completed=sum(row["status"] == "completed" for row in self.rows))
                (self.directory / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
                with (self.directory / "requests.csv").open("w", newline="", encoding="utf-8") as stream:
                    writer = csv.DictWriter(stream, fieldnames=["request_id", "crane", "status",
                                                               "requested_at", "entered_at", "exited_at"])
                    writer.writeheader()
                    writer.writerows(self.rows)


class LiveRuntime:
    def __init__(self, root):
        self.loop = asyncio.new_event_loop()
        self.lock = asyncio.Lock()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.yard = LiveYard(root)
        try:
            self.submit(self.yard.start())
        except BaseException:
            self.close()
            raise

    # sends bounded http work to the yard's asyncio loop
    def submit(self, coroutine):
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout=15)

    # reads the current run after any active reset finishes
    async def state(self):
        async with self.lock:
            return await self.yard.state()

    # checks the session against the run that will execute the action
    async def action(self, payload, token):
        async with self.lock:
            if not secrets.compare_digest(token.encode(), self.yard.session_token.encode()):
                raise PermissionError("The live yard session changed. Refresh and try again.")
            if payload.get("action") != "reset":
                return await self.yard.action(payload)
            previous = self.yard
            fresh = LiveYard(previous.directory.parent)
            try:
                await fresh.start()
            except BaseException:
                await fresh.close()
                raise
            self.yard = fresh
            await previous.close()
            return await fresh.state()

    async def close_yard(self):
        async with self.lock:
            await self.yard.close()

    # releases the yard and its event loop
    def close(self):
        try:
            self.submit(self.close_yard())
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join()
            self.loop.close()


def main():
    parser = argparse.ArgumentParser(description="Run an interactive local CraneLock yard")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--root", type=Path, default=Path("run/evaluation"))
    args = parser.parse_args()
    live = LiveRuntime(args.root)
    try:
        with make_server(args.root, args.port, live=live) as server:
            print(f"Live crane yard: http://127.0.0.1:{server.server_port}/live", flush=True)
            print(f"Evidence: {live.yard.directory.resolve()}", flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
    finally:
        live.close()


if __name__ == "__main__":
    main()
