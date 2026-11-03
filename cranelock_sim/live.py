import argparse
import asyncio
import csv
from dataclasses import asdict, replace
from datetime import datetime
import json
from pathlib import Path
import secrets
import threading
import time
import uuid

import grpc

from cranelock.agent import Mode
from cranelock.controller import CentralController
from cranelock.controller_service import ControllerReturnWorker
from cranelock.gate_client import GateClient
from cranelock.reconciliation import ReconciliationClient
from cranelock_gate.client import Client
from cranelock_gate.core import Gate
from cranelock_gate.server import RunningServer
from cranelock_gate.storage import initialize
from .agents import AgentCluster, wall_clock
from .controller import ControllerConnection, RPCStats, TeamControllerService
from .dashboard import make_server
from .experiment import new_config
from .lane import Lane
from .network import PeerNetwork


# owns one interactive experiment and its actual grpc services
class LiveYard:
    def __init__(self, root, *, automatic=False):
        self.root = Path(root)
        self.session_token = secrets.token_urlsafe(32)
        self.lock = asyncio.Lock()
        self.event_lock = threading.Lock()
        self.cluster = self.gate = self.server = self.service = self.channel = None
        self.tick = None
        self.connections = []
        self.events, self.rows = [], []
        self.stream = None
        self.directory = None
        self.automatic = automatic
        self.return_worker = None

    # writes component events without exposing signing keys
    def emit(self, event, **fields):
        record = dict(time=round(time.monotonic() - self.origin, 3), event=event, **fields)
        with self.event_lock:
            self.events.append(record)
            if len(self.events) > 4000:
                self.events = self.events[-3000:]
            if self.stream:
                self.stream.write(json.dumps(record) + "\n")
                self.stream.flush()

    # starts a new yard while preserving previous experiment files
    async def start(self):
        self.directory = self.root / ("live-" + datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
        self.directory.mkdir(parents=True, exist_ok=False)
        self.events, self.rows, self.waiting = [], [], []
        self.reservation = self.stale = None
        self.recovery = dict(votes=0, required=2, status="Not requested", signers=[])
        self.origin = time.monotonic()
        self.observations = True
        self.gate_online = self.controller_online = True
        self.last_decision = "Yard ready"
        self.stream = (self.directory / "events.jsonl").open("x", encoding="utf-8")
        config, self.keys = new_config(3)
        self.config = replace(config, grant_ttl_seconds=30)
        (self.directory / "experiment.json").write_text(json.dumps(dict(model="interactive-yard",
            scenario="automatic-recovery" if self.automatic else "operator-controlled",
            time_basis="host_monotonic_seconds")), encoding="utf-8")
        (self.directory / "gate-config.json").write_text(json.dumps(asdict(self.config)), encoding="utf-8")
        self.database = self.directory / "gate.sqlite"
        initialize(self.database, self.config, wall_clock())
        self.gate = Gate(self.database, self.config, wall_clock)
        self.server = RunningServer(self.gate, "127.0.0.1:0")
        self.gate_address = f"127.0.0.1:{self.server.port}"
        self.channel = grpc.insecure_channel(self.gate_address)
        self.client = Client(self.channel, self.config, self.keys, timeout=0.6)
        self.controller = CentralController(self.config, self.keys["controller"], self.channel)
        await asyncio.to_thread(self.controller.install_central, "crane-1")
        self.service = TeamControllerService(self.controller)
        self.controller_port = self.service.port
        self.connections = [ControllerConnection(self.controller_port, RPCStats()) for _ in self.config.members]
        self.network = PeerNetwork(self.config.members, self.emit)

        # supplies a private gate connection for each crane
        def gate_factory(identity):
            return GateClient(grpc.insecure_channel(self.gate_address), self.config,
                              {identity: self.keys[identity]}, identity, timeout=0.6)

        self.cluster = AgentCluster(list(self.config.members), self.directory, self.emit,
            recovery_config=self.config, keys=self.keys, timeout=0.8, initial_mode=Mode.CENTRAL,
            network=self.network, gate_factory=gate_factory, auto_recover=self.automatic,
            controller_probes=dict(zip(self.config.members, (connection.probe for connection in self.connections))))
        self.lane = Lane(self.client, wall_clock, self.emit)
        await asyncio.to_thread(self.lane.observe)
        await self.cluster.start()
        if self.automatic:
            self.return_worker = ControllerReturnWorker(self.controller, self.cluster.addresses,
                timeout=0.3, emit=lambda raw: self.emit("controller_event", controller_event=json.loads(raw)))
        self.cached = await asyncio.to_thread(self.client.state)
        self.emit("workload_started", mode="CENTRAL")
        self.tick = asyncio.create_task(self.observe_loop())

    # continuously samples the physical model unless sensor loss is injected
    async def observe_loop(self):
        while True:
            await asyncio.sleep(0.3)
            async with self.lock:
                if self.observations and self.gate_online:
                    try:
                        await asyncio.to_thread(self.lane.observe)
                    except (grpc.RpcError, RuntimeError):
                        pass
                if self.automatic and self.gate_online:
                    try:
                        if await self.connections[0].probe():
                            await self.return_worker.step()
                        await self.snapshot()
                        await self.dispatch()
                    except (grpc.RpcError, RuntimeError, ValueError) as error:
                        self.last_decision = f"Automatic coordination waiting: {error}"

    # retains an unentered request when authority revokes its reservation
    def revoke_reservation(self):
        if self.reservation:
            self.stale = self.reservation
            row = self.reservation["row"]
            row["status"] = "waiting"
            if row not in self.waiting:
                self.waiting.insert(0, row)
            self.reservation = None

    # reconciles local authority with member-signed network requests
    async def reconcile_local(self, signer, state):
        addresses = {identity: address for identity, address in self.cluster.addresses.items()
                     if identity not in self.cluster.stopped
                     and self.network.groups[identity] == self.network.groups[signer]}
        await ReconciliationClient(self.config, signer, self.keys[signer], timeout=1).reconcile(addresses, state)

    # reserves access using the real controller or token holder
    async def dispatch(self):
        if not self.waiting or self.reservation or self.lane.occupant or not self.gate_online:
            return
        row = next((row for row in self.waiting if row["crane"] not in self.cluster.stopped), None)
        if row is None:
            return
        identity = row["crane"]
        state = await asyncio.to_thread(self.client.state)
        if state["mode"] == "CENTRAL":
            if not self.controller_online:
                self.last_decision = "Controller unavailable; waiting for majority recovery"
                return
            result = await self.connections[0].assign(identity)
            if not result["allowed"]:
                raise RuntimeError(result["reason"])
            await self.controller.reconcile_remote(self.active_addresses())
        else:
            owner = state["holder"]
            if owner in self.cluster.stopped:
                self.last_decision = "Holder unavailable; recover authority first"
                return
            if owner != identity:
                reply = await self.cluster.agents[identity].request_token()
                if not reply.accepted:
                    raise RuntimeError(reply.reason)
                if not await self.cluster.agents[owner].transfer_next():
                    self.last_decision = "Token transfer blocked; no entry permission"
                    return
                await self.reconcile_local(identity, await asyncio.to_thread(self.client.state))
        state = await asyncio.to_thread(self.client.state)
        result = await asyncio.to_thread(self.client.call, identity, "RequestGrant",
                                        epoch=state["epoch"], revision=state["revision"])
        self.last_decision = result["reason"]
        self.emit("grant_decision", crane=identity, allowed=result["allowed"], reason=result["reason"])
        if result["allowed"] and result["action_applied"]:
            self.waiting.remove(row)
            self.reservation = dict(crane=identity, grant=result["grant"], authority=state, row=row)
            self.emit("grant_issued", crane=identity, request_id=row["request_id"],
                      grant_id=result["grant"]["id"], epoch=state["epoch"], revision=state["revision"])
            self.last_decision = f"{identity} reserved the lane; click Enter lane"

    # returns endpoints that are still running
    def active_addresses(self):
        return {identity: address for identity, address in self.cluster.addresses.items()
                if identity not in self.cluster.stopped}

    # confirms deliberate simulated removal under the physical lane lock
    def operator_clear(self):
        with self.lane.lock:
            if self.lane.occupant:
                self.emit("trusted_simulator_removal", crane=self.lane.occupant)
                self.lane.occupant = self.lane.request_id = None
            state = self.client.state()
            return self.client.call("operator", "ClearLane", session_id=state["session_id"],
                observation_revision=state["observation_revision"],
                sequence=state["observation_sequence"] + 1, observed_at_ms=round(wall_clock()*1000),
                reason="UI operator explicitly confirms simulated lane removal and emptiness")

    # applies one serialized operator action
    async def action(self, payload):
        async with self.lock:
            action = payload.get("action")
            identity = payload.get("crane", "crane-2")
            if identity not in self.config.members:
                raise ValueError("Unknown crane")
            try:
                self.emit("operator_action", action=action, crane=identity)
                if action == "request":
                    if identity in self.cluster.stopped:
                        raise ValueError("Restart this crane before requesting access")
                    if (not any(row["crane"] == identity for row in self.waiting)
                            and not (self.reservation and self.reservation["crane"] == identity)
                            and self.lane.occupant != identity):
                        row = dict(request_id=uuid.uuid4().hex, crane=identity,
                            requested_at=time.monotonic()-self.origin, entered_at=None,
                            exited_at=None, status="waiting")
                        self.rows.append(row)
                        self.waiting.append(row)
                        self.emit("request", crane=identity, request_id=row["request_id"])
                    await self.dispatch()
                    if not self.gate_online:
                        self.last_decision = "Gate offline; request waits without entry permission"
                elif action == "enter":
                    if not self.reservation or self.reservation["crane"] != identity:
                        raise ValueError("This crane has no current reservation")
                    if identity in self.cluster.stopped:
                        raise ValueError("Stopped cranes cannot move")
                    pending = self.reservation
                    accepted = await asyncio.to_thread(self.lane.enter, identity,
                        pending["row"]["request_id"], pending["grant"], pending["authority"])
                    self.last_decision = "Entry accepted by gate" if accepted else "Entry rejected; no movement"
                    if accepted:
                        pending["row"].update(entered_at=time.monotonic()-self.origin, status="occupied")
                        self.stale = pending
                        self.reservation = None
                elif action == "exit":
                    if self.lane.occupant != identity or identity in self.cluster.stopped:
                        raise ValueError("Only the running physical occupant may exit")
                    row = next(row for row in reversed(self.rows) if row["request_id"] == self.lane.request_id)
                    if self.observations and self.gate_online:
                        await asyncio.to_thread(self.lane.exit, identity, row["request_id"])
                    else:
                        with self.lane.lock:
                            self.lane.occupant = self.lane.request_id = None
                            self.emit("physical_exit", crane=identity, request_id=row["request_id"])
                    row.update(exited_at=time.monotonic()-self.origin, status="completed")
                    await self.dispatch()
                elif action == "stop_agent":
                    if identity not in self.cluster.stopped:
                        await self.cluster.stop(identity)
                elif action == "restart_agent":
                    await self.cluster.restart(identity)
                elif action == "stop_controller":
                    await asyncio.to_thread(self.service.close)
                    self.controller_online = False
                    self.emit("controller_stopped", occupant=self.lane.occupant)
                elif action == "restore_controller":
                    if not self.controller_online:
                        self.service = TeamControllerService(self.controller, port=self.controller_port)
                        self.controller_online = True
                    if self.automatic:
                        self.emit("controller_restored")
                        return await self.snapshot()
                    state = await asyncio.to_thread(self.client.state)
                    holder = state["holder"] if state["holder"] not in self.cluster.stopped else next(iter(self.active_addresses()))
                    await asyncio.to_thread(self.controller.install_central, holder)
                    await self.controller.reconcile_remote(self.active_addresses())
                    self.revoke_reservation()
                    self.emit("controller_restored")
                    await self.dispatch()
                elif action in {"partition", "isolate", "heal"}:
                    members = list(self.config.members)
                    holder = (await asyncio.to_thread(self.client.state))["holder"]
                    groups = ([members] if action == "heal" else [[member] for member in members]
                              if action == "isolate" else [[holder], [member for member in members if member != holder]])
                    self.network.partition(groups)
                elif action == "recover":
                    if self.automatic:
                        raise ValueError("Automatic workers coordinate recovery in this yard")
                    if identity in self.cluster.stopped:
                        raise ValueError("Stopped cranes cannot propose recovery")
                    state = await asyncio.to_thread(self.client.state)
                    holder = state["holder"]
                    if holder in self.cluster.stopped or self.network.groups[holder] != self.network.groups[identity]:
                        holder = identity
                    cert = await self.cluster.agents[identity].propose_recovery(state, holder)
                    if cert is None:
                        attempt = next((event["agent_event"] for event in reversed(self.events)
                            if event["event"] == "agent_event"
                            and event["agent_event"].get("agent_id") == identity
                            and event["agent_event"]["event"] in {"RECOVERY_NO_QUORUM", "RECOVERY_REJECTED"}), {})
                        reason = attempt.get("reason", "NO_QUORUM")
                        self.recovery = dict(votes=attempt.get("votes", 0), required=self.config.majority,
                                             status=reason, signers=[])
                        self.last_decision = f"{reason}; authority unchanged"
                    else:
                        self.recovery = dict(votes=len(cert["signatures"]), required=self.config.majority,
                            status="Certificate formed", signers=[vote["signer_id"] for vote in cert["signatures"]])
                        result = await asyncio.to_thread(self.client.call, identity, "InstallAuthority",
                            certificate=cert, expected_epoch=state["epoch"], expected_revision=state["revision"])
                        self.last_decision = result["reason"]
                        if result["allowed"]:
                            self.cluster.authority_epoch = result["state"]["epoch"]
                            self.revoke_reservation()
                            await self.reconcile_local(identity, result["state"])
                            self.recovery["status"] = "Installed at gate"
                            self.emit("controller_fallback_installed", epoch=result["state"]["epoch"],
                                      holder=holder, occupant=self.lane.occupant)
                elif action == "stale_entry":
                    if not self.stale:
                        raise ValueError("No earlier permission has been saved yet")
                    old = self.stale
                    result = await asyncio.to_thread(self.client.call, "simulator", "ValidateEntry",
                        epoch=old["authority"]["epoch"], revision=old["authority"]["revision"],
                        holder=old["crane"], grant_id=old["grant"]["id"], session_id=old["grant"]["id"])
                    self.last_decision = result["reason"]
                    self.emit("stale_permission_decision", allowed=result["allowed"], reason=result["reason"])
                elif action == "clear":
                    result = await asyncio.to_thread(self.operator_clear)
                    if not result["allowed"]:
                        raise RuntimeError(result["reason"])
                    self.stale = self.reservation or self.stale
                    self.reservation = None
                    self.last_decision = "Operator confirmed simulated clearance"
                    await self.dispatch()
                elif action == "sensor":
                    self.observations = not self.observations
                    self.last_decision = "Position observations resumed" if self.observations else "Position observations stopped"
                elif action == "gate":
                    self.revoke_reservation()
                    if self.gate_online:
                        self.save_gate_events()
                        await asyncio.to_thread(self.server.close)
                        self.gate_online = False
                    else:
                        self.gate = Gate(self.database, self.config, wall_clock)
                        self.server = RunningServer(self.gate, self.gate_address)
                        self.gate_online = True
                    self.emit("gate_availability", available=self.gate_online)
                else:
                    raise ValueError("Unknown action")
            except (grpc.RpcError, RuntimeError, ValueError) as error:
                self.last_decision = str(error)
                self.emit("action_denied", action=action, reason=str(error))
            return await self.snapshot()

    # returns actual gate, agent and physical state for the interface
    async def snapshot(self):
        fresh = False
        if self.gate_online:
            try:
                self.cached = await asyncio.to_thread(self.client.state)
                fresh = True
            except (grpc.RpcError, RuntimeError):
                pass
        if fresh and self.reservation and (not self.cached["grant"] or
                self.cached["grant"]["id"] != self.reservation["grant"]["id"]):
            self.revoke_reservation()
            self.last_decision = "Reservation expired or revoked; request a fresh turn"
        agents = []
        for identity, agent in self.cluster.agents.items():
            agents.append(dict(id=identity, running=identity not in self.cluster.stopped,
                mode=agent.state.mode.name, holder=agent.state.token_holder,
                epoch=agent.state.current_epoch, revision=agent.state.revision,
                has_token=agent.state.has_token, queue=list(agent.state.request_queue),
                suspected=sorted(agent.suspected), controller_suspected=agent.controller_suspected))
        with self.event_lock:
            events = list(self.events)
        if self.automatic:
            installed = next((event["agent_event"] for event in reversed(events)
                if event["event"] == "agent_event"
                and event["agent_event"]["event"] == "AUTOMATIC_RECOVERY_INSTALLED"), None)
            if installed:
                self.recovery = dict(votes=installed["votes"], required=self.config.majority,
                    status="Automatically installed at gate", signers=installed["signers"])
        significant = [event for event in events if event["event"] != "peer_rpc_blocked"
            and (event["event"] != "agent_event" or event["agent_event"]["event"] in {
                "TOKEN_HOLDER_SUSPECTED", "CONTROLLER_SUSPECTED", "RECOVERY_VOTE_CAST",
                "RECOVERY_NO_QUORUM", "RECOVERY_REJECTED", "RECOVERY_CERTIFICATE_FORMED",
                "NETWORK_RECONCILIATION_ACCEPTED", "TOKEN_TRANSFER_RECEIVED",
                "AUTOMATIC_RECOVERY_INSTALLED", "AUTOMATION_WAITING"})]
        return dict(session_token=self.session_token, automatic=self.automatic,
            gate=self.cached, gate_online=self.gate_online, gate_fresh=fresh,
            controller_online=self.controller_online, observations=self.observations,
            occupant=self.lane.occupant, agents=agents, waiting=[row["crane"] for row in self.waiting],
            reservation=self.reservation["crane"] if self.reservation else None,
            recovery=self.recovery, decision=self.last_decision, events=significant[-70:],
            groups=self.network.groups, completed=sum(row["status"] == "completed" for row in self.rows),
            evidence=str(self.directory.resolve()))

    # serializes snapshots with operator actions
    async def state(self):
        async with self.lock:
            return await self.snapshot()

    # exports the durable gate audit when storage is open
    def save_gate_events(self):
        if self.gate and not self.gate.closed:
            (self.directory / "gate-events.jsonl").write_text("".join(
                json.dumps(event)+"\n" for event in self.gate.events()), encoding="utf-8")

    # stops all services and retains evidence
    async def close(self):
        if self.tick:
            self.tick.cancel()
            await asyncio.gather(self.tick, return_exceptions=True)
        if self.cluster:
            await self.cluster.close()
        if self.return_worker:
            await self.return_worker.close()
        for connection in self.connections:
            await connection.close()
        if self.service:
            await asyncio.to_thread(self.service.close)
        self.save_gate_events()
        if self.server and self.gate_online:
            await asyncio.to_thread(self.server.close)
        if self.channel:
            self.channel.close()
        if self.directory and self.directory.is_dir():
            (self.directory / "summary.json").write_text(json.dumps(dict(model="interactive-yard",
                requested=len(self.rows), completed=sum(row["status"] == "completed" for row in self.rows),
                safety_violations=int(self.cluster.violation is not None) if self.cluster else 0,
                limitations="Interactive local yard; operator-controlled actions, not a performance benchmark.")),
                encoding="utf-8")
            with (self.directory / "requests.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=["request_id", "crane", "requested_at",
                                                           "entered_at", "exited_at", "status"])
                writer.writeheader()
                writer.writerows(self.rows)
        if self.stream:
            self.stream.close()
            self.stream = None


# bridges the http threads to one asyncio service loop
class LiveRuntime:
    def __init__(self, root, *, automatic=False):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.yard = LiveYard(root, automatic=automatic)
        try:
            asyncio.run_coroutine_threadsafe(self.yard.start(), self.loop).result(timeout=15)
        except BaseException:
            self.close()
            raise

    # waits for bounded simulator actions
    def call(self, action, payload=None):
        task = self.yard.state() if action == "state" else self.yard.action(payload)
        return asyncio.run_coroutine_threadsafe(task, self.loop).result(timeout=15)

    # releases the worker and every grpc service
    def close(self):
        try:
            asyncio.run_coroutine_threadsafe(self.yard.close(), self.loop).result(timeout=15)
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(timeout=3)
            self.loop.close()


# serves the live yard alongside the original read-only evaluation pages
def main():
    parser = argparse.ArgumentParser(description="Interactive CraneLock yard")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--bind", choices=["127.0.0.1", "0.0.0.0"], default="127.0.0.1")
    parser.add_argument("--root", type=Path, default=Path("run/evaluation"))
    parser.add_argument("--automatic", action="store_true", help="recover and return authority automatically after detected failures")
    args = parser.parse_args()
    live = LiveRuntime(args.root, automatic=args.automatic)
    try:
        with make_server(args.root, args.port, args.bind, live=live) as server:
            print(f"Live crane yard: http://127.0.0.1:{server.server_port}/live", flush=True)
            print("Stop with Ctrl+C. Evaluation remains available at /.", flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
    finally:
        live.close()


if __name__ == "__main__":
    main()
