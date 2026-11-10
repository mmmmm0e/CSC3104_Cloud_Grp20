"""Bounded normal-operation integration of real agents, gate RPCs and physical lane.

Services run on ephemeral loopback ports in one host process. The adapter paces
the existing transfer worker at physical exits; it does not invent token owners.
"""
import asyncio
import csv
import json
import math
import time
from pathlib import Path

import grpc

from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from cranelock.agent import Agent, Mode
from cranelock_gate.client import Client, certificate
from cranelock_gate.core import Gate
from cranelock_gate.server import RunningServer
from cranelock_gate.storage import initialize
from .experiment import new_config, percentile, require
from .lane import Lane, SafetyViolation


def wall_clock():
    """Wall time at the signed observation protocol's millisecond resolution."""
    return (time.time_ns() // 1_000_000) / 1000


def token_owner(states):
    owners = [identity for identity, state in states.items() if state.has_token]
    if len(owners) > 1:
        raise SafetyViolation(f"Multiple agents hold usable tokens: {owners}")
    return owners[0] if owners else None


class AgentCluster:
    def __init__(self, members, output, emit, *, recovery_config=None, keys=None, timeout=3,
                 network=None, controller_probes=None, initial_mode=Mode.LOCAL, peer_interceptor=None):
        self.members, self.output, self.emit = members, output, emit
        self.agents, self.channels, self.stubs = {}, {}, {}
        self.violation = None
        self.stopped = set()
        self.recovery_config, self.keys, self.timeout = recovery_config, keys, timeout
        self.network = network
        self.authority_epoch = 0
        self.addresses = {}
        self.controller_probes = controller_probes or {}
        self.initial_mode, self.peer_interceptor = initial_mode, peer_interceptor

    def on_event(self, event):
        self.emit("agent_event", agent_event=event)
        # Same-event-loop callbacks observe every ownership transition, including
        # the relinquish-before-send gap. Never throw out of an Agent callback.
        try:
            token_owner({identity: agent.state for identity, agent in self.agents.items()
                         if identity not in self.stopped
                         and agent.state.current_epoch == self.authority_epoch})
        except SafetyViolation as error:
            self.violation = error

    async def start(self):
        for identity in self.members:
            peers = {peer: "127.0.0.1:1" for peer in self.members if peer != identity}
            self.agents[identity] = Agent(identity, peers, self.members[0], epoch=0, mode=self.initial_mode,
                interval=0.05, timeout=self.timeout, state_file=self.output / f"{identity}.json",
                event_sink=self.on_event, recovery_config=self.recovery_config,
                recovery_private_key=self.keys[identity] if self.recovery_config else None,
                controller_probe=self.controller_probes.get(identity))
        ports = {}
        for identity, agent in self.agents.items():
            ports[identity] = await agent.start("127.0.0.1:0", auto_transfer=False,
                interceptors=(self.network.interceptor(identity),) if self.network else ())
            # start() schedules workers immediately. Rewire placeholder channels
            # before those workers use them; closing an in-use channel can kill
            # a heartbeat task with a channel-usage exception.
            for task in agent.tasks:
                task.cancel()
            await asyncio.gather(*agent.tasks, return_exceptions=True)
            agent.tasks = []
        self.addresses = {identity: f"127.0.0.1:{port}" for identity, port in ports.items()}
        for identity, agent in self.agents.items():
            for peer in agent.peers:
                await agent.channels[peer].close()
                address = f"127.0.0.1:{ports[peer]}"
                agent.peers[peer] = address
                agent.channels[peer] = grpc.aio.insecure_channel(address,
                    interceptors=(self.peer_interceptor,) if self.peer_interceptor else ())
                agent.stubs[peer] = rpc.CraneAgentStub(agent.channels[peer])
            channel = grpc.aio.insecure_channel(f"127.0.0.1:{ports[identity]}")
            self.channels[identity] = channel
            self.stubs[identity] = rpc.CraneAgentStub(channel)
        await asyncio.wait_for(asyncio.gather(
            *(channel.channel_ready() for channel in self.channels.values())), timeout=5)
        for agent in self.agents.values():
            agent.tasks = [asyncio.create_task(agent.heartbeat_loop()),
                           asyncio.create_task(agent.monitor_loop())]
            if agent.controller_probe:
                agent.tasks.append(asyncio.create_task(agent.controller_watch_loop()))
        self.emit("cluster_started", addresses={k: f"127.0.0.1:{v}" for k, v in ports.items()})

    async def snapshot(self, stage):
        active = [identity for identity in self.members if identity not in self.stopped]
        states = dict(zip(active, await asyncio.gather(*(
            self.stubs[identity].GetAgentState(pb.Empty(), timeout=3)
            for identity in active))))
        if self.violation:
            raise self.violation
        owner = token_owner({identity: state for identity, state in states.items()
                             if state.epoch == self.authority_epoch})
        self.emit("agent_snapshot", stage=stage, owner=owner, authority_epoch=self.authority_epoch,
                  agents={identity: dict(
            has_token=state.has_token, holder=state.token_holder, epoch=state.epoch,
            mode=pb.Mode.Name(state.mode),
            queue=list(state.request_queue), pending_transfer=state.pending_transfer_id)
            for identity, state in states.items()})
        return owner, states

    async def handoff(self, client, lane, expected):
        owner, _ = await self.snapshot("before_handoff")
        if owner == expected:
            return
        if lane.occupant is not None:
            raise SafetyViolation("Adapter attempted handoff before physical exit")
        old = await asyncio.to_thread(client.state)
        if owner is None or old["holder"] != owner:
            raise SafetyViolation("Agent and gate disagree before handoff")
        if not await self.agents[owner].transfer_next():
            raise RuntimeError("Agent could not complete queued gRPC token transfer")
        new_owner, _ = await self.snapshot("after_token_transfer")
        if new_owner != expected:
            raise SafetyViolation(f"Expected queued holder {expected}, got {new_owner}")
        # Deliver the old holder's actual announcement over gRPC before a later
        # handoff. Otherwise a third agent may still know an earlier holder.
        sender = self.agents[owner]
        announcement = pb.HeartbeatMessage(agent_id=owner, epoch=sender.state.current_epoch,
            token_holder=sender.state.token_holder, mode=sender.state.mode,
            has_token=sender.state.has_token, outgoing_transfer_id=sender.last_handoff,
            sent_unix_seconds=wall_clock())
        # uses the control rpc budget for the unchanged handoff announcement
        async def announce(peer):
            try:
                await sender.stubs[peer].Heartbeat(announcement, timeout=self.timeout)
            except grpc.aio.AioRpcError as error:
                self.emit("handoff_announcement_unavailable", sender=owner, receiver=peer,
                          reason=error.code().name)

        for attempt in range(3):
            await asyncio.gather(*(announce(peer) for peer in sender.peers))
            _, states = await self.snapshot("after_handoff_announcement")
            if all(state.token_holder == expected for state in states.values()):
                break
            if attempt < 2:
                self.emit("handoff_announcement_retry", sender=owner, holder=expected,
                          transfer_id=announcement.outgoing_transfer_id, attempt=attempt + 2)
                await asyncio.sleep(.05)
        else:
            raise RuntimeError("Holder announcement did not reach every agent")
        require(await asyncio.to_thread(client.call, owner, "TransferHolder",
            epoch=old["epoch"], revision=old["revision"], destination=expected))
        self.emit("gate_handoff", sender=owner, receiver=expected)

    async def stop(self, identity):
        self.stopped.add(identity)
        await self.agents[identity].close()

    async def restart(self, identity):
        if identity not in self.stopped:
            raise ValueError("Stop an agent before restarting it")
        path = self.output / f"{identity}.json"
        # Preserve the exact stale disk state as evidence; do not reset it.
        (self.output / f"{identity}.before-restart.json").write_bytes(path.read_bytes())
        peers = {peer: address for peer, address in self.addresses.items() if peer != identity}
        agent = Agent(identity, peers, self.members[0], epoch=0, interval=0.05,
            timeout=self.timeout, state_file=path, event_sink=self.on_event,
            recovery_config=self.recovery_config,
            recovery_private_key=self.keys[identity] if self.recovery_config else None)
        self.agents[identity] = agent
        # Keep the old endpoint so surviving peer channels reconnect normally.
        try:
            await agent.start(self.addresses[identity], auto_transfer=False,
                interceptors=(self.network.interceptor(identity),) if self.network else ())
        except Exception:
            await agent.close()
            raise
        self.stopped.remove(identity)
        await asyncio.wait_for(self.channels[identity].channel_ready(), timeout=5)
        self.emit("agent_restarted", agent_id=identity, address=self.addresses[identity])
        return agent

    async def close(self):
        await asyncio.gather(*(agent.close() for identity, agent in self.agents.items()
                              if identity not in self.stopped))
        await asyncio.gather(*(channel.close() for channel in self.channels.values()))


async def run_agent_demo(output, *, rounds=2, crossing_time=0.1):
    """Each round serves A, B, C; B/C queue at A while A occupies the lane."""
    if type(rounds) is not int or rounds < 1:
        raise ValueError("rounds must be a positive integer")
    if not math.isfinite(crossing_time) or not 0 < crossing_time <= 1:
        raise ValueError("crossing_time must be finite and in (0, 1] seconds")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    settings = dict(model="three-agent-grpc-normal-handoff", rounds=rounds,
                    crossing_time=crossing_time, time_basis="host_monotonic_seconds")
    (output / "experiment.json").write_text(json.dumps(settings, indent=2), encoding="utf-8")
    origin = time.perf_counter()
    events, rows = [], []

    def emit(event, **fields):
        events.append(dict(time=time.perf_counter() - origin, event=event, **fields))

    config, keys = new_config(3)
    members = list(config.members)
    cluster = AgentCluster(members, output, emit)
    gate, server, channel = None, None, None
    try:
        initialize(output / "gate.sqlite", config, wall_clock())
        gate = Gate(output / "gate.sqlite", config, wall_clock)
        server = RunningServer(gate, "127.0.0.1:0")
        channel = grpc.insecure_channel(f"127.0.0.1:{server.port}")
        await asyncio.to_thread(grpc.channel_ready_future(channel).result, timeout=5)
        client = Client(channel, config, keys)
        state = await asyncio.to_thread(client.state)
        # Initial provisioning, not a recovery vote round.
        cert = certificate(config, keys, state, members[0], 0)
        require(await asyncio.to_thread(client.call, members[0], "InstallAuthority",
            certificate=cert, expected_epoch=state["epoch"], expected_revision=state["revision"]))
        lane = Lane(client, wall_clock, emit)
        require(await asyncio.to_thread(lane.observe))
        await cluster.start()
        workload_start = time.perf_counter()

        async def request(identity, round_number):
            row = dict(request_id=f"round-{round_number}-{identity}", crane=identity,
                       requested_at=time.perf_counter() - origin, entered_at=None,
                       exited_at=None, status="waiting")
            rows.append(row)
            emit("request", request_id=row["request_id"], crane=identity)
            owner, _ = await cluster.snapshot("request")
            if owner != identity:
                reply = await cluster.agents[identity].request_token()
                if not reply.accepted:
                    raise RuntimeError(f"Token request rejected: {reply.reason}")
            return row

        for round_number in range(1, rounds + 1):
            jobs = {members[0]: await request(members[0], round_number)}
            for identity in members:
                row = jobs[identity]
                require(await asyncio.to_thread(lane.observe))
                await cluster.handoff(client, lane, identity)
                owner, states = await cluster.snapshot("before_entry")
                state = await asyncio.to_thread(client.state)
                if (owner != identity or states[identity].epoch != state["epoch"]
                        or state["holder"] != identity):
                    raise SafetyViolation("Entry crane lacks matching agent and gate authority")
                grant = await asyncio.to_thread(client.call, identity, "RequestGrant",
                    epoch=state["epoch"], revision=state["revision"])
                require(grant)
                emit("grant_issued", request_id=row["request_id"], crane=identity,
                     grant_id=grant["grant"]["id"], epoch=state["epoch"], revision=state["revision"])
                if not await asyncio.to_thread(lane.enter, identity, row["request_id"],
                                                grant["grant"], state):
                    raise RuntimeError("Gate denied normal holder entry")
                row.update(entered_at=time.perf_counter() - origin, status="occupied")
                if identity == members[0]:
                    # Actual gRPC requests populate A's queue while A is inside.
                    for waiter in members[1:]:
                        jobs[waiter] = await request(waiter, round_number)
                        denial = await asyncio.to_thread(client.call, waiter, "RequestGrant",
                            epoch=state["epoch"], revision=state["revision"])
                        if denial["allowed"] or denial["reason"] != "WRONG_HOLDER":
                            raise SafetyViolation("Gate failed to fence a waiting nonholder")
                        emit("nonholder_denied", crane=waiter, reason=denial["reason"])
                await asyncio.sleep(crossing_time)
                owner, _ = await cluster.snapshot("occupied")
                if owner != identity or lane.occupant != identity:
                    raise SafetyViolation("Holder changed before physical exit")
                await asyncio.to_thread(lane.exit, identity, row["request_id"])
                row.update(exited_at=time.perf_counter() - origin, status="completed")
        elapsed = time.perf_counter() - workload_start
        owner, _ = await cluster.snapshot("finished")
        final = await asyncio.to_thread(client.state)
        if lane.occupant is not None or final["lane_state"] != "CLEAR" or final["holder"] != owner:
            raise SafetyViolation("Final lane/authority did not reconcile")
        waits = [row["entered_at"] - row["requested_at"] for row in rows]
        summary = dict(**settings, requested=len(rows), completed=len(rows),
            elapsed_seconds=elapsed, mean_wait_seconds=sum(waits) / len(waits),
            p95_wait_seconds=percentile(waits, 0.95), safety_violations=0,
            completed_by_crane={member: sum(row["crane"] == member for row in rows)
                                for member in members},
            final_holder=owner, final_lane_state=final["lane_state"],
            limitations="Single-process loopback services; adapter-paced transfers; no failures or baselines.")
        (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary
    except Exception as error:
        emit("experiment_failed", error=type(error).__name__, detail=str(error))
        (output / "failure.json").write_text(json.dumps(dict(
            error=type(error).__name__, detail=str(error)), indent=2), encoding="utf-8")
        raise
    finally:
        await cluster.close()
        if channel:
            channel.close()
        if gate:
            with (output / "gate-events.jsonl").open("w", encoding="utf-8") as stream:
                for event in gate.events():
                    stream.write(json.dumps(event) + "\n")
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
