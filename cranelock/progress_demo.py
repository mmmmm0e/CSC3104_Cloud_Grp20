# runs the members 1 to 3 progress demo
import argparse
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import time
import uuid

import grpc

from cranelock.agent import Agent, Mode
from cranelock import protocol_pb2_grpc as rpc
from cranelock_gate.client import Client, certificate, may_enter
from cranelock_gate.core import Gate
from cranelock_gate.demo import create_demo_config
from cranelock_gate.server import RunningServer
from cranelock_gate.storage import initialize


# writes readable steps and structured evidence
class Evidence:
    def __init__(self, directory):
        self.directory = directory
        self.steps = []
        self.heartbeats = set()
        self.console = (directory / "demo.log").open("x", encoding="utf-8")
        self.agents = (directory / "agents.jsonl").open("x", encoding="utf-8")

    # records actual agent events
    def event(self, value):
        if value["event"] == "HEARTBEAT_RECEIVED":
            self.heartbeats.add((value["agent_id"], value["peer_id"]))
        self.agents.write(json.dumps(value) + "\n")
        self.agents.flush()

    # checks and records a demo result
    def check(self, label, passed, **details):
        record = dict(timestamp=datetime.now(timezone.utc).isoformat(),
                      step=label, passed=bool(passed), **details)
        self.steps.append(record)
        line = (f"{record['timestamp']} [{'PASS' if passed else 'FAIL'}] "
                f"{label} {json.dumps(details)}")
        print(line, flush=True)
        self.console.write(line + "\n")
        self.console.flush()
        if not passed:
            raise RuntimeError(f"demo check failed: {label}")

    # saves the final summary
    def close(self, error):
        summary = dict(passed=error is None, error=error, steps=self.steps,
                       scope="local demo with real gRPC; runner controls orchestration")
        (self.directory / "summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8")
        self.console.close()
        self.agents.close()


# waits for real heartbeat failure detection
async def wait_for(predicate, timeout=8):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("timed out waiting for agent state")
        await asyncio.sleep(0.05)


# runs token handoff, partition recovery and gate fencing
async def scenario(directory, evidence):
    config, keys = create_demo_config(directory / "identities")
    config = replace(config, observation_timeout_seconds=30)
    database = directory / "gate.sqlite"
    initialize(database, config, time.time())
    gate = Gate(database, config)
    server = None
    channel = None
    agents = {}
    addresses = {member: "127.0.0.1:1" for member in config.members}

    # starts one agent with its own durable vote state
    async def start_agent(identity):
        agent = Agent(identity,
            {peer: address for peer, address in addresses.items() if peer != identity},
            "crane-1", epoch=0, interval=0.1, timeout=0.8,
            state_file=directory / f"{identity}.json", event_sink=evidence.event,
            recovery_config=config, recovery_private_key=keys[identity])
        agents[identity] = agent
        port = await agent.start("127.0.0.1:0", auto_transfer=False)
        for task in agent.tasks:
            task.cancel()
        await asyncio.gather(*agent.tasks, return_exceptions=True)
        agent.tasks = []
        addresses[identity] = f"127.0.0.1:{port}"
        return agent

    # reconnects active peers after startup or restart
    async def connect_agents():
        tasks = [task for agent in agents.values() for task in agent.tasks]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for identity, agent in agents.items():
            for peer in agent.peers:
                await agent.channels[peer].close()
                agent.channels[peer] = grpc.aio.insecure_channel(addresses[peer])
                agent.stubs[peer] = rpc.CraneAgentStub(agent.channels[peer])
            agent.tasks = [asyncio.create_task(agent.heartbeat_loop()),
                           asyncio.create_task(agent.monitor_loop())]

    try:
        server = RunningServer(gate, "127.0.0.1:0")
        channel = grpc.insecure_channel(f"127.0.0.1:{server.port}")
        client = Client(channel, config, keys)

        # keeps synchronous gate calls off the agent event loop
        async def call(actor, operation, **fields):
            return await asyncio.to_thread(client.call, actor, operation, **fields)

        # reads the authoritative gate state
        async def state():
            return await asyncio.to_thread(client.state)

        # requests a grant for one authority snapshot
        async def grant(actor, snapshot):
            return await call(actor, "RequestGrant", epoch=snapshot["epoch"],
                              revision=snapshot["revision"])

        # sends explicit simulated clearance
        async def clear():
            current = await state()
            return await call("operator", "ClearLane", session_id=current["session_id"],
                observation_revision=current["observation_revision"],
                sequence=current["observation_sequence"] + 1,
                observed_at_ms=int(time.time() * 1000),
                reason="demo runner confirms simulated lane is empty")

        current = await state()
        bootstrap = certificate(config, keys, current, "crane-1", 0,
                                mode="CENTRAL", signers=[config.controller_id])
        result = await call("controller", "InstallAuthority", certificate=bootstrap,
            expected_epoch=current["epoch"], expected_revision=current["revision"])
        evidence.check("gate bootstrap", result["action_applied"], epoch=0)
        for identity in config.members:
            await start_agent(identity)
        await connect_agents()
        await wait_for(lambda: len(evidence.heartbeats) == 6)

        result = await clear()
        evidence.check("simulated lane clearance", result["action_applied"])
        reply = await agents["crane-2"].request_token()
        evidence.check("member 1 queues token request", reply.accepted,
                       queue=list(agents["crane-1"].state.request_queue))
        transferred = await agents["crane-1"].transfer_next()
        evidence.check("member 1 transfers token", transferred and
                       agents["crane-2"].state.has_token and
                       not agents["crane-1"].state.has_token)
        current = await state()
        result = await call("crane-1", "TransferHolder", epoch=current["epoch"],
            revision=current["revision"], destination="crane-2")
        evidence.check("gate records normal handoff", result["action_applied"])
        current = await state()
        for agent in agents.values():
            await agent.reconcile(current["epoch"], current["holder"], Mode.CENTRAL)
        result = await grant("crane-2", current)
        evidence.check("member 3 grants current holder", result["action_applied"])
        reserved = result["state"]
        result = await call("simulator", "ValidateEntry", epoch=reserved["epoch"],
            revision=reserved["revision"], holder="crane-2",
            grant_id=result["grant"]["id"], session_id=reserved["session_id"])
        evidence.check("simulated entry accepted", may_enter(result),
                       lane_state=result["state"]["lane_state"])
        old = await state()

        await agents.pop("crane-2").close()
        await agents.pop("crane-3").close()
        await wait_for(lambda: "crane-2" in agents["crane-1"].suspected)
        evidence.check("member 1 detects missing holder", True,
                       suspected=sorted(agents["crane-1"].suspected))
        cert = await agents["crane-1"].propose_recovery(old, "crane-1")
        evidence.check("member 2 minority cannot recover", cert is None,
                       reachable_members=1, required=config.majority)

        returning = await start_agent("crane-3")
        evidence.check("restarted agent blocks ownership", not returning.state.has_token
                       and returning.state.mode == Mode.REJOINING)
        current = await state()
        await returning.reconcile(current["epoch"], current["holder"], Mode.CENTRAL)
        await connect_agents()
        await wait_for(lambda: "crane-2" in returning.suspected)
        cert = await agents["crane-1"].propose_recovery(current, "crane-1")
        evidence.check("member 2 majority signs recovery", cert is not None,
                       votes=len(cert["signatures"]) if cert else 0,
                       required=config.majority)
        result = await call("crane-1", "InstallAuthority", certificate=cert,
            expected_epoch=current["epoch"], expected_revision=current["revision"])
        evidence.check("member 3 installs next epoch", result["action_applied"]
                       and result["state"]["epoch"] == old["epoch"] + 1,
                       epoch=result["state"]["epoch"], holder=result["state"]["holder"])
        current = await state()
        for agent in agents.values():
            await agent.reconcile(current["epoch"], current["holder"], Mode.LOCAL)
        evidence.check("active agents reconcile accepted authority",
                       sum(agent.state.has_token for agent in agents.values()) == 1)
        result = await grant("crane-2", old)
        evidence.check("old holder fenced", result["reason"] == "STALE_EPOCH",
                       reason=result["reason"])
        result = await grant("crane-1", current)
        evidence.check("recovery preserves occupancy block",
                       result["reason"] == "LANE_BUSY", reason=result["reason"])
        result = await clear()
        evidence.check("simulated operator confirms clearance", result["action_applied"])
        result = await grant("crane-1", await state())
        evidence.check("replacement holder receives fresh grant", result["action_applied"],
                       epoch=result["state"]["epoch"])
    finally:
        await asyncio.gather(*(agent.close() for agent in agents.values()))
        if channel:
            channel.close()
        events = gate.events()
        (directory / "gate.jsonl").write_text(
            "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
        if server:
            await asyncio.to_thread(server.close)
        else:
            gate.close()


# creates a fresh run directory and starts the demo
def main():
    parser = argparse.ArgumentParser(description="Members 1 to 3 progress demo")
    parser.add_argument("--output", type=Path, help="new output directory; must not exist")
    args = parser.parse_args()
    directory = args.output or Path("run") / (
        "progress-" + datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
    directory.mkdir(parents=True, exist_ok=False)
    print(f"Evidence directory: {directory.resolve()}", flush=True)
    evidence = Evidence(directory)
    error = None
    try:
        asyncio.run(scenario(directory, evidence))
    except BaseException as failure:
        error = f"{type(failure).__name__}: {failure}"
        raise
    finally:
        evidence.close(error)


if __name__ == "__main__":
    main()
