import asyncio

import grpc
import pytest

from cranelock.agent import Agent
from cranelock.protocol_pb2_grpc import CraneAgentStub
from cranelock_gate.client import Client
from cranelock_gate.core import Gate
from cranelock_gate.server import RunningServer
from cranelock_gate.storage import initialize
from cranelock_sim.experiment import new_config
from conftest import Clock


# exercises a fixed five-member quorum with multiple unavailable peer endpoints
@pytest.mark.parametrize("initial", [("crane-3", "crane-4", "crane-5"), ("crane-4", "crane-5")])
def test_multiple_unavailable_members_and_identical_retry(tmp_path, initial):
    async def scenario():
        config, keys = new_config(5)
        clock = Clock()
        initialize(tmp_path / "gate.sqlite", config, clock())
        gate = Gate(tmp_path / "gate.sqlite", config, clock)
        server = RunningServer(gate, "127.0.0.1:0")
        channel = grpc.insecure_channel(f"127.0.0.1:{server.port}")
        client = Client(channel, config, keys)
        agents, ports = {}, {}
        from cranelock_gate.client import certificate
        state = client.state()
        bootstrap = certificate(config, keys, state, "crane-1", 0)
        assert client.call("crane-1", "InstallAuthority", certificate=bootstrap,
            expected_epoch=-1, expected_revision=0)["allowed"]
        state = client.state()

        # starts real peer services while retaining absent members in the quorum
        async def start(identity):
            agent = Agent(identity, {peer: "127.0.0.1:1" for peer in config.members if peer != identity},
                "crane-1", epoch=0, revision=1, state_file=tmp_path / f"{identity}.json",
                recovery_config=config, recovery_private_key=keys[identity], event_sink=lambda event: None,
                interval=.02, timeout=.12)
            agents[identity] = agent
            ports[identity] = await agent.start("127.0.0.1:0", auto_transfer=False)
            for task in agent.tasks:
                task.cancel()
            await asyncio.gather(*agent.tasks, return_exceptions=True)
            agent.tasks = []
            agent.suspected.update(set(config.members) - set(agents))
            agent.suspected.add("crane-1")

        # reconnects only the services that actually exist
        async def connect():
            for identity, agent in agents.items():
                for peer, port in ports.items():
                    if peer != identity:
                        await agent.channels[peer].close()
                        agent.channels[peer] = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
                        agent.stubs[peer] = CraneAgentStub(agent.channels[peer])

        try:
            for identity in initial:
                await start(identity)
            await connect()
            proposer = agents[initial[-1]]
            cert = await proposer.propose_recovery(state, "crane-3")
            if len(initial) == 2:
                assert cert is None and config.majority == 3
                assert client.state()["epoch"] == 0 and not proposer.state.has_token
                recorded = dict(proposer.vote_state)
                await start("crane-3")
                await connect()
                cert = await proposer.propose_recovery(state, "crane-3")
                assert proposer.vote_state == recorded
            assert cert is not None and len(cert["signatures"]) == 3
            assert {vote["signer_id"] for vote in cert["signatures"]} == set(agents)
            result = client.call(proposer.state.agent_id, "InstallAuthority", certificate=cert,
                expected_epoch=state["epoch"], expected_revision=state["revision"])
            assert result["allowed"] and result["state"]["holder"] == "crane-3"
            assert result["state"]["epoch"] == 1 and result["state"]["lane_state"] == "UNKNOWN"
            assert client.call("crane-3", "RequestGrant", epoch=1, revision=2)["reason"] == "LANE_UNKNOWN"
            assert client.call("crane-1", "RequestGrant", epoch=0, revision=1)["reason"] == "STALE_EPOCH"
        finally:
            await asyncio.gather(*(agent.close() for agent in agents.values()))
            channel.close()
            server.close()
    asyncio.run(scenario())
