import asyncio

import grpc

from cranelock.agent import Agent
from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from cranelock_sim.network import PeerNetwork


# delivers two real peer requests in reversed order without duplicating authority
def test_peer_delay_reorders_concurrent_requests(tmp_path):
    async def scenario():
        events = []
        network = PeerNetwork(["crane-1", "crane-2", "crane-3"], lambda event, **fields: events.append((event, fields)))
        agent = Agent("crane-1", {"crane-2": "127.0.0.1:1", "crane-3": "127.0.0.1:1"},
            "crane-1", state_file=tmp_path / "agent.json", event_sink=lambda event: None)
        port = await agent.start("127.0.0.1:0", auto_transfer=False,
                                 interceptors=(network.interceptor("crane-1"),))
        for task in agent.tasks:
            task.cancel()
        await asyncio.gather(*agent.tasks, return_exceptions=True)
        network.set_delay(0, 0.15, seed=15)
        try:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                await channel.channel_ready()
                stub = rpc.CraneAgentStub(channel)
                first = stub.TokenRequest(pb.TokenRequestMessage(agent_id="crane-3", epoch=1), timeout=2)
                # waits until the first request has consumed its configured delay
                while not any(event == "peer_rpc_delayed" for event, fields in events):
                    await asyncio.sleep(0.001)
                second = stub.TokenRequest(pb.TokenRequestMessage(agent_id="crane-2", epoch=1), timeout=2)
                replies = await asyncio.gather(first, second)
                assert all(reply.accepted for reply in replies)
                assert list(agent.state.request_queue) == ["crane-2", "crane-3"]
                assert agent.state.has_token and agent.state.token_holder == "crane-1"
        finally:
            await agent.close()
    asyncio.run(scenario())
