import asyncio
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace

import grpc
import pytest

from cranelock.agent import Agent, Mode
from cranelock.gate_client import GateClient
from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from cranelock.reconciliation import DOMAIN, ReconciliationClient, ReconciliationError
from cranelock_gate.client import Client
from cranelock_gate.crypto import canonical
from cranelock_gate.server import RunningServer
from cranelock_sim.agents import AgentCluster
from cranelock_sim.agents import token_owner
from cranelock_sim.controller import ControllerConnection, RPCStats, TeamControllerService
from cranelock_sim.failures import until
from cranelock_sim.lane import Lane
from cranelock.controller import CentralController


# counts only gate-confirmed authority at the newest observed revision
def test_checker_distinguishes_fenced_claims_from_conflicting_valid_tokens(tmp_path):
    cluster = AgentCluster(["a", "b"], tmp_path, lambda *args, **kwargs: None,
                           gate_factory=lambda identity: None)
    states = {"a": SimpleNamespace(epoch=0, revision=1, has_token=True),
              "b": SimpleNamespace(epoch=0, revision=2, has_token=True)}
    assert token_owner(cluster.current_authority_states(states)) == "b"
    states["a"].revision = 2
    with pytest.raises(RuntimeError, match="Multiple agents"):
        token_owner(cluster.current_authority_states(states))


# starts a real agent endpoint backed by the trusted gate
@asynccontextmanager
async def endpoint(h, tmp_path, *, server_credentials=None, client_credentials=None):
    server = RunningServer(h.gate, "127.0.0.1:0")
    channel = grpc.insecure_channel(f"127.0.0.1:{server.port}")
    agent = Agent("crane-1", {"crane-2": "127.0.0.1:1", "crane-3": "127.0.0.1:1"},
        "crane-1", epoch=0, state_file=tmp_path / "agent.json", event_sink=lambda event: None,
        gate=GateClient(channel, h.config, h.keys, "crane-1", timeout=0.1))
    peer = None
    try:
        port = await agent.start("127.0.0.1:0", auto_transfer=False, credentials=server_credentials)
        address = f"127.0.0.1:{port}"
        peer = (grpc.aio.secure_channel(address, client_credentials) if client_credentials
                else grpc.aio.insecure_channel(address))
        yield agent, rpc.CraneAgentStub(peer), address, server
    finally:
        if peer:
            await peer.close()
        await agent.close()
        await asyncio.to_thread(server.close)


# proves the controller and local recovery callers use signed network requests
@pytest.mark.parametrize("mode,signer", [("CENTRAL", "controller"), ("LOCAL", "crane-2")])
def test_remote_reconciliation_confirms_gate_authority(h, tmp_path, mode, signer):
    assert h.install(mode=mode)["allowed"]

    async def scenario():
        async with endpoint(h, tmp_path) as (agent, stub, address, server):
            state = h.state
            await ReconciliationClient(h.config, signer, h.keys[signer]).reconcile(
                {"crane-1": address}, state)
            assert (agent.state.current_epoch, agent.state.revision, agent.state.token_holder,
                    agent.state.mode) == (state["epoch"], state["revision"], state["holder"], Mode[mode])
    asyncio.run(scenario())


# supports authenticated transport as well as signed requests
def test_reconciliation_mutual_tls_rejects_anonymous_transport(h, tmp_path):
    from test_tls import credentials
    assert h.install(mode="CENTRAL")["allowed"]
    ca, server_key, server_cert, client_key, client_cert = credentials()
    secure_server = grpc.ssl_server_credentials([(server_key, server_cert)], ca, require_client_auth=True)
    secure_client = grpc.ssl_channel_credentials(ca, client_key, client_cert)

    async def scenario():
        async with endpoint(h, tmp_path, server_credentials=secure_server,
                            client_credentials=secure_client) as (agent, stub, address, server):
            await ReconciliationClient(h.config, "controller", h.keys["controller"],
                credentials=secure_client).reconcile({"crane-1": address}, h.state)
            with pytest.raises(ReconciliationError):
                await ReconciliationClient(h.config, "controller", h.keys["controller"],
                    credentials=grpc.ssl_channel_credentials(ca), timeout=0.2).reconcile(
                        {"crane-1": address}, h.state)
    asyncio.run(scenario())


# refuses forged or misdirected authority requests
@pytest.mark.parametrize("attack", ["signature", "domain", "target", "membership", "lane",
    "noncanonical", "extra", "boolean_epoch", "peer_central", "operator"])
def test_invalid_remote_requests_cannot_change_authority(h, tmp_path, attack):
    assert h.install(mode="CENTRAL")["allowed"]

    async def scenario():
        async with endpoint(h, tmp_path) as (agent, stub, address, server):
            request = ReconciliationClient(h.config, "controller", h.keys["controller"]).request("crane-1", h.state)
            payload = json.loads(request.payload_json)
            signer = "controller"
            if attack == "target":
                payload["target_agent_id"] = "crane-2"
            elif attack in {"membership", "lane"}:
                payload[attack + "_id"] = "other"
            elif attack == "extra":
                payload["grant_permission"] = True
            elif attack == "boolean_epoch":
                payload["epoch"] = True
            elif attack in {"peer_central", "operator"}:
                signer = "crane-2" if attack == "peer_central" else "operator"
                payload["actor_id"] = signer
            raw = canonical(payload)
            signature = h.keys[signer].sign((b"wrong-domain" if attack == "domain" else DOMAIN) + raw)
            if attack == "signature":
                signature = b"bad"
            if attack == "noncanonical":
                raw = json.dumps(payload).encode()
            reply = await stub.ReconcileAuthority(pb.AuthorityReconciliation(
                payload_json=raw, signature=signature), timeout=1)
            assert not reply.accepted
            assert agent.state.token_holder == "crane-1" and agent.state.revision == h.state["revision"]
    asyncio.run(scenario())


# accepts retries but refuses replay after authority changes
def test_reconciliation_replay_cannot_restore_old_holder(h, tmp_path):
    assert h.install(mode="CENTRAL")["allowed"]

    async def scenario():
        async with endpoint(h, tmp_path) as (agent, stub, address, server):
            client = ReconciliationClient(h.config, "controller", h.keys["controller"])
            old = client.request("crane-1", h.state)
            assert (await stub.ReconcileAuthority(old)).accepted
            assert (await stub.ReconcileAuthority(old)).accepted
            assert h.install("crane-2", epoch=0, mode="CENTRAL")["allowed"]
            await client.reconcile({"crane-1": address}, h.state)
            assert not (await stub.ReconcileAuthority(old)).accepted
            assert agent.state.token_holder == "crane-2" and not agent.state.has_token
    asyncio.run(scenario())


# missing gate access prevents reconciliation and new admissions
def test_gate_outage_suspends_remote_reconciliation(h, tmp_path):
    assert h.install()["allowed"]

    async def scenario():
        async with endpoint(h, tmp_path) as (agent, stub, address, server):
            client = ReconciliationClient(h.config, "controller", h.keys["controller"], timeout=0.5)
            state = h.state
            await asyncio.to_thread(server.close)
            with pytest.raises(ReconciliationError, match="GATE_AUTHORITY_UNCONFIRMED"):
                await client.reconcile({"crane-1": address}, state)
            assert not agent.state.has_token and agent.state.mode == Mode.REJOINING
            assert not (await agent.request_token()).accepted
    asyncio.run(scenario())


# transport loss is never reported as a successful handover
def test_unreachable_agent_fails_the_reconciliation(h):
    assert h.install()["allowed"]

    async def scenario():
        client = ReconciliationClient(h.config, "controller", h.keys["controller"], timeout=0.1)
        with pytest.raises(ReconciliationError):
            await client.reconcile({"crane-1": "127.0.0.1:1"}, h.state)
    asyncio.run(scenario())


# a total service outage cannot turn a cached grant into physical entry
def test_total_outage_blocks_cached_grant_and_reconciliation(h, tmp_path):
    h.ready()
    issued = h.grant()
    state = issued["state"]

    async def scenario():
        async with endpoint(h, tmp_path) as (agent, stub, address, server):
            gate_address = f"127.0.0.1:{server.port}"
            trace = []
            with grpc.insecure_channel(gate_address) as channel:
                lane = Lane(Client(channel, h.config, h.keys, timeout=0.1), h.clock,
                            lambda event, **fields: trace.append(event))
                await asyncio.to_thread(server.close)
                await agent.close()
                assert not await asyncio.to_thread(lane.enter, "crane-1", "cached",
                                                   issued["grant"], state)
                assert lane.occupant is None and trace == ["entry_uncertain"]
                with pytest.raises(ReconciliationError):
                    await ReconciliationClient(h.config, "controller", h.keys["controller"],
                        timeout=0.1).reconcile({"crane-1": address}, state)
    asyncio.run(scenario())


# stale occupancy evidence still blocks fresh grant requests
def test_remote_reconciliation_does_not_clear_unknown_lane(h, tmp_path):
    h.ready()
    h.clock.advance(h.config.observation_timeout_seconds + 1)

    async def scenario():
        async with endpoint(h, tmp_path) as (agent, stub, address, server):
            state = h.state
            assert state["lane_state"] == "UNKNOWN"
            await ReconciliationClient(h.config, "controller", h.keys["controller"]).reconcile(
                {"crane-1": address}, state)
            assert h.grant()["reason"] == "LANE_UNKNOWN"
    asyncio.run(scenario())


# blocks only controller links while the real service and peer links stay healthy
def test_controller_link_loss_can_recover_without_stopping_controller(h, tmp_path):
    assert h.install(mode="CENTRAL")["allowed"]

    async def scenario():
        class Link(grpc.aio.UnaryUnaryClientInterceptor):
            blocked = False

            async def intercept_unary_unary(self, continuation, details, request):
                if self.blocked:
                    raise grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, None, None, "controller link blocked")
                return await continuation(details, request)

        link = Link()
        gate_server = RunningServer(h.gate, "127.0.0.1:0")
        channel = grpc.insecure_channel(f"127.0.0.1:{gate_server.port}")
        controller = CentralController(h.config, h.keys["controller"], channel)
        service = TeamControllerService(controller)
        stats = RPCStats()
        connections = [ControllerConnection(service.port, stats, interceptors=(link,)) for _ in range(3)]
        observer = ControllerConnection(service.port, stats)
        cluster = AgentCluster(list(h.config.members), tmp_path, lambda *args, **kwargs: None,
            recovery_config=h.config, keys=h.keys, timeout=0.3, initial_mode=Mode.CENTRAL,
            controller_probes=dict(zip(h.config.members, (connection.probe for connection in connections))))
        try:
            await cluster.start()
            assert all(await asyncio.gather(*(connection.probe() for connection in connections)))
            link.blocked = True
            assert await observer.probe() and not service.stopped
            await until(lambda: all(agent.controller_suspected for agent in cluster.agents.values()),
                        3, "controller-link suspicion")
            cert = await cluster.agents["crane-2"].propose_recovery(h.state, "crane-1")
            assert cert is not None
            client = Client(channel, h.config, h.keys)
            state = h.state
            result = await asyncio.to_thread(client.call, "crane-2", "InstallAuthority",
                certificate=cert, expected_epoch=state["epoch"], expected_revision=state["revision"])
            assert result["allowed"] and result["state"]["mode"] == "LOCAL"
            assert result["state"]["holder"] == "crane-1" and await observer.probe()
        finally:
            await cluster.close()
            await asyncio.gather(*(connection.close() for connection in connections), observer.close())
            await asyncio.to_thread(service.close)
            channel.close()
            await asyncio.to_thread(gate_server.close)
    asyncio.run(scenario())
