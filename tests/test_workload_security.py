"""Real mTLS workload listener backed by the unchanged signed Gate."""
import asyncio
import json
import uuid

import grpc
import pytest

from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc, recovery
from cranelock.agent import Agent, Mode
from cranelock.gate_client import GateClient
from cranelock.workload import WorkloadService, load_allowlist, start_workload
from cranelock_gate.server import RunningServer
from cranelock_sim.workload_tls import IDENTITY, write
from cranelock_sim.workload_client import GrantAttempt
from cranelock_sim.experiment import LocalGateClient
from cranelock_sim.lane import Lane


def test_ambiguous_attempt_keeps_id_and_snapshot_until_gate_decision():
    class Stub:
        def __init__(self):
            self.requests = []
        def RequestGrant(self, request, timeout):
            self.requests.append((request.request_id, request.expected_epoch, request.expected_revision))
            if len(self.requests) == 1:
                raise grpc.RpcError("lost response")
            return pb.WorkloadGrantResponse(request_id=request.request_id, reason="GATE_UNAVAILABLE")
    stub = Stub()
    attempt = GrantAttempt(3, 7)
    with pytest.raises(grpc.RpcError):
        attempt.call(stub)
    result = attempt.call(stub)
    assert attempt.unresolved(result)
    assert stub.requests[0] == stub.requests[1]


def test_allowlist_binds_crane_and_lane(tmp_path):
    path = tmp_path/"allow.json"
    path.write_text(json.dumps(dict(crane_id="crane-1", lane_id="lane-1",
                                    allowed_workload_uri_sans=[IDENTITY])))
    assert IDENTITY in load_allowlist(path, "crane-1", "lane-1")
    for crane, lane in [("crane-2", "lane-1"), ("crane-1", "lane-2")]:
        with pytest.raises(ValueError):
            load_allowlist(path, crane, lane)
    path.write_text(json.dumps(dict(crane_id="crane-1", lane_id="lane-1", allowed_workload_uri_sans=[])))
    with pytest.raises(ValueError):
        load_allowlist(path, "crane-1", "lane-1")


def test_mtls_grant_replay_and_authorization(h, tmp_path):
    async def run():
        h.ready()
        tls = tmp_path/"tls"
        write(tls, extra_identities=["urn:cranelock:workload:other"])
        gate_server = RunningServer(h.gate, "127.0.0.1:0")
        gate = GateClient(grpc.insecure_channel(f"127.0.0.1:{gate_server.port}"), h.config, h.keys, "crane-1")
        events = []
        agent = Agent("crane-1", {p:"localhost:1" for p in h.config.members if p != "crane-1"},
                      "crane-1", epoch=0, gate=gate, event_sink=events.append, state_file=tmp_path/"agent.json")
        credentials = grpc.ssl_server_credentials([((tls/"server-key.pem").read_bytes(),
            (tls/"server.pem").read_bytes())], root_certificates=(tls/"ca.pem").read_bytes(), require_client_auth=True)
        server = service = None
        try:
            peer_port = await agent.start("127.0.0.1:0", auto_transfer=False)
            server, service, port = await start_workload(agent, "127.0.0.1:0", credentials, {IDENTITY})
            client_credentials = grpc.ssl_channel_credentials((tls/"ca.pem").read_bytes(),
                (tls/"client-key.pem").read_bytes(), (tls/"client.pem").read_bytes())
            async with grpc.aio.secure_channel(f"127.0.0.1:{port}", client_credentials) as channel:
                stub = rpc.CraneWorkloadStub(channel)
                req = pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()),
                    expected_epoch=h.state["epoch"], expected_revision=h.state["revision"])
                bad = await stub.RequestGrant(pb.WorkloadGrantRequest(request_id="not-a-uuid"))
                assert bad.reason == "INVALID_REQUEST_ID"
                missing = await stub.RequestGrant(pb.WorkloadGrantRequest(request_id=str(uuid.uuid4())))
                assert missing.reason == "MISSING_AUTHORITY"
                stale = await stub.RequestGrant(pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()),
                    expected_epoch=req.expected_epoch+1, expected_revision=req.expected_revision))
                assert stale.reason == "STALE_AUTHORITY"
                agent.state.has_token = False
                nonholder = await stub.RequestGrant(pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()),
                    expected_epoch=req.expected_epoch, expected_revision=req.expected_revision))
                assert nonholder.reason == "NOT_HOLDER"
                agent.state.has_token = True
                agent.pending = pb.TokenTransferMessage(transfer_id="pending")
                pending = await stub.RequestGrant(pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()),
                    expected_epoch=req.expected_epoch, expected_revision=req.expected_revision))
                assert pending.reason == "TRANSFER_PENDING"
                agent.pending = None
                result = await stub.RequestGrant(req)
                assert result.allowed and result.action_applied and not result.replayed
                assert json.loads(result.grant_json)["holder"] == "crane-1"
                replay = await stub.RequestGrant(req)
                assert replay.allowed and replay.replayed and not replay.action_applied
                conflict = await stub.RequestGrant(pb.WorkloadGrantRequest(request_id=req.request_id,
                    expected_epoch=req.expected_epoch+1, expected_revision=req.expected_revision))
                assert conflict.reason == "REQUEST_ID_CONFLICT"
                assert json.loads((tmp_path/"agent.json").read_text())["workload_requests"][req.request_id] == [
                    IDENTITY, req.expected_epoch, req.expected_revision]
                lane = Lane(LocalGateClient(h.gate, h.config, h.keys), h.clock, lambda *args, **kwargs: None)
                assert await asyncio.to_thread(lane.enter, "crane-1", req.request_id,
                                               json.loads(result.grant_json), h.state)
                assert lane.occupant == "crane-1"
                await asyncio.to_thread(lane.exit, "crane-1", req.request_id)
                service.allowed = {"urn:cranelock:workload:other"}
                with pytest.raises(grpc.aio.AioRpcError) as denied:
                    await stub.RequestGrant(req)
                assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
                with pytest.raises(grpc.aio.AioRpcError) as wrong_service:
                    await rpc.CraneAgentStub(channel).GetAgentState(pb.Empty())
                assert wrong_service.value.code() == grpc.StatusCode.UNIMPLEMENTED
            async with grpc.aio.insecure_channel(f"127.0.0.1:{peer_port}") as peer_channel:
                with pytest.raises(grpc.aio.AioRpcError) as wrong_service:
                    await rpc.CraneWorkloadStub(peer_channel).RequestGrant(req)
                assert wrong_service.value.code() == grpc.StatusCode.UNIMPLEMENTED
            assert any(e["event"] == "WORKLOAD_GRANT_RESULT" and e["workload_identity"] == IDENTITY
                       and e["request_id"] == req.request_id for e in events)
            anonymous = grpc.ssl_channel_credentials((tls/"ca.pem").read_bytes())
            async with grpc.aio.secure_channel(f"127.0.0.1:{port}", anonymous) as channel:
                with pytest.raises(grpc.aio.AioRpcError):
                    await rpc.CraneWorkloadStub(channel).RequestGrant(req, timeout=1)
            service.allowed = {IDENTITY}
            other = grpc.ssl_channel_credentials((tls/"ca.pem").read_bytes(),
                (tls/"extra-0-key.pem").read_bytes(), (tls/"extra-0.pem").read_bytes())
            async with grpc.aio.secure_channel(f"127.0.0.1:{port}", other) as channel:
                with pytest.raises(grpc.aio.AioRpcError) as denied:
                    await rpc.CraneWorkloadStub(channel).RequestGrant(req)
                assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
            other_tls = tmp_path/"other-ca"
            write(other_tls)
            untrusted = grpc.ssl_channel_credentials((tls/"ca.pem").read_bytes(),
                (other_tls/"client-key.pem").read_bytes(), (other_tls/"client.pem").read_bytes())
            async with grpc.aio.secure_channel(f"127.0.0.1:{port}", untrusted) as channel:
                with pytest.raises(grpc.aio.AioRpcError):
                    await rpc.CraneWorkloadStub(channel).RequestGrant(req, timeout=1)
        finally:
            if server:
                await server.stop(0)
                await asyncio.gather(*tuple(service.tasks), return_exceptions=True)
            await agent.close()
            gate_server.close()
    asyncio.run(run())


def test_cancelled_rpc_keeps_authority_lock_until_gate_finishes(h, tmp_path):
    async def run():
        h.ready()
        tls = tmp_path/"tls"
        write(tls)
        gate_server = RunningServer(h.gate, "127.0.0.1:0")
        gate = GateClient(grpc.insecure_channel(f"127.0.0.1:{gate_server.port}"), h.config, h.keys, "crane-1")
        agent = Agent("crane-1", {p:"localhost:1" for p in h.config.members if p != "crane-1"},
                      "crane-1", epoch=0, gate=gate, event_sink=lambda e: None, state_file=tmp_path/"agent.json")
        credentials = grpc.ssl_server_credentials([((tls/"server-key.pem").read_bytes(),
            (tls/"server.pem").read_bytes())], root_certificates=(tls/"ca.pem").read_bytes(), require_client_auth=True)
        started, release = asyncio.Event(), asyncio.Event()
        original = gate.request_grant
        async def delayed(*args):
            started.set()
            await release.wait()
            return await original(*args)
        gate.request_grant = delayed
        server = service = None
        try:
            await agent.start("127.0.0.1:0", auto_transfer=False)
            server, service, port = await start_workload(agent, "127.0.0.1:0", credentials, {IDENTITY})
            creds = grpc.ssl_channel_credentials((tls/"ca.pem").read_bytes(),
                (tls/"client-key.pem").read_bytes(), (tls/"client.pem").read_bytes())
            async with grpc.aio.secure_channel(f"127.0.0.1:{port}", creds) as channel:
                req = pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()), expected_epoch=h.state["epoch"],
                                              expected_revision=h.state["revision"])
                call = rpc.CraneWorkloadStub(channel).RequestGrant(req, timeout=.3)
                await asyncio.wait_for(started.wait(), 2)
                with pytest.raises(grpc.aio.AioRpcError) as error:
                    await call
                assert error.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
                assert agent.authority_lock.locked() and service.tasks
                transition = asyncio.create_task(agent.reconcile_from_gate())
                assert not await agent.reconcile_controller_return()
                agent.state.request_queue.append("crane-2")
                transfer = asyncio.create_task(agent.transfer_next())
                await asyncio.sleep(.05)
                assert not transition.done() and not transfer.done()
                release.set()
                await asyncio.wait_for(asyncio.gather(*tuple(service.tasks)), 3)
                assert await asyncio.wait_for(transition, 3)
                assert not await asyncio.wait_for(transfer, 3)
                replay = await rpc.CraneWorkloadStub(channel).RequestGrant(req)
                assert replay.replayed and not replay.action_applied
        finally:
            release.set()
            if server:
                await server.stop(0)
                await asyncio.gather(*tuple(service.tasks), return_exceptions=True)
            await agent.close()
            gate_server.close()
    asyncio.run(run())


def test_recovery_vote_gate_failure_suspends_inflight_grant(h, tmp_path):
    async def run():
        h.ready()
        gate_server = RunningServer(h.gate, "127.0.0.1:0")
        gate = GateClient(grpc.insecure_channel(f"127.0.0.1:{gate_server.port}"), h.config, h.keys, "crane-1")
        events = []
        agent = Agent("crane-1", {p:"localhost:1" for p in h.config.members if p != "crane-1"},
            "crane-1", epoch=0, gate=gate, event_sink=events.append, state_file=tmp_path/"agent.json",
            recovery_config=h.config, recovery_private_key=h.keys["crane-1"])
        started, release = asyncio.Event(), asyncio.Event()
        original_grant, original_state = gate.request_grant, gate.state
        async def delayed(*args):
            started.set()
            await release.wait()
            return await original_grant(*args)
        gate.request_grant = delayed
        try:
            assert await agent.reconcile_from_gate()
            state = h.state
            proposal = recovery.proposal(h.config, state, "crane-2")
            vote = pb.RecoveryProposal(proposer_id="crane-2", proposal_json=recovery.canonical(proposal),
                proposer_signature=recovery.sign(h.keys["crane-2"], proposal))
            req = pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()), expected_epoch=state["epoch"],
                                          expected_revision=state["revision"])
            service = WorkloadService(agent, {IDENTITY})
            task = asyncio.create_task(service._execute(req, IDENTITY))
            await asyncio.wait_for(started.wait(), 2)
            async def failed_state():
                raise RuntimeError("simulated concurrent GetState failure")
            gate.state = failed_state
            answer = await agent.VoteRecovery(vote, None)
            assert not answer.accepted and agent.state.mode == Mode.REJOINING
            release.set()
            result = await asyncio.wait_for(task, 3)
            assert not result.allowed and result.reason == "GATE_UNCERTAIN"
            assert not result.grant_json and result.action_applied
            assert h.state["grant"] is not None
            assert any(e["event"] == "WORKLOAD_GATE_OUTCOME_UNCERTAIN" and e["gate_allowed"] for e in events)
            gate.state = original_state
            assert await agent.reconcile_from_gate()
            retry = await service._execute(req, IDENTITY)
            assert retry.allowed and retry.replayed and not retry.action_applied
        finally:
            release.set()
            await agent.close()
            gate_server.close()
    asyncio.run(run())


def test_gate_commit_timeout_and_same_id_retry_after_restart(h, tmp_path):
    async def run():
        h.ready()
        server = RunningServer(h.gate, "127.0.0.1:0")
        state_path = tmp_path / "agent.json"
        def new_agent():
            gate = GateClient(grpc.insecure_channel(f"127.0.0.1:{server.port}"),
                              h.config, h.keys, "crane-1")
            agent = Agent("crane-1", {p:"localhost:1" for p in h.config.members if p != "crane-1"},
                          "crane-1", epoch=0, gate=gate, state_file=state_path, event_sink=lambda e: None)
            return agent, gate
        agent, gate = new_agent()
        try:
            assert await agent.reconcile_from_gate()
            state = h.state
            req = pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()), expected_epoch=state["epoch"],
                                          expected_revision=state["revision"])
            original = gate.request_grant
            async def commit_then_timeout(*args):
                assert (await original(*args))["allowed"]
                raise grpc.RpcError("response lost after commit")
            gate.request_grant = commit_then_timeout
            result = await WorkloadService(agent, {IDENTITY})._execute(req, IDENTITY)
            assert not result.allowed and result.reason == "GATE_UNCERTAIN"
            assert h.state["grant"] is not None
            await agent.close()
            agent, gate = new_agent()
            assert agent.state.mode == Mode.REJOINING
            assert await agent.reconcile_from_gate()
            replay = await WorkloadService(agent, {IDENTITY})._execute(req, IDENTITY)
            assert replay.allowed and replay.replayed and not replay.action_applied
        finally:
            await agent.close()
            server.close()
    asyncio.run(run())


def test_gate_cached_denial_stays_denied_after_clearance(h, tmp_path):
    async def run():
        h.ready()
        assert h.observe("OCCUPIED")["allowed"]
        server = RunningServer(h.gate, "127.0.0.1:0")
        gate = GateClient(grpc.insecure_channel(f"127.0.0.1:{server.port}"), h.config, h.keys, "crane-1")
        agent = Agent("crane-1", {p:"localhost:1" for p in h.config.members if p != "crane-1"},
                      "crane-1", epoch=0, gate=gate, state_file=tmp_path/"agent.json", event_sink=lambda e: None)
        try:
            assert await agent.reconcile_from_gate()
            state = h.state
            req = pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()), expected_epoch=state["epoch"],
                                          expected_revision=state["revision"])
            service = WorkloadService(agent, {IDENTITY})
            denied = await service._execute(req, IDENTITY)
            assert not denied.allowed and denied.reason == "LANE_BUSY" and not denied.replayed
            assert h.observe("CLEAR")["allowed"]
            replay = await service._execute(req, IDENTITY)
            assert not replay.allowed and replay.reason == "LANE_BUSY" and replay.replayed
        finally:
            await agent.close()
            server.close()
    asyncio.run(run())


def test_real_mtls_cross_crane_allowlists(h, tmp_path):
    async def run():
        h.ready()
        tls = tmp_path/"tls"
        other_identity = "urn:cranelock:workload:other"
        write(tls, extra_identities=[other_identity])
        gate_server = RunningServer(h.gate, "127.0.0.1:0")
        server_credentials = grpc.ssl_server_credentials([((tls/"server-key.pem").read_bytes(),
            (tls/"server.pem").read_bytes())], root_certificates=(tls/"ca.pem").read_bytes(), require_client_auth=True)
        agents, servers = [], []
        try:
            ports = {}
            for identity, allowed in (("crane-1", IDENTITY), ("crane-2", other_identity)):
                gate = GateClient(grpc.insecure_channel(f"127.0.0.1:{gate_server.port}"), h.config, h.keys, identity)
                agent = Agent(identity, {p:"localhost:1" for p in h.config.members if p != identity},
                    "crane-1", epoch=0, gate=gate, state_file=tmp_path/f"{identity}.json", event_sink=lambda e: None)
                agents.append(agent)
                await agent.start("127.0.0.1:0", auto_transfer=False)
                server, service, ports[identity] = await start_workload(agent, "127.0.0.1:0", server_credentials, {allowed})
                servers.append((server, service))
            def credentials(role):
                return grpc.ssl_channel_credentials((tls/"ca.pem").read_bytes(),
                    (tls/f"{role}-key.pem").read_bytes(), (tls/f"{role}.pem").read_bytes())
            req = pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()), expected_epoch=h.state["epoch"],
                                          expected_revision=h.state["revision"])
            async with grpc.aio.secure_channel(f"127.0.0.1:{ports['crane-2']}", credentials("client")) as channel:
                with pytest.raises(grpc.aio.AioRpcError) as denied:
                    await rpc.CraneWorkloadStub(channel).RequestGrant(req)
                assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
            async with grpc.aio.secure_channel(f"127.0.0.1:{ports['crane-2']}", credentials("extra-0")) as channel:
                assert (await rpc.CraneWorkloadStub(channel).RequestGrant(req)).reason == "NOT_HOLDER"
        finally:
            for server, service in servers:
                await server.stop(0)
                await asyncio.gather(*tuple(service.tasks), return_exceptions=True)
            await asyncio.gather(*(agent.close() for agent in agents))
            gate_server.close()
    asyncio.run(run())
