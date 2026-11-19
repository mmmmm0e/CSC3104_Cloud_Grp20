"""Regression tests for the review fixes: successor rotation, vote supersession,
peer mutual TLS, init cleanup and controller provenance."""
import asyncio
from types import SimpleNamespace

import grpc
import pytest

from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc, recovery
from cranelock.agent import Agent, Mode
from cranelock.peer_tls import PeerTLS
from cranelock_gate import storage
from cranelock_gate.tls import generate_agents


# --- recovery successor rotation -------------------------------------------

def successor(members, failed):
    fake = SimpleNamespace(recovery_config=SimpleNamespace(members=dict.fromkeys(members)))
    return Agent.recovery_successor(fake, failed)


def test_successor_rotates_and_wraps():
    members = [f"crane-{i}" for i in range(1, 6)]
    assert successor(members, "crane-1") == "crane-2"
    assert successor(members, "crane-3") == "crane-4"
    assert successor(members, "crane-5") == "crane-1"
    assert successor(members, "unknown") is None


def test_two_failed_members_do_not_livelock_recovery():
    """With crane-1 and crane-2 down, recovery must reach a live member instead
    of alternating 1 -> 2 -> 1 forever (the previous lowest-other-ID policy)."""
    members = [f"crane-{i}" for i in range(1, 6)]
    down = {"crane-1", "crane-2"}
    holder, chain = "crane-1", []
    for _ in range(len(members)):
        holder = successor(members, holder)
        chain.append(holder)
        if holder not in down:
            break
    assert chain == ["crane-2", "crane-3"]


def test_every_member_is_reachable_whatever_fails():
    members = [f"crane-{i}" for i in range(1, 11)]
    for start in members:
        seen, holder = set(), start
        for _ in members:
            holder = successor(members, holder)
            seen.add(holder)
        assert seen == set(members)


# --- vote supersession -------------------------------------------------------

def proposal(h, revision, holder="crane-2"):
    return recovery.proposal(h.config, dict(epoch=0, revision=revision), holder)


def test_vote_moves_only_to_strictly_newer_revision(h):
    votes, proposals = {}, {}
    old = proposal(h, 1)
    recovery.remember_proposal(votes, proposals, old)
    newer = proposal(h, 3)
    # without a verified live snapshot the original lock is kept
    assert recovery.retry_block(votes, proposals, newer) == "STALE_VOTE_REVISION"
    with pytest.raises(recovery.RecoveryError, match="CONFLICTING_VOTE"):
        recovery.remember_proposal(dict(votes), dict(proposals), newer)
    assert recovery.retry_block(votes, proposals, newer, allow_supersede=True) is None
    recovery.remember_proposal(votes, proposals, newer, allow_supersede=True)
    assert proposals["1"] == newer
    recovery.validate_saved(h.config, votes, proposals)
    # never back to an older revision, never another holder at the same revision
    for payload in (old, proposal(h, 2), proposal(h, 3, holder="crane-3")):
        assert recovery.retry_block(votes, proposals, payload, allow_supersede=True)
        with pytest.raises(recovery.RecoveryError, match="CONFLICTING_VOTE"):
            recovery.remember_proposal(votes, proposals, payload, allow_supersede=True)
    assert proposals["1"] == newer


def test_legacy_digest_only_vote_stays_locked(h):
    votes, proposals = {}, {}
    recovery.remember_proposal(votes, proposals, proposal(h, 1))
    proposals.clear()
    with pytest.raises(recovery.RecoveryError, match="CONFLICTING_VOTE"):
        recovery.remember_proposal(votes, proposals, proposal(h, 5), allow_supersede=True)


def test_standalone_voter_without_gate_keeps_old_lock(h, tmp_path):
    async def scenario():
        agent = Agent("crane-2", {"crane-1": "127.0.0.1:1", "crane-3": "127.0.0.1:2"}, "crane-1",
                      epoch=0, state_file=tmp_path / "a.json", recovery_config=h.config,
                      recovery_private_key=h.keys["crane-2"], event_sink=lambda e: None)
        try:
            agent.suspected.add("crane-1")
            old = proposal(h, 0)
            agent._vote_recovery(recovery.canonical(old), "crane-3", recovery.sign(h.keys["crane-3"], old))
            newer = proposal(h, 4)
            with pytest.raises(recovery.RecoveryError, match="CONFLICTING_VOTE"):
                agent._vote_recovery(recovery.canonical(newer), "crane-3",
                                     recovery.sign(h.keys["crane-3"], newer))
        finally:
            await agent.close()
    asyncio.run(scenario())


# --- peer mutual TLS ---------------------------------------------------------

MEMBERS = ["crane-1", "crane-2", "crane-3"]


@pytest.fixture(scope="module")
def peer_files():
    return generate_agents(MEMBERS, names=["localhost", "127.0.0.1"])


def tls_for(files, identity):
    return PeerTLS(files["ca.pem"], files[f"{identity}.pem"], files[f"{identity}-key.pem"])


def test_peer_certificate_must_identify_agent(peer_files, tmp_path):
    with pytest.raises(ValueError, match="does not identify"):
        Agent("crane-1", {"crane-2": "127.0.0.1:1", "crane-3": "127.0.0.1:2"}, "crane-1",
              peer_tls=tls_for(peer_files, "crane-2"), event_sink=lambda e: None)


def test_generate_agents_rejects_reserved_or_duplicate_ids():
    with pytest.raises(ValueError):
        generate_agents(["crane-1", "crane-1", "crane-2"])
    with pytest.raises(ValueError):
        generate_agents(["monitor", "crane-1", "crane-2"])


def test_peer_mtls_authenticates_and_binds_identity(peer_files):
    async def scenario():
        agent = Agent("crane-1", {"crane-2": "127.0.0.1:1", "crane-3": "127.0.0.1:2"}, "crane-1",
                      peer_tls=tls_for(peer_files, "crane-1"), event_sink=lambda e: None)
        port = await agent.start("127.0.0.1:0", auto_transfer=False)
        address = f"127.0.0.1:{port}"
        heartbeat = lambda sender: pb.HeartbeatMessage(agent_id=sender, epoch=1, token_holder="crane-1",
                                                       mode=pb.LOCAL)
        try:
            # a member presenting its own certificate is accepted
            async with tls_for(peer_files, "crane-2").channel("crane-1", address) as channel:
                reply = await rpc.CraneAgentStub(channel).Heartbeat(heartbeat("crane-2"), timeout=3)
                assert reply.accepted
                # ...but cannot claim to be another member
                with pytest.raises(grpc.aio.AioRpcError) as error:
                    await rpc.CraneAgentStub(channel).Heartbeat(heartbeat("crane-3"), timeout=3)
                assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
                with pytest.raises(grpc.aio.AioRpcError) as error:
                    await rpc.CraneAgentStub(channel).TokenRequest(
                        pb.TokenRequestMessage(agent_id="crane-3", epoch=1), timeout=3)
                assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
            # the read-only monitor certificate may read state but not act as a peer
            monitor = PeerTLS(peer_files["ca.pem"], peer_files["monitor.pem"], peer_files["monitor-key.pem"])
            async with monitor.channel("crane-1", address) as channel:
                state = await rpc.CraneAgentStub(channel).GetAgentState(pb.Empty(), timeout=3)
                assert state.agent_id == "crane-1"
                with pytest.raises(grpc.aio.AioRpcError) as error:
                    await rpc.CraneAgentStub(channel).Heartbeat(heartbeat("crane-2"), timeout=3)
                assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
            # a client pinned to the wrong peer identity refuses the server
            async with tls_for(peer_files, "crane-2").channel("crane-3", address) as channel:
                with pytest.raises(grpc.aio.AioRpcError) as error:
                    await rpc.CraneAgentStub(channel).Heartbeat(heartbeat("crane-2"), timeout=3)
                assert error.value.code() == grpc.StatusCode.UNAVAILABLE
            # plaintext clients cannot connect at all
            async with grpc.aio.insecure_channel(address) as channel:
                with pytest.raises(grpc.aio.AioRpcError) as error:
                    await rpc.CraneAgentStub(channel).Heartbeat(heartbeat("crane-2"), timeout=3)
                assert error.value.code() == grpc.StatusCode.UNAVAILABLE
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_peer_mtls_agents_exchange_token(peer_files):
    async def scenario():
        ports, agents = {}, {}
        for identity in MEMBERS:
            peers = {p: "127.0.0.1:1" for p in MEMBERS if p != identity}
            agents[identity] = Agent(identity, peers, "crane-1", interval=.05, timeout=.5,
                                     peer_tls=tls_for(peer_files, identity), event_sink=lambda e: None)
        try:
            for identity, agent in agents.items():
                ports[identity] = await agent.start("127.0.0.1:0")
            # point each agent's mTLS channels at the real ephemeral ports
            for identity, agent in agents.items():
                for peer in agent.peers:
                    await agent.channels[peer].close()
                    agent.channels[peer] = agent.peer_tls.channel(peer, f"127.0.0.1:{ports[peer]}")
                    agent.stubs[peer] = rpc.CraneAgentStub(agent.channels[peer])
            assert (await agents["crane-2"].request_token()).accepted
            for _ in range(100):
                if agents["crane-2"].state.has_token:
                    break
                await asyncio.sleep(.05)
            assert agents["crane-2"].state.has_token and not agents["crane-1"].state.has_token
            assert agents["crane-3"].state.mode == Mode.LOCAL and not agents["crane-3"].suspected
        finally:
            for agent in agents.values():
                await agent.close()
    asyncio.run(scenario())


# --- init cleanup and provenance --------------------------------------------

def test_interrupted_init_leaves_no_partial_database(h, tmp_path, monkeypatch):
    target = tmp_path / "fresh" / "gate.sqlite"

    def broken(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(storage, "connect", broken)
    with pytest.raises(OSError, match="disk full"):
        storage.initialize(target, h.config, 0.0)
    assert not target.exists()
    monkeypatch.undo()
    storage.initialize(target, h.config, 0.0)  # a retry now succeeds
    assert target.is_file()


def test_init_never_removes_an_existing_database(h):
    h.gate.close()  # release the Gate's process lease so init reaches touch()
    with pytest.raises(FileExistsError):
        storage.initialize(h.path, h.config, 0.0)
    from cranelock_gate.core import Gate
    h.gate = Gate(h.path, h.config, h.clock)
    assert h.gate.get_state()["allowed"]


def test_controller_provenance_tracks_controller_source():
    import hashlib
    import inspect
    from cranelock import controller
    from cranelock_sim.controller import CONTROLLER_SOURCE
    source = open(inspect.getsourcefile(controller), "rb").read().replace(b"\r\n", b"\n")
    assert CONTROLLER_SOURCE == "sha256:" + hashlib.sha256(source).hexdigest()[:12]
