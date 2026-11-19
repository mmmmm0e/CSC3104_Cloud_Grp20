import asyncio
import json

import grpc
import pytest

from cranelock.agent import Agent
from cranelock import recovery, protocol_pb2 as pb
from cranelock.gate_client import GateClient
from cranelock_gate.crypto import canonical
from cranelock_gate.server import RunningServer


# creates an isolated durable voter without background recovery workers
def voter(h, tmp_path, identity="crane-2", gate=None):
    return Agent(identity, {peer: "127.0.0.1:1" for peer in h.config.members if peer != identity},
        "crane-1", epoch=0, revision=h.state["revision"], gate=gate,
        state_file=tmp_path / f"{identity}.json", recovery_config=h.config,
        recovery_private_key=h.keys[identity], event_sink=lambda event: None, interval=.02, timeout=.1)


# signs a real recovery rpc envelope
def request(h, payload):
    return pb.RecoveryProposal(proposer_id="crane-3", proposal_json=canonical(payload),
        proposer_signature=recovery.sign(h.keys["crane-3"], payload))


# a stale gate read must not consume a previously unused vote
def test_stale_snapshot_is_rejected_before_persisting_vote(h, tmp_path):
    async def scenario():
        assert h.install()["allowed"]
        old = h.state
        assert h.observe()["allowed"]
        assert h.call("crane-1", "TransferHolder", epoch=0, revision=old["revision"], destination="crane-3")["allowed"]
        server = RunningServer(h.gate, "127.0.0.1:0")
        agent = voter(h, tmp_path, gate=GateClient(grpc.insecure_channel(f"127.0.0.1:{server.port}"), h.config, h.keys, "crane-2"))
        try:
            assert await agent.reconcile_from_gate()
            agent.suspected.add("crane-3")
            assert await agent.propose_recovery(old) is None
            reply = await agent.VoteRecovery(request(h, recovery.proposal(h.config, old, "crane-2")), None)
            assert not reply.accepted and reply.reason == "STALE_GATE_SNAPSHOT"
            assert not agent.vote_state and not agent.vote_proposals
        finally:
            await agent.close()
            server.close()
    asyncio.run(scenario())


# revision changes do not unlock old votes even after restart
def test_revision_retry_remains_locked_across_restart(h, tmp_path):
    async def scenario():
        assert h.install()["allowed"]
        payload = recovery.proposal(h.config, h.state, "crane-2")
        agent = voter(h, tmp_path)
        agent.suspected.add("crane-1")
        raw = canonical(payload)
        agent._vote_recovery(raw, "crane-3", recovery.sign(h.keys["crane-3"], payload))
        original = dict(agent.vote_state)
        await agent.close()
        assert h.observe()["allowed"]
        assert h.call("crane-1", "TransferHolder", epoch=0, revision=1, destination="crane-3")["allowed"]
        agent = voter(h, tmp_path)
        try:
            await agent.reconcile(0, "crane-3", revision=2)
            agent.suspected.add("crane-3")
            assert await agent.propose_recovery(h.state, "crane-2") is None
            assert agent.vote_state == original and agent.vote_proposals["1"] == payload
            assert agent.recovery_blocked[-1] == "STALE_VOTE_REVISION"
            with pytest.raises(recovery.RecoveryError, match="CONFLICTING_VOTE"):
                newer = recovery.proposal(h.config, h.state, "crane-2")
                agent._vote_recovery(canonical(newer), "crane-3", recovery.sign(h.keys["crane-3"], newer))
            assert h.state["epoch"] == 0
        finally:
            await agent.close()
    asyncio.run(scenario())


# identical retries can finish after temporary peer loss without rewriting votes
def test_identical_vote_retry_and_legacy_state(h, tmp_path):
    async def scenario():
        assert h.install()["allowed"]
        agent = voter(h, tmp_path)
        agent.suspected.add("crane-1")
        payload = recovery.proposal(h.config, h.state, "crane-2")
        raw, signature = canonical(payload), recovery.sign(h.keys["crane-3"], payload)
        _, first = agent._vote_recovery(raw, "crane-3", signature)
        await agent.close()
        # legacy files only stored digests and still forbid equivocation
        path = tmp_path / "crane-2.json"
        saved = json.loads(path.read_text())
        saved.pop("vote_proposals")
        path.write_text(json.dumps(saved))
        agent = voter(h, tmp_path)
        try:
            await agent.reconcile(0, "crane-1", revision=1)
            agent.suspected.add("crane-1")
            _, retried = agent._vote_recovery(raw, "crane-3", signature)
            assert first == retried and agent.vote_proposals["1"] == payload
            changed = dict(payload, holder="crane-3")
            with pytest.raises(recovery.RecoveryError, match="CONFLICTING_VOTE"):
                agent._vote_recovery(canonical(changed), "crane-3", recovery.sign(h.keys["crane-3"], changed))
        finally:
            await agent.close()
    asyncio.run(scenario())


# an install retry is historical acknowledgement rather than another epoch change
def test_identical_install_reuses_request_id_after_lost_reply(h):
    async def scenario():
        assert h.install()["allowed"]
        state = h.state
        payload = recovery.proposal(h.config, state, "crane-2")
        certificate = recovery.certificate(payload, {member: recovery.sign(h.keys[member], payload)
            for member in ("crane-2", "crane-3")})
        server = RunningServer(h.gate, "127.0.0.1:0")
        client = GateClient(grpc.insecure_channel(f"127.0.0.1:{server.port}"), h.config, h.keys, "crane-2")
        try:
            first = await client.install_recovery(certificate)
            second = await client.install_recovery(certificate)
            assert first["allowed"] and first["action_applied"]
            assert second["allowed"] and second["replayed"] and not second["action_applied"]
            assert first["request_id"] == second["request_id"]
            assert h.state["epoch"] == 1 and h.state["revision"] == 2
        finally:
            await client.close()
            server.close()
    asyncio.run(scenario())


# corrupt proposal metadata cannot silently replace a durable vote
def test_saved_proposal_must_match_vote_digest(h):
    payload = recovery.proposal(h.config, dict(epoch=0, revision=1), "crane-2")
    votes, proposals = {}, {}
    recovery.remember_proposal(votes, proposals, payload)
    proposals["1"]["expected_revision"] = 2
    with pytest.raises(ValueError, match="does not match"):
        recovery.validate_saved(h.config, votes, proposals)


# invalid legacy records fail closed rather than appearing unvoted
@pytest.mark.parametrize("votes", [{"1": None}, {"01": "a" * 64}, {"1": "x" * 64}])
def test_invalid_legacy_vote_state_is_rejected(h, votes):
    with pytest.raises(ValueError, match="Invalid durable recovery vote"):
        recovery.validate_saved(h.config, votes, {})


# a revision race during collection abandons that attempt; once the live Gate has
# moved past the old snapshot, a vote for the newer revision replaces it
def test_gate_changes_after_own_vote_before_certificate(h, tmp_path):
    async def scenario():
        assert h.install()["allowed"]
        state = h.state
        server = RunningServer(h.gate, "127.0.0.1:0")
        agent = voter(h, tmp_path, gate=GateClient(grpc.insecure_channel(f"127.0.0.1:{server.port}"), h.config, h.keys, "crane-2"))

        # injects a normal gate handoff after the proposer's vote is durable
        class RacingPeer:
            async def VoteRecovery(self, message, timeout):
                assert agent.vote_state and agent.vote_proposals
                assert h.observe()["allowed"]
                assert h.call("crane-1", "TransferHolder", epoch=0, revision=1, destination="crane-3")["allowed"]
                return pb.RecoveryVote(accepted=True, voter_id="crane-3", signature=recovery.sign(
                    h.keys["crane-3"], recovery.parse_proposal(message.proposal_json, h.config)))

        try:
            assert await agent.reconcile_from_gate()
            agent.suspected.add("crane-1")
            agent.stubs["crane-3"] = RacingPeer()
            assert await agent.propose_recovery(state, "crane-2") is None
            digest = dict(agent.vote_state)
            stale_payload = agent.vote_proposals["1"]
            assert stale_payload["expected_revision"] == 1
            assert await agent.reconcile_from_gate()
            agent.suspected.add("crane-3")

            class HonestPeer:
                async def VoteRecovery(self, message, timeout):
                    return pb.RecoveryVote(accepted=True, voter_id="crane-3", signature=recovery.sign(
                        h.keys["crane-3"], recovery.parse_proposal(message.proposal_json, h.config)))

            agent.stubs["crane-3"] = HonestPeer()
            certificate = await agent.propose_recovery(h.state, "crane-2")
            assert certificate is not None
            assert certificate["payload"]["expected_revision"] == h.state["revision"] == 2
            assert agent.vote_state != digest and agent.vote_proposals["1"] == certificate["payload"]
            # the abandoned certificate can never install: the Gate moved past its snapshot
            stale = recovery.certificate(stale_payload, {m: recovery.sign(h.keys[m], stale_payload)
                                                        for m in ("crane-2", "crane-3")})
            assert h.call("crane-2", "InstallAuthority", certificate=stale, expected_epoch=0,
                          expected_revision=1)["reason"] == "STALE_REVISION"
            assert h.call("crane-2", "InstallAuthority", certificate=certificate, expected_epoch=0,
                          expected_revision=2)["allowed"]
            assert (h.state["epoch"], h.state["holder"]) == (1, "crane-2")
            # a vote can never move back to an older revision
            assert recovery.retry_block(agent.vote_state, agent.vote_proposals, stale_payload,
                                        allow_supersede=True) == "STALE_VOTE_REVISION"
        finally:
            await agent.close()
            server.close()
    asyncio.run(scenario())


# a separately accepted next epoch permits a new vote without erasing history
def test_new_accepted_epoch_allows_next_recovery_vote(h, tmp_path):
    async def scenario():
        assert h.install()["allowed"]
        agent = voter(h, tmp_path)
        try:
            agent.suspected.add("crane-1")
            old = recovery.proposal(h.config, h.state, "crane-2")
            agent._vote_recovery(canonical(old), "crane-3", recovery.sign(h.keys["crane-3"], old))
            locked = agent.vote_state["1"]
            # the other two configured signers can independently advance authority
            assert h.install("crane-3", signers=["crane-1", "crane-3"])["allowed"]
            await agent.reconcile(1, "crane-3", revision=h.state["revision"])
            agent.suspected.add("crane-3")
            assert await agent.propose_recovery(h.state, "crane-2") is None
            assert set(agent.vote_state) == {"1", "2"} and agent.vote_state["1"] == locked
        finally:
            await agent.close()
    asyncio.run(scenario())
