"""Crash recovery for Gate handoffs; never infer ownership from a peer."""
import asyncio
import json
import os
import subprocess
import sys

import grpc
import pytest

from cranelock import protocol_pb2 as pb
from cranelock.agent import Agent, Mode
from cranelock.gate_client import GateClient
from cranelock_gate.client import command
from cranelock_gate.server import RunningServer


CHILD = r'''
import asyncio, os, sys
import grpc
from cranelock.agent import Agent
from cranelock import protocol_pb2 as pb
from cranelock.gate_client import GateClient

async def run():
    port, config, keys, path, phase = sys.argv[1:]
    gate = GateClient.connect('127.0.0.1:' + port, config, keys, 'crane-1', timeout=2)
    agent = Agent('crane-1', {'crane-2':'localhost:1','crane-3':'localhost:2'}, 'crane-1',
                  epoch=0, gate=gate, state_file=path, event_sink=lambda event: None)
    assert await agent.reconcile_from_gate()
    for peer in ('crane-2', 'crane-3'):
        reply = await agent.TokenRequest(pb.TokenRequestMessage(agent_id=peer, epoch=0, revision=1), None)
        assert reply.accepted
    original = gate.transfer
    async def crash(payload):
        if phase == 'after_commit':
            assert (await original(payload))['allowed']
        os._exit(42)
    gate.transfer = crash
    await agent.transfer_next()
    raise AssertionError('expected crash')
asyncio.run(run())
'''


class DirectReceiver:
    def __init__(self, agent, *, accept=True, lose_ack=False):
        self.agent, self.accept, self.lose_ack = agent, accept, lose_ack
        self.messages = []

    async def TokenTransfer(self, request, timeout):
        copy = pb.TokenTransferMessage()
        copy.CopyFrom(request)
        self.messages.append(copy)
        if not self.accept:
            return pb.Reply(accepted=False)
        result = await self.agent.TokenTransfer(request, None)
        return pb.Reply(accepted=False) if self.lose_ack else result


def client(h, server, identity):
    return GateClient(grpc.insecure_channel(f"127.0.0.1:{server.port}"), h.config, h.keys, identity, timeout=.5)


@pytest.mark.parametrize("phase", ["before_send", "after_commit"])
def test_real_sender_crash_replays_durable_command_and_delivers_queue(h, tmp_path, phase):
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    path = tmp_path / "sender.json"
    try:
        result = subprocess.run([sys.executable, "-B", "-c", CHILD, str(server.port),
                                 str(tmp_path / "config.json"), str(tmp_path / "private-keys.json"),
                                 str(path), phase], capture_output=True, text=True, timeout=20,
                                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        assert result.returncode == 42, (result.stdout, result.stderr)
        saved = json.loads(path.read_text())
        original = saved["gate_attempt"]["command"]
        assert saved["request_queue"] == ["crane-2", "crane-3"]
        assert saved["gate_attempt"]["queue"] == ["crane-3"]
        assert h.state["holder"] == ("crane-2" if phase == "after_commit" else "crane-1")
        before = path.read_bytes()
        with pytest.raises(ValueError, match="requires Gate configuration"):
            Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                  "crane-1", epoch=0, state_file=path, event_sink=lambda event: None)
        assert path.read_bytes() == before

        async def scenario():
            sender = Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                           "crane-1", epoch=0, gate=client(h, server, "crane-1"),
                           state_file=path, event_sink=lambda event: None)
            receiver = Agent("crane-2", {"crane-1": "localhost:1", "crane-3": "localhost:2"},
                             "crane-1", epoch=0, gate=client(h, server, "crane-2"),
                             state_file=tmp_path / "receiver.json", event_sink=lambda event: None)
            try:
                assert sender.gate_attempt["command"] == original
                assert await sender.reconcile_from_gate()
                assert not sender.state.has_token
                assert not (await sender.TokenRequest(pb.TokenRequestMessage(
                    agent_id="crane-3", epoch=sender.state.current_epoch,
                    revision=sender.state.revision), None)).accepted
                assert await receiver.reconcile_from_gate()
                if phase == "after_commit":
                    assert receiver.incoming_pending and not receiver.state.has_token
                    assert not (await receiver.TokenRequest(pb.TokenRequestMessage(
                        agent_id="crane-3", epoch=0, revision=2), None)).accepted
                stub = DirectReceiver(receiver)
                sender.stubs["crane-2"] = stub
                assert await sender.transfer_next()
                assert len(stub.messages) == 1 and stub.messages[0].transfer_id == original["request_id"]
                assert list(receiver.state.request_queue) == ["crane-3"]
                assert receiver.state.has_token and not sender.state.has_token
                assert json.loads(path.read_text())["gate_attempt"] is None
                assert json.loads(path.read_text())["pending_transfer"] is None
                allowed = [e for e in h.gate.events() if e["operation"] == "TransferHolder" and e["allowed"]]
                assert len(allowed) in (1, 2)  # A replay is audited, not applied again.
                assert {e["request_id"] for e in allowed} == {original["request_id"]}
                assert h.state["revision"] == 2 and h.state["holder"] == "crane-2"
            finally:
                await sender.close()
                await receiver.close()
        asyncio.run(scenario())
    finally:
        server.close()


def test_gate_agent_requires_durable_state():
    with pytest.raises(ValueError, match="durable state file"):
        Agent("a", {"b": "localhost:1"}, "a", gate=object())


def test_gate_transition_identity_survives_restart_and_replay(h):
    h.ready()
    payload = command(h.config, "crane-1", "TransferHolder", request_id="durable-transition",
                      epoch=0, revision=1, destination="crane-2")
    assert h.send(payload)["allowed"]
    expected = dict(kind="TRANSFER", epoch=0, revision=2, holder="crane-2",
                    actor_id="crane-1", request_id="durable-transition")
    assert h.state["authority_transition"] == expected
    h.restart()
    assert h.state["authority_transition"] == expected
    replay = h.send(payload)
    assert replay["allowed"] and replay["replayed"] and not replay["action_applied"]
    assert replay["state"]["authority_transition"] == expected
    installed = h.install("crane-2", epoch=0, mode="CENTRAL")
    assert installed["allowed"] and installed["state"]["authority_transition"]["kind"] == "INSTALL"
    assert installed["state"]["last_handoff"] == {k: expected[k] for k in (
        "epoch", "revision", "holder", "actor_id", "request_id")}


def test_fresh_receiver_uses_gate_transition_and_rejects_wrong_notification(h, tmp_path):
    h.ready()
    result = h.call("crane-1", "TransferHolder", epoch=0, revision=1, destination="crane-2")
    assert result["allowed"]
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        # Its configured holder already matches the Gate; the transition
        # marker still reveals that a handoff queue has not been delivered.
        receiver = Agent("crane-2", {"crane-1": "localhost:1", "crane-3": "localhost:2"},
                         "crane-2", epoch=0, gate=client(h, server, "crane-2"),
                         state_file=tmp_path / "fresh.json", event_sink=lambda event: None)
        try:
            assert await receiver.reconcile_from_gate()
            assert receiver.incoming_pending and not receiver.state.has_token
            bad = pb.TokenTransferMessage(sender_id="crane-1", receiver_id="crane-2", holder="crane-2",
                                          epoch=0, revision=2, transfer_id="different-id")
            assert not (await receiver.TokenTransfer(bad, None)).accepted
            assert receiver.incoming_pending and not receiver.state.has_token
            bad.transfer_id = result["request_id"]
            bad.request_queue.append("crane-3")
            assert (await receiver.TokenTransfer(bad, None)).accepted
            assert receiver.state.has_token and list(receiver.state.request_queue) == ["crane-3"]
        finally:
            await receiver.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


def test_same_holder_recovery_retains_acknowledged_queue(h, tmp_path):
    assert h.install("crane-1", mode="CENTRAL")["allowed"]
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        path = tmp_path / "holder.json"
        def make_agent():
            return Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                         "crane-1", epoch=0, gate=client(h, server, "crane-1"),
                         state_file=path, event_sink=lambda event: None)
        holder = make_agent()
        try:
            assert await holder.reconcile_from_gate()
            for peer in ("crane-2", "crane-3"):
                assert (await holder.TokenRequest(pb.TokenRequestMessage(
                    agent_id=peer, epoch=0, revision=1), None)).accepted
            assert h.install("crane-1", epoch=1, mode="LOCAL")["allowed"]
            assert await holder.reconcile_from_gate()
            assert holder.state.has_token and list(holder.state.request_queue) == ["crane-2", "crane-3"]
        finally:
            await holder.close()
        holder = make_agent()
        try:
            assert await holder.reconcile_from_gate()
            assert list(holder.state.request_queue) == ["crane-2", "crane-3"]
        finally:
            await holder.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


def test_restart_with_higher_epoch_hint_does_not_erase_saved_queue(h, tmp_path):
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        path = tmp_path / "holder.json"
        holder = Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                       "crane-1", epoch=0, gate=client(h, server, "crane-1"),
                       state_file=path, event_sink=lambda event: None)
        try:
            assert await holder.reconcile_from_gate()
            assert (await holder.TokenRequest(pb.TokenRequestMessage(
                agent_id="crane-2", epoch=0, revision=1), None)).accepted
        finally:
            await holder.close()
        assert h.install("crane-1", epoch=1, mode="LOCAL")["allowed"]
        holder = Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                       "crane-1", epoch=1, gate=client(h, server, "crane-1"),
                       state_file=path, event_sink=lambda event: None)
        try:
            assert holder.state.mode == Mode.REJOINING and not holder.state.has_token
            assert list(holder.state.request_queue) == ["crane-2"]
            assert await holder.reconcile_from_gate()
            assert holder.state.has_token and list(holder.state.request_queue) == ["crane-2"]
        finally:
            await holder.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


def test_received_handoff_survives_higher_epoch_hint_and_install(h, tmp_path):
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        path = tmp_path / "receiver.json"
        def make_receiver(epoch):
            return Agent("crane-2", {"crane-1": "localhost:1", "crane-3": "localhost:2"},
                         "crane-1", epoch=epoch, gate=client(h, server, "crane-2"),
                         state_file=path, event_sink=lambda event: None)
        receiver = make_receiver(0)
        try:
            assert await receiver.reconcile_from_gate()
            result = h.call("crane-1", "TransferHolder", epoch=0, revision=1, destination="crane-2")
            assert result["allowed"]
            message = pb.TokenTransferMessage(sender_id="crane-1", receiver_id="crane-2",
                holder="crane-2", epoch=0, revision=2, transfer_id=result["request_id"],
                request_queue=["crane-3"])
            assert (await receiver.TokenTransfer(message, None)).accepted
            assert (await receiver.TokenTransfer(message, None)).accepted
            assert list(receiver.state.request_queue) == ["crane-3"]
            saved_receipts = json.loads(path.read_text())["gate_received_transfers"]
            assert saved_receipts
        finally:
            await receiver.close()

        receiver = make_receiver(1)
        next_receiver = None
        try:
            assert not receiver.state.has_token
            assert json.loads(path.read_text())["gate_received_transfers"] == saved_receipts
            # A configured epoch is not proof that Gate authority advanced.
            assert await receiver.reconcile_from_gate()
            assert receiver.state.has_token and not receiver.incoming_pending
            assert (await receiver.TokenTransfer(message, None)).accepted
            assert list(receiver.state.request_queue) == ["crane-3"]
            assert h.install("crane-2", epoch=1, mode="LOCAL")["allowed"]
            # The receiver has not yet observed the installation: even then,
            # the old message is checked against current Gate authority. The
            # Gate still records it as the latest handoff to the still-current
            # holder, so a duplicate is acknowledged without changing the queue.
            assert (await receiver.TokenTransfer(message, None)).accepted
            assert await receiver.reconcile_from_gate()
            assert receiver.state.has_token and not receiver.incoming_pending
            assert (await receiver.TokenTransfer(message, None)).accepted
            assert list(receiver.state.request_queue) == ["crane-3"]
            next_receiver = Agent("crane-3", {"crane-1": "localhost:1", "crane-2": "localhost:2"},
                "crane-2", epoch=0, gate=client(h, server, "crane-3"),
                state_file=tmp_path / "next.json", event_sink=lambda event: None)
            assert await next_receiver.reconcile_from_gate()
            receiver.stubs["crane-3"] = DirectReceiver(next_receiver)
            assert await receiver.transfer_next()
            assert next_receiver.state.has_token and not receiver.state.has_token
            assert not (await receiver.TokenTransfer(message, None)).accepted
        finally:
            await receiver.close()
            if next_receiver:
                await next_receiver.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


@pytest.mark.parametrize("failure", ["STORAGE_UNAVAILABLE", "INVALID_SIGNATURE",
                                     "REQUEST_ID_CONFLICT", "WRONG_REQUEST_ID"])
def test_uncached_denial_keeps_inflight_command_across_restart(h, tmp_path, failure):
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        path = tmp_path / "sender.json"
        def make_sender():
            return Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                         "crane-1", epoch=0, gate=client(h, server, "crane-1"),
                         state_file=path, event_sink=lambda event: None)
        sender = make_sender()
        receiver = None
        delayed = None
        release = asyncio.Event()
        try:
            assert await sender.reconcile_from_gate()
            for peer in ("crane-2", "crane-3"):
                assert (await sender.TokenRequest(pb.TokenRequestMessage(
                    agent_id=peer, epoch=0, revision=1), None)).accepted
            reads = []
            real_state = sender.gate.state
            async def record_state():
                state = await real_state()
                reads.append((state["epoch"], state["revision"], state["holder"]))
                return state
            sender.gate.state = record_state
            async def delayed_send(payload):
                await release.wait()
                return await asyncio.to_thread(h.send, payload)
            async def uncached_failure(payload):
                nonlocal delayed
                delayed = asyncio.create_task(delayed_send(dict(payload)))
                if failure in ("STORAGE_UNAVAILABLE", "INVALID_SIGNATURE"):
                    return h.gate._unavailable(failure)
                return dict(allowed=False, reason=("REQUEST_ID_CONFLICT" if failure == "REQUEST_ID_CONFLICT"
                                                    else "STALE_REVISION"),
                            replayed=False, action_applied=False,
                            request_id=payload["request_id"] if failure == "REQUEST_ID_CONFLICT" else "other",
                            state=h.state, grant=None)
            sender.gate.transfer = uncached_failure
            assert not await sender.transfer_next()
            assert reads == [(0, 1, "crane-1"), (0, 1, "crane-1")]
            assert h.state["holder"] == "crane-1"
            original = sender.gate_attempt["command"]
            assert not sender.state.has_token
            assert json.loads(path.read_text())["gate_attempt"]["command"] == original
            await sender.close()
            sender = make_sender()
            assert sender.gate_attempt["command"] == original
            assert await sender.reconcile_from_gate() and not sender.state.has_token
            release.set()
            assert (await delayed)["allowed"]
            assert h.state["holder"] == "crane-2"
            receiver = Agent("crane-2", {"crane-1": "localhost:1", "crane-3": "localhost:2"},
                "crane-1", epoch=0, gate=client(h, server, "crane-2"),
                state_file=tmp_path / "receiver.json", event_sink=lambda event: None)
            assert await receiver.reconcile_from_gate()
            sender.stubs["crane-2"] = DirectReceiver(receiver)
            assert await sender.transfer_next()
            assert receiver.state.has_token and not sender.state.has_token
            assert list(receiver.state.request_queue) == ["crane-3"]
            assert h.state["authority_transition"]["request_id"] == original["request_id"]
        finally:
            release.set()
            if delayed and not delayed.done():
                await delayed
            await sender.close()
            if receiver:
                await receiver.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


def test_receipt_survives_controller_reassignment_away_and_back(h, tmp_path):
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        receiver = Agent("crane-2", {"crane-1": "localhost:1", "crane-3": "localhost:2"},
            "crane-1", epoch=0, gate=client(h, server, "crane-2"),
            state_file=tmp_path / "receiver.json", event_sink=lambda event: None)
        next_receiver = None
        try:
            assert await receiver.reconcile_from_gate()
            result = h.call("crane-1", "TransferHolder", epoch=0, revision=1, destination="crane-2")
            assert result["allowed"]
            message = pb.TokenTransferMessage(sender_id="crane-1", receiver_id="crane-2",
                holder="crane-2", epoch=0, revision=2, transfer_id=result["request_id"])
            assert (await receiver.TokenTransfer(message, None)).accepted
            assert h.install("crane-1", epoch=0, mode="CENTRAL")["allowed"]
            assert await receiver.reconcile_from_gate() and not receiver.state.has_token
            assert h.install("crane-2", epoch=0, mode="CENTRAL")["allowed"]
            assert await receiver.reconcile_from_gate()
            assert receiver.state.has_token and not receiver.incoming_pending
            # Still the Gate's latest handoff to the current holder: duplicate no-op.
            assert (await receiver.TokenTransfer(message, None)).accepted
            assert receiver.state.has_token and not receiver.state.request_queue
            assert h.install("crane-1", epoch=0, mode="CENTRAL")["allowed"]
            assert await receiver.reconcile_from_gate() and not receiver.state.has_token
            later = h.call("crane-1", "TransferHolder", epoch=0, revision=5, destination="crane-2")
            assert later["allowed"] and await receiver.reconcile_from_gate()
            assert receiver.incoming_pending and not receiver.state.has_token
            assert not (await receiver.TokenTransfer(message, None)).accepted
            new_message = pb.TokenTransferMessage(sender_id="crane-1", receiver_id="crane-2",
                holder="crane-2", epoch=0, revision=6, transfer_id=later["request_id"])
            assert (await receiver.TokenTransfer(new_message, None)).accepted
            assert (await receiver.TokenTransfer(new_message, None)).accepted
            assert (await receiver.TokenRequest(pb.TokenRequestMessage(
                agent_id="crane-3", epoch=0, revision=6), None)).accepted
            next_receiver = Agent("crane-3", {"crane-1": "localhost:1", "crane-2": "localhost:2"},
                "crane-2", epoch=0, gate=client(h, server, "crane-3"),
                state_file=tmp_path / "next.json", event_sink=lambda event: None)
            assert await next_receiver.reconcile_from_gate()
            receiver.stubs["crane-3"] = DirectReceiver(next_receiver)
            assert await receiver.transfer_next()
            assert next_receiver.state.has_token and not receiver.state.has_token
        finally:
            await receiver.close()
            if next_receiver:
                await next_receiver.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


def test_new_holder_waits_for_queue_even_after_restart_and_without_sender(h, tmp_path):
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        path = tmp_path / "receiver.json"
        def make_agent():
            return Agent("crane-2", {"crane-1": "localhost:1", "crane-3": "localhost:2"},
                         "crane-1", epoch=0, gate=client(h, server, "crane-2"),
                         state_file=path, event_sink=lambda event: None)
        receiver = make_agent()
        try:
            assert await receiver.reconcile_from_gate()
            assert h.call("crane-1", "TransferHolder", epoch=0, revision=1, destination="crane-2")["allowed"]
            assert await receiver.reconcile_from_gate()
            assert receiver.incoming_pending and not receiver.state.has_token
            assert not (await receiver.TokenRequest(pb.TokenRequestMessage(
                agent_id="crane-3", epoch=0, revision=2), None)).accepted
            assert not await receiver.transfer_next()
        finally:
            await receiver.close()
        receiver = make_agent()
        try:
            assert await receiver.reconcile_from_gate()
            assert receiver.incoming_pending and not receiver.state.has_token
            assert not await receiver.transfer_next()
        finally:
            await receiver.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


@pytest.mark.parametrize("delivered", [False, True], ids=["not-delivered", "ack-lost"])
def test_pending_notification_survives_restart_until_receiver_acknowledges(h, tmp_path, delivered):
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        path = tmp_path / "sender.json"
        def make_sender():
            return Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                         "crane-1", epoch=0, gate=client(h, server, "crane-1"),
                         state_file=path, event_sink=lambda event: None)
        sender = make_sender()
        receiver = Agent("crane-2", {"crane-1": "localhost:1", "crane-3": "localhost:2"},
                         "crane-1", epoch=0, gate=client(h, server, "crane-2"),
                         state_file=tmp_path / "receiver.json", event_sink=lambda event: None)
        try:
            assert await sender.reconcile_from_gate() and await receiver.reconcile_from_gate()
            for peer in ("crane-2", "crane-3"):
                assert (await sender.TokenRequest(pb.TokenRequestMessage(
                    agent_id=peer, epoch=0, revision=1), None)).accepted
            sender.stubs["crane-2"] = DirectReceiver(receiver, accept=delivered, lose_ack=delivered)
            assert not await sender.transfer_next()
            transfer_id = sender.pending.transfer_id
            assert json.loads(path.read_text())["pending_transfer"]["transfer_id"] == transfer_id
            if delivered:
                assert (await receiver.TokenRequest(pb.TokenRequestMessage(
                    agent_id="crane-1", epoch=0, revision=2), None)).accepted
            await sender.close()
            sender = make_sender()
            assert sender.pending and sender.pending.transfer_id == transfer_id
            assert await sender.reconcile_from_gate() and not sender.state.has_token
            sender.stubs["crane-2"] = DirectReceiver(receiver)
            assert await sender.transfer_next()
            assert receiver.state.has_token
            assert list(receiver.state.request_queue) == (["crane-3", "crane-1"] if delivered else ["crane-3"])
            assert json.loads(path.read_text())["pending_transfer"] is None
        finally:
            await sender.close()
            await receiver.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


def test_same_target_snapshot_does_not_prove_this_command_committed(h, tmp_path):
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        path = tmp_path / "sender.json"
        sender = Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                       "crane-1", epoch=0, gate=client(h, server, "crane-1"),
                       state_file=path, event_sink=lambda event: None)
        try:
            assert await sender.reconcile_from_gate()
            for peer in ("crane-2", "crane-3"):
                assert (await sender.TokenRequest(pb.TokenRequestMessage(
                    agent_id=peer, epoch=0, revision=1), None)).accepted
            original = sender.gate.transfer
            async def not_sent(payload):
                raise RuntimeError("injected loss before command was sent")
            sender.gate.transfer = not_sent
            assert not await sender.transfer_next()
            request_id = sender.gate_attempt["command"]["request_id"]
            assert h.install("crane-2", epoch=0, mode="CENTRAL")["allowed"]
            assert (h.state["epoch"], h.state["revision"], h.state["holder"]) == (0, 2, "crane-2")
            sender.gate.transfer = original
            assert not await sender.transfer_next()
            assert sender.handoff_blocked == "GATE_OUTCOME_SUPERSEDED"
            assert sender.gate_attempt["command"]["request_id"] == request_id
            assert sender.gate_attempt["queue"] == ["crane-3"]
            assert not sender.state.has_token and sender.pending is None
            saved = json.loads(path.read_text())
            assert saved["handoff_blocked"] == "GATE_OUTCOME_SUPERSEDED"
            assert saved["gate_attempt"]["queue"] == ["crane-3"]
            assert not any(e["operation"] == "TransferHolder" and e["allowed"] for e in h.gate.events())
        finally:
            await sender.close()
        restarted = Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                          "crane-1", epoch=0, gate=client(h, server, "crane-1"),
                          state_file=path, event_sink=lambda event: None)
        try:
            assert restarted.handoff_blocked == "GATE_OUTCOME_SUPERSEDED"
            assert not await restarted.transfer_next()
            assert not restarted.state.has_token
            assert restarted.gate_attempt["command"]["request_id"] == request_id
        finally:
            await restarted.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


def test_denied_old_command_restores_queue_if_authority_returns_to_sender(h, tmp_path):
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        path = tmp_path / "sender.json"
        def make_sender():
            return Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                         "crane-1", epoch=0, gate=client(h, server, "crane-1"),
                         state_file=path, event_sink=lambda event: None)
        sender = make_sender()
        try:
            assert await sender.reconcile_from_gate()
            for peer in ("crane-2", "crane-3"):
                assert (await sender.TokenRequest(pb.TokenRequestMessage(
                    agent_id=peer, epoch=0, revision=1), None)).accepted
            original = sender.gate.transfer
            async def unavailable(payload):
                raise RuntimeError("injected unresolved command")
            sender.gate.transfer = unavailable
            assert not await sender.transfer_next()
            request_id = sender.gate_attempt["command"]["request_id"]
            assert h.call("crane-1", "TransferHolder", epoch=0, revision=1, destination="crane-2")["allowed"]
            assert not await sender.transfer_next()
            assert not sender.state.request_queue and sender.gate_attempt
            assert h.install("crane-1", epoch=0, mode="CENTRAL")["allowed"]
            sender.gate.transfer = original
            assert not await sender.transfer_next()
            assert sender.gate_attempt is None and sender.state.has_token
            assert list(sender.state.request_queue) == ["crane-2", "crane-3"]
            assert json.loads(path.read_text())["request_queue"] == ["crane-2", "crane-3"]
            assert any(e["operation"] == "TransferHolder" and e["request_id"] == request_id
                       and not e["allowed"] for e in h.gate.events())
        finally:
            await sender.close()
        restarted = make_sender()
        try:
            assert await restarted.reconcile_from_gate()
            assert list(restarted.state.request_queue) == ["crane-2", "crane-3"]
        finally:
            await restarted.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


def test_install_keeping_holder_does_not_strand_pending_notification(h, tmp_path):
    """A controller install that keeps the destination as holder (e.g. CENTRAL
    restored mid-handoff) must not block delivery of the committed handoff."""
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        sender = Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                       "crane-1", epoch=0, gate=client(h, server, "crane-1"),
                       state_file=tmp_path / "sender.json", event_sink=lambda event: None)
        receiver = Agent("crane-2", {"crane-1": "localhost:1", "crane-3": "localhost:2"},
                         "crane-1", epoch=0, gate=client(h, server, "crane-2"),
                         state_file=tmp_path / "receiver.json", event_sink=lambda event: None)
        try:
            assert await sender.reconcile_from_gate() and await receiver.reconcile_from_gate()
            for peer in ("crane-2", "crane-3"):
                assert (await sender.TokenRequest(pb.TokenRequestMessage(
                    agent_id=peer, epoch=0, revision=1), None)).accepted
            sender.stubs["crane-2"] = DirectReceiver(receiver, accept=False)
            assert not await sender.transfer_next()
            transfer_id = sender.pending.transfer_id
            assert h.install("crane-2", epoch=0, mode="CENTRAL")["allowed"]
            assert await receiver.reconcile_from_gate()
            assert receiver.incoming_pending and not receiver.state.has_token
            retry = DirectReceiver(receiver)
            sender.stubs["crane-2"] = retry
            assert await sender.transfer_next()
            assert [m.transfer_id for m in retry.messages] == [transfer_id]
            assert sender.pending is None and not sender.handoff_blocked
            assert receiver.state.has_token and not receiver.incoming_pending
            assert list(receiver.state.request_queue) == ["crane-3"]
            assert (receiver.state.current_epoch, receiver.state.revision) == (0, 3)
        finally:
            await sender.close()
            await receiver.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


def test_superseded_pending_notification_stays_durable_and_never_sent_stale(h, tmp_path):
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        path = tmp_path / "sender.json"
        def make_sender():
            return Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                         "crane-1", epoch=0, gate=client(h, server, "crane-1"),
                         state_file=path, event_sink=lambda event: None)
        sender = make_sender()
        receiver = Agent("crane-2", {"crane-1": "localhost:1", "crane-3": "localhost:2"},
                         "crane-1", epoch=0, gate=client(h, server, "crane-2"),
                         state_file=tmp_path / "receiver.json", event_sink=lambda event: None)
        try:
            assert await sender.reconcile_from_gate() and await receiver.reconcile_from_gate()
            for peer in ("crane-2", "crane-3"):
                assert (await sender.TokenRequest(pb.TokenRequestMessage(
                    agent_id=peer, epoch=0, revision=1), None)).accepted
            sender.stubs["crane-2"] = DirectReceiver(receiver, accept=False)
            assert not await sender.transfer_next()
            transfer_id = sender.pending.transfer_id
            # The controller reassigns the lane elsewhere: this handoff is superseded.
            assert h.install("crane-3", epoch=0, mode="CENTRAL")["allowed"]
            retry = DirectReceiver(receiver)
            sender.stubs["crane-2"] = retry
            assert not await sender.transfer_next()
            assert not retry.messages and sender.handoff_blocked == "DELIVERY_SUPERSEDED"
            saved = json.loads(path.read_text())
            assert saved["pending_transfer"]["transfer_id"] == transfer_id
            assert saved["pending_transfer"]["request_queue"] == ["crane-3"]
            await sender.close()
            sender = make_sender()
            assert sender.pending.transfer_id == transfer_id and sender.handoff_blocked
            assert not await sender.transfer_next() and not sender.state.has_token
            assert list(sender.pending.request_queue) == ["crane-3"]
        finally:
            await sender.close()
            await receiver.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()


def test_corrupt_saved_gate_attempt_fails_startup_without_rewriting_state(h, tmp_path):
    h.ready()
    server = RunningServer(h.gate, "127.0.0.1:0")
    async def scenario():
        path = tmp_path / "sender.json"
        agent = Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                      "crane-1", epoch=0, gate=client(h, server, "crane-1"),
                      state_file=path, event_sink=lambda event: None)
        try:
            assert await agent.reconcile_from_gate()
            assert (await agent.TokenRequest(pb.TokenRequestMessage(
                agent_id="crane-2", epoch=0, revision=1), None)).accepted
            async def unavailable(payload):
                raise RuntimeError("injected uncertainty")
            agent.gate.transfer = unavailable
            assert not await agent.transfer_next()
        finally:
            await agent.close()
        saved = json.loads(path.read_text())
        saved["gate_attempt"]["command"]["actor_id"] = "crane-2"
        path.write_text(json.dumps(saved))
        corrupt_bytes = path.read_bytes()
        gate = client(h, server, "crane-1")
        try:
            with pytest.raises(ValueError, match="Invalid persisted Gate command"):
                Agent("crane-1", {"crane-2": "localhost:1", "crane-3": "localhost:2"},
                      "crane-1", epoch=0, gate=gate, state_file=path, event_sink=lambda event: None)
            assert path.read_bytes() == corrupt_bytes
        finally:
            await gate.close()
    try:
        asyncio.run(scenario())
    finally:
        server.close()
