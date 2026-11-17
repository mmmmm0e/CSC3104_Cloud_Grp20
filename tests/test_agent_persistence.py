import asyncio
import json
import os
from types import SimpleNamespace
import uuid

import pytest

from cranelock import protocol_pb2 as pb, recovery
from cranelock.agent import Agent, Mode, PersistenceError
from cranelock.workload import WorkloadService


class Gate:
    def __init__(self):
        self.authority = dict(epoch=1, revision=3, holder="crane-2", mode="LOCAL")
        self.queries = 0
        self.grants = 0
        self.client = SimpleNamespace(config=SimpleNamespace(lane_id="lane"))

    async def state(self):
        self.queries += 1
        return dict(self.authority)

    async def request_grant(self, *args):
        self.grants += 1
        raise AssertionError("grant must not be attempted while persistence fails")

    async def close(self):
        pass


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(.005)
    await asyncio.wait_for(wait(), 2)


@pytest.mark.parametrize("stage", ["fsync", "replace"])
@pytest.mark.parametrize("holder", ["crane-1", "crane-2"])
def test_stale_observer_retries_persistence_and_checks_gate(tmp_path, monkeypatch, stage, holder):
    async def scenario():
        gate, events = Gate(), []
        path = tmp_path / "agent.json"
        agent = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-2", epoch=1,
                      gate=gate, state_file=path, interval=.02, timeout=.2, event_sink=events.append)
        try:
            assert await agent.reconcile_from_gate()
            old = json.loads(path.read_text())
            agent.state.request_queue.append("crane-2")
            gate.authority.update(revision=4, holder=holder)
            real = getattr(os, stage)
            blocked = True

            def fail(*args):
                if blocked:
                    raise PermissionError(13, "injected persistence failure")
                return real(*args)

            monkeypatch.setattr(os, stage, fail)
            # A peer's claim is only a refresh signal. The Gate supplies authority.
            await agent.Heartbeat(pb.HeartbeatMessage(agent_id="crane-2", epoch=1,
                revision=99, token_holder="crane-1", has_token=True), None)
            task = asyncio.create_task(agent.run_background(agent.rejoin_loop))
            agent.tasks.append(task)
            await until(lambda: sum(e["event"] == "STATE_PERSIST_FAILED" for e in events) >= 2)
            assert not task.done()
            assert agent.state.mode == Mode.REJOINING and not agent.state.has_token
            assert not agent.state.request_queue
            assert agent.authority_refresh_needed
            assert json.loads(path.read_text()) == old
            assert all(e["stage"] == stage for e in events if e["event"] == "STATE_PERSIST_FAILED")
            assert not (await agent.TokenRequest(pb.TokenRequestMessage(agent_id="crane-2",
                epoch=1, revision=4), None)).accepted
            service = WorkloadService(agent, {"workload"})
            result = await service._execute(pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()),
                expected_epoch=1, expected_revision=4), "workload")
            assert not result.allowed and result.reason == "AUTHORITY_UNCERTAIN"
            assert gate.grants == 0
            blocked = False
            await until(lambda: agent.state.mode == Mode.LOCAL and agent.state.revision == 4)
            saved = json.loads(path.read_text())
            assert saved["revision"] == 4 and saved["holder"] == holder
            assert not agent.state.has_token
            if holder == "crane-1":
                assert agent.incoming_pending
                assert (await agent.TokenTransfer(pb.TokenTransferMessage(
                    sender_id="crane-2", receiver_id="crane-1", holder="crane-1",
                    epoch=1, revision=4, transfer_id="actual-handoff"), None)).accepted
                assert agent.state.has_token
            assert gate.queries >= 4
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_refresh_arriving_during_gate_query_is_not_lost(tmp_path):
    async def scenario():
        gate, events = Gate(), []
        agent = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-2", epoch=1,
                      gate=gate, state_file=tmp_path / "agent.json", interval=.02,
                      timeout=.2, event_sink=events.append)
        try:
            assert await agent.reconcile_from_gate()
            entered, release = asyncio.Event(), asyncio.Event()
            original = gate.state
            async def delayed():
                snapshot = await original()
                entered.set()
                await release.wait()
                return snapshot
            gate.state = delayed
            await agent.Heartbeat(pb.HeartbeatMessage(agent_id="crane-2", epoch=1, revision=4), None)
            task = asyncio.create_task(agent.run_background(agent.rejoin_loop))
            agent.tasks.append(task)
            await asyncio.wait_for(entered.wait(), 2)
            gate.authority.update(revision=4, holder="crane-1")
            await agent.Heartbeat(pb.HeartbeatMessage(agent_id="crane-2", epoch=1, revision=4), None)
            release.set()
            await until(lambda: agent.state.revision == 4)
            assert not agent.state.has_token and agent.incoming_pending and gate.queries >= 3
            assert (await agent.TokenTransfer(pb.TokenTransferMessage(
                sender_id="crane-2", receiver_id="crane-1", holder="crane-1",
                epoch=1, revision=4, transfer_id="actual-handoff"), None)).accepted
            assert agent.state.has_token
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_unexpected_background_exit_is_observable_and_blocks_reactivation(tmp_path):
    async def scenario():
        gate, events = Gate(), []
        gate.authority["holder"] = "crane-1"
        agent = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-1", epoch=1,
                      gate=gate, state_file=tmp_path / "agent.json", event_sink=events.append)
        try:
            assert await agent.reconcile_from_gate()
            assert agent.state.has_token
            async def broken():
                raise ValueError("injected bug")
            with pytest.raises(ValueError, match="injected bug"):
                await agent.run_background(broken)
            assert any(e["event"] == "BACKGROUND_FAILED" and e["worker"] == "broken" for e in events)
            assert agent.state.mode == Mode.REJOINING and not agent.state.has_token
            assert not await agent.reconcile_from_gate()
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_uncertain_binding_write_is_retained_and_never_granted(tmp_path, monkeypatch):
    async def scenario():
        gate, events = Gate(), []
        gate.authority["holder"] = "crane-1"
        path = tmp_path / "agent.json"
        agent = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-1", epoch=1,
                      gate=gate, state_file=path, event_sink=events.append)
        try:
            assert await agent.reconcile_from_gate()
            original = os.replace
            def committed_then_error(*args):
                original(*args)
                raise OSError("injected ambiguous replace outcome")
            monkeypatch.setattr(os, "replace", committed_then_error)
            request_id = str(uuid.uuid4())
            request = pb.WorkloadGrantRequest(request_id=request_id, expected_epoch=1, expected_revision=3)
            service = WorkloadService(agent, {"workload"})
            result = await service._execute(request, "workload")
            assert result.reason == "STATE_UNAVAILABLE" and not result.allowed
            assert not agent.state.has_token and agent.state.mode == Mode.REJOINING
            assert agent.workload_requests[request_id] == ["workload", 1, 3]
            assert json.loads(path.read_text())["workload_requests"][request_id] == ["workload", 1, 3]
            assert gate.grants == 0
            assert (await service._execute(request, "another-workload")).reason == "REQUEST_ID_CONFLICT"
            monkeypatch.setattr(os, "replace", original)
            assert await agent.reconcile_from_gate()
            assert agent.state.has_token
            assert json.loads(path.read_text())["workload_requests"][request_id] == ["workload", 1, 3]
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_failed_vote_write_cannot_sign_or_forget_vote(h, tmp_path, monkeypatch):
    async def scenario():
        gate = Gate()
        path = tmp_path / "voter.json"
        peers = {p: "localhost:1" for p in h.config.members if p != "crane-1"}
        agent = Agent("crane-1", peers, "crane-2", epoch=1, gate=gate, state_file=path,
                      recovery_config=h.config, recovery_private_key=h.keys["crane-1"],
                      event_sink=lambda event: None)
        try:
            assert await agent.reconcile_from_gate()
            agent.suspected.add("crane-2")
            payload = recovery.proposal(h.config, gate.authority, "crane-1")
            raw = recovery.canonical(payload)
            signature = recovery.sign(h.keys["crane-2"], payload)
            original = os.replace
            def fail(*args):
                raise PermissionError(13, "injected vote write failure")
            monkeypatch.setattr(os, "replace", fail)
            with pytest.raises(PersistenceError):
                agent._vote_recovery(raw, "crane-2", signature)
            assert agent.vote_state and not agent.state.has_token and agent.state.mode == Mode.REJOINING
            assert not json.loads(path.read_text())["vote_state"]
            monkeypatch.setattr(os, "replace", original)
            assert await agent.reconcile_from_gate()
            assert json.loads(path.read_text())["vote_state"] == agent.vote_state
            conflicting = dict(payload, holder="crane-3")
            with pytest.raises(recovery.RecoveryError, match="CONFLICTING_VOTE"):
                agent._vote_recovery(recovery.canonical(conflicting), "crane-2",
                                     recovery.sign(h.keys["crane-2"], conflicting))
        finally:
            await agent.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("stage", ["write", "flush", "fsync", "replace"])
def test_failed_write_preserves_durable_records_and_cleans_own_temp(tmp_path, monkeypatch, stage):
    async def scenario():
        path = tmp_path / "voter.tmp"
        unrelated = tmp_path / "unrelated.tmp"
        unrelated.write_text("keep")
        events = []
        agent = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-2", epoch=1,
                      state_file=path, event_sink=events.append)
        try:
            agent.vote_state = {"1": "previous-vote"}
            agent.vote_proposals = {"1": {"holder": "crane-2"}}
            agent.workload_requests = {"request": ["workload", 1, 3]}
            agent.persist()
            original_bytes = path.read_bytes()
            agent.vote_state["2"] = "new-vote"
            agent.workload_requests["new-request"] = ["workload", 2, 4]

            class FailingStream:
                def __init__(self, stream):
                    self.stream = stream
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    return self.stream.__exit__(*args)
                def write(self, value):
                    if stage == "write":
                        self.stream.write(value[:1])
                        raise PermissionError(13, "injected write failure")
                    return self.stream.write(value)
                def flush(self):
                    if stage == "flush":
                        raise PermissionError(13, "injected flush failure")
                    return self.stream.flush()
                def fileno(self):
                    return self.stream.fileno()

            with monkeypatch.context() as patcher:
                if stage in {"write", "flush"}:
                    real_fdopen = os.fdopen
                    patcher.setattr(os, "fdopen", lambda *args, **kwargs:
                                    FailingStream(real_fdopen(*args, **kwargs)))
                else:
                    def fail(*args):
                        raise PermissionError(13, f"injected {stage} failure")
                    patcher.setattr(os, stage, fail)
                with pytest.raises(PersistenceError):
                    agent.persist()
            assert path.read_bytes() == original_bytes
            assert unrelated.read_text() == "keep"
            assert list(tmp_path.glob(".voter.tmp.*.tmp")) == []
            assert events[-1]["event"] == "STATE_PERSIST_FAILED" and events[-1]["stage"] == stage
            assert agent.state.mode == Mode.REJOINING and not agent.state.has_token
        finally:
            await agent.close()
        restarted = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-2", epoch=1,
                          state_file=path, event_sink=lambda event: None)
        try:
            assert restarted.vote_state == {"1": "previous-vote"}
            assert restarted.vote_proposals == {"1": {"holder": "crane-2"}}
            assert restarted.workload_requests == {"request": ["workload", 1, 3]}
            assert restarted.state.mode == Mode.REJOINING and not restarted.state.has_token
        finally:
            await restarted.close()
    asyncio.run(scenario())


def test_same_stem_state_files_have_distinct_concurrent_temporaries(tmp_path, monkeypatch):
    async def scenario():
        events = []
        a = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-2",
                  state_file=tmp_path / "same.json", event_sink=events.append)
        b = Agent("crane-2", {"crane-1": "localhost:1"}, "crane-1",
                  state_file=tmp_path / "same.state", event_sink=events.append)
        unrelated = tmp_path / "same.tmp"
        unrelated.write_text("keep")
        original = os.replace
        sources = []
        def overlapping(source, destination):
            sources.append(str(source))
            if len(sources) == 1:
                b.persist()
            return original(source, destination)
        try:
            with monkeypatch.context() as patcher:
                patcher.setattr(os, "replace", overlapping)
                a.persist()
            assert len(sources) == 2 and sources[0] != sources[1]
            assert json.loads(a.path.read_text())["agent_id"] == "crane-1"
            assert json.loads(b.path.read_text())["agent_id"] == "crane-2"
            assert unrelated.read_text() == "keep"
        finally:
            await asyncio.gather(a.close(), b.close())
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["unexpected_oserror", "retry_log", "persistence_log"])
def test_worker_or_reporting_failure_latches_before_ownership_can_resume(tmp_path, monkeypatch, failure):
    async def scenario():
        gate, events = Gate(), []
        gate.authority["holder"] = "crane-1"
        def sink(event):
            events.append(event)
            if ((failure == "retry_log" and event["event"] == "BACKGROUND_RETRY") or
                    (failure == "persistence_log" and event["event"] == "STATE_PERSIST_FAILED")):
                raise OSError("injected log failure")
        agent = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-1", epoch=1,
                      gate=gate, state_file=tmp_path / "agent.json", event_sink=sink)
        try:
            assert await agent.reconcile_from_gate() and agent.state.has_token
            calls = 0
            async def broken():
                nonlocal calls
                calls += 1
                if failure == "unexpected_oserror":
                    raise OSError("unexpected worker error")
                agent.persist()
            with monkeypatch.context() as patcher:
                if failure != "unexpected_oserror":
                    def fail(*args):
                        raise PermissionError(13, "injected replace failure")
                    patcher.setattr(os, "replace", fail)
                with pytest.raises(OSError):
                    await agent.run_background(broken)
            assert calls == 1 and agent.background_failure
            assert agent.state.mode == Mode.REJOINING and not agent.state.has_token
            assert not await agent.reconcile_from_gate()
            service = WorkloadService(agent, {"workload"})
            result = await service._execute(pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()),
                expected_epoch=1, expected_revision=3), "workload")
            assert not result.allowed and gate.grants == 0
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_successful_votes_and_bindings_survive_failed_write_and_restart(h, tmp_path, monkeypatch):
    async def scenario():
        gate = Gate()
        path = tmp_path / "voter.tmp"
        peers = {p: "localhost:1" for p in h.config.members if p != "crane-1"}
        def make_agent():
            return Agent("crane-1", peers, "crane-2", epoch=1, gate=gate, state_file=path,
                         recovery_config=h.config, recovery_private_key=h.keys["crane-1"],
                         event_sink=lambda event: None)
        agent = make_agent()
        try:
            assert await agent.reconcile_from_gate()
            agent.suspected.add("crane-2")
            payload = recovery.proposal(h.config, gate.authority, "crane-1")
            agent._vote_recovery(recovery.canonical(payload), "crane-2", recovery.sign(h.keys["crane-2"], payload))
            agent.workload_requests["bound-request"] = ["workload", 1, 3]
            agent.persist()
            expected_votes = dict(agent.vote_state)
            with monkeypatch.context() as patcher:
                def fail(*args):
                    raise PermissionError(13, "injected replace failure")
                patcher.setattr(os, "replace", fail)
                with pytest.raises(PersistenceError):
                    agent.persist()
        finally:
            await agent.close()
        restarted = make_agent()
        try:
            assert restarted.vote_state == expected_votes
            assert restarted.workload_requests == {"bound-request": ["workload", 1, 3]}
            assert restarted.state.mode == Mode.REJOINING and not restarted.state.has_token
            assert await restarted.reconcile_from_gate()
            restarted.suspected.add("crane-2")
            conflicting = dict(payload, holder="crane-3")
            with pytest.raises(recovery.RecoveryError, match="CONFLICTING_VOTE"):
                restarted._vote_recovery(recovery.canonical(conflicting), "crane-2",
                                         recovery.sign(h.keys["crane-2"], conflicting))
        finally:
            await restarted.close()
    asyncio.run(scenario())


def test_supervised_worker_cancellation_during_close_is_not_a_failure(tmp_path):
    async def scenario():
        events = []
        agent = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-2", epoch=1,
                      gate=Gate(), state_file=tmp_path / "agent.json", event_sink=events.append)
        await agent.start("127.0.0.1:0")
        names = [task.get_name() for task in agent.tasks]
        assert len(names) == len(set(names))
        with pytest.raises(RuntimeError, match="Agent already started"):
            await agent.start("127.0.0.1:0")
        assert [task.get_name() for task in agent.tasks] == names
        await agent.close()
        assert all(task.done() for task in agent.tasks)
        assert agent.background_failure is None
        assert not any(e["event"] == "BACKGROUND_FAILED" for e in events)
    asyncio.run(scenario())


def test_failed_worker_blocks_trusted_reconciliation_without_gate(tmp_path):
    async def scenario():
        agent = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-1", epoch=1,
                      state_file=tmp_path / "agent.json", event_sink=lambda event: None)
        async def broken():
            raise ValueError("worker failed")
        try:
            with pytest.raises(ValueError):
                await agent.run_background(broken)
            with pytest.raises(RuntimeError, match="Required background worker failed"):
                await agent.reconcile(1, "crane-1")
            assert not agent.state.has_token and agent.state.mode == Mode.REJOINING
            assert not (await agent.TokenRequest(pb.TokenRequestMessage(
                agent_id="crane-2", epoch=1), None)).accepted
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_transferred_queue_and_identity_survive_restart_and_retries(tmp_path):
    async def scenario():
        gate = Gate()
        path = tmp_path / "receiver.json"
        peers = {"crane-2": "localhost:1", "crane-3": "localhost:2"}
        def make_agent():
            return Agent("crane-1", peers, "crane-2", epoch=1, gate=gate,
                         state_file=path, event_sink=lambda event: None)
        agent = make_agent()
        message = pb.TokenTransferMessage(sender_id="crane-2", receiver_id="crane-1", holder="crane-1",
                                          epoch=1, revision=4, transfer_id="handoff", request_queue=["crane-3"])
        try:
            assert await agent.reconcile_from_gate()
            gate.authority.update(revision=4, holder="crane-1")
            assert (await agent.TokenTransfer(message, None)).accepted
            assert (await agent.TokenRequest(pb.TokenRequestMessage(
                agent_id="crane-2", epoch=1, revision=4), None)).accepted
            assert list(agent.state.request_queue) == ["crane-3", "crane-2"]
        finally:
            await agent.close()
        restarted = make_agent()
        try:
            assert restarted.state.mode == Mode.REJOINING and not restarted.state.has_token
            assert list(restarted.state.request_queue) == ["crane-3", "crane-2"]
            assert await restarted.reconcile_from_gate() and restarted.state.has_token
            assert (await restarted.TokenTransfer(message, None)).accepted
            assert list(restarted.state.request_queue) == ["crane-3", "crane-2"]
            changed = pb.TokenTransferMessage()
            changed.CopyFrom(message)
            del changed.request_queue[:]
            assert not (await restarted.TokenTransfer(changed, None)).accepted
            assert list(restarted.state.request_queue) == ["crane-3", "crane-2"]
            gate.authority.update(revision=5, holder="crane-2")
            assert not (await restarted.TokenTransfer(message, None)).accepted
            assert not restarted.state.has_token and restarted.state.revision == 5
        finally:
            await restarted.close()
    asyncio.run(scenario())


def test_receiver_restart_after_second_write_failure_keeps_queue_for_retry(tmp_path, monkeypatch):
    async def scenario():
        gate = Gate()
        path = tmp_path / "receiver.json"
        peers = {"crane-2": "localhost:1", "crane-3": "localhost:2"}
        def make_agent():
            return Agent("crane-1", peers, "crane-2", epoch=1, gate=gate,
                         state_file=path, event_sink=lambda event: None)
        agent = make_agent()
        message = pb.TokenTransferMessage(sender_id="crane-2", receiver_id="crane-1", holder="crane-1",
                                          epoch=1, revision=4, transfer_id="handoff", request_queue=["crane-3"])
        try:
            assert await agent.reconcile_from_gate()
            gate.authority.update(revision=4, holder="crane-1")
            original = os.replace
            calls = 0
            def fail_second(source, destination):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise PermissionError(13, "injected second receiver write failure")
                return original(source, destination)
            with monkeypatch.context() as patcher:
                patcher.setattr(os, "replace", fail_second)
                with pytest.raises(PersistenceError):
                    await agent.TokenTransfer(message, None)
            saved = json.loads(path.read_text())
            assert calls == 2 and saved["request_queue"] == ["crane-3"]
            assert not saved["gate_received_transfers"]
            assert not agent.state.has_token and agent.state.mode == Mode.REJOINING
        finally:
            await agent.close()
        restarted = make_agent()
        try:
            assert list(restarted.state.request_queue) == ["crane-3"]
            assert await restarted.reconcile_from_gate()
            assert (await restarted.TokenTransfer(message, None)).accepted
            assert list(restarted.state.request_queue) == ["crane-3"]
            assert json.loads(path.read_text())["gate_received_transfers"]
        finally:
            await restarted.close()
    asyncio.run(scenario())


def test_controller_return_preserves_accepted_queue_when_holder_is_unchanged(tmp_path):
    async def scenario():
        gate = Gate()
        gate.authority["holder"] = "crane-1"
        path = tmp_path / "holder.json"
        def make_agent():
            return Agent("crane-1", {"crane-2": "localhost:1"}, "crane-1", epoch=1,
                         gate=gate, state_file=path, event_sink=lambda event: None)
        agent = make_agent()
        try:
            assert await agent.reconcile_from_gate()
            assert (await agent.TokenRequest(pb.TokenRequestMessage(
                agent_id="crane-2", epoch=1, revision=3), None)).accepted
            gate.authority.update(revision=4, mode="CENTRAL")
            assert await agent.reconcile_controller_return()
            assert list(agent.state.request_queue) == ["crane-2"]
            assert json.loads(path.read_text())["request_queue"] == ["crane-2"]
        finally:
            await agent.close()
        restarted = make_agent()
        try:
            assert await restarted.reconcile_from_gate()
            assert list(restarted.state.request_queue) == ["crane-2"]
        finally:
            await restarted.close()
    asyncio.run(scenario())


def test_new_central_term_requires_fresh_controller_failure_timeout(tmp_path):
    async def scenario():
        gate = Gate()
        now = [100.0]
        async def unavailable():
            return False
        agent = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-2", epoch=1,
                      gate=gate, state_file=tmp_path / "agent.json", clock=lambda: now[0],
                      controller_probe=unavailable, interval=.02, timeout=.2, event_sink=lambda event: None)
        try:
            assert await agent.reconcile_from_gate() and agent.state.mode == Mode.LOCAL
            agent.controller_suspected = True
            agent.last_controller_seen = 0
            gate.authority.update(revision=4, mode="CENTRAL")
            assert await agent.reconcile_from_gate()
            assert not agent.controller_suspected and agent.controller_return_pending
            assert agent.last_controller_seen == 100.0 and not agent.automatic_recovery_needed()
            now[0] += .21
            task = asyncio.create_task(agent.controller_watch_loop())
            agent.tasks.append(task)
            await until(lambda: agent.controller_suspected)
            assert agent.automatic_recovery_needed()
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_gate_outage_does_not_restart_same_controller_term_timeout(tmp_path):
    async def scenario():
        gate = Gate()
        gate.authority["mode"] = "CENTRAL"
        now = [100.0]
        async def unavailable_controller():
            return False
        agent = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-2", epoch=1,
                      gate=gate, state_file=tmp_path / "agent.json", clock=lambda: now[0],
                      controller_probe=unavailable_controller, interval=.02, timeout=.2,
                      event_sink=lambda event: None)
        try:
            assert await agent.reconcile_from_gate() and agent.last_controller_seen == 100.0
            original_state = gate.state
            async def unavailable_gate():
                raise RuntimeError("injected Gate outage")
            now[0] += .19
            gate.state = unavailable_gate
            assert not await agent.reconcile_from_gate() and agent.state.mode == Mode.REJOINING
            gate.state = original_state
            assert await agent.reconcile_from_gate()
            assert agent.last_controller_seen == 100.0
            gate.authority.update(revision=4, holder="crane-1", authority_transition=dict(
                kind="TRANSFER", epoch=1, revision=4, holder="crane-1", actor_id="crane-2", request_id="handoff"))
            assert await agent.reconcile_from_gate()
            assert agent.incoming_pending and not agent.state.has_token
            assert agent.last_controller_seen == 100.0
            now[0] += .02
            task = asyncio.create_task(agent.controller_watch_loop())
            agent.tasks.append(task)
            await until(lambda: agent.controller_suspected)
            assert agent.automatic_recovery_needed()
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_new_central_install_resets_stale_suspicion_without_mode_change(tmp_path):
    async def scenario():
        gate = Gate()
        gate.authority.update(mode="CENTRAL", authority_transition=dict(
            kind="INSTALL", epoch=1, revision=3, holder="crane-2", actor_id="controller", request_id="first"))
        now = [100.0]
        async def unavailable_controller():
            return False
        agent = Agent("crane-1", {"crane-2": "localhost:1"}, "crane-2", epoch=1,
                      gate=gate, state_file=tmp_path / "agent.json", clock=lambda: now[0],
                      controller_probe=unavailable_controller, interval=.02, timeout=.2,
                      event_sink=lambda event: None)
        try:
            assert await agent.reconcile_from_gate() and agent.state.mode == Mode.CENTRAL
            agent.controller_suspected = True
            agent.last_controller_seen = 0
            now[0] += 1
            gate.authority.update(revision=4, authority_transition=dict(
                kind="INSTALL", epoch=1, revision=4, holder="crane-2", actor_id="controller", request_id="second"))
            assert await agent.reconcile_from_gate()
            assert not agent.controller_suspected and agent.controller_return_pending
            assert agent.last_controller_seen == 101.0 and not agent.automatic_recovery_needed()
        finally:
            await agent.close()
    asyncio.run(scenario())
