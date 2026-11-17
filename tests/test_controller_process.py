"""Real gate/controller/three-agent CLI processes; no in-process services or mocks.

The driver supplies workload through TokenRequest and signed lane observations.
Only the agent processes transfer the holder, collect votes and install fallback.
Gate and controller connections use mTLS; peer RPCs use the current loopback API.
"""
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

import grpc
import pytest

from cranelock import protocol_pb2 as pb, recovery
from cranelock.protocol_pb2_grpc import CraneAgentStub, CraneControllerStub
from cranelock_gate.client import Client, certificate, may_enter
from cranelock_gate.demo import create_demo_config
from cranelock_gate.tls import generate


ROOT = Path(__file__).resolve().parents[1]
TRANSIENT = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}


class ProcessCluster:
    def __init__(self, directory):
        self.directory = directory
        self.config, self.keys = create_demo_config(directory)
        self.records, self.channels, self.reservations = [], [], {}
        self.live = {}
        self.metrics = {"poll_interval_seconds": .03, "agent_timeout_seconds": 1.2,
                        "required_majority": self.config.majority,
                        "gate_controller_transport": "mutual_tls", "peer_transport": "loopback_plaintext"}
        self.phases = {}
        self.handoffs = []
        self.controller_generation = 0
        for name in ("gate", "controller", *self.config.members):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            self.reservations[name] = sock
        self.addresses = {name: f"127.0.0.1:{sock.getsockname()[1]}"
                          for name, sock in self.reservations.items()}
        for name, data in generate().items():
            (directory / name).write_bytes(data)
        for identity in (*self.config.members, self.config.controller_id):
            self.key_path(identity).write_text(json.dumps({identity: self.keys[identity].private_bytes_raw().hex()}))
        self.credentials = grpc.ssl_channel_credentials(
            (directory / "ca.pem").read_bytes(), (directory / "client-key.pem").read_bytes(),
            (directory / "client.pem").read_bytes())
        channel = grpc.secure_channel(self.addresses["gate"], self.credentials)
        self.channels.append(channel)
        self.gate = Client(channel, self.config, self.keys, timeout=.5)
        self.stubs = {}
        for identity in self.config.members:
            channel = grpc.insecure_channel(self.addresses[identity])
            self.channels.append(channel)
            self.stubs[identity] = CraneAgentStub(channel)

    def key_path(self, identity):
        return self.directory / f"{identity}.key.json"

    def tls_args(self, prefix, server=False):
        role = "server" if server else "client"
        return [f"--{prefix}-ca", str(self.directory / "ca.pem"),
                f"--{prefix}-cert", str(self.directory / f"{role}.pem"),
                f"--{prefix}-key", str(self.directory / f"{role}-key.pem")]

    def command(self, *args):
        return [sys.executable, "-B", "-u", "-m", *map(str, args)]

    def start(self, name, command, address_key=None):
        if address_key in self.reservations:
            self.reservations.pop(address_key).close()
        log = self.directory / f"{name}.log"
        stream = log.open("w", encoding="utf-8")
        try:
            process = subprocess.Popen(command, cwd=ROOT, stdout=stream, stderr=stream,
                env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        except BaseException:
            stream.close()
            raise
        record = dict(name=name, process=process, log=log, stream=stream)
        self.records.append(record)
        self.live[name] = process
        return process

    def assert_alive(self):
        for name, process in self.live.items():
            if process.poll() is not None:
                record = next(record for record in self.records if record["process"] is process)
                pytest.fail(f"{name} exited ({process.returncode}): {record['log'].read_text(encoding='utf-8')[-3000:]}")

    def wait(self, label, predicate, timeout=15):
        deadline = time.monotonic() + timeout
        last_error = None
        while time.monotonic() < deadline:
            self.assert_alive()
            try:
                result = predicate()
                if result:
                    return result
            except grpc.RpcError as error:
                if error.code() not in TRANSIENT:
                    raise
                last_error = error.code().name
            time.sleep(.03)
        pytest.fail(f"Timed out waiting for {label}; last RPC error: {last_error}; logs: {self.directory}")

    def events(self, identity):
        events = []
        for line in (self.directory / f"{identity}.log").read_text(encoding="utf-8").splitlines():
            # Ignore native gRPC diagnostics and a final line still being written.
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(event)
        return events

    def event(self, identity, kind, after=0):
        return next((event for event in self.events(identity)
                     if event.get("event") == kind and event["monotonic"] >= after), None)

    def states(self, members=None):
        return {identity: self.stubs[identity].GetAgentState(pb.Empty(), timeout=.5)
                for identity in (members if members is not None else self.config.members)}

    def converged(self, authority, members):
        states = self.states(members)
        return states if all(
            (state.agent_id, state.epoch, state.revision, state.token_holder, state.mode, state.has_token)
            == (identity, authority["epoch"], authority["revision"], authority["holder"],
                getattr(pb, authority["mode"]), identity == authority["holder"])
            and not state.pending_transfer_id for identity, state in states.items()) else None

    def observe(self, lane="CLEAR"):
        state = self.gate.state()
        result = self.gate.call(self.config.simulator_id, "ReportLaneState",
            session_id=state["session_id"], sequence=state["observation_sequence"] + 1,
            observation_revision=state["observation_revision"],
            observed_at_ms=int(time.time() * 1000), lane_state=lane)
        assert result["allowed"], result["reason"]

    def start_controller(self):
        self.controller_generation += 1
        name = f"controller-{self.controller_generation}"
        began = time.monotonic()
        process = self.start(name, self.command("cranelock.run_controller",
            "--gate-config", self.directory / "config.json", "--controller-key", self.key_path(self.config.controller_id),
            "--gate-address", self.addresses["gate"], "--listen", self.addresses["controller"],
            "--gate-timeout", "1", *self.tls_args("gate-tls"), *self.tls_args("tls", server=True)), "controller")
        return name, process, began

    def boot(self):
        init = subprocess.run(self.command("cranelock_gate.server", "init",
            "--config", self.directory / "config.json", "--database", self.directory / "gate.sqlite"),
            cwd=ROOT, capture_output=True, text=True, timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        assert init.returncode == 0, init.stderr
        self.start("gate", self.command("cranelock_gate.server", "serve",
            "--config", self.directory / "config.json", "--database", self.directory / "gate.sqlite",
            "--bind", self.addresses["gate"], "--metrics-port", "0", *self.tls_args("tls", server=True)), "gate")
        initial = self.wait("real Trusted Gate", self.gate.state)
        assert (initial["epoch"], initial["revision"]) == (-1, 0)
        # Explicit bootstrap only; all later LOCAL installs come from real agents.
        result = self.gate.call(self.config.controller_id, "InstallAuthority",
            certificate=certificate(self.config, self.keys, initial, "crane-1", 0, "CENTRAL", [self.config.controller_id]),
            expected_epoch=-1, expected_revision=0)
        assert result["allowed"]
        self.phases["bootstrap"] = result["state"]
        self.controller_name, self.controller, _ = self.start_controller()
        self.wait("controller listener", lambda: "Heartbeat endpoint listening" in
                  (self.directory / f"{self.controller_name}.log").read_text(encoding="utf-8"))
        for identity in self.config.members:
            args = self.command("cranelock", "run", "--id", identity, "--listen", self.addresses[identity],
                "--holder", "crane-1", "--epoch", "0", "--mode", "CENTRAL",
                "--state-file", self.directory / f"{identity}.json", "--interval", ".1", "--timeout", "1.2",
                "--gate-address", self.addresses["gate"], "--gate-config", self.directory / "config.json",
                "--gate-keys", self.key_path(identity), "--recovery-config", self.directory / "config.json",
                "--recovery-key", self.key_path(identity), "--controller-address", self.addresses["controller"],
                *self.tls_args("gate-tls"), *self.tls_args("controller-tls"))
            for peer in self.config.members:
                if peer != identity:
                    args += ["--peer", f"{peer}={self.addresses[peer]}"]
            self.start(identity, args, identity)
        self.wait("all three CENTRAL agents", lambda: self.converged(self.phases["bootstrap"], self.config.members))
        self.wait("controller liveness at every agent", lambda: all(
            self.event(identity, "CONTROLLER_HEARTBEAT_RECEIVED") for identity in self.config.members))
        pids = [process.pid for process in self.live.values()]
        assert len(pids) == len(set(pids)) == 5 and os.getpid() not in pids
        # Actual mTLS endpoints must reject clients without a certificate.
        for address, stub_type, method, request in (
            (self.addresses["gate"], None, None, None),
            (self.addresses["controller"], CraneControllerStub, "Heartbeat",
             pb.ControllerHeartbeatRequest(agent_id="crane-1")),
        ):
            with grpc.secure_channel(address, grpc.ssl_channel_credentials(
                    (self.directory / "ca.pem").read_bytes())) as channel:
                with pytest.raises(grpc.RpcError):
                    if stub_type is None:
                        Client(channel, self.config, {}, timeout=.2).state()
                    else:
                        getattr(stub_type(channel), method)(request, timeout=.2)
        self.handoff("crane-2", list(self.config.members), "central_handoff")

    def handoff(self, destination, members, phase):
        self.observe()
        before = self.gate.state()
        sender = before["holder"]
        self.wait("holder ready to accept workload", lambda: self.converged(before, members))
        request_id = uuid.uuid4().hex
        result = self.stubs[sender].TokenRequest(pb.TokenRequestMessage(agent_id=destination,
            epoch=before["epoch"], revision=before["revision"], request_id=request_id), timeout=1)
        assert result.accepted, result.reason
        authority = self.wait("gate-authorized peer handoff", lambda: (
            state if (state := self.gate.state())["holder"] == destination else None))
        assert (authority["epoch"], authority["revision"], authority["mode"]) == (
            before["epoch"], before["revision"] + 1, before["mode"])
        self.wait("peer token receipt and observer convergence", lambda: self.converged(authority, members))
        sent = next(event for event in reversed(self.events(sender)) if event["event"] == "GATE_TRANSFER_ACCEPTED")
        received = next(event for event in reversed(self.events(destination)) if event["event"] == "TOKEN_TRANSFER_RECEIVED")
        assert sent["request_id"] == received["transfer_id"] and received["peer_id"] == sender
        self.handoffs.append(dict(phase=phase, sender=sender, receiver=destination,
                                  request_id=sent["request_id"]))
        self.phases[phase] = authority
        return sent["request_id"]

    def crash(self, name):
        process = self.live.pop(name)
        began = time.monotonic()
        process.kill()
        process.wait(timeout=5)
        assert process.poll() is not None
        return began

    def detection(self, members, failed_at):
        def detected():
            state = self.gate.state()
            if state["mode"] == "LOCAL":
                self.metrics.setdefault("authority_recovery_seconds", time.monotonic() - failed_at)
            found = {identity: self.event(identity, "CONTROLLER_SUSPECTED", failed_at)
                     for identity in members}
            return found if all(found.values()) else None

        events = self.wait("controller failure detection", detected)
        # Use each event's monotonic clock on this same OS host, not wall time.
        self.metrics["failure_detection_seconds"] = {
            identity: self.event(identity, "CONTROLLER_SUSPECTED", failed_at)["monotonic"] - failed_at
            for identity in members}
        return events

    def audit(self):
        result = subprocess.run(self.command("cranelock_gate.server", "export-events",
            "--config", self.directory / "config.json", "--database", self.directory / "gate.sqlite"),
            cwd=ROOT, capture_output=True, text=True, timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        assert result.returncode == 0, result.stderr
        (self.directory / "gate-audit.jsonl").write_text(result.stdout)
        events = [json.loads(line) for line in result.stdout.splitlines()]
        for handoff in self.handoffs:
            matched = [event for event in events if event["operation"] == "TransferHolder"
                       and event["request_id"] == handoff["request_id"] and event["allowed"]]
            assert len(matched) == 1 and matched[0]["holder"] == handoff["receiver"]
        return events

    def close(self):
        errors = []
        for record in reversed(self.records):
            process = record["process"]
            try:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=4)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=4)
            except Exception as error:
                errors.append(f"{record['name']}: {type(error).__name__}")
            finally:
                record["stream"].close()
        for channel in self.channels:
            channel.close()
        for sock in self.reservations.values():
            sock.close()
        self.metrics["phases"] = {phase: {key: state[key] for key in ("epoch", "revision", "holder", "mode")}
                                  for phase, state in self.phases.items()}
        self.metrics["handoffs"] = self.handoffs
        self.metrics["processes"] = [dict(name=record["name"], pid=record["process"].pid,
                                         exit_code=record["process"].poll()) for record in self.records]
        self.metrics["all_processes_stopped"] = all(record["process"].poll() is not None for record in self.records)
        (self.directory / "controller-process-results.json").write_text(json.dumps(self.metrics, indent=2))
        assert not errors and self.metrics["all_processes_stopped"], errors


@pytest.fixture
def cluster(tmp_path):
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.boot()
        yield cluster
    finally:
        cluster.close()


@pytest.mark.parametrize("lose_nonholder", [False, True], ids=["controller-only", "two-of-three"])
def test_real_controller_process_failure_handoff_and_restoration(cluster, lose_nonholder, record_property):
    members = list(cluster.config.members)
    cluster.metrics["scenario"] = "two-of-three" if lose_nonholder else "controller-only"
    central = cluster.gate.state()
    assert (central["epoch"], central["revision"], central["holder"]) == (0, 2, "crane-2")
    if lose_nonholder:
        cluster.crash("crane-1")
        members.remove("crane-1")
    cluster.metrics["surviving_agents"] = members
    failed_at = cluster.crash(cluster.controller_name)
    cluster.detection(members, failed_at)
    local = cluster.wait("majority LOCAL installation", lambda: (
        state if (state := cluster.gate.state())["mode"] == "LOCAL" else None))
    cluster.metrics.setdefault("authority_recovery_seconds", time.monotonic() - failed_at)
    assert (local["epoch"], local["revision"], local["holder"]) == (1, 3, "crane-2")
    cluster.phases["fallback"] = local
    cluster.wait("LOCAL convergence", lambda: cluster.converged(local, members))
    digest = hashlib.sha256(recovery.canonical(recovery.proposal(cluster.config, central, "crane-2"))).hexdigest()
    votes = {identity: json.loads((cluster.directory / f"{identity}.json").read_text())["vote_state"]
             for identity in members}
    assert sum(saved.get("1") == digest for saved in votes.values()) >= cluster.config.majority
    formed = [event for identity in members for event in cluster.events(identity)
              if event["event"] == "RECOVERY_CERTIFICATE_FORMED"]
    assert formed and all(event["votes"] >= cluster.config.majority for event in formed)
    assert any(event["event"] == "RECOVERY_VOTE_CAST" for identity in members for event in cluster.events(identity))
    transfer_id = cluster.handoff("crane-3", members, "local_handoff")
    assert cluster.controller.poll() is not None
    local = cluster.gate.state()
    delayed_local = certificate(cluster.config, cluster.keys, local, "crane-1", local["epoch"] + 1)
    cluster.observe()
    grant = cluster.gate.call("crane-3", "RequestGrant", epoch=1, revision=4)
    assert grant["allowed"]
    entry = cluster.gate.call(cluster.config.simulator_id, "ValidateEntry", epoch=1, revision=4,
        holder="crane-3", grant_id=grant["grant"]["id"], session_id=grant["state"]["session_id"])
    assert may_enter(entry)
    name, restarted, restart_at = cluster.start_controller()

    def restored_gate():
        cluster.observe("OCCUPIED")  # A trusted sensor keeps observing the admitted occupant.
        state = cluster.gate.state()
        return state if state["mode"] == "CENTRAL" else None

    restored = cluster.wait("returning controller restores the latest holder", restored_gate)
    cluster.metrics["gate_restoration_seconds"] = time.monotonic() - restart_at
    assert (restored["epoch"], restored["revision"], restored["holder"], restored["lane_state"]) == (
        1, 5, "crane-3", "OCCUPIED")

    def restored_agents():
        cluster.observe("OCCUPIED")
        return cluster.converged(restored, members) if all(
            cluster.event(identity, "CONTROLLER_RESTORED", restart_at) for identity in members) else None

    cluster.wait("all surviving agents reconcile CENTRAL", restored_agents)
    cluster.metrics["all_agents_restoration_seconds"] = time.monotonic() - restart_at
    cluster.phases["restored"] = restored
    assert restarted.poll() is None and name in cluster.live
    failures = {}
    for label, actor, epoch, revision, reason in (
        ("stale_epoch", "crane-2", 0, 2, "STALE_EPOCH"),
        ("stale_revision", "crane-2", 1, 3, "STALE_REVISION"),
        ("wrong_holder", "crane-2", 1, 5, "WRONG_HOLDER"),
    ):
        denied = cluster.gate.call(actor, "RequestGrant", epoch=epoch, revision=revision)
        assert not denied["allowed"] and denied["reason"] == reason
        denied_entry = cluster.gate.call(cluster.config.simulator_id, "ValidateEntry", epoch=epoch,
            revision=revision, holder=actor, grant_id=grant["grant"]["id"], session_id=grant["state"]["session_id"])
        assert not denied_entry["allowed"] and denied_entry["reason"] == reason
        failures[label] = reason
    old_central = certificate(cluster.config, cluster.keys, central, "crane-1", 0, "CENTRAL", [cluster.config.controller_id])
    for actor, cert, reason in ((cluster.config.controller_id, old_central, "STALE_EPOCH"),
                                ("crane-1", delayed_local, "STALE_REVISION")):
        payload = cert["payload"]
        denied = cluster.gate.call(actor, "InstallAuthority", certificate=cert,
            expected_epoch=payload["expected_epoch"], expected_revision=payload["expected_revision"])
        assert not denied["allowed"] and denied["reason"] == reason
    stale_transfer = cluster.stubs["crane-3"].TokenTransfer(pb.TokenTransferMessage(sender_id="crane-2",
        receiver_id="crane-3", holder="crane-3", epoch=1, revision=4, transfer_id=transfer_id), timeout=1)
    assert not stale_transfer.accepted
    failures.update(stale_central_certificate="STALE_EPOCH", delayed_local_certificate="STALE_REVISION",
                    delayed_peer_transfer="REJECTED")
    blocked = cluster.gate.call("crane-3", "RequestGrant", epoch=1, revision=5)
    assert blocked["reason"] == "LANE_BUSY" and not blocked["allowed"]
    failures["occupied_lane"] = blocked["reason"]
    cluster.observe("CLEAR")  # Explicit trusted observation of the previous occupant's exit.
    current_grant = cluster.gate.call("crane-3", "RequestGrant", epoch=1, revision=5)
    assert current_grant["allowed"]
    valid_entry = cluster.gate.call(cluster.config.simulator_id, "ValidateEntry", epoch=1, revision=5,
        holder="crane-3", grant_id=current_grant["grant"]["id"], session_id=current_grant["state"]["session_id"])
    assert may_enter(valid_entry)
    failures["current_holder_entry_after_exit"] = "ALLOWED"
    unchanged = cluster.gate.state()
    assert (unchanged["epoch"], unchanged["revision"], unchanged["holder"], unchanged["mode"]) == (
        1, 5, "crane-3", "CENTRAL")
    cluster.wait("no stale RPC changed token ownership", lambda: cluster.converged(restored, members))
    audit = cluster.audit()
    fallback_installs = [event for event in audit if event["operation"] == "InstallAuthority"
                         and event["allowed"] and event["epoch"] == 1 and event["revision"] == 3]
    assert len(fallback_installs) == 1
    assert {event["reason"] for event in audit} >= {"STALE_EPOCH", "STALE_REVISION", "WRONG_HOLDER", "LANE_BUSY"}
    cluster.metrics["fencing_results"] = failures
    cluster.metrics["durable_voters"] = sorted(identity for identity, saved in votes.items() if saved.get("1") == digest)
    cluster.metrics["certificate_vote_counts"] = [event["votes"] for event in formed]
    for metric, value in cluster.metrics.items():
        if metric.endswith("_seconds"):
            record_property(metric, json.dumps(value))
    print(json.dumps({"scenario": "two-of-three" if lose_nonholder else "controller-only", "metrics": cluster.metrics}))


def test_real_controller_loss_without_majority_cannot_install_local(cluster):
    cluster.metrics.update(scenario="no-majority", surviving_agents=["crane-2"])
    before = cluster.gate.state()
    cluster.crash("crane-1")
    cluster.crash("crane-3")
    failed_at = cluster.crash(cluster.controller_name)
    cluster.detection(["crane-2"], failed_at)
    cluster.wait("repeated no-quorum observations", lambda: len([
        event for event in cluster.events("crane-2") if event["event"] == "RECOVERY_NO_QUORUM"]) >= 2)
    unchanged = cluster.gate.state()
    assert (unchanged["epoch"], unchanged["revision"], unchanged["mode"], unchanged["holder"]) == (
        before["epoch"], before["revision"], "CENTRAL", "crane-2")
    cluster.metrics["no_quorum_observed_seconds"] = time.monotonic() - failed_at
    assert not any(event["event"] == "RECOVERY_CERTIFICATE_FORMED" for event in cluster.events("crane-2"))
    saved = json.loads((cluster.directory / "crane-2.json").read_text())["vote_state"]
    assert "1" in saved
    name, process, restart_at = cluster.start_controller()
    cluster.wait("real controller liveness returns without rewriting authority", lambda:
        cluster.event("crane-2", "CONTROLLER_RESTORED", restart_at)
        and cluster.converged(before, ["crane-2"]))
    cluster.metrics["all_agents_restoration_seconds"] = time.monotonic() - restart_at
    assert process.poll() is None
    assert cluster.gate.state()["revision"] == before["revision"]
    assert not any(event["operation"] == "InstallAuthority" and event["allowed"] and event["epoch"] > 0
                   for event in cluster.audit())
    cluster.phases["no_quorum_return"] = cluster.gate.state()


def test_real_controller_fallback_preserves_live_holders_accepted_queue(cluster):
    members = list(cluster.config.members)
    before = cluster.gate.state()
    assert before["holder"] == "crane-2"
    cluster.observe("OCCUPIED")
    request = pb.TokenRequestMessage(agent_id="crane-3", epoch=before["epoch"],
                                     revision=before["revision"], request_id=uuid.uuid4().hex)
    assert cluster.stubs["crane-2"].TokenRequest(request, timeout=1).accepted
    cluster.wait("accepted request held while lane occupied", lambda:
        list(cluster.states(["crane-2"])["crane-2"].request_queue) == ["crane-3"])
    failed_at = cluster.crash(cluster.controller_name)
    cluster.detection(members, failed_at)
    local = cluster.wait("LOCAL fallback with same holder", lambda: (
        state if (state := cluster.gate.state())["mode"] == "LOCAL" else None))
    assert local["holder"] == "crane-2" and local["epoch"] == before["epoch"] + 1
    cluster.wait("LOCAL convergence with accepted request", lambda:
        cluster.converged(local, members) and
        list(cluster.states(["crane-2"])["crane-2"].request_queue) == ["crane-3"])
    saved = json.loads((cluster.directory / "crane-2.json").read_text())
    assert saved["request_queue"] == ["crane-3"]
    cluster.observe("CLEAR")
    moved = cluster.wait("queued request makes progress", lambda: (
        state if (state := cluster.gate.state())["holder"] == "crane-3" else None))
    cluster.wait("queued handoff converges", lambda: cluster.converged(moved, members))
