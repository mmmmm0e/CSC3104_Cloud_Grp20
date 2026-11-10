import asyncio
from dataclasses import asdict
import json
import os
import socket
import subprocess
import sys
import time
from types import ModuleType
from unittest.mock import AsyncMock

import grpc
import pytest

from cranelock import __main__ as launcher
from cranelock.agent import Agent, Mode
from cranelock.controller import CentralController
from cranelock import protocol_pb2 as pb
from cranelock.protocol_pb2_grpc import CraneAgentStub
from cranelock.gate_client import GateClient
from cranelock_gate.server import RunningServer
from test_auto_recovery import until


class StartupReached(Exception):
    pass


@pytest.fixture
def capture_agent(monkeypatch):
    instances = []

    class CapturedAgent(Agent):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            instances.append(self)

        async def start(self, *args):
            raise StartupReached()

        async def close(self):
            self.closed = True
            await super().close()

    monkeypatch.setattr(launcher, "Agent", CapturedAgent)
    return instances


@pytest.fixture
def options(h, tmp_path):
    key = tmp_path / "crane-1.key.json"
    key.write_text(json.dumps({"crane-1": h.keys["crane-1"].private_bytes_raw().hex()}))
    base = ["run", "--id", "crane-1", "--listen", "127.0.0.1:0",
            "--peer", "crane-2=127.0.0.1:1", "--peer", "crane-3=127.0.0.1:2",
            "--holder", "crane-1", "--state-file", str(tmp_path / "agent.json")]
    gate = ["--gate-address", "127.0.0.1:1", "--gate-config", str(tmp_path / "config.json"),
            "--gate-keys", str(key)]
    recovery = ["--recovery-config", str(tmp_path / "config.json"), "--recovery-key", str(key)]
    return base, gate, recovery


@pytest.mark.parametrize("features", ["plain", "gate", "recovery", "controller", "heartbeat", "recovery_alias"])
def test_launcher_passes_optional_features_and_closes_on_start_failure(
        options, h, monkeypatch, capture_agent, features):
    base, gate, recovery = options
    args = base + (gate if features != "plain" else [])
    if features in {"recovery", "controller", "heartbeat"}:
        args += recovery
    if features == "recovery_alias":
        args += ["--enable-recovery"]
    module = ModuleType("launcher_probe_fixture")

    async def probe():
        return True

    module.probe = probe
    monkeypatch.setitem(sys.modules, module.__name__, module)
    if features == "controller":
        args += ["--controller-probe", module.__name__ + ":probe"]
    clients = []
    connect = launcher.ControllerHeartbeatClient

    def tracked_client(*args, **kwargs):
        client = connect(*args, **kwargs)
        client.close = AsyncMock(wraps=client.close)
        clients.append(client)
        return client

    monkeypatch.setattr(launcher, "ControllerHeartbeatClient", tracked_client)
    if features == "heartbeat":
        args += ["--controller-address", "127.0.0.1:50052"]
    with pytest.raises(StartupReached):
        asyncio.run(launcher.main(args))
    agent, = capture_agent
    assert agent.closed
    assert (agent.gate is not None) == (features != "plain")
    assert (agent.recovery_config is not None) == (features in {"recovery", "controller", "heartbeat", "recovery_alias"})
    if agent.recovery_config:
        assert agent.recovery_config == h.config
        assert agent.recovery_private_key.public_key().public_bytes_raw().hex() == h.config.members["crane-1"]
        assert set(agent.gate.client.keys) == {"crane-1"}
    else:
        assert agent.recovery_private_key is None
    if features == "heartbeat":
        client, = clients
        assert agent.controller_probe == client.probe
        assert client.agent_id == "crane-1" and client.controller_id == h.config.controller_id
        assert client.close.await_count == 1
    else:
        assert not clients
        assert agent.controller_probe is (probe if features == "controller" else None)


@pytest.mark.parametrize("invalid", [
    "missing_config", "bad_config", "membership", "missing_key", "bad_key", "wrong_key",
    "shared_keys", "partial_recovery", "no_gate", "different_lane", "different_member_key",
    "bad_probe", "sync_probe", "probe_arguments", "probe_without_recovery",
    "bad_peer", "self_peer", "duplicate_peer", "tls_missing_file", "bad_state",
    "heartbeat_without_recovery", "both_probes", "controller_remote_without_tls",
    "controller_partial_tls", "controller_tls_without_address", "controller_bad_tls",
    "heartbeat_bad_state",
    "enable_without_gate", "enable_shared_keys", "enable_with_explicit_recovery",
])
def test_invalid_launcher_configuration_never_starts_or_exposes_keys(
        invalid, options, h, tmp_path, monkeypatch, capture_agent, capsys):
    base, gate, recovery = options
    args = base + gate + recovery
    key_file = tmp_path / "crane-1.key.json"
    config_file = tmp_path / "config.json"
    secret = h.keys["crane-1"].private_bytes_raw().hex()
    if invalid == "missing_config":
        args[args.index("--recovery-config") + 1] = str(tmp_path / "absent.json")
    elif invalid == "bad_config":
        config_file.write_text("[]")
    elif invalid == "membership":
        args[args.index("--peer") + 1] = "unknown=127.0.0.1:1"
    elif invalid == "missing_key":
        key_file.unlink()
    elif invalid == "bad_key":
        key_file.write_text(json.dumps({"crane-1": "secret-invalid-material"}))
    elif invalid == "wrong_key":
        key_file.write_text(json.dumps({"crane-1": h.keys["crane-2"].private_bytes_raw().hex()}))
    elif invalid in {"shared_keys", "enable_shared_keys"}:
        key_file.write_text(json.dumps({p: k.private_bytes_raw().hex() for p, k in h.keys.items()}))
        if invalid == "enable_shared_keys":
            args = base + gate + ["--enable-recovery"]
    elif invalid == "enable_without_gate":
        args = base + ["--enable-recovery"]
    elif invalid == "enable_with_explicit_recovery":
        args += ["--enable-recovery"]
    elif invalid == "partial_recovery":
        args = base + gate + recovery[:2]
    elif invalid == "no_gate":
        args = base + recovery
    elif invalid in {"different_lane", "different_member_key"}:
        changed = asdict(h.config)
        if invalid == "different_lane":
            changed["lane_id"] = "other-lane"
        else:
            changed["members"]["crane-2"], changed["members"]["crane-3"] = (
                changed["members"]["crane-3"], changed["members"]["crane-2"])
        other = tmp_path / "other.json"
        other.write_text(json.dumps(changed))
        args[args.index("--recovery-config") + 1] = str(other)
    elif invalid == "bad_probe":
        args += ["--controller-probe", "cranelock.missing:probe"]
    elif invalid in {"sync_probe", "probe_arguments"}:
        module = ModuleType("invalid_probe_fixture")

        async def required_argument(address):
            return True

        module.probe = (lambda: True) if invalid == "sync_probe" else required_argument
        monkeypatch.setitem(sys.modules, module.__name__, module)
        args += ["--controller-probe", module.__name__ + ":probe"]
    elif invalid == "probe_without_recovery":
        args = base + ["--controller-probe", "cranelock.missing:probe"]
    elif invalid == "heartbeat_without_recovery":
        args = base + gate + ["--controller-address", "127.0.0.1:50052"]
    elif invalid == "both_probes":
        args += ["--controller-address", "127.0.0.1:50052",
                 "--controller-probe", "cranelock.missing:probe"]
    elif invalid == "controller_remote_without_tls":
        args += ["--controller-address", "192.0.2.1:50052"]
    elif invalid == "controller_partial_tls":
        args += ["--controller-address", "127.0.0.1:50052", "--controller-tls-ca", "missing.pem"]
    elif invalid == "controller_tls_without_address":
        args += ["--controller-tls-ca", "missing.pem"]
    elif invalid == "controller_bad_tls":
        args += ["--controller-address", "127.0.0.1:50052"]
        for option in ("--controller-tls-ca", "--controller-tls-cert", "--controller-tls-key"):
            args += [option, str(tmp_path / "missing.pem")]
    elif invalid == "bad_peer":
        args[args.index("--peer") + 1] = "missing-address"
    elif invalid == "self_peer":
        args[args.index("--peer") + 1] = "crane-1=127.0.0.1:1"
    elif invalid == "duplicate_peer":
        args += ["--peer", "crane-2=127.0.0.1:3"]
    elif invalid == "tls_missing_file":
        for option in ("--gate-tls-ca", "--gate-tls-cert", "--gate-tls-key"):
            args += [option, str(tmp_path / "missing.pem")]
    elif invalid in {"bad_state", "heartbeat_bad_state"}:
        (tmp_path / "agent.json").write_text("not JSON")
        if invalid == "heartbeat_bad_state":
            args += ["--controller-address", "127.0.0.1:50052"]
    heartbeats = []
    heartbeat_client = launcher.ControllerHeartbeatClient

    def tracked_heartbeat(*args, **kwargs):
        client = heartbeat_client(*args, **kwargs)
        client.close = AsyncMock(wraps=client.close)
        heartbeats.append(client)
        return client

    monkeypatch.setattr(launcher, "ControllerHeartbeatClient", tracked_heartbeat)
    connections = []
    connect = GateClient.connect

    def tracked_connect(*args, **kwargs):
        client = connect(*args, **kwargs)
        client.close = AsyncMock(wraps=client.close)
        connections.append(client)
        return client

    monkeypatch.setattr(GateClient, "connect", tracked_connect)
    with pytest.raises(SystemExit) as error:
        asyncio.run(launcher.main(args))
    assert error.value.code == 2
    assert not capture_agent
    assert all(client.close.await_count == 1 for client in connections)
    assert all(client.close.await_count == 1 for client in heartbeats)
    output = capsys.readouterr()
    assert secret not in output.out + output.err
    assert "secret-invalid-material" not in output.out + output.err
    assert "Traceback" not in output.err


@pytest.mark.parametrize("with_tls", [False, True])
def test_launcher_controller_lifecycle_preserves_local_handoff(h, tmp_path, monkeypatch, with_tls):
    async def scenario():
        assert h.install(mode="CENTRAL")["allowed"]
        server = RunningServer(h.gate, "127.0.0.1:0")
        channel = grpc.insecure_channel(f"127.0.0.1:{server.port}")
        controller = CentralController(h.config, h.keys["controller"], channel)
        agents, clients, tasks = {}, [], []
        events = []
        server_credentials, tls_args = None, []
        if with_tls:
            from test_tls import credentials
            ca, server_key, server_cert, client_key, client_cert = credentials()
            server_credentials = grpc.ssl_server_credentials(
                [(server_key, server_cert)], ca, require_client_auth=True)
            for name, value in (("ca", ca), ("cert", client_cert), ("key", client_key)):
                path = tmp_path / f"controller-{name}.pem"
                path.write_bytes(value)
                tls_args += [f"--controller-tls-{name}", str(path)]

        class LiveAgent(Agent):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs, event_sink=events.append)
                agents[self.state.agent_id] = self

        heartbeat_client = launcher.ControllerHeartbeatClient

        def tracked_client(*args, **kwargs):
            client = heartbeat_client(*args, **kwargs)
            client.close = AsyncMock(wraps=client.close)
            clients.append(client)
            return client

        monkeypatch.setattr(launcher, "Agent", LiveAgent)
        monkeypatch.setattr(launcher, "ControllerHeartbeatClient", tracked_client)
        sockets = [socket.socket() for _ in range(2)]
        try:
            for sock in sockets:
                sock.bind(("127.0.0.1", 0))
            addresses = {f"crane-{i + 1}": f"127.0.0.1:{sock.getsockname()[1]}"
                         for i, sock in enumerate(sockets)}
        finally:
            for sock in sockets:
                sock.close()
        try:
            port = await controller.start_heartbeat_server(credentials=server_credentials)
            for identity, address in addresses.items():
                key = tmp_path / f"{identity}.key.json"
                key.write_text(json.dumps({identity: h.keys[identity].private_bytes_raw().hex()}))
                args = ["run", "--id", identity, "--listen", address, "--holder", "crane-1",
                        "--mode", "CENTRAL", "--epoch", "0", "--interval", ".05", "--timeout", ".5",
                        "--state-file", str(tmp_path / f"{identity}.json"),
                        "--gate-address", f"127.0.0.1:{server.port}",
                        "--gate-config", str(tmp_path / "config.json"), "--gate-keys", str(key),
                        "--recovery-config", str(tmp_path / "config.json"), "--recovery-key", str(key),
                        "--controller-address", f"127.0.0.1:{port}"]
                for peer in h.config.members:
                    if peer != identity:
                        args += ["--peer", f"{peer}={addresses.get(peer, '127.0.0.1:1')}"]
                tasks.append(asyncio.create_task(launcher.main(args + tls_args)))
            await until(lambda: len(agents) == 2 and sum(
                e["event"] == "CONTROLLER_HEARTBEAT_RECEIVED" for e in events) >= 2)
            await controller.stop_heartbeat_server()
            await until(lambda: all(a.state.mode == Mode.LOCAL and a.controller_suspected
                                    for a in agents.values()))
            assert h.observe()["allowed"]
            assert (await agents["crane-2"].request_token()).accepted
            await until(lambda: h.state["holder"] == "crane-2" and all(
                a.state.revision == 3 and a.state.token_holder == "crane-2"
                for a in agents.values()))
            await controller.start_heartbeat_server(
                f"127.0.0.1:{port}", credentials=server_credentials)
            await until(lambda: all(a.state.mode == Mode.CENTRAL and a.state.revision == 4
                                    and not a.controller_suspected for a in agents.values()))
            assert (h.state["epoch"], h.state["revision"], h.state["holder"]) == (1, 4, "crane-2")
            assert agents["crane-2"].state.has_token and not agents["crane-1"].state.has_token
            assert h.call("crane-1", "RequestGrant", epoch=1, revision=2)["reason"] == "STALE_REVISION"
            assert all(not task.done() for task in tasks)
        finally:
            for task in tasks:
                task.cancel()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            await controller.stop_heartbeat_server()
            channel.close()
            server.close()
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
        assert len(clients) == 2 and all(client.close.await_count == 1 for client in clients)
        assert all(all(task.done() for task in agent.tasks) for agent in agents.values())
    asyncio.run(scenario())


def test_standalone_launchers_automatically_recover_using_agent_only_keys(h, tmp_path):
    assert h.install("crane-1")["allowed"]
    server = RunningServer(h.gate, "127.0.0.1:0")
    sockets = [socket.socket() for _ in range(2)]
    for sock in sockets:
        sock.bind(("127.0.0.1", 0))
    addresses = {f"crane-{i + 2}": f"127.0.0.1:{sock.getsockname()[1]}"
                 for i, sock in enumerate(sockets)}
    for sock in sockets:
        sock.close()
    processes, streams, channels, stubs = [], [], [], {}
    try:
        for identity, address in addresses.items():
            key = tmp_path / f"{identity}.key.json"
            key.write_text(json.dumps({identity: h.keys[identity].private_bytes_raw().hex()}))
            args = [sys.executable, "-m", "cranelock", "run", "--id", identity, "--listen", address,
                    "--holder", "crane-1", "--epoch", "0", "--state-file", str(tmp_path / f"{identity}.json"),
                    "--interval", ".05", "--timeout", ".8", "--gate-address", f"127.0.0.1:{server.port}",
                    "--gate-config", str(tmp_path / "config.json"), "--gate-keys", str(key),
                    "--recovery-config", str(tmp_path / "config.json"), "--recovery-key", str(key),
                    "--peer", "crane-1=127.0.0.1:1"]
            for peer, endpoint in addresses.items():
                if peer != identity:
                    args += ["--peer", f"{peer}={endpoint}"]
            stream = (tmp_path / f"{identity}.log").open("w")
            streams.append(stream)
            processes.append(subprocess.Popen(args, stdout=stream, stderr=stream,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0))
            channel = grpc.insecure_channel(address)
            channels.append(channel)
            stubs[identity] = CraneAgentStub(channel)
        for channel in channels:
            grpc.channel_ready_future(channel).result(timeout=10)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            states = {identity: stub.GetAgentState(pb.Empty(), timeout=1) for identity, stub in stubs.items()}
            if all(state.epoch == 1 for state in states.values()):
                break
            time.sleep(.05)
        assert all(state.epoch == 1 and state.revision == 2 for state in states.values())
        assert states["crane-2"].has_token and not states["crane-3"].has_token
        assert h.state["holder"] == "crane-2" and h.state["epoch"] == 1
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=5)
        for channel in channels:
            channel.close()
        for stream in streams:
            stream.close()
        server.close()
    for identity in addresses:
        assert json.loads((tmp_path / f"{identity}.json").read_text())["vote_state"]
        assert h.keys[identity].private_bytes_raw().hex() not in (tmp_path / f"{identity}.log").read_text()
