import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
from unittest.mock import AsyncMock, Mock

import grpc
import pytest

from cranelock import run_controller as launcher
from cranelock.controller import CentralController, ControllerHeartbeatClient
from cranelock_gate.server import RunningServer
from cranelock_gate.tls import generate


@pytest.fixture
def options(h, tmp_path):
    key = tmp_path / "controller.key.json"
    key.write_text(json.dumps({h.config.controller_id: h.keys[h.config.controller_id].private_bytes_raw().hex()}))
    return ["--gate-config", str(tmp_path / "config.json"), "--controller-key", str(key),
            "--gate-address", "127.0.0.1:1", "--listen", "127.0.0.1:0", "--gate-timeout", ".2"]


def replace_option(options, name, value):
    options[options.index(name) + 1] = str(value)


@pytest.fixture
def tls_files(tmp_path):
    files = generate()
    for name, data in files.items():
        (tmp_path / name).write_bytes(data)
    return tmp_path


def tls_options(directory, prefix):
    role = "client" if prefix == "gate-tls" else "server"
    return [f"--{prefix}-ca", str(directory / "ca.pem"),
            f"--{prefix}-cert", str(directory / f"{role}.pem"),
            f"--{prefix}-key", str(directory / f"{role}-key.pem")]


def server_credentials(directory):
    return grpc.ssl_server_credentials(
        [((directory / "server-key.pem").read_bytes(), (directory / "server.pem").read_bytes())],
        (directory / "ca.pem").read_bytes(), require_client_auth=True)


def client_credentials(directory):
    return grpc.ssl_channel_credentials((directory / "ca.pem").read_bytes(),
        (directory / "client-key.pem").read_bytes(), (directory / "client.pem").read_bytes())


@pytest.fixture
def runtime(monkeypatch):
    controllers, channels = [], []
    open_channel = launcher.open_channel

    class TrackedController(CentralController):
        async def start_heartbeat_server(self, *args, **kwargs):
            self.port = await super().start_heartbeat_server(*args, **kwargs)
            return self.port

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.start_heartbeat_server = AsyncMock(wraps=self.start_heartbeat_server)
            self.stop_heartbeat_server = AsyncMock(wraps=self.stop_heartbeat_server)
            controllers.append(self)

    def tracked_channel(*args, **kwargs):
        channel = open_channel(*args, **kwargs)
        channel.close = Mock(wraps=channel.close)
        channels.append(channel)
        return channel

    monkeypatch.setattr(launcher, "CentralController", TrackedController)
    monkeypatch.setattr(launcher, "open_channel", tracked_channel)
    return controllers, channels


async def started(task, controllers):
    async with asyncio.timeout(4):
        while not controllers or controllers[0]._heartbeat_server is None:
            if task.done():
                pytest.fail(f"Launcher exited before serving: {task.result()}")
            await asyncio.sleep(.01)
    return controllers[0].port


def assert_closed(runtime):
    controllers, channels = runtime
    assert len(channels) == 1
    channels[0].close.assert_called_once_with()
    for controller in controllers:
        controller.stop_heartbeat_server.assert_awaited_once_with()
        assert controller._heartbeat_server is None


@pytest.mark.parametrize("mode", ["CENTRAL", "LOCAL"])
@pytest.mark.parametrize("with_tls", [False, True])
def test_controller_launcher_serves_existing_authority_and_closes_on_cancel(
        h, options, tls_files, runtime, mode, with_tls, capsys):
    assert h.install("crane-2", mode=mode)["allowed"]
    assert h.observe()["allowed"] and h.grant()["allowed"] and h.enter()["allowed"]
    server = RunningServer(h.gate, "127.0.0.1:0",
                           credentials=server_credentials(tls_files) if with_tls else None)
    replace_option(options, "--gate-address", f"127.0.0.1:{server.port}")
    if with_tls:
        options += tls_options(tls_files, "gate-tls") + tls_options(tls_files, "tls")

    async def scenario():
        previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        task = asyncio.create_task(launcher.main(options))
        clients = []
        try:
            port = await started(task, runtime[0])
            controller, = runtime[0]
            client = ControllerHeartbeatClient(f"127.0.0.1:{port}", "crane-1", h.config.controller_id,
                credentials=client_credentials(tls_files) if with_tls else None, timeout=.3)
            clients.append(client)
            assert await client.probe()
            state = h.state
            assert (state["epoch"], state["revision"], state["holder"], state["mode"]) == (
                0, 1 if mode == "CENTRAL" else 2, "crane-2", "CENTRAL")
            assert state["lane_state"] == "OCCUPIED"
            assert set(controller.client.keys) == {h.config.controller_id}
            assert controller.client.timeout == .2
            if with_tls:
                anonymous = ControllerHeartbeatClient(f"127.0.0.1:{port}", "crane-1",
                                                      h.config.controller_id, timeout=.1)
                clients.append(anonymous)
                with pytest.raises(grpc.RpcError):
                    await anonymous.probe()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            for client in clients:
                await client.close()
        assert {sig: signal.getsignal(sig) for sig in previous} == previous
        assert_closed(runtime)
    try:
        asyncio.run(scenario())
    finally:
        server.close()
    output = capsys.readouterr()
    assert "Controller active:" in output.out and "Heartbeat endpoint listening on 127.0.0.1:" in output.out
    assert h.keys["controller"].private_bytes_raw().hex() not in output.out + output.err


@pytest.mark.parametrize("invalid", [
    "missing_config", "bad_config", "config_schema", "missing_key", "bad_key_json",
    "bad_key_hex", "bad_key_length", "key_schema", "wrong_identity", "shared_keys",
    "bad_gate_address", "bad_listener", "gate_port_zero", "bad_ipv6", "remote_gate", "remote_listener",
    "timeout_nan", "timeout_inf", "timeout_zero", "timeout_negative",
])
def test_invalid_controller_configuration_never_connects_or_exposes_keys(
        h, tmp_path, options, monkeypatch, capsys, invalid):
    key = tmp_path / "controller.key.json"
    if invalid == "missing_config":
        replace_option(options, "--gate-config", tmp_path / "missing.json")
    elif invalid == "bad_config":
        (tmp_path / "config.json").write_text("secret-invalid-json")
    elif invalid == "config_schema":
        (tmp_path / "config.json").write_text("[]")
    elif invalid == "missing_key":
        key.unlink()
    elif invalid == "bad_key_json":
        key.write_text("secret-invalid-json")
    elif invalid in {"bad_key_hex", "bad_key_length"}:
        key.write_text(json.dumps({h.config.controller_id: "secret-not-hex" if invalid == "bad_key_hex" else "00"}))
    elif invalid == "key_schema":
        key.write_text("[]")
    elif invalid == "wrong_identity":
        key.write_text(json.dumps({"crane-1": h.keys["crane-1"].private_bytes_raw().hex()}))
    elif invalid == "shared_keys":
        key.write_text((tmp_path / "private-keys.json").read_text())
    elif invalid in {"bad_gate_address", "gate_port_zero", "remote_gate"}:
        replace_option(options, "--gate-address", {
            "bad_gate_address": "http://localhost:1", "gate_port_zero": "127.0.0.1:0",
            "remote_gate": "gate.example:50051"}[invalid])
    elif invalid in {"bad_listener", "bad_ipv6", "remote_listener"}:
        replace_option(options, "--listen", {"bad_listener": "127.0.0.1:65536",
                       "bad_ipv6": "[:::]:1", "remote_listener": "0.0.0.0:50052"}[invalid])
    elif invalid.startswith("timeout_"):
        replace_option(options, "--gate-timeout", {
            "timeout_nan": "nan", "timeout_inf": "inf", "timeout_zero": "0", "timeout_negative": "-1"}[invalid])
    connect = Mock(side_effect=AssertionError("Invalid configuration attempted a connection"))
    monkeypatch.setattr(launcher, "open_channel", connect)
    with pytest.raises(SystemExit) as error:
        asyncio.run(launcher.main(options))
    assert error.value.code == 2
    connect.assert_not_called()
    output = capsys.readouterr()
    assert "Traceback" not in output.err and "secret-" not in output.err
    assert all(key.private_bytes_raw().hex() not in output.out + output.err for key in h.keys.values())


def test_mismatched_controller_key_is_rejected_before_gate_rpc(h, tmp_path, options, runtime, capsys):
    (tmp_path / "controller.key.json").write_text(json.dumps({
        h.config.controller_id: h.keys["crane-1"].private_bytes_raw().hex()}))
    with pytest.raises(SystemExit) as error:
        asyncio.run(launcher.main(options))
    assert error.value.code == 2
    assert not runtime[0]
    assert_closed(runtime)
    assert h.state["epoch"] == -1
    output = capsys.readouterr()
    assert "does not match" in output.err and "Traceback" not in output.err
    assert h.keys["crane-1"].private_bytes_raw().hex() not in output.err


def test_unavailable_gate_fails_with_bounded_io_and_closes_channel(h, options, runtime, capsys):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        replace_option(options, "--gate-address", f"127.0.0.1:{sock.getsockname()[1]}")
        assert asyncio.run(asyncio.wait_for(launcher.main(options), 2)) == 1
    assert_closed(runtime)
    runtime[0][0].start_heartbeat_server.assert_not_awaited()
    output = capsys.readouterr()
    assert "connect to the Trusted Gate" in output.err and "Controller active" not in output.out


def test_uninitialized_gate_requires_explicit_bootstrap(h, options, runtime, capsys):
    server = RunningServer(h.gate, "127.0.0.1:0")
    replace_option(options, "--gate-address", f"127.0.0.1:{server.port}")
    try:
        assert asyncio.run(launcher.main(options)) == 1
        assert (h.state["epoch"], h.state["revision"], h.state["mode"]) == (-1, 0, "UNINITIALIZED")
        runtime[0][0].start_heartbeat_server.assert_not_awaited()
        assert_closed(runtime)
    finally:
        server.close()
    assert "explicitly bootstrap authority" in capsys.readouterr().err


@pytest.mark.parametrize("change", ["transfer", "recovery"])
def test_launcher_restoration_retries_latest_authority(h, options, runtime, monkeypatch, change):
    assert h.install()["allowed"] and h.install()["allowed"]
    assert h.observe()["allowed"]
    get_state = CentralController.get_state
    snapshots = []

    def concurrent_change(self):
        state = get_state(self)
        snapshots.append(state)
        if len(snapshots) == 2:  # The restoration decision, after the launcher's preflight read.
            result = (h.call("crane-1", "TransferHolder", epoch=1, revision=2, destination="crane-3")
                      if change == "transfer" else h.install("crane-3"))
            assert result["allowed"]
        return state

    monkeypatch.setattr(CentralController, "get_state", concurrent_change)
    server = RunningServer(h.gate, "127.0.0.1:0")
    replace_option(options, "--gate-address", f"127.0.0.1:{server.port}")

    async def scenario():
        task = asyncio.create_task(launcher.main(options))
        try:
            await started(task, runtime[0])
            assert (h.state["epoch"], h.state["revision"], h.state["holder"], h.state["mode"]) == (
                1 if change == "transfer" else 2, 4, "crane-3", "CENTRAL")
            assert len(snapshots) == 3
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert_closed(runtime)
    try:
        asyncio.run(scenario())
    finally:
        server.close()


@pytest.mark.parametrize("prefix", ["gate-tls", "tls"])
@pytest.mark.parametrize("invalid", ["partial", "missing", "bad_ca", "bad_cert", "mismatched_key", "wrong_purpose", "expired"])
def test_invalid_tls_never_connects_or_exposes_material(
        h, options, tls_files, monkeypatch, capsys, prefix, invalid):
    tls_args = tls_options(tls_files, prefix)
    role = "client" if prefix == "gate-tls" else "server"
    other = "server" if role == "client" else "client"
    if invalid == "partial":
        tls_args = tls_args[:2]
    elif invalid == "missing":
        replace_option(tls_args, f"--{prefix}-key", tls_files / "missing.pem")
    elif invalid in {"bad_ca", "bad_cert"}:
        (tls_files / ("ca.pem" if invalid == "bad_ca" else f"{role}.pem")).write_text("secret-invalid-pem")
    elif invalid == "mismatched_key":
        replace_option(tls_args, f"--{prefix}-key", tls_files / f"{other}-key.pem")
    elif invalid == "wrong_purpose":
        replace_option(tls_args, f"--{prefix}-key", tls_files / f"{other}-key.pem")
        replace_option(tls_args, f"--{prefix}-cert", tls_files / f"{other}.pem")
    elif invalid == "expired":
        for name, data in generate(days=0).items():
            (tls_files / name).write_bytes(data)
    connect = Mock(side_effect=AssertionError("Invalid TLS attempted a connection"))
    monkeypatch.setattr(launcher, "open_channel", connect)
    with pytest.raises(SystemExit) as error:
        asyncio.run(launcher.main(options + tls_args))
    assert error.value.code == 2
    connect.assert_not_called()
    output = capsys.readouterr()
    assert "secret-invalid-pem" not in output.err and "PRIVATE KEY" not in output.err
    assert "Traceback" not in output.err


def test_untrusted_gate_certificate_fails_before_restoration(h, options, tls_files, tmp_path, runtime):
    assert h.install()["allowed"]
    other = tmp_path / "other-pki"
    other.mkdir()
    for name, data in generate().items():
        (other / name).write_bytes(data)
    server = RunningServer(h.gate, "127.0.0.1:0", credentials=server_credentials(tls_files))
    replace_option(options, "--gate-address", f"127.0.0.1:{server.port}")
    try:
        assert asyncio.run(launcher.main(options + tls_options(other, "gate-tls"))) == 1
        assert (h.state["epoch"], h.state["revision"], h.state["mode"]) == (0, 1, "LOCAL")
        runtime[0][0].start_heartbeat_server.assert_not_awaited()
        assert_closed(runtime)
    finally:
        server.close()


@pytest.mark.parametrize("shutdown", [signal.SIGINT, signal.SIGTERM] + (
    [signal.SIGBREAK] if hasattr(signal, "SIGBREAK") else []))
def test_signal_shutdown_stops_server_closes_channel_and_restores_handlers(h, options, runtime, shutdown):
    assert h.install(mode="CENTRAL")["allowed"]
    server = RunningServer(h.gate, "127.0.0.1:0")
    replace_option(options, "--gate-address", f"127.0.0.1:{server.port}")

    async def scenario():
        previous = signal.getsignal(shutdown)
        task = asyncio.create_task(launcher.main(options))
        try:
            await started(task, runtime[0])
            signal.raise_signal(shutdown)
            assert await asyncio.wait_for(task, 2) == 0
            assert signal.getsignal(shutdown) == previous
            assert_closed(runtime)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    try:
        asyncio.run(scenario())
    finally:
        server.close()


def test_listener_start_failure_closes_resources_and_redacts_errors(h, options, runtime, monkeypatch, capsys):
    assert h.install(mode="CENTRAL")["allowed"]
    secret = h.keys["controller"].private_bytes_raw().hex()
    monkeypatch.setattr(CentralController, "start_heartbeat_server", AsyncMock(side_effect=RuntimeError(secret)))
    server = RunningServer(h.gate, "127.0.0.1:0")
    replace_option(options, "--gate-address", f"127.0.0.1:{server.port}")
    try:
        assert asyncio.run(launcher.main(options)) == 1
        assert_closed(runtime)
    finally:
        server.close()
    output = capsys.readouterr()
    assert "start the controller heartbeat listener" in output.err
    assert secret not in output.err and "Controller active" not in output.out


@pytest.mark.parametrize("shutdown", ["cancel", "signal"])
def test_shutdown_during_listener_start_does_not_abandon_server(
        h, options, runtime, monkeypatch, capsys, shutdown):
    assert h.install(mode="CENTRAL")["allowed"]
    gate = RunningServer(h.gate, "127.0.0.1:0")
    replace_option(options, "--gate-address", f"127.0.0.1:{gate.port}")

    async def scenario():
        bound, release = asyncio.Event(), asyncio.Event()
        servers = []
        factory = grpc.aio.server

        def delayed_server(*args, **kwargs):
            server = factory(*args, **kwargs)
            start = server.start

            async def delayed_start():
                await start()
                bound.set()
                await release.wait()

            server.start = delayed_start
            servers.append(server)
            return server

        monkeypatch.setattr(grpc.aio, "server", delayed_server)
        task = asyncio.create_task(launcher.main(options))
        try:
            await asyncio.wait_for(bound.wait(), 2)
            assert runtime[0][0]._heartbeat_server is None
            if shutdown == "cancel":
                task.cancel()
                await asyncio.sleep(0)
            else:
                signal.raise_signal(signal.SIGTERM)
                await asyncio.sleep(0)
            release.set()
            if shutdown == "cancel":
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 2)
            else:
                assert await asyncio.wait_for(task, 2) == 0
            await asyncio.wait_for(servers[0].wait_for_termination(), 2)
            assert_closed(runtime)
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            for server in servers:
                await server.stop(0)
    try:
        asyncio.run(scenario())
    finally:
        gate.close()
    assert "Controller active" not in capsys.readouterr().out


def test_controller_module_has_explicit_cli_without_import_side_effects():
    result = subprocess.run([sys.executable, "-B", "-m", "cranelock.run_controller", "--help"],
        capture_output=True, text=True, timeout=5,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    assert result.returncode == 0
    assert all(option in result.stdout for option in (
        "--gate-config", "--controller-key", "--gate-address", "--listen", "--gate-timeout",
        "--gate-tls-ca", "--gate-tls-cert", "--gate-tls-key", "--tls-ca", "--tls-cert", "--tls-key"))
