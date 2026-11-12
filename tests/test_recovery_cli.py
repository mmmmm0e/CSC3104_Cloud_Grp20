import json
import os
import socket
import subprocess
import sys
import time

import grpc
import pytest

from cranelock_gate.server import RunningServer
from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc


# verifies the container entry point really enables durable recovery voting
@pytest.mark.parametrize("recovery_style", ["explicit", "alias"])
def test_recovery_cli_processes_collect_majority(h, tmp_path, recovery_style):
    assert h.install()["allowed"]
    addresses = {}
    for member in ("crane-2", "crane-3"):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            addresses[member] = f"127.0.0.1:{sock.getsockname()[1]}"
    gate = RunningServer(h.gate, "127.0.0.1:0")
    processes, streams, channels = [], [], []
    try:
        for member in addresses:
            key_path = tmp_path / f"{member}-key.json"
            key_path.write_text(json.dumps({
                member: h.keys[member].private_bytes_raw().hex()
            }), encoding="utf-8")
            args = [sys.executable, "-B", "-u", "-m", "cranelock", "run", "--id", member, "--listen", addresses[member],
                "--holder", "crane-1", "--epoch", "0", "--interval", ".1", "--timeout", ".8",
                "--state-file", str(tmp_path / f"{member}.json"), "--gate-address", f"127.0.0.1:{gate.port}",
                "--gate-config", str(tmp_path / "config.json"),
                "--gate-keys", str(key_path)]
            if recovery_style == "alias":
                args += ["--enable-recovery"]
            else:
                args += ["--recovery-config", str(tmp_path / "config.json"),
                         "--recovery-key", str(key_path)]
            for peer in h.config.members:
                if peer != member:
                    args.extend(["--peer", f"{peer}={addresses.get(peer, '127.0.0.1:1')}"])
            stream = (tmp_path / f"{member}.log").open("w", encoding="utf-8")
            streams.append(stream)
            processes.append(subprocess.Popen(args, stdout=stream, stderr=stream,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0))
        # Interpreter/import/startup time is not the recovery budget. Require
        # actual listening, reconciled agent RPCs before timing the assertion.
        stubs = {}
        for member, address in addresses.items():
            channel = grpc.insecure_channel(address)
            channels.append(channel)
            stubs[member] = rpc.CraneAgentStub(channel)
        startup_deadline = time.monotonic() + 30
        ready = set()
        while len(ready) < len(stubs) and time.monotonic() < startup_deadline:
            assert all(process.poll() is None for process in processes)
            for member, stub in stubs.items():
                try:
                    state = stub.GetAgentState(pb.Empty(), timeout=.5)
                    if state.agent_id == member and state.mode == pb.LOCAL:
                        ready.add(member)
                except grpc.RpcError as error:
                    if error.code() not in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
                        raise
            time.sleep(.05)
        assert ready == set(stubs), "Recovery CLI agents did not become RPC-ready"
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline and h.state["epoch"] == 0:
            assert all(process.poll() is None for process in processes)
            time.sleep(.05)
        assert h.state["epoch"] == 1 and h.state["holder"] == "crane-2"
        for member in addresses:
            saved = json.loads((tmp_path / f"{member}.json").read_text())
            assert saved["vote_proposals"]["1"]["holder"] == "crane-2"
        assert h.grant()["reason"] == "LANE_UNKNOWN"
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=5)
        for stream in streams:
            stream.close()
        for channel in channels:
            channel.close()
        gate.close()
