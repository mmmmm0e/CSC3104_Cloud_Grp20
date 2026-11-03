import json
import os
import socket
import subprocess
import sys
import time

import grpc

from cranelock_gate.server import RunningServer


# verifies the container entry point really enables durable recovery voting
def test_recovery_cli_processes_collect_majority(h, tmp_path):
    assert h.install()["allowed"]
    addresses = {}
    for member in ("crane-2", "crane-3"):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            addresses[member] = f"127.0.0.1:{sock.getsockname()[1]}"
    gate = RunningServer(h.gate, "127.0.0.1:0")
    processes, streams = [], []
    try:
        for member in addresses:
            args = [sys.executable, "-m", "cranelock", "run", "--id", member, "--listen", addresses[member],
                "--holder", "crane-1", "--epoch", "0", "--interval", ".1", "--timeout", ".8",
                "--state-file", str(tmp_path / f"{member}.json"), "--gate-address", f"127.0.0.1:{gate.port}",
                "--gate-config", str(tmp_path / "config.json"), "--gate-keys", str(tmp_path / "private-keys.json"),
                "--enable-recovery"]
            for peer in h.config.members:
                if peer != member:
                    args.extend(["--peer", f"{peer}={addresses.get(peer, '127.0.0.1:1')}"])
            stream = (tmp_path / f"{member}.log").open("w", encoding="utf-8")
            streams.append(stream)
            processes.append(subprocess.Popen(args, stdout=stream, stderr=stream,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0))
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
        gate.close()
