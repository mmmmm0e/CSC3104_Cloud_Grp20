import json
import os
import socket
import subprocess
import sys

import grpc

from cranelock.controller import CentralController
from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from cranelock_gate.server import RunningServer


# proves reconciliation crosses process boundaries over grpc
def test_controller_reconciles_independent_agent_process(h, tmp_path):
    assert h.install(mode="CENTRAL")["allowed"]
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        address = f"127.0.0.1:{reserved.getsockname()[1]}"
    gate = RunningServer(h.gate, "127.0.0.1:0")
    target = f"127.0.0.1:{gate.port}"
    log_path = tmp_path / "network-agent.log"
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    process = None
    try:
        with log_path.open("w", encoding="utf-8") as log:
            process = subprocess.Popen([sys.executable, "-m", "cranelock", "run",
                "--id", "crane-1", "--holder", "crane-1", "--epoch", "0", "--listen", address,
                "--peer", "crane-2=127.0.0.1:1", "--peer", "crane-3=127.0.0.1:1",
                "--state-file", str(tmp_path / "remote-agent.json"),
                "--gate-address", target, "--gate-config", str(tmp_path / "config.json"),
                "--gate-keys", str(tmp_path / "private-keys.json")],
                stdout=log, stderr=log, creationflags=flags)
            with grpc.insecure_channel(address) as channel, grpc.insecure_channel(target) as gate_channel:
                grpc.channel_ready_future(channel).result(timeout=10)
                controller = CentralController(h.config, h.keys["controller"], gate_channel)
                result = controller.install_central("crane-2")
                assert result["allowed"]
                reply = subprocess.run([sys.executable, "-m", "cranelock.controller",
                    "--agent", f"crane-1={address}", "--reconcile", "--gate-address", target,
                    "--gate-config", str(tmp_path / "config.json"),
                    "--gate-keys", str(tmp_path / "private-keys.json")],
                    capture_output=True, text=True, timeout=10, creationflags=flags)
                assert reply.returncode == 0, reply.stdout + reply.stderr
                assert json.loads(reply.stdout)["holder"] == "crane-2"
                state = rpc.CraneAgentStub(channel).GetAgentState(pb.Empty(), timeout=2)
                assert state.token_holder == "crane-2" and not state.has_token
                assert state.revision == result["state"]["revision"]
    finally:
        if process:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=5)
        gate.close()
    events = [json.loads(line) for line in log_path.read_text().splitlines() if line.startswith("{")]
    assert any(event["event"] == "NETWORK_RECONCILIATION_ACCEPTED" for event in events)
