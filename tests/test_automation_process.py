import json
import os
import socket
import subprocess
import sys
import time

import grpc

from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from cranelock_gate.server import RunningServer


# allocates loopback endpoints for independent service processes
def free_address():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{listener.getsockname()[1]}"


# proves the shipped clis recover and return without an integration caller
def test_automatic_recovery_and_return_across_processes(h, tmp_path):
    assert h.install(mode="CENTRAL")["allowed"]
    assert h.observe("OCCUPIED")["allowed"]
    addresses = {identity: free_address() for identity in h.config.members}
    controller_address = free_address()
    gate = RunningServer(h.gate, "127.0.0.1:0")
    processes, logs, channels = [], [], []
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    gate_options = ["--gate-address", f"127.0.0.1:{gate.port}", "--gate-config",
        str(tmp_path / "config.json"), "--gate-keys", str(tmp_path / "private-keys.json")]

    # retains subprocess output so a failed transition remains diagnosable
    def launch(arguments, label):
        log = (tmp_path / f"{label}.log").open("w", encoding="utf-8")
        logs.append(log)
        process = subprocess.Popen([sys.executable, *arguments], stdout=log, stderr=log, creationflags=flags)
        processes.append(process)
        return process

    # waits for committed gate state and checks subprocess health
    def wait_state(mode, epoch):
        deadline = time.monotonic() + 18
        while time.monotonic() < deadline:
            state = h.state
            if state["mode"] == mode and state["epoch"] == epoch:
                return state
            time.sleep(0.05)
        diagnostics = {path.name: path.read_text()[-6000:] for path in tmp_path.glob("*.log")}
        raise AssertionError(f"Did not reach {mode}/{epoch}: {h.state}; {diagnostics}")

    controller_args = ["-m", "cranelock.controller", "--serve", "--listen", controller_address,
        "--interval", "0.1", "--timeout", "0.5", *gate_options]
    for identity, address in addresses.items():
        controller_args.extend(["--agent", f"{identity}={address}"])
    try:
        controller = launch(controller_args, "controller-before")
        with grpc.insecure_channel(controller_address) as channel:
            grpc.channel_ready_future(channel).result(timeout=10)
        agents = {}
        for identity, address in addresses.items():
            arguments = ["-m", "cranelock", "run", "--id", identity, "--listen", address,
                "--holder", "crane-1", "--epoch", "0", "--mode", "CENTRAL", "--interval", "0.1",
                "--timeout", "0.8", "--state-file", str(tmp_path / f"{identity}.json"),
                "--auto-recover", "--controller-address", controller_address, *gate_options]
            for peer, endpoint in addresses.items():
                if peer != identity:
                    arguments.extend(["--peer", f"{peer}={endpoint}"])
            agents[identity] = launch(arguments, identity)
        stubs = {}
        for identity, address in addresses.items():
            channel = grpc.insecure_channel(address)
            channels.append(channel)
            grpc.channel_ready_future(channel).result(timeout=10)
            stubs[identity] = rpc.CraneAgentStub(channel)
        # lets all participants observe healthy peers before the injected outage
        time.sleep(1)
        controller.terminate()
        controller.wait(timeout=5)
        state = wait_state("LOCAL", 1)
        assert state["holder"] == "crane-1" and state["lane_state"] == "OCCUPIED"
        agents["crane-1"].terminate()
        agents["crane-1"].wait(timeout=5)
        state = wait_state("LOCAL", 2)
        assert state["holder"] == "crane-2" and state["lane_state"] == "OCCUPIED"
        # queues work at the accepted local holder before controller return
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            reply = stubs["crane-2"].TokenRequest(pb.TokenRequestMessage(agent_id="crane-3",
                epoch=state["epoch"], revision=state["revision"]), timeout=1)
            if reply.accepted:
                break
            time.sleep(0.05)
        assert reply.accepted
        launch(controller_args, "controller-after")
        returned = wait_state("CENTRAL", 2)
        assert returned["holder"] == "crane-2" and returned["revision"] > state["revision"]
        assert returned["lane_state"] == "OCCUPIED"
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            holder = stubs["crane-2"].GetAgentState(pb.Empty(), timeout=1)
            if holder.mode == pb.CENTRAL and holder.revision == returned["revision"]:
                break
            time.sleep(0.05)
        assert holder.mode == pb.CENTRAL and list(holder.request_queue) == ["crane-3"]
        assert all(process.poll() is None for identity, process in agents.items() if identity != "crane-1")
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=5)
        for channel in channels:
            channel.close()
        for log in logs:
            log.close()
        gate.close()


# bootstraps only an empty gate before serving the controller
def test_controller_service_bootstraps_empty_gate(h, tmp_path):
    gate = RunningServer(h.gate, "127.0.0.1:0")
    address = free_address()
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    process = None
    try:
        with (tmp_path / "bootstrap.log").open("w", encoding="utf-8") as log:
            process = subprocess.Popen([sys.executable, "-m", "cranelock.controller", "--serve",
                "--listen", address, "--bootstrap-holder", "crane-1", "--agent", "crane-1=127.0.0.1:1",
                "--gate-address", f"127.0.0.1:{gate.port}", "--gate-config", str(tmp_path / "config.json"),
                "--gate-keys", str(tmp_path / "private-keys.json")], stdout=log, stderr=log, creationflags=flags)
            with grpc.insecure_channel(address) as channel:
                grpc.channel_ready_future(channel).result(timeout=10)
            assert h.state["epoch"] == 0 and h.state["mode"] == "CENTRAL"
            assert h.state["holder"] == "crane-1" and h.state["lane_state"] == "UNKNOWN"
    finally:
        if process:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=5)
        gate.close()


# a controller return cannot overwrite authority changed since its snapshot
def test_controller_return_rejects_stale_snapshot(h):
    from cranelock.controller import CentralController

    assert h.install()["allowed"]
    observed = h.state
    assert h.install(holder="crane-2")["allowed"]
    accepted = h.state
    gate = RunningServer(h.gate, "127.0.0.1:0")
    try:
        with grpc.insecure_channel(f"127.0.0.1:{gate.port}") as channel:
            controller = CentralController(h.config, h.keys["controller"], channel)
            result = controller.install_central("crane-1", expected_state=observed)
            assert not result["allowed"] and result["reason"] == "STALE_EPOCH"
            assert h.state["holder"] == accepted["holder"] and h.state["epoch"] == accepted["epoch"]
            assert h.state["mode"] == "LOCAL"
    finally:
        gate.close()
