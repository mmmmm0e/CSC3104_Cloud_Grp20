import argparse
from dataclasses import asdict, replace
import json
from pathlib import Path
import shutil
import subprocess
import time
import uuid

import grpc

from cranelock import protocol_pb2 as pb
from cranelock.protocol_pb2_grpc import CraneAgentStub
from cranelock_gate.client import Client, certificate
from cranelock_sim.experiment import new_config


# prepares isolated identities, persistent volumes and six real service containers
def prepare(output):
    root = Path(__file__).resolve().parents[1]
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    config, keys = new_config(5)
    config = replace(config, observation_timeout_seconds=120)
    public = output / "public"
    public.mkdir()
    (public / "config.json").write_text(json.dumps(asdict(config), indent=2), encoding="utf-8")
    project = "cranelock-recovery-" + uuid.uuid4().hex[:10]
    image = project + ":local"
    data = dict(type="volume", source="gate-data", target="/data")
    mount = dict(type="bind", source=str(public), target="/config", read_only=True)
    base = dict(image=image, build=dict(context=str(root), dockerfile="Dockerfile.agent"),
        entrypoint=["python", "-m", "cranelock_gate.server"], read_only=True,
        tmpfs=["/tmp"], volumes=[data, mount])
    services = {
        "init": dict(base, command=["init", "--config", "/config/config.json", "--database", "/data/gate.sqlite"]),
        "gate": dict(base, command=["serve", "--config", "/config/config.json", "--database", "/data/gate.sqlite",
            "--bind", "0.0.0.0:50051", "--metrics-port", "0", "--allow-insecure-development"],
            depends_on={"init": dict(condition="service_completed_successfully")},
            ports=[f"127.0.0.1::{port}" for port in range(50051, 50057)],
            healthcheck=dict(test=["CMD", "python", "-m", "cranelock_gate.healthcheck"], interval="2s", timeout="2s", retries=10))}
    volumes = {"gate-data": {}}
    ports = {member: 50052 + index for index, member in enumerate(config.members)}
    for member, port in ports.items():
        identity = output / member
        identity.mkdir()
        (identity / "config.json").write_text(json.dumps(asdict(config)), encoding="utf-8")
        (identity / "private-keys.json").write_text(json.dumps({member: keys[member].private_bytes_raw().hex()}), encoding="utf-8")
        arguments = ["run", "--id", member, "--listen", f"0.0.0.0:{port}", "--holder", "crane-1",
            "--epoch", "0", "--mode", "LOCAL", "--state-file", "/data/state.json", "--interval", "0.1",
            "--timeout", "0.8", "--gate-address", "127.0.0.1:50051", "--gate-config", "/config/config.json",
            "--gate-keys", "/config/private-keys.json", "--recovery-config", "/config/config.json", "--recovery-key", "/config/private-keys.json"]
        for peer, endpoint in ports.items():
            if peer != member:
                arguments.extend(["--peer", f"{peer}=127.0.0.1:{endpoint}"])
        services[member] = dict(image=image, network_mode="service:gate", command=arguments,
            depends_on={"gate": dict(condition="service_healthy")}, read_only=True, tmpfs=["/tmp"],
            volumes=[dict(type="volume", source=member + "-data", target="/data"),
                     dict(type="bind", source=str(identity), target="/config", read_only=True)])
        volumes[member + "-data"] = {}
    path = output / "compose.json"
    path.write_text(json.dumps(dict(name=project, services=services, volumes=volumes), indent=2), encoding="utf-8")
    return output, path, config, keys, ports


# validates recovery after two crashes, loss of quorum and a persisted restart
def validate(output, *, prepare_only=False):
    if not prepare_only and shutil.which("docker") is None:
        raise RuntimeError("Docker CLI is unavailable. Use --prepare-only to generate the container experiment.")
    output, compose_file, config, keys, ports = prepare(output)
    record = dict(prepared=True, runtime_validated=False, passed=False, checks=[],
        scope="Five separate crane containers and a gate, sharing the gate network namespace. Loopback development transport; no network partition or remote TLS claim.")
    channels = []
    prefix = ["docker", "compose", "-f", str(compose_file)]

    # records docker commands without printing generated private keys
    def compose(*arguments, check=True, timeout=120):
        result = subprocess.run([*prefix, *arguments], capture_output=True, text=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(f"Docker command failed: {' '.join(arguments)}\n{result.stderr}")
        return result.stdout

    # polls actual rpc state with a bounded deadline
    def wait(predicate, label, timeout=25):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if predicate():
                    record["checks"].append(label)
                    print(f"[PASS] {label}", flush=True)
                    return
            except grpc.RpcError:
                pass
            time.sleep(.1)
        raise RuntimeError(f"Timed out: {label}")

    # uses dynamically published loopback host ports
    def channel_for(port):
        target = compose("port", "gate", str(port)).strip().splitlines()[0]
        channel = grpc.insecure_channel(target)
        channels.append(channel)
        grpc.channel_ready_future(channel).result(timeout=20)
        return channel

    try:
        if prepare_only:
            return record
        compose("build", "gate", timeout=600)
        compose("up", "-d", "--no-build", *config.members)
        client = Client(channel_for(50051), config, keys, timeout=1)
        stubs = {member: CraneAgentStub(channel_for(port)) for member, port in ports.items()}
        state = client.state()
        bootstrap = certificate(config, keys, state, "crane-1", 0)
        result = client.call("crane-1", "InstallAuthority", certificate=bootstrap,
            expected_epoch=state["epoch"], expected_revision=state["revision"])
        assert result["allowed"], result["reason"]
        state = client.state()
        observed = client.call("simulator", "ReportLaneState", session_id=state["session_id"],
            observation_revision=state["observation_revision"], sequence=state["observation_sequence"] + 1,
            observed_at_ms=int(time.time() * 1000), lane_state="OCCUPIED")
        assert observed["allowed"], observed["reason"]
        wait(lambda: all((state.epoch, state.revision, state.mode) == (0, 1, pb.LOCAL)
                         for state in (stub.GetAgentState(pb.Empty(), timeout=1) for stub in stubs.values())),
             "all five agent services started")
        time.sleep(1)
        compose("stop", "crane-1", "crane-5")
        wait(lambda: client.state()["epoch"] == 1 and client.state()["holder"] == "crane-2",
             "three survivors recover after two container stops")
        for member in ("crane-2", "crane-3", "crane-4"):
            saved = json.loads(compose("exec", "-T", member, "cat", "/data/state.json"))
            assert saved["vote_proposals"]["1"]["holder"] == "crane-2"
        assert client.call("crane-1", "RequestGrant", epoch=0, revision=1)["reason"] == "STALE_EPOCH"
        current = client.state()
        assert not client.call("crane-2", "RequestGrant", epoch=1, revision=current["revision"])["allowed"]
        record["checks"].append("stale holder and occupied-lane admissions rejected")
        compose("stop", "crane-2")
        time.sleep(4)
        assert client.state()["epoch"] == 1 and client.state()["holder"] == "crane-2"
        record["checks"].append("two remaining agents cannot replace authority")
        compose("start", "crane-1")
        wait(lambda: client.state()["epoch"] == 2 and client.state()["holder"] == "crane-1",
             "restored majority completes the identical pending proposal")
        current = client.state()
        assert current["lane_state"] in {"OCCUPIED", "UNKNOWN"}
        assert not client.call("crane-1", "RequestGrant", epoch=2, revision=current["revision"])["allowed"]
        record.update(runtime_validated=True, passed=True, final_state=current)
        return record
    except BaseException as error:
        record["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        if not prepare_only:
            try:
                (output / "containers.log").write_text(compose("logs", "--no-color", check=False), encoding="utf-8")
                (output / "gate-events.jsonl").write_text(compose("exec", "-T", "gate", "python", "-m",
                    "cranelock_gate.server", "export-events", "--config", "/config/config.json",
                    "--database", "/data/gate.sqlite", check=False), encoding="utf-8")
            finally:
                for channel in channels:
                    channel.close()
                compose("down", "--volumes", "--remove-orphans", check=False)
        (output / "summary.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
        print(f"Evidence: {output}", flush=True)


# runs only this experiment's containers and preserves diagnostic evidence
def main():
    parser = argparse.ArgumentParser(description="Validate majority recovery in real isolated Docker containers")
    parser.add_argument("--output", type=Path, default=Path("run") / ("recovery-containers-" + uuid.uuid4().hex[:8]))
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    validate(args.output, prepare_only=args.prepare_only)


if __name__ == "__main__":
    main()
