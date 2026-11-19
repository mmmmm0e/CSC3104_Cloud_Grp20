"""Local simulation deployment handoff. Run from any directory with the project venv."""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.workload_credentials import PORT as WORKLOAD_PORT, host_endpoint, provision_workload, require_credentials

BASE = ROOT / "demo-data/integrated"
SERVICES = ("gate", "controller", "crane-1", "crane-2", "crane-3")
CRANE_IDS = SERVICES[2:]
PEER_TLS = BASE / "tls/agents"
PEER_TLS_FILES = ("ca.pem", "monitor.pem", "monitor-key.pem") + tuple(
    f"{crane}{suffix}.pem" for crane in CRANE_IDS for suffix in ("", "-key"))
COMPOSE = ["docker", "compose", "-p", "cranelock-integrated",
           "-f", str(ROOT / "deploy/compose.integrated.yaml"),
           "-f", str(ROOT / "deploy/compose.integrated.agents.yaml"),
           "-f", str(ROOT / "deploy/compose.monitoring.yaml")]


@contextmanager
def validation_lock():
    directory = ROOT / 'run/platform-validation'
    directory.mkdir(parents=True, exist_ok=True)
    stream = (directory / 'validator.lock').open('a+')
    try:
        stream.write('0'); stream.flush(); stream.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError('Another platform validator is active; do not overlap faults') from None
        yield
    finally:
        stream.close()


def compose(*args, **kwargs):
    return subprocess.run(COMPOSE + list(args), cwd=ROOT, check=True, **kwargs)


def provision(directory=BASE, *, include_test_identity=False):
    from cranelock_gate.demo import create_demo_config
    from cranelock_gate.tls import write
    directory = Path(directory)
    if directory.exists():
        raise ValueError(f"Refusing to overwrite existing experiment: {directory}")
    config, keys = create_demo_config(directory)
    signing = directory / "signing"
    signing.mkdir()
    for identity, key in keys.items():
        (signing / f"{identity}.json").write_text(
            json.dumps({identity: key.private_bytes_raw().hex()}) + "\n")
    for service in ("gate", "controller"):
        write(directory / "tls" / service, names=[service, "localhost", "127.0.0.1"])
    provision_workload(directory, include_test_identity=include_test_identity)
    provision_peer_tls(directory)
    print("Development configuration and credentials created. No authority or clearance installed.")
    return config


def provision_peer_tls(directory=BASE):
    """Issue one crane-to-crane mTLS identity per crane, plus a read-only monitor.

    Adds only tls/agents/ to an existing experiment; refuses to overwrite it."""
    from cranelock_gate.tls import write_agents
    directory = Path(directory)
    if not (directory / "config.json").is_file():
        raise ValueError("Provision the experiment first")
    return write_agents(directory / "tls/agents", CRANE_IDS)


def require_peer_tls(directory=BASE):
    target = Path(directory) / "tls/agents"
    if not all((target / name).is_file() for name in PEER_TLS_FILES):
        raise ValueError("Crane-to-crane TLS identities missing: run scripts/integrated.py provision-peer-tls")
    return target


def peer_channel(crane, identity="monitor", address=None):
    """Host channel to a crane's peer port over mutual TLS.

    ``identity`` is the certificate presented: the read-only ``monitor`` for
    state queries, or a crane ID when acting as that crane (cranes reject peer
    messages whose claimed sender differs from the certificate)."""
    import grpc
    directory = require_peer_tls()
    credentials = grpc.ssl_channel_credentials(
        (directory / "ca.pem").read_bytes(), (directory / f"{identity}-key.pem").read_bytes(),
        (directory / f"{identity}.pem").read_bytes())
    # Pin the expected crane identity rather than the dialled host name.
    return grpc.secure_channel(address or endpoints()["agents"][crane]["host"], credentials,
                               options=(("grpc.ssl_target_name_override", crane),))


def state_command(crane):
    """In-container `cranelock state` over the crane's own mounted peer identity."""
    return ["python", "-m", "cranelock", "state", "127.0.0.1:50051",
            "--tls-ca", "/peer-tls/ca.pem", "--tls-cert", "/peer-tls/agent.pem",
            "--tls-key", "/peer-tls/agent-key.pem", "--server-name", crane]


def monitoring_secret():
    import secrets
    path = BASE / "grafana-password"
    if not (BASE / "config.json").is_file():
        raise ValueError("Provision the experiment first")
    if not path.exists():
        with path.open("x") as stream:
            stream.write(secrets.token_urlsafe(24) + "\n")


def gate_client():
    import grpc
    from cranelock_gate.client import Client
    from cranelock_gate.config import Config
    from cranelock_gate.demo import load_keys
    tls = BASE / "tls/gate"
    credentials = grpc.ssl_channel_credentials(
        root_certificates=(tls / "ca.pem").read_bytes(),
        private_key=(tls / "client-key.pem").read_bytes(),
        certificate_chain=(tls / "client.pem").read_bytes())
    channel = grpc.secure_channel("localhost:50061", credentials)
    client = Client(channel, Config.load(BASE / "config.json"),
                    load_keys(BASE / "signing/controller.json"))
    return channel, client


def simulator_gate_client():
    """Gate mTLS plus only the simulator's signing key, for physical models."""
    import grpc
    from cranelock_gate.client import Client
    from cranelock_gate.config import Config
    from cranelock_gate.demo import load_keys
    tls = BASE / "tls/gate"
    credentials = grpc.ssl_channel_credentials(
        root_certificates=(tls / "ca.pem").read_bytes(),
        private_key=(tls / "client-key.pem").read_bytes(),
        certificate_chain=(tls / "client.pem").read_bytes())
    channel = grpc.secure_channel("localhost:50061", credentials)
    try:
        config = Config.load(BASE / "config.json")
        keys = load_keys(BASE / "signing" / f"{config.simulator_id}.json")
        if set(keys) != {config.simulator_id}:
            raise ValueError("Simulator signing file must contain only its identity")
        return channel, Client(channel, config, keys)
    except Exception:
        channel.close()
        raise


def bootstrap():
    from cranelock_gate.client import certificate
    channel, client = gate_client()
    try:
        state = client.state()
        if state["epoch"] != -1:
            raise ValueError("Authority already installed; use up, not bootstrap/init.")
        cert = certificate(client.config, client.keys, state, "crane-1", 0,
                           mode="CENTRAL", signers=[client.config.controller_id])
        result = client.call(client.config.controller_id, "InstallAuthority",
                             certificate=cert, expected_epoch=state["epoch"],
                             expected_revision=state["revision"])
        if not result["allowed"]:
            raise ValueError(f"Bootstrap rejected: {result['reason']}")
        print("Installed CENTRAL epoch 0, holder crane-1; lane remains UNKNOWN.")
    finally:
        channel.close()


def endpoints():
    return {
        "project": "cranelock-integrated",
        "gate": {"host": "localhost:50061", "internal": "gate:50051", "tls": True},
        "controller": {"host": "localhost:50062", "internal": "controller:50052", "tls": True},
        "agents": {f"crane-{i}": {"host": f"127.0.0.1:{50100+i}",
                                     "internal": f"crane-{i}:50051", "tls": True}
                   for i in (1, 2, 3)},
        "workload_agents": {crane: {"host": host_endpoint(crane),
                                    "internal": f"{crane}:{WORKLOAD_PORT}", "tls": True}
                            for crane in SERVICES[2:]},
        "workload_config": str(BASE / "workload/config.json"),
        "containers": {s: f"cranelock-integrated-{s}-1" for s in SERVICES},
        "config": str(BASE / "config.json"),
        "signing_directory": str(BASE / "signing"),
        "gate_tls_directory": str(BASE / "tls/gate"),
        "controller_tls_directory": str(BASE / "tls/controller"),
        "peer_tls_directory": str(PEER_TLS),
        "gate_metrics": "http://127.0.0.1:9190/metrics",
    }


def check():
    """Check real host-published endpoints; no workload or physical clearance."""
    import grpc
    from cranelock import protocol_pb2 as pb
    from cranelock import protocol_pb2_grpc as rpc
    channel, client = gate_client()
    try:
        authority = client.state()
        if authority["mode"] != "CENTRAL":
            raise ValueError("Expected CENTRAL baseline; inspect state before running checks.")
        for identity, endpoint in endpoints()["agents"].items():
            with peer_channel(identity) as agent_channel:
                state = rpc.CraneAgentStub(agent_channel).GetAgentState(pb.Empty(), timeout=3)
            if (state.agent_id != identity or state.epoch != authority["epoch"]
                    or state.revision != authority["revision"]
                    or state.token_holder != authority["holder"] or state.mode != pb.CENTRAL
                    or state.has_token != (identity == authority["holder"])
                    or any(peer.status != "AVAILABLE" for peer in state.peers)):
                raise ValueError(f"{identity}: authority or peer convergence check failed; run state.")
            # A client without a crane-CA certificate must not reach the peer port.
            with grpc.insecure_channel(endpoint["host"]) as plaintext:
                try:
                    rpc.CraneAgentStub(plaintext).GetAgentState(pb.Empty(), timeout=3)
                except grpc.RpcError:
                    pass
                else:
                    raise ValueError(f"{identity}: peer port accepted a plaintext client")
            print(f"PASS {identity}: peer mutual TLS, authority, token ownership and peer availability")
        tls = BASE / "tls/controller"
        credentials = grpc.ssl_channel_credentials(
            root_certificates=(tls / "ca.pem").read_bytes(),
            private_key=(tls / "client-key.pem").read_bytes(),
            certificate_chain=(tls / "client.pem").read_bytes())
        with grpc.secure_channel("localhost:50062", credentials) as controller_channel:
            response = rpc.CraneControllerStub(controller_channel).Heartbeat(
                pb.ControllerHeartbeatRequest(agent_id="crane-1"), timeout=3)
        if not response.available or response.controller_id != client.config.controller_id:
            raise ValueError("Controller heartbeat identity/availability check failed")
        final = client.state()
        if any(final[key] != authority[key] for key in ("epoch", "revision", "holder", "mode")):
            raise ValueError("Authority changed during endpoint checks; inspect logs")
        print(f"PASS gate and controller host mutual TLS; CENTRAL epoch {final['epoch']}, revision {final['revision']}")
        print("No physical crossings exercised; lane occupancy unchanged.")
    finally:
        channel.close()


def record_fault(action, service):
    output = ROOT / "run/platform-validation"
    output.mkdir(parents=True, exist_ok=True)
    record = {"action": action, "service": service,
              "started_unix_seconds": time.time(),
              "started_monotonic_seconds": time.monotonic()}
    try:
        if action == "kill":
            subprocess.run(["docker", "kill", "--signal", "SIGKILL", f"cranelock-integrated-{service}-1"], check=True)
        else:
            # Compose start may also start dependencies. A fault action must touch
            # exactly one service, especially while the controller is stopped.
            subprocess.run(["docker", action, f"cranelock-integrated-{service}-1"], check=True)
        record["success"] = True
    except subprocess.CalledProcessError:
        record["success"] = False
        raise
    finally:
        record["finished_unix_seconds"] = time.time()
        record["finished_monotonic_seconds"] = time.monotonic()
        with (output / "fault-events.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("init", "bootstrap", "up", "status", "state", "check", "endpoints", "logs",
                 "provision-peer-tls"):
        commands.add_parser(name)
    for name in ("provision", "provision-workload"):
        command = commands.add_parser(name)
        command.add_argument("--with-test-identity", action="store_true",
                             help="Issue a separate, unallowlisted client for local auth validation")
    for name in ("stop", "start", "restart", "kill"):
        command = commands.add_parser(name)
        command.add_argument("service", choices=SERVICES, nargs="?" if name == "stop" else None)
    args = parser.parse_args(argv)
    try:
        if args.command == "provision":
            provision(include_test_identity=args.with_test_identity)
        elif args.command == "provision-workload":
            print(provision_workload(BASE, include_test_identity=args.with_test_identity))
        elif args.command == "provision-peer-tls":
            print(provision_peer_tls(BASE))
        elif args.command == "init":
            # Explicitly new volumes only. initialize() refuses an existing database.
            compose("build", "gate", "controller")
            compose("run", "--rm", "--no-deps", "gate", "init", "--config",
                    "/config/config.json", "--database", "/data/gate.sqlite")
            compose("up", "-d", "--wait", "--wait-timeout", "60", "gate")
            bootstrap()
        elif args.command == "bootstrap":
            bootstrap()
        elif args.command == "up":
            require_credentials(BASE)
            require_peer_tls(BASE)
            monitoring_secret()
            compose("build", "gate", "controller")
            compose("up", "-d", "--wait", "--wait-timeout", "60")
            print("Containers started; run state to verify authority and peer convergence.")
        elif args.command == "status":
            compose("ps", "-a")
        elif args.command == "check":
            check()
        elif args.command == "state":
            channel, client = gate_client()
            try:
                print(json.dumps({"gate": client.state()}, indent=2), flush=True)
            finally:
                channel.close()
            for service in SERVICES[2:]:
                compose("exec", "-T", service, *state_command(service))
        elif args.command == "endpoints":
            print(json.dumps(endpoints(), indent=2))
        elif args.command == "logs":
            output = ROOT / "run/platform-validation"
            output.mkdir(parents=True, exist_ok=True)
            path = output / f"integrated-{time.time_ns()}.log"
            with path.open("w") as stream:
                compose("logs", "--no-color", stdout=stream, stderr=subprocess.STDOUT)
            print(path)
        elif args.command == "stop" and args.service is None:
            compose("stop")
            print("Integrated containers stopped; persistent volumes retained.")
        else:
            record_fault(args.command, args.service)
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
