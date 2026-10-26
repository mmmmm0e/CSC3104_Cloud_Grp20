"""Generate development identities and run a real gRPC standalone demonstration."""
import argparse
import json
import time
from dataclasses import asdict
from pathlib import Path

import grpc
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from .client import Client, certificate, may_enter
from .config import Config
from .core import Gate
from .server import RunningServer
from .storage import initialize


def create_demo_config(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    config_file = directory / "config.json"
    private_file = directory / "private-keys.json"
    if config_file.exists() or private_file.exists():
        raise FileExistsError("Demo config already exists; refusing to overwrite identities")
    keys = {identity: Ed25519PrivateKey.generate() for identity in
            ["crane-1", "crane-2", "crane-3", "controller", "simulator", "operator"]}
    public = {identity: key.public_key().public_bytes_raw().hex() for identity, key in keys.items()}
    config = Config("lane-1", "group20-demo-v1",
                    {identity: public[identity] for identity in ["crane-1", "crane-2", "crane-3"]},
                    "controller", "simulator", "operator",
                    {identity: public[identity] for identity in ["controller", "simulator", "operator"]})
    config_file.write_text(json.dumps(asdict(config), indent=2), encoding="utf-8")
    private_file.write_text(json.dumps({identity: key.private_bytes_raw().hex()
                                       for identity, key in keys.items()}, indent=2), encoding="utf-8")
    return config, keys


def load_keys(path):
    return {identity: Ed25519PrivateKey.from_private_bytes(bytes.fromhex(value))
            for identity, value in json.loads(Path(path).read_text()).items()}


def scenario(client):
    results = []

    def check(label, result, expected):
        if result["reason"] != expected:
            raise AssertionError(f"{label}: expected {expected}, got {result}")
        results.append(dict(scenario=label, allowed=result["allowed"], reason=result["reason"]))
        return result

    state = client.state()
    cert = certificate(client.config, client.keys, state, "crane-1", state["epoch"] + 1)
    result = check("majority establishes authority", client.call("crane-1", "InstallAuthority",
                   certificate=cert, expected_epoch=state["epoch"], expected_revision=state["revision"]), "OK")
    state = result["state"]
    check("unknown lane blocks holder", client.call("crane-1", "RequestGrant",
          epoch=state["epoch"], revision=state["revision"]), "LANE_UNKNOWN")
    state = client.state()
    check("trusted simulator confirms clearance", client.call("simulator", "ReportLaneState",
          session_id=state["session_id"], sequence=state["observation_sequence"] + 1,
          observation_revision=state["observation_revision"],
          observed_at_ms=int(time.time() * 1000), lane_state="CLEAR"), "OK")
    state = client.state()
    grant = check("valid holder receives grant", client.call("crane-1", "RequestGrant",
          epoch=state["epoch"], revision=state["revision"]), "OK")["grant"]
    state = client.state()
    entry = check("entry boundary accepts grant", client.call("simulator", "ValidateEntry",
          epoch=state["epoch"], revision=state["revision"], holder="crane-1",
          grant_id=grant["id"], session_id=state["session_id"]), "OK")
    assert may_enter(entry)
    check("occupied lane blocks another grant", client.call("crane-1", "RequestGrant",
          epoch=state["epoch"], revision=state["revision"]), "LANE_BUSY")
    cert = certificate(client.config, client.keys, state, "crane-2", state["epoch"] + 1)
    new = check("quorum recovers while lane occupied", client.call("crane-2", "InstallAuthority",
                certificate=cert, expected_epoch=state["epoch"], expected_revision=state["revision"]), "OK")
    check("stale holder fenced", client.call("crane-1", "RequestGrant",
          epoch=state["epoch"], revision=state["revision"]), "STALE_EPOCH")
    state = new["state"]
    check("new holder also waits for physical clearance", client.call("crane-2", "RequestGrant",
          epoch=state["epoch"], revision=state["revision"]), "LANE_BUSY")
    check("operator confirms clearance", client.call("operator", "ClearLane",
          session_id=state["session_id"], sequence=state["observation_sequence"] + 1,
          observation_revision=state["observation_revision"],
          observed_at_ms=int(time.time() * 1000), reason="Simulation confirms previous crane removed"), "OK")
    check("replacement holder receives safe grant", client.call("crane-2", "RequestGrant",
          epoch=state["epoch"], revision=state["revision"]), "OK")
    return results


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)
    setup = sub.add_parser("setup")
    setup.add_argument("--directory", default="demo-data")
    run = sub.add_parser("run")
    run.add_argument("--directory", default="demo-data")
    run.add_argument("--target", help="Use an already running DEVELOPMENT gRPC server")
    args = parser.parse_args()
    directory = Path(args.directory)
    if args.action == "setup":
        config, _ = create_demo_config(directory)
        initialize(directory / "gate.sqlite", config, time.time())
        print("Development keys/config/database created. Private keys are simulation fixtures only.")
        return
    config = Config.load(directory / "config.json")
    keys = load_keys(directory / "private-keys.json")
    server = None
    if not args.target:
        server = RunningServer(Gate(directory / "gate.sqlite", config), "127.0.0.1:0")
        target = f"127.0.0.1:{server.port}"
    else:
        target = args.target
    try:
        with grpc.insecure_channel(target) as channel:
            grpc.channel_ready_future(channel).result(timeout=5)
            client = Client(channel, config, keys)
            print(json.dumps(scenario(client), indent=2))
    finally:
        if server:
            server.close()


if __name__ == "__main__":
    main()
