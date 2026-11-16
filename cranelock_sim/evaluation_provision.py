"""Provision isolated evaluation credentials in a separate setup process.

These are ephemeral test credentials, not production PKI. The host account that
starts this process can still read all files; service mounts enforce the Docker
boundary, not isolation between processes running as that same host account.
"""
import base64
import json
from dataclasses import asdict
from pathlib import Path
import shutil
import sys

from cranelock_gate.client import certificate, command
from cranelock_gate.crypto import COMMAND_DOMAIN, canonical
from cranelock_gate.tls import write as write_gate_tls
from .experiment import new_config
from .workload_tls import write as write_workload_tls


def provision(output):
    output = Path(output)
    private = output / ".credentials"
    private.mkdir(mode=0o700)
    config, keys = new_config(3)
    (output / "gate-config.json").write_text(json.dumps(asdict(config), indent=2), encoding="utf-8")
    for role in ("gate", "controller", *config.members, "driver", "provision"):
        (private / role).mkdir(mode=0o700)
    # The private parent prevents other host users from traversing credentials.
    # Individual bind-mounted service roots must be readable by container UID 10001.
    for role in ("gate", "controller", *config.members):
        (private / role).chmod(0o755)
    (output / "state").mkdir(mode=0o700)
    for role in ("gate", "controller", *config.members):
        state = output / "state" / role
        state.mkdir(mode=0o777)
        state.chmod(0o777)
    for identity in ("controller", "simulator", *config.members):
        destination = private / ("driver" if identity == "simulator" else identity)
        (destination / f"{identity}.key.json").write_text(
            json.dumps({identity: keys[identity].private_bytes_raw().hex()}), encoding="utf-8")

    state = dict(epoch=-1, revision=0)
    cert = certificate(config, keys, state, "crane-1", 0, mode="LOCAL")
    payload = command(config, "crane-1", "InstallAuthority", certificate=cert,
                      expected_epoch=-1, expected_revision=0)
    raw = canonical(payload)
    (private / "driver" / "bootstrap-command.json").write_text(json.dumps(dict(
        payload=base64.b64encode(raw).decode(),
        signature=base64.b64encode(keys["crane-1"].sign(COMMAND_DOMAIN + raw)).decode())), encoding="utf-8")

    def distribute(source, destination, files):
        destination.mkdir(parents=True)
        for name in files:
            shutil.copyfile(source / name, destination / name)

    staging = private / "provision"
    gate_tls, controller_tls = staging / "gate-tls", staging / "controller-tls"
    write_gate_tls(gate_tls, names=["gate", "127.0.0.1"])
    write_gate_tls(controller_tls, names=["controller", "127.0.0.1"])
    distribute(gate_tls, private / "gate" / "gate-tls", ["ca.pem", "server.pem", "server-key.pem"])
    for role in ("controller", "driver", *config.members):
        distribute(gate_tls, private / role / "gate-tls", ["ca.pem", "client.pem", "client-key.pem"])
    distribute(controller_tls, private / "controller" / "controller-tls", ["ca.pem", "server.pem", "server-key.pem"])
    for identity in config.members:
        distribute(controller_tls, private / identity / "controller-tls", ["ca.pem", "client.pem", "client-key.pem"])
        workload = staging / (identity + "-workload")
        write_workload_tls(workload)
        distribute(workload, private / identity / "workload-tls", ["ca.pem", "server.pem", "server-key.pem"])
        distribute(workload, private / "driver" / (identity + "-workload"), ["ca.pem", "client.pem", "client-key.pem"])


if __name__ == "__main__":
    provision(sys.argv[1])
