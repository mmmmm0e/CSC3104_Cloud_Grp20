"""Run repeated requests against Member 6's existing integrated Docker deployment."""
import argparse
from contextlib import ExitStack
from datetime import datetime
import json
from pathlib import Path
import shutil
import subprocess

import grpc

from integrated import ROOT, compose, endpoints, record_fault, validation_lock
from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from cranelock_gate.client import Client
from cranelock_gate.config import Config
from cranelock_gate.demo import load_keys
from cranelock_sim.deployed import SCENARIOS, run_workload


class ExternalDeployment:
    def __init__(self, stack):
        self.endpoints = endpoints()
        manifest = self.endpoints
        # Capture only public container metadata, not inspect's environment/secrets.
        template = ('{"id":{{json .Id}},"name":{{json .Name}},'
                    '"image":{{json .Image}},"running":{{json .State.Running}},'
                    '"project":{{json (index .Config.Labels "com.docker.compose.project")}}}')
        inspected = subprocess.check_output(["docker", "inspect", "--format", template,
            *manifest["containers"].values()], text=True)
        inventory = [json.loads(line) for line in inspected.splitlines() if line.strip()]
        if (len(inventory) != len(manifest["containers"]) or
                any(not item["running"] or item["project"] != manifest["project"] for item in inventory)):
            raise RuntimeError("The complete integrated container deployment must be running before testing")
        manifest["container_inventory"] = inventory
        config = Config.load(manifest["config"])
        tls = Path(manifest["gate_tls_directory"])
        credentials = grpc.ssl_channel_credentials(
            root_certificates=(tls/"ca.pem").read_bytes(),
            private_key=(tls/"client-key.pem").read_bytes(),
            certificate_chain=(tls/"client.pem").read_bytes())
        channel = stack.enter_context(grpc.secure_channel(manifest["gate"]["host"], credentials))
        signing = Path(manifest["signing_directory"])
        def actor_client(identity):
            keys = load_keys(signing/f"{identity}.json")
            if set(keys) != {identity}:
                raise ValueError(f"Expected an actor-only signing file for {identity}")
            return Client(channel, config, keys, timeout=2)
        self.simulator = actor_client(config.simulator_id)
        self.cranes = {identity: actor_client(identity) for identity in config.members}
        self.stubs = {identity: rpc.CraneAgentStub(stack.enter_context(grpc.insecure_channel(endpoint["host"])))
                      for identity, endpoint in manifest["agents"].items()}

    def agent(self, identity):
        return self.stubs[identity].GetAgentState(pb.Empty(), timeout=2)

    def request(self, holder, identity, state, request_id):
        return self.stubs[holder].TokenRequest(pb.TokenRequestMessage(agent_id=identity,
            epoch=state["epoch"], revision=state["revision"], request_id=request_id), timeout=.5)

    def fault(self, action, service):
        record_fault(action, service)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--new-empty-simulation", action="store_true", required=True,
                        help="Start a new synthetic empty lane; no other workload runner may be active")
    parser.add_argument("--scenario", choices=SCENARIOS, default="normal")
    parser.add_argument("--requests", type=int, default=18)
    parser.add_argument("--seed", type=int, default=20)
    parser.add_argument("--duration", type=float, default=30, help="Failure observation window; healthy batches finish all requests")
    parser.add_argument("--crossing-time", type=float, default=.1)
    parser.add_argument("--output", type=Path,
                        default=ROOT/datetime.now().strftime("run/deployed-evaluation/%Y%m%d-%H%M%S-%f"))
    args = parser.parse_args()
    if args.output.exists():
        parser.exit(1, f"Refusing existing output directory: {args.output}\n")
    if shutil.which("docker") is None:
        parser.exit(1, "Docker CLI is unavailable. Install/start Docker Desktop and follow docs/PLATFORM_HANDOFF.md.\n")
    try:
        with validation_lock(), ExitStack() as stack:
            deployment = ExternalDeployment(stack)
            try:
                result = run_workload(deployment, args.output, scenario=args.scenario, seed=args.seed,
                    requests=args.requests, duration=args.duration, crossing_time=args.crossing_time,
                    new_empty_simulation=args.new_empty_simulation)
            finally:
                # Cumulative component logs are supporting evidence, never used to
                # subtract monotonic clocks from different processes/containers.
                if args.output.is_dir():
                    try:
                        with (args.output/"container.log").open("w", encoding="utf-8") as stream:
                            compose("logs", "--no-color", stdout=stream, stderr=subprocess.STDOUT)
                    except (OSError, subprocess.CalledProcessError):
                        print("Container log export failed; workload/fault evidence is retained.")
            print(json.dumps(result, indent=2))
            print("Saved deployed workload:", args.output)
            if not result["passed"]:
                parser.exit(1, "Workload completed but service restoration failed; inspect restoration.json.\n")
    except (ValueError, RuntimeError, OSError, grpc.RpcError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
