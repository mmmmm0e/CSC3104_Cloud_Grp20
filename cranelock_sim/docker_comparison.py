"""Paired Docker experiments using the team's services and the common workload runner.

Creates only uniquely named experiment containers/networks. The existing integrated
deployment is never stopped, reset or reused. No Docker daemon socket is mounted.
"""
import argparse
import asyncio
import csv
from datetime import datetime
import json
import math
import os
from pathlib import Path
import random
import socket
import subprocess
import uuid

import grpc

from .autonomous import run_case
from .benefits import make_workload
from .comparison import MODELS, write_csv
from .comparison import workload_hash
from .benefits import paired

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "cranelock-comparison:local"
SCENARIOS = ("normal", "controller-outage", "holder-failure")
LIMITS = ("Five separate Docker containers on one Docker Desktop host and a private bridge network. "
          "Same image, CPU/memory limits, seeded demand and gate for all three models. "
          "Team CentralController uses a test-only assignment RPC adapter; deployed agents own "
          "LOCAL handoff and CraneLock recovery. Synthetic lane observations and host-driven FIFO demand. "
          "Timing includes driver polling/signing costs; this is not a concurrent-load stress test. "
          "Fresh authority state per case. Gate and controller heartbeat use development mutual TLS; "
          "peer and test-assignment RPCs are plaintext on the private bridge. This is a containerised evaluation "
          "deployment, not the integrated monitoring stack or physical port hardware. "
          "Host log-receipt times are not cross-container failure detection measurements.")


def docker(*args, **kwargs):
    return subprocess.run(["docker", *map(str, args)], check=True, capture_output=True,
                          text=True, timeout=90, **kwargs)


class Container:
    def __init__(self, identity):
        self.pid = identity  # Identity slot shared with the process runner; exported as container_ids.
        self.waiter = subprocess.Popen(["docker", "wait", identity], stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)


class DockerCluster:
    containerized = True
    limitations = LIMITS

    def __init__(self, output, emit):
        self.output, self.emit = output, emit
        self.name = "cranelock-eval-" + uuid.uuid4().hex[:12]
        self.processes, self.channels, self.stopped = {}, [], set()
        self.addresses, self.offsets, self.streams, self.loggers = {}, {}, {}, {}
        self.image = json.loads(docker("image", "inspect", IMAGE, "--format", "{{json .Id}}").stdout)
        docker("network", "create", "--label", "cranelock.evaluation=true", self.name)

    def gate_channel(self, address):
        tls = self.output/".credentials"/"driver"/"gate-tls"
        credentials = grpc.ssl_channel_credentials((tls/"ca.pem").read_bytes(),
            (tls/"client-key.pem").read_bytes(), (tls/"client.pem").read_bytes())
        return grpc.secure_channel(address, credentials)

    def base(self, name):
        return ["run", "--detach", "--name", f"{self.name}-{name}",
                "--label", f"cranelock.evaluation={self.name}",
                "--network", self.name, "--network-alias", name,
                "--cpus", "1", "--memory", "256m", *self.mounts(name)]

    def mounts(self, name, *, credentials=True):
        mounts = ["--mount", f"type=bind,source={self.output/'gate-config.json'},target=/config/gate-config.json,readonly",
                  "--mount", f"type=bind,source={self.output/'state'/name},target=/state"]
        if credentials:
            mounts += ["--mount", f"type=bind,source={self.output/'.credentials'/name},target=/credentials,readonly"]
        return mounts

    def translate(self, arguments, name):
        paths = [(self.output / ".credentials" / name, "/credentials"),
                 (self.output / "state" / name, "/state"),
                 (self.output / "gate-config.json", "/config/gate-config.json")]
        result = []
        for value in map(str, arguments):
            value = value.replace("\\", "/")
            for source, target in paths:
                value = value.replace(str(source).replace("\\", "/"), target)
            for host, internal in self.addresses.items():
                value = value.replace(host, internal)
            result.append(value)
        return result

    def initialize(self, arguments):
        # Gate initialization receives only public configuration and its own state directory.
        return docker("run", "--rm", "--network", self.name,
                      *self.mounts("gate", credentials=False),
                      self.image, *self.translate(arguments, "gate"))

    def spawn(self, name, *arguments):
        arguments = list(map(str, arguments))
        ports = []
        bind_flag = "--bind" if name == "gate" else "--listen"
        if name != "controller":
            address = arguments[arguments.index(bind_flag)+1]
            port = int(address.rsplit(":", 1)[1])
            ports.append(port)
            self.addresses[address] = f"{name}:{port}"
            arguments[arguments.index(bind_flag)+1] = f"0.0.0.0:{port}"
            if "--workload-listen" in arguments:
                workload_index = arguments.index("--workload-listen") + 1
                workload_port = int(arguments[workload_index].rsplit(":", 1)[1])
                ports.append(workload_port)
                arguments[workload_index] = f"0.0.0.0:{workload_port}"
        else:
            # Reserve both host ports together, then bind/publish the same numbers.
            with socket.socket() as a, socket.socket() as b:
                a.bind(("127.0.0.1", 0)); b.bind(("127.0.0.1", 0))
                assignment, heartbeat = a.getsockname()[1], b.getsockname()[1]
            ports.extend([assignment, heartbeat])
            self.addresses[f"127.0.0.1:{heartbeat}"] = f"controller:{heartbeat}"
            arguments += ["--assignment-bind", f"0.0.0.0:{assignment}",
                          "--heartbeat-bind", f"0.0.0.0:{heartbeat}",
                          "--tls-directory", str(self.output/".credentials"/name/"controller-tls"),
                          "--gate-tls-directory", str(self.output/".credentials"/name/"gate-tls")]
        if "--controller-address" in arguments:
            for flag, filename in [("ca", "ca.pem"), ("cert", "client.pem"), ("key", "client-key.pem")]:
                arguments += [f"--controller-tls-{flag}", str(self.output/".credentials"/name/"controller-tls"/filename)]
        if name == "gate":
            for flag, filename in [("ca", "ca.pem"), ("cert", "server.pem"), ("key", "server-key.pem")]:
                arguments += [f"--tls-{flag}", str(self.output/".credentials"/name/"gate-tls"/filename)]
        elif name.startswith("crane-"):
            for flag, filename in [("ca", "ca.pem"), ("cert", "client.pem"), ("key", "client-key.pem")]:
                arguments += [f"--gate-tls-{flag}", str(self.output/".credentials"/name/"gate-tls"/filename)]
        command = self.base(name)
        for port in ports:
            command += ["--publish", f"127.0.0.1:{port}:{port}"]
        identity = docker(*command, self.image, *self.translate(arguments, name)).stdout.strip()
        self.processes[name] = Container(identity)
        stream = (self.output/f"{name}.log").open("wb")
        self.streams[name] = stream
        self.loggers[name] = subprocess.Popen(["docker", "logs", "--follow", identity],
            stdout=stream, stderr=stream,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        return self.processes[name]

    def check(self):
        for name, container in self.processes.items():
            if name not in self.stopped and container.waiter.poll() is not None:
                raise RuntimeError(f"{name} container stopped unexpectedly")

    def collect(self, force=False):
        for name, container in self.processes.items():
            if not name.startswith("crane-") and not force:
                continue
            with (self.output/f"{name}.log").open("rb") as stream:
                stream.seek(self.offsets.get(name, 0))
                while True:
                    start = stream.tell()
                    line = stream.readline()
                    if not line or not line.endswith(b"\n"):
                        self.offsets[name] = start
                        break
                    try:
                        event = json.loads(line)
                    except (ValueError, UnicodeDecodeError):
                        continue
                    if isinstance(event, dict) and "event" in event:
                        # No subtraction between Windows and Linux monotonic clocks.
                        self.emit("agent_event", agent_event=event, time_basis="host_log_receipt")

    async def stop(self, name):
        if name in self.stopped:
            return
        self.stopped.add(name)
        await asyncio.to_thread(docker, "kill", self.processes[name].pid)

    def inventory(self):
        return dict(image_id=self.image, network=self.name, cpus_per_service=1,
                    memory_bytes_per_service=268435456, services={
                        n: dict(id=c.pid, name=f"{self.name}-{n}") for n,c in self.processes.items()})

    async def close(self):
        failures = []
        try:
            await asyncio.to_thread(self.collect, True)
        finally:
            for name, container in self.processes.items():
                try:
                    logs = await asyncio.to_thread(docker, "logs", container.pid)
                    (self.output/f"{name}-complete.log").write_text(logs.stdout+logs.stderr, encoding="utf-8")
                except Exception as error:
                    failures.append(f"{name} log export: {error}")
                try:
                    await asyncio.to_thread(docker, "rm", "--force", container.pid)
                except Exception as error:
                    failures.append(f"{name} removal: {error}")
            try:
                await asyncio.to_thread(docker, "network", "rm", self.name)
            except Exception as error:
                failures.append(str(error))
            for channel in self.channels:
                channel.close()
            for process in [*self.loggers.values(), *(c.waiter for c in self.processes.values())]:
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=10)
            for stream in self.streams.values():
                stream.close()
            (self.output/"cleanup.json").write_text(json.dumps(dict(succeeded=not failures, errors=failures), indent=2))
        if failures:
            raise RuntimeError("Experiment container cleanup failed: " + "; ".join(failures))


async def run_matrix(output, seeds, requests, duration, crossing_time, detection_timeout, scenarios=SCENARIOS):
    output.mkdir(parents=True, exist_ok=False)
    results = []
    for seed in seeds:
        workload = make_workload(seed, requests, duration)
        order = [(scenario, model) for scenario in scenarios for model in MODELS]
        random.Random(seed).shuffle(order)
        for scenario, model in order:
            rows = [dict(r, requested_at=0.) for r in workload] if scenario == "normal" else workload
            result = await run_case(output/f"seed-{seed}"/scenario/model, model, scenario, rows,
                seed=seed, duration=duration, crossing_time=crossing_time,
                detection_timeout=detection_timeout, cluster_factory=DockerCluster)
            case_path = output/f"seed-{seed}"/scenario/model
            expected = (result["completed"] == requests if scenario == "normal" else
                result["post_fault_new_entries"] == 0 if model == "central-lock" and scenario == "controller-outage" else
                result["completed"] == 0 if model == "basic-token" and scenario == "holder-failure" else
                result["resumed_within_window"] is True)
            if model == "cranelock":
                expected = expected and (result["final_mode"] == "CENTRAL" and result["final_epoch"] == 0
                    if scenario == "normal" else result["final_mode"] == "LOCAL" and result["final_epoch"] > 0)
            if not expected:
                result["passed"] = False
                (case_path/"summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
                (case_path/"failure.json").write_text(json.dumps(dict(error="UnexpectedOutcome",
                    detail="Observed completion or authority outcome did not match the model's expected behavior.")))
                raise RuntimeError(f"Unexpected outcome: seed {seed} {scenario} {model}; evidence retained in {case_path}")
            results.append(result)
            write_csv(output/"comparison.csv", results)
            (output/"summary.json").write_text(json.dumps(dict(evaluation="benefits-docker-v1",
                passed=len(results)==len(seeds)*len(scenarios)*len(MODELS),
                expected_runs=len(seeds)*len(scenarios)*len(MODELS), runs=len(results),
                limitations=LIMITS, results=results), indent=2), encoding="utf-8")
            print(f"[PASS] seed={seed} {scenario} {model}: {result['completed']}/{requests}; "
                  f"post-fault entries={result['post_fault_new_entries']}", flush=True)
    return results


def audit_matrix(output, seeds=(7,20,3104), scenarios=SCENARIOS):
    """Verify complete paired evidence, including expected stalls; never fills missing data."""
    output = Path(output)
    results, images = [], set()
    identities = [(seed,scenario,model) for seed in seeds for scenario in scenarios for model in MODELS]
    for seed, scenario, model in identities:
        path = output/f"seed-{seed}"/scenario/model
        result = json.loads((path/"summary.json").read_text(encoding="utf-8"))
        manifest = json.loads((path/"experiment.json").read_text(encoding="utf-8"))
        if (path/"failure.json").exists() or not result["passed"] or not json.loads((path/"cleanup.json").read_text())["succeeded"]:
            raise ValueError(f"Failed or unclean experiment: {path}")
        if (result['seed'],result['scenario'],result['model']) != (seed,scenario,model):
            raise ValueError(f"Identity mismatch: {path}")
        if result['workload_sha256'] != workload_hash(json.loads((path/'workload.json').read_text())):
            raise ValueError(f"Workload hash mismatch: {path}")
        if len(set(manifest['container_ids'].values())) != 5 or result['execution_mode'] != 'docker_containers':
            raise ValueError(f"Missing five-container evidence: {path}")
        images.add(result['docker']['image_id'])
        with (path/'requests.csv').open(newline='',encoding='utf-8') as stream:
            rows = list(csv.DictReader(stream))
        window = result['total_completion_seconds'] if scenario=='normal' else result['duration_seconds']
        completed = sum(bool(r['exited_at']) and float(r['exited_at']) <= window for r in rows)
        admitted = sum(bool(r['entered_at']) and float(r['entered_at']) <= window for r in rows)
        if (len(rows),completed,admitted) != (result['requested'],result['completed'],result['accepted']):
            raise ValueError(f"CSV totals differ: {path}")
        events = [json.loads(line) for line in (path/'events.jsonl').read_text().splitlines()]
        occupant = None
        entries = exits = 0
        for event in events:
            if event['event']=='physical_entry':
                if occupant is not None:
                    raise ValueError(f"Overlapping physical entries: {path}")
                occupant = event['request_id']; entries += 1
            elif event['event']=='physical_exit':
                if occupant != event['request_id']:
                    raise ValueError(f"Unmatched physical exit: {path}")
                occupant = None; exits += 1
        if occupant or entries != sum(bool(r['entered_at']) for r in rows) or exits != sum(bool(r['exited_at']) for r in rows):
            raise ValueError(f"Physical events differ from CSV: {path}")
        if scenario != 'normal':
            after = [float(r['entered_at']) for r in rows if r['entered_at'] and
                     result['fault_completed_at_seconds'] <= float(r['entered_at']) <= window]
            expected = min(after)-result['fault_completed_at_seconds'] if after else None
            if expected != result['fault_command_end_to_entry_seconds']:
                raise ValueError(f"Fault timing differs from CSV: {path}")
            if model == 'cranelock':
                voters = [identity for identity in ('crane-1','crane-2','crane-3')
                    if json.loads((path/f'{identity}.json').read_text()).get('vote_state')]
                if len(voters) < 2 or result['final_mode'] != 'LOCAL' or result['final_epoch'] <= 0:
                    raise ValueError(f"Missing majority recovery evidence: {path}")
        results.append(result)
    if len(images) != 1:
        raise ValueError('Matrix mixes container images')
    # Each seeded scenario must have identical input/settings across all models.
    for seed in seeds:
        for scenario in scenarios:
            selected = [r for r in results if r['seed']==seed and r['scenario']==scenario]
            fields = ('workload_sha256','requested','duration_seconds','crossing_time_seconds',
                      'detection_timeout_seconds','heartbeat_interval_seconds','host_rpc_timeout_seconds')
            if len({tuple(r.get(f) for f in fields) for r in selected}) != 1:
                raise ValueError(f'Unmatched inputs: seed {seed} {scenario}')
            if len({(r['docker']['cpus_per_service'],r['docker']['memory_bytes_per_service']) for r in selected}) != 1:
                raise ValueError(f'Unmatched resource limits: seed {seed} {scenario}')
    verified = dict(passed=True,runs=len(results),image_id=next(iter(images)),
        requested=sum(r['requested'] for r in results),completed=sum(r['completed'] for r in results),
        detected_safety_violations=sum(r['safety_violations'] for r in results),
        controller_outage_extra_entries=paired(results,'controller-outage','central-lock','post_fault_new_entries'),
        holder_failure_extra_completed=paired(results,'holder-failure','basic-token','completed'),
        normal_completion_cost_seconds=paired(results,'normal','central-lock','total_completion_seconds'))
    (output/'verification.json').write_text(json.dumps(verified,indent=2),encoding='utf-8')
    return verified


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=Path(datetime.now().strftime("run/evaluation/docker-comparison-%Y%m%d-%H%M%S")))
    parser.add_argument("--seeds", nargs="+", type=int, default=[7,20,3104])
    parser.add_argument("--requests", type=int, default=18)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--crossing-time", type=float, default=.1)
    parser.add_argument("--detection-timeout", type=float, default=2)
    parser.add_argument("--scenario", choices=["all",*SCENARIOS], default="all")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--audit", type=Path, help="Verify an existing complete matrix instead of running containers")
    args = parser.parse_args()
    if args.audit:
        print(json.dumps(audit_matrix(args.audit,args.seeds,
            SCENARIOS if args.scenario=='all' else [args.scenario]),indent=2))
        return
    if (len(set(args.seeds)) != len(args.seeds) or args.requests < 3 or
            any(not math.isfinite(v) or v <= 0 for v in (args.duration,args.crossing_time,args.detection_timeout)) or
            args.detection_timeout <= .05 or args.duration < 6*args.detection_timeout):
        parser.error("Use unique seeds, >=3 requests, positive finite times and duration >=6 detection timeouts")
    if args.output.exists():
        parser.error("Output directory already exists; choose a fresh directory")
    if args.build:
        subprocess.run(["docker","build","-f",str(ROOT/"deploy/Dockerfile.comparison"),"-t",IMAGE,str(ROOT)], check=True)
    from scripts.integrated import validation_lock
    scenarios = SCENARIOS if args.scenario=='all' else [args.scenario]
    with validation_lock():
        asyncio.run(run_matrix(args.output.resolve(),args.seeds,args.requests,args.duration,
            args.crossing_time,args.detection_timeout,scenarios))
        print(json.dumps(audit_matrix(args.output,args.seeds,scenarios),indent=2))


if __name__ == "__main__":
    main()
