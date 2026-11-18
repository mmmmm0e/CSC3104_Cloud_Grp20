"""Logical-time normal-traffic experiment against the actual signed gate core.

The scheduler is a fixture, not the distributed crane protocol. Logical-time
latency measures workload queuing, excluding host CPU, RPC and signing time.
"""
import csv
import heapq
import json
import math
import random
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cranelock_gate.client import certificate, command
from cranelock_gate.config import Config
from cranelock_gate.core import Gate
from cranelock_gate.crypto import COMMAND_DOMAIN, canonical
from cranelock_gate.storage import initialize
from .lane import Lane


@dataclass(frozen=True)
class Experiment:
    cranes: int = 3
    duration: float = 60.0
    arrival_rate: float = 2.0  # Aggregate Poisson requests/second.
    crossing_time: float = 0.5
    seed: int = 20

    def __post_init__(self):
        if type(self.cranes) is not int or self.cranes < 3:
            raise ValueError("cranes must be an integer >= 3")
        for name in ("duration", "arrival_rate", "crossing_time"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")


class Clock:
    def __init__(self):
        self.elapsed = 0.0

    def __call__(self):
        # Gate observations use integer milliseconds. Keep the gate's test clock
        # at the same resolution so fresh evidence never predates its barrier.
        return (1_800_000_000_000 + round(self.elapsed * 1000)) / 1000


class LocalGateClient:
    """Same call/state boundary as Client; real signatures, SQLite and gate rules."""
    def __init__(self, gate, config, keys):
        self.gate, self.config, self.keys = gate, config, keys
        self.operations = 0

    def state(self):
        response = self.gate.get_state()
        if not response["allowed"]:
            raise RuntimeError(response["reason"])
        return response["state"]

    def call(self, actor, operation, **fields):
        self.operations += 1
        payload = command(self.config, actor, operation,
                          request_id=f"sim-command-{self.operations}", **fields)
        raw = canonical(payload)
        return self.gate.execute(operation, raw,
            self.keys[actor].sign(COMMAND_DOMAIN + raw))


def new_config(count):
    members = [f"crane-{i + 1}" for i in range(count)]
    services = ["controller", "simulator", "operator"]
    keys = {identity: Ed25519PrivateKey.generate() for identity in members + services}
    public = {identity: key.public_key().public_bytes_raw().hex()
              for identity, key in keys.items()}
    return Config("lane-1", "group20-evaluation-v1",
        {identity: public[identity] for identity in members}, *services,
        {identity: public[identity] for identity in services}), keys


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def run_experiment(settings, output):
    """Run one fresh yard, retain config/physical trace/CSV/summary/database."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    (output / "experiment.json").write_text(
        json.dumps(asdict(settings), indent=2), encoding="utf-8")
    config, keys = new_config(settings.cranes)
    clock = Clock()
    database = output / "gate.sqlite"
    initialize(database, config, clock())
    events, requests = [], []

    def emit(event, **fields):
        events.append(dict(time=clock.elapsed, event=event, **fields))

    gate = Gate(database, config, clock)
    try:
        client = LocalGateClient(gate, config, keys)
        state = client.state()
        # Provisioning fixture only: this is NOT quorum collection or recovery.
        holder = next(iter(config.members))
        cert = certificate(config, keys, state, holder, 0)
        result = client.call(holder, "InstallAuthority", certificate=cert,
                             expected_epoch=state["epoch"], expected_revision=state["revision"])
        require(result)
        lane = Lane(client, clock, emit)
        require(lane.observe())
        # Independent random generators ensure comparable arrival traces
# when the number of simulated cranes changes.
        arrival_rng = random.Random(settings.seed)
        assignment_rng = random.Random(settings.seed ^ 0xC0FFEE)

        schedule = []
        time = arrival_rng.expovariate(settings.arrival_rate)

        while time < settings.duration:
            index = len(requests)
            row = dict(
                request_id=f"request-{index + 1}",
                crane=assignment_rng.choice(list(config.members)),
                requested_at=time,
                entered_at=None,
                exited_at=None,
                status="waiting",
            )
            requests.append(row)
            heapq.heappush(schedule, (time, 1, index))
            time += arrival_rng.expovariate(settings.arrival_rate)
        waiting = deque()
        while schedule:
            time, kind, index = heapq.heappop(schedule)
            if time > settings.duration:
                break
            clock.elapsed = time
            row = requests[index]
            if kind == 0:
                lane.exit(row["crane"], row["request_id"])
                row.update(exited_at=time, status="completed")
            else:
                waiting.append(index)
                emit("request", request_id=row["request_id"], crane=row["crane"])
            if lane.occupant is None and waiting:
                index = waiting.popleft()
                row = requests[index]
                require(lane.observe())
                state = client.state()
                if state["holder"] != row["crane"]:
                    require(client.call(state["holder"], "TransferHolder",
                        epoch=state["epoch"], revision=state["revision"], destination=row["crane"]))
                state = client.state()
                grant = client.call(row["crane"], "RequestGrant",
                                    epoch=state["epoch"], revision=state["revision"])
                require(grant)
                if not lane.enter(row["crane"], row["request_id"], grant["grant"], state):
                    raise RuntimeError("Normal-traffic entry denied; inspect gate audit")
                row.update(entered_at=time, status="occupied")
                heapq.heappush(schedule, (time + settings.crossing_time, 0, index))
        clock.elapsed = settings.duration
        waits = [row["entered_at"] - row["requested_at"] for row in requests
                 if row["entered_at"] is not None]
        counts = {member: sum(row["status"] == "completed" and row["crane"] == member
                             for row in requests) for member in config.members}
        completed = sum(counts.values())
        squared = sum(count ** 2 for count in counts.values())
        summary = dict(model="local-gate-normal-traffic", time_basis="logical_seconds",
            **asdict(settings), requested=len(requests), accepted=len(waits), completed=completed,
            waiting=sum(row["status"] == "waiting" for row in requests),
            occupied=sum(row["status"] == "occupied" for row in requests),
            throughput_per_minute=completed * 60 / settings.duration,
            mean_wait_seconds=sum(waits) / len(waits) if waits else None,
            p95_wait_seconds=percentile(waits, 0.95), p99_wait_seconds=percentile(waits, 0.99),
            completed_by_crane=counts,
            jain_fairness=completed ** 2 / (settings.cranes * squared) if squared else None,
            signed_gate_commands=client.operations, safety_violations=0,
            recovery_latency_seconds=None,
            limitations="Logical FIFO scheduler; no distributed agents, recovery, network or baselines.")
        (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        with (output / "requests.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=["request_id", "crane", "requested_at",
                                                       "entered_at", "exited_at", "status"])
            writer.writeheader()
            writer.writerows(requests)
        return summary
    except Exception as error:
        emit("experiment_failed", error=type(error).__name__, detail=str(error))
        (output / "failure.json").write_text(json.dumps(dict(
            error=type(error).__name__, detail=str(error)), indent=2), encoding="utf-8")
        raise
    finally:
        with (output / "events.jsonl").open("w", encoding="utf-8") as stream:
            for event in events:
                stream.write(json.dumps(event) + "\n")
        gate.close()


def require(result):
    if not result["allowed"] or not result["action_applied"] or result["replayed"]:
        raise RuntimeError(f"Gate operation failed: {result['reason']}")
