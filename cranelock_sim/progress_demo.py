import argparse
import asyncio
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid

from .agents import run_agent_demo
from .failures import run_partition_suite, run_recovery_suite, run_restart_suite


# compares physical movements with accepted gate entries
def verify_evidence(directory, summary):
    events = [json.loads(line) for line in
              (directory / "events.jsonl").read_text(encoding="utf-8").splitlines()]
    audit = [json.loads(line) for line in
             (directory / "gate-events.jsonl").read_text(encoding="utf-8").splitlines()]
    entries = [event for event in events if event["event"] == "physical_entry"]
    accepted = [event for event in audit if event["operation"] == "ValidateEntry"
                and event["allowed"] and not event["replayed"]]
    physical = [(event["crane"], event["epoch"], event["revision"]) for event in entries]
    digital = [(event["holder"], event["epoch"], event["revision"]) for event in accepted]
    if physical != digital or summary.get("safety_violations") != 0:
        raise RuntimeError(f"physical/gate evidence mismatch in {directory}")
    no_quorum = summary.get("recovered") is False
    if no_quorum:
        safe_final = (not entries and summary.get("final_epoch") == 0
                      and summary["final_holder"] == "crane-1")
    else:
        safe_final = summary["final_lane_state"] == "CLEAR"
    if summary.get("passed") is False or not safe_final:
        raise RuntimeError(f"scenario did not finish safely in {directory}")
    return len(entries)


# runs member 4 scenarios with the existing agents and gate
async def run_progress_demo(output, progress=print):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    scope = ("Members 1 to 4, real loopback gRPC agents and gate, simulator lane and "
             "safety checks; local runner orchestration, no teammate controller or Kubernetes.")
    summary = dict(model="members-1-to-4-progress", passed=False, scope=scope, scenarios={})
    with (output / "demo.log").open("x", encoding="utf-8") as log:
        # records timestamped progress on screen and disk
        def emit(message):
            line = f"{datetime.now(timezone.utc).isoformat()} {message}"
            log.write(line + "\n")
            log.flush()
            progress(line)

        # checks saved evidence before reporting success
        def record(name, result, directory):
            count = verify_evidence(directory, result)
            summary["scenarios"][name] = dict(
                passed=True, directory=directory.relative_to(output).as_posix(),
                physical_entries=count, safety_violations=result["safety_violations"],
                final_holder=result["final_holder"], final_lane_state=result["final_lane_state"],
                recovered=result.get("recovered"), recovered_epoch=result.get("recovered_epoch"),
                completed=result.get("completed"),
                failure_detection_seconds=result.get("failure_detection_seconds"),
                recovery_latency_seconds=result.get("recovery_latency_seconds"))
            emit(f"[PASS] {name}: {count} accepted physical entries, zero safety violations")

        try:
            emit("[START] members 1 to 4 local progress demo")
            emit("[RUN] normal token handoff and simulator crossings")
            result = await run_agent_demo(output / "normal", rounds=2, crossing_time=0.05)
            record("normal", result, output / "normal")
            for name, runner in (("recovery", run_recovery_suite),
                                 ("partition", run_partition_suite),
                                 ("restart", run_restart_suite)):
                emit(f"[RUN] {name} scenarios")
                suite = await runner(output / name, progress=emit)
                for scenario, result in suite["scenarios"].items():
                    record(f"{name}/{scenario}", result, output / name / scenario)
            summary.update(passed=True, scenario_count=len(summary["scenarios"]),
                physical_entries=sum(item["physical_entries"]
                                     for item in summary["scenarios"].values()),
                safety_violations=0)
            emit(f"[PASS] all {summary['scenario_count']} scenarios completed")
        except BaseException as error:
            summary["error"] = f"{type(error).__name__}: {error}"
            emit(f"[FAIL] {summary['error']}")
            raise
        finally:
            (output / "summary.json").write_text(
                json.dumps(summary, indent=2), encoding="utf-8")
            with (output / "scenarios.csv").open("w", newline="", encoding="utf-8") as stream:
                fields = ["scenario", "passed", "directory", "physical_entries",
                    "safety_violations", "final_holder", "final_lane_state", "recovered",
                    "recovered_epoch", "completed", "failure_detection_seconds",
                    "recovery_latency_seconds"]
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                for name, item in summary["scenarios"].items():
                    writer.writerow(dict(scenario=name, **item))
    return summary


# creates fresh presentation evidence
def main():
    parser = argparse.ArgumentParser(description="Connected members 1 to 4 progress demo")
    parser.add_argument("--output", type=Path, help="new evidence directory")
    args = parser.parse_args()
    output = args.output or Path("run/evaluation") / (
        "progress-" + datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
    print(f"Evidence directory: {output.resolve()}", flush=True)
    asyncio.run(run_progress_demo(output, progress=lambda line: print(line, flush=True)))
    print(f"View saved runs: python -m cranelock_sim.dashboard --root \"{output.resolve()}\"")


if __name__ == "__main__":
    main()
