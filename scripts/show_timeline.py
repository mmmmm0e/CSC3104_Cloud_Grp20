"""Readable timeline of a saved workload run, showing the token holder at every step (for demos).

Reads events.jsonl from a run folder (default: the newest folder in
run/deployed-evaluation) and prints one line per event. It changes nothing.

    python scripts/show_timeline.py            # newest run
    python scripts/show_timeline.py <folder>   # a specific run
"""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / "run/deployed-evaluation"


def newest_run():
    runs = [path for path in RUNS.iterdir() if (path / "events.jsonl").is_file()]
    if not runs:
        raise SystemExit(f"No saved runs found in {RUNS}")
    return max(runs, key=lambda path: (path / "events.jsonl").stat().st_mtime)


def main():
    folder = Path(sys.argv[1]) if len(sys.argv) > 1 else newest_run()
    events = [json.loads(line) for line in (folder / "events.jsonl").read_text().splitlines() if line.strip()]
    try:
        settings = json.loads((folder / "experiment.json").read_text())
    except (OSError, ValueError):
        settings = {}
    scenario = settings.get("scenario", "?")

    # Authority recorded by the Gate when the run began.
    holder, mode, epoch = settings.get("initial_holder"), settings.get("initial_mode"), settings.get("initial_epoch")
    lines = []

    def add(time, text):
        lines.append(f"{time:6.2f}s  {text:<60} {holder or '?'}")

    for event in events:
        kind, time = event["event"], event["time"]
        if kind == "workload_started":
            mode = event.get("mode", mode)
            add(time, f"run starts ({event['requests']} requests, mode {mode}, epoch {epoch})")
        elif kind == "external_authority_observed":
            old_holder, old_mode, old_epoch = holder, mode, epoch
            holder, mode, epoch = event["holder"], event["mode"], event["epoch"]
            if old_epoch is not None and epoch != old_epoch:
                add(time, f">> NEW AUTHORITY: epoch {old_epoch} -> {epoch}, {mode}, holder {holder}")
            elif mode != old_mode:
                add(time, f">> mode changed {old_mode} -> {mode}")
            elif holder != old_holder:
                add(time, f">> token passed {old_holder} -> {holder}")
        elif kind == "request":
            add(time, f"{event['request_id']}: {event['crane']} asks to cross")
        elif kind == "external_token_request":
            holder = event["holder"]
            add(time, f"{event['crane']} asks {event['holder']} for the token")
        elif kind == "physical_entry":
            holder = event["crane"]  # The Gate only admits the current token holder.
            add(time, f"   {event['crane']} ENTERS the lane ({event['request_id']})")
        elif kind == "physical_exit":
            add(time, f"   {event['crane']} leaves the lane ({event['request_id']})")
        elif kind == "fault_command_started":
            add(time, f"!! FAULT: {event['action']} {event['service']}")
        elif kind == "controller_stopped":
            add(time, "!! controller is down")
        elif kind == "holder_stopped":
            add(time, f"!! {event['crane']} (the token holder) has crashed")

    print(f"Run: {folder.name}   scenario: {scenario}")
    print(f"{'TIME':>7}  {'WHAT HAPPENED':<60} TOKEN HOLDER")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
