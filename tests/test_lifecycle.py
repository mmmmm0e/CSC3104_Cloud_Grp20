import asyncio
import csv
import json

import grpc
import pytest

from cranelock.controller import CentralController
from cranelock_sim.lifecycle import run_lifecycle
from cranelock_sim.progress_demo import verify_evidence


# runs the actual central and local controller cycle
@pytest.fixture(scope="module")
def lifecycle_run(tmp_path_factory):
    output = tmp_path_factory.mktemp("lifecycle") / "cycle"
    summary = asyncio.run(run_lifecycle(output, progress=lambda line: None))
    events = [json.loads(line) for line in (output / "events.jsonl").read_text().splitlines()]
    return output, summary, events


# checks gate authority and queue reconciliation across both mode changes
def test_actual_controller_return_keeps_queue_epoch_and_occupancy(lifecycle_run):
    output, summary, events = lifecycle_run
    assert summary["passed"] and summary["completed"] == 3
    assert summary["controller_implementation"] == "cranelock.controller.CentralController"
    assert summary["final_mode"] == "CENTRAL" and summary["final_epoch"] == 1
    assert summary["queue_preserved"] and verify_evidence(output, summary) == 3
    fallback = next(event for event in events if event["event"] == "controller_fallback_installed")
    returned = next(event for event in events if event["event"] == "controller_return_installed")
    assert fallback["occupant"] == "crane-1" and returned["occupant"] == "crane-2"
    assert returned["pending_queue"] == ["crane-3"]
    snapshots = [event for event in events if event["event"] == "agent_snapshot"]
    final = snapshots[-1]
    assert all(state["mode"] == "CENTRAL" for state in final["agents"].values())
    gate_events = [json.loads(line) for line in (output / "gate-events.jsonl").read_text().splitlines()]
    assert any(event["reason"] == "STALE_EPOCH" for event in gate_events)
    assert any(event["reason"] == "STALE_REVISION" for event in gate_events)
    assert sum(event["reason"] == "LANE_BUSY" for event in gate_events) == 2
    assert sum(event["operation"] == "TransferHolder" and event["allowed"] for event in gate_events) == 1
    assert [event["crane"] for event in events if event["event"] == "central_assignment"] == [
        "crane-1", "crane-3"]
    with (output / "requests.csv").open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert all(float(row["requested_at"]) <= float(row["entered_at"]) < float(row["exited_at"])
               for row in rows)
    assert [row["crane"] for row in rows] == ["crane-1", "crane-2", "crane-3"]
    owner = None
    for event in events:
        if event["event"] == "physical_entry":
            assert owner is None
            owner = event["crane"]
        elif event["event"] == "physical_exit":
            assert event["crane"] == owner
            owner = None
    assert owner is None


# keeps the lane blocked if the returning controller cannot install authority
def test_failed_controller_return_does_not_admit_next_crane(tmp_path, monkeypatch):
    original = CentralController.install_central
    calls = []

    def fail_return(self, holder, **options):
        calls.append(holder)
        if len(calls) == 2:
            raise RuntimeError("injected controller return failure")
        return original(self, holder, **options)

    monkeypatch.setattr(CentralController, "install_central", fail_return)
    output = tmp_path / "failure"
    with pytest.raises(grpc.aio.AioRpcError, match="injected controller return failure"):
        asyncio.run(run_lifecycle(output, progress=lambda line: None))
    assert len(calls) == 2 and not (output / "summary.json").exists()
    assert (output / "failure.json").exists()
    events = [json.loads(line) for line in (output / "events.jsonl").read_text().splitlines()]
    assert not any(event["event"] == "physical_entry" and event["crane"] == "crane-3" for event in events)
    assert not any(event["event"] == "controller_return_installed" for event in events)
