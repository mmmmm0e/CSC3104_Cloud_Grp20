import asyncio
import json

import pytest

from cranelock_sim import progress_demo


# verifies all connected scenarios and dashboard evidence
def test_members_1_to_4_demo(tmp_path):
    from cranelock_sim.dashboard import Results

    output = tmp_path / "progress"
    result = asyncio.run(progress_demo.run_progress_demo(output, progress=lambda line: None))
    assert result["passed"] and result["scenario_count"] == 8
    assert result["scenarios"]["normal"]["completed"] == 6
    minority = result["scenarios"]["partition/no-majority"]
    assert not minority["recovered"] and minority["physical_entries"] == 0
    assert result["scenarios"]["restart/during-occupancy"]["final_holder"] == "crane-1"
    for item in result["scenarios"].values():
        detail = Results(output).detail(item["directory"])
        assert detail["status"] == "passed"
        assert "requests.csv" in detail["artifacts"]
    assert json.loads((output / "summary.json").read_text())["passed"]
    assert "all 8 scenarios completed" in (output / "demo.log").read_text()


# retains failure evidence without reporting success
def test_demo_keeps_failure_log(tmp_path, monkeypatch):
    async def fail(*args, **kwargs):
        raise RuntimeError("injected demo failure")

    monkeypatch.setattr(progress_demo, "run_agent_demo", fail)
    output = tmp_path / "failed"
    with pytest.raises(RuntimeError, match="injected demo failure"):
        asyncio.run(progress_demo.run_progress_demo(output, progress=lambda line: None))
    summary = json.loads((output / "summary.json").read_text())
    assert not summary["passed"] and "injected demo failure" in summary["error"]
    assert "[FAIL]" in (output / "demo.log").read_text()


# preserves previously recorded results
def test_demo_refuses_existing_directory(tmp_path):
    marker = tmp_path / "keep.txt"
    marker.write_text("keep")
    with pytest.raises(FileExistsError):
        asyncio.run(progress_demo.run_progress_demo(tmp_path))
    assert marker.read_text() == "keep"
