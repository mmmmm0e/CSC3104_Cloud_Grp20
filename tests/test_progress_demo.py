import json
import os
from pathlib import Path
import subprocess
import sys


# checks the demo command and its saved evidence
def test_progress_demo_creates_evidence(tmp_path):
    directory = tmp_path / "demo"
    result = subprocess.run(
        [sys.executable, "-m", "cranelock.progress_demo", "--output", str(directory)],
        cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True,
        timeout=30, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    assert result.returncode == 0, result.stdout + result.stderr
    summary = json.loads((directory / "summary.json").read_text())
    assert summary["passed"] and all(step["passed"] for step in summary["steps"])
    agent_events = [json.loads(line) for line in
                    (directory / "agents.jsonl").read_text().splitlines()]
    assert any(event["event"] == "TOKEN_HOLDER_SUSPECTED" for event in agent_events)
    assert any(event["event"] == "RECOVERY_NO_QUORUM" for event in agent_events)
    assert any(event["event"] == "RECOVERY_CERTIFICATE_FORMED" for event in agent_events)
    gate_events = [json.loads(line) for line in
                   (directory / "gate.jsonl").read_text().splitlines()]
    assert any(event["reason"] == "STALE_EPOCH" for event in gate_events)
    assert any(event["reason"] == "LANE_BUSY" for event in gate_events)
    assert "replacement holder receives fresh grant" in (directory / "demo.log").read_text()
