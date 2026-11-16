"""Runner contract tests with a real signed gate; not Docker runtime evidence."""
import json
from types import SimpleNamespace

import pytest

from cranelock_sim.deployed import run_workload
from cranelock_sim.experiment import LocalGateClient


class DeploymentDouble:
    def __init__(self, h, *, recover=True):
        self.h, self.recover = h, recover
        self.calls, self.stopped = [], set()
        assert h.install(mode="CENTRAL")["allowed"]
        self.simulator = LocalGateClient(h.gate, h.config, h.keys)
        class Stub:
            def __init__(self, identity):
                self.identity = identity
            def RequestGrant(self, request, timeout):
                result = h.call(self.identity, "RequestGrant", request_id=request.request_id,
                    epoch=request.expected_epoch, revision=request.expected_revision)
                return SimpleNamespace(**{key: result[key] for key in
                    ("request_id", "allowed", "reason", "replayed", "action_applied")},
                    grant_json=json.dumps(result["grant"]).encode() if result["grant"] else b"")
        self.workloads = {identity: Stub(identity) for identity in h.config.members}
        self.endpoints = {"scope": "test double backed by actual signed gate"}

    def agent(self, identity):
        state = self.h.state
        return SimpleNamespace(epoch=state["epoch"], revision=state["revision"],
            token_holder=state["holder"], has_token=identity==state["holder"],
            mode=1 if state["mode"]=="CENTRAL" else 2, pending_transfer_id="",
            request_queue=[], peers=[SimpleNamespace(status="AVAILABLE")])

    def request(self, holder, identity, state, request_id):
        self.calls.append(("request", holder, identity))
        result = self.h.call(holder, "TransferHolder", epoch=state["epoch"],
                             revision=state["revision"], destination=identity)
        return SimpleNamespace(accepted=result["allowed"])

    def fault(self, action, service):
        self.calls.append((action, service))
        if action == "start":
            self.stopped.discard(service)
            return
        self.stopped.add(service)
        if self.recover:
            holder = self.h.state["holder"]
            if service == holder:
                holder = next(member for member in self.h.config.members if member not in self.stopped)
            assert self.h.install(holder, mode="LOCAL")["allowed"]


def run(h, backend, path, **options):
    settings = dict(requests=6, duration=3, crossing_time=.1,
        new_empty_simulation=True, clock=h.clock, sleep=h.clock.advance, wall_clock=h.clock)
    settings.update(options)
    return run_workload(backend, path, **settings)


@pytest.mark.parametrize("scenario", ["normal", "controller-outage", "holder-failure"])
def test_external_runner_uses_gate_and_restores_only_its_faults(h, tmp_path, scenario):
    backend = DeploymentDouble(h)
    output = tmp_path / scenario
    result = run(h, backend, output, scenario=scenario)
    assert result["passed"] and result["restoration_succeeded"]
    assert result["completed"] > 0 and result["safety_violations"] == 0
    assert not backend.stopped
    if scenario == "normal":
        assert result["completed"] == 6 and result["duration_seconds"] is None
        assert not any(c[0] == "kill" for c in backend.calls)
    else:
        assert result["final_mode"] == "LOCAL"
        assert result["fault_command_start_to_entry_seconds"] >= 0
        assert result["final_epoch"] > result["initial_epoch"]
    if scenario == "holder-failure":
        killed = [c[1] for c in backend.calls if c[0] == "kill"]
        started = [c[1] for c in backend.calls if c[0] == "start"]
        assert killed == ["controller", result["initial_holder"]]
        assert started == list(reversed(killed))
    events = [json.loads(line) for line in (output/"events.jsonl").read_text().splitlines()]
    assert sum(e["event"] == "request" for e in events) == 6
    assert sum(e["event"] == "physical_entry" for e in events) >= result["completed"]


def test_external_runner_refuses_existing_physical_state(h, tmp_path):
    backend = DeploymentDouble(h)
    assert h.observe()["allowed"]
    with pytest.raises(RuntimeError, match="UNKNOWN occupancy"):
        run(h, backend, tmp_path/"busy")
    assert backend.calls == []
    assert h.state["lane_state"] == "CLEAR"


def test_external_runner_does_not_manufacture_recovery(h, tmp_path):
    backend = DeploymentDouble(h, recover=False)
    with pytest.raises(RuntimeError, match="replacement LOCAL authority"):
        run(h, backend, tmp_path/"no-recovery", scenario="controller-outage")
    assert not backend.stopped
    assert not (tmp_path/"no-recovery/summary.json").exists()
    assert (tmp_path/"no-recovery/failure.json").exists()


def test_external_runner_requires_explicit_empty_model_and_never_overwrites(h, tmp_path):
    backend = DeploymentDouble(h)
    output = tmp_path/"results"
    with pytest.raises(ValueError, match="Explicit"):
        run_workload(backend, output)
    assert not output.exists()
    output.mkdir()
    marker = output/"events.jsonl"
    marker.write_text("existing evidence")
    with pytest.raises(FileExistsError):
        run(h, backend, output)
    assert marker.read_text() == "existing evidence"


def test_controller_loss_during_occupancy_preserves_the_physical_lane(h, tmp_path):
    backend = DeploymentDouble(h)
    output = tmp_path/"occupied"
    result = run(h, backend, output, scenario="controller-outage", crossing_time=.9)
    events = [json.loads(line) for line in (output/"events.jsonl").read_text().splitlines()]
    assert any(e["event"] == "external_authority_observed" and e["mode"] == "LOCAL"
               and e["occupant"] is not None for e in events)
    occupant = None
    for event in events:
        if event["event"] == "physical_entry":
            assert occupant is None
            occupant = event["crane"]
        elif event["event"] == "physical_exit":
            assert occupant == event["crane"]
            occupant = None
    assert occupant is None and result["passed"]


def test_failed_service_restoration_is_not_reported_as_a_pass(h, tmp_path):
    backend = DeploymentDouble(h)
    original = backend.fault
    def fault(action, service):
        if action == "start":
            raise RuntimeError("Docker start failed")
        original(action, service)
    backend.fault = fault
    result = run(h, backend, tmp_path/"restore-failed", scenario="controller-outage")
    assert not result["passed"] and not result["restoration_succeeded"]
    assert not json.loads((tmp_path/"restore-failed/summary.json").read_text())["passed"]


def test_unexpected_recovery_invalidates_healthy_results(h, tmp_path):
    backend = DeploymentDouble(h)
    original = backend.request
    def request(*args):
        answer = original(*args)
        assert h.install(h.state["holder"], mode="LOCAL")["allowed"]
        return answer
    backend.request = request
    with pytest.raises(RuntimeError, match="Unexpected authority recovery"):
        run(h, backend, tmp_path/"unexpected")


def load_external_script(monkeypatch):
    import importlib.util
    from pathlib import Path
    directory = Path(__file__).resolve().parents[1]/"scripts"
    monkeypatch.syspath_prepend(str(directory))
    spec = importlib.util.spec_from_file_location("external_workload_adapter", directory/"external_workload.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_external_clients_use_only_simulator_key_and_record_container_identity(tmp_path, monkeypatch):
    from contextlib import ExitStack
    module = load_external_script(monkeypatch)
    from integrated import provision
    directory = tmp_path/"credentials"
    provision(directory)
    manifest = module.endpoints()
    manifest.update(config=str(directory/"config.json"), signing_directory=str(directory/"signing"),
                    gate_tls_directory=str(directory/"tls/gate"))
    from cranelock_sim.workload_tls import write
    tls = directory/"workload-tls"
    write(tls)
    workload_config = directory/"workload.json"
    workload_config.write_text(json.dumps(dict(ca=str(tls/"ca.pem"), cert=str(tls/"client.pem"),
        key=str(tls/"client-key.pem"), agents={f"crane-{i}": f"localhost:{50200+i}" for i in (1,2,3)})))
    inventory = [dict(id=f"container-{i}", image=f"image-{i}", running=True,
                      project=manifest["project"], name=name) for i, name in enumerate(manifest["containers"].values())]
    monkeypatch.setattr(module, "endpoints", lambda: manifest)
    def inspect(command, **options):
        assert command[:3] == ["docker", "inspect", "--format"]
        assert "Env" not in command[3]
        return "\n".join(json.dumps(item) for item in inventory)
    monkeypatch.setattr(module.subprocess, "check_output", inspect)
    with ExitStack() as stack:
        deployment = module.ExternalDeployment(stack, workload_config)
        assert set(deployment.simulator.keys) == {"simulator"}
        assert set(deployment.workloads) == {"crane-1", "crane-2", "crane-3"}
        assert not hasattr(deployment, "cranes")
        assert deployment.endpoints["container_inventory"] == inventory


def test_external_adapter_rejects_stopped_or_wrong_project_containers(monkeypatch):
    from contextlib import ExitStack
    module = load_external_script(monkeypatch)
    manifest = module.endpoints()
    inventory = [dict(id=str(i), running=True, project=manifest["project"])
                 for i in manifest["containers"]]
    inventory[0]["running"] = False
    monkeypatch.setattr(module.subprocess, "check_output",
                        lambda *args, **kwargs: "\n".join(json.dumps(item) for item in inventory))
    with ExitStack() as stack, pytest.raises(RuntimeError, match="complete integrated"):
        module.ExternalDeployment(stack, None)
    inventory[0].update(running=True, project="unrelated-project")
    with ExitStack() as stack, pytest.raises(RuntimeError, match="complete integrated"):
        module.ExternalDeployment(stack, None)


def test_external_smoke_loads_only_simulator_signing_key(tmp_path, monkeypatch):
    import importlib.util
    import sys
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]/"scripts"))
    from integrated import provision
    import integrated
    import cranelock_gate.demo as demo
    from cranelock_gate.client import Client
    directory = tmp_path/"credentials"
    provision(directory)
    monkeypatch.setattr(integrated, "BASE", directory)
    original = demo.load_keys
    loaded = []
    def simulator_only(path):
        loaded.append(Path(path).name)
        if Path(path).name != "simulator.json":
            raise AssertionError("Smoke runner attempted to load a non-simulator key")
        return original(path)
    monkeypatch.setattr(demo, "load_keys", simulator_only)
    monkeypatch.setattr(Client, "state", lambda self: {"mode": "LOCAL", "lane_state": "UNKNOWN", "grant": None})
    script = Path(__file__).resolve().parents[1]/"scripts"/"external_smoke.py"
    spec = importlib.util.spec_from_file_location("secure_external_smoke", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["external_smoke", "--new-empty-simulation", "--workload-config", "unused"])
    with pytest.raises(RuntimeError, match="Requires CENTRAL"):
        module.main()
    assert loaded == ["simulator.json"]
