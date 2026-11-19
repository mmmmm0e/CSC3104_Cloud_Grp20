"""Credential isolation and no-overwrite checks for local deployment provisioning."""
import importlib.util
import base64
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography import x509
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import ExtendedKeyUsageOID


def load_helper():
    path = Path(__file__).resolve().parents[1] / "scripts/integrated.py"
    spec = importlib.util.spec_from_file_location("platform_handoff", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_provision_keys_match_membership_and_do_not_overwrite(tmp_path):
    helper = load_helper()
    directory = tmp_path / "experiment"
    config = helper.provision(directory)
    for identity, public in {**config.members, **config.service_keys}.items():
        values = json.loads((directory / "signing" / f"{identity}.json").read_text())
        assert set(values) == {identity}
        key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(values[identity]))
        assert key.public_key().public_bytes_raw().hex() == public
    before = (directory / "config.json").read_bytes()
    with pytest.raises(ValueError, match="Refusing to overwrite"):
        helper.provision(directory)
    assert (directory / "config.json").read_bytes() == before


def test_workload_provisioning_preserves_experiment_and_isolates_credentials(tmp_path):
    helper = load_helper()
    directory = tmp_path / "experiment"
    config = helper.provision(directory, include_test_identity=True)
    from scripts.workload_credentials import DENIED_IDENTITY, IDENTITY

    workload = directory / "workload"
    ca = x509.load_pem_x509_certificate((workload / "runner/ca.pem").read_bytes())
    assert (workload / "runner/ca.pem").read_bytes() != (directory / "tls/gate/ca.pem").read_bytes()
    options = json.loads((workload / "config.json").read_text())
    assert options["agents"] == {f"crane-{i}": f"localhost:5020{i}" for i in (1, 2, 3)}
    assert Path(options["key"]) == workload / "runner/client-key.pem"
    for role, identity in (("runner", IDENTITY), ("negative-test", DENIED_IDENTITY)):
        cert = x509.load_pem_x509_certificate((workload / role / "client.pem").read_bytes())
        cert.verify_directly_issued_by(ca)
        san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        assert list(san) == [x509.UniformResourceIdentifier(identity)]
        assert ExtendedKeyUsageOID.CLIENT_AUTH in cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    keys = set()
    for crane in config.members:
        files = workload / crane
        server = x509.load_pem_x509_certificate((files / "server.pem").read_bytes())
        server.verify_directly_issued_by(ca)
        keys.add(server.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo))
        san = server.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        assert {crane, f"{crane}.cranelock.svc.cluster.local", "localhost"} <= set(san.get_values_for_type(x509.DNSName))
        assert json.loads((files / "allowlist.json").read_text()) == {
            "crane_id": crane, "lane_id": config.lane_id, "allowed_workload_uri_sans": [IDENTITY]}
        assert not (files / "client-key.pem").exists()
    assert len(keys) == 3
    signing = directory / "signing/crane-1.json"
    original = signing.read_bytes()
    with pytest.raises(ValueError, match="Refusing to overwrite existing workload credentials"):
        helper.provision_workload(directory)
    assert signing.read_bytes() == original


def test_kubernetes_keeps_runner_key_out_of_pods_and_public_manifest(tmp_path, monkeypatch):
    helper = load_helper()
    directory = tmp_path / "experiment"
    helper.provision(directory)
    monkeypatch.syspath_prepend(str(Path(helper.__file__).parent))
    import kubernetes

    workloads = {crane: dict(image="cranelock-agent:integrated", command=["run", "--workload-listen", "0.0.0.0:50053"],
                            volumes=[dict(type="volume", target="/data"),
                                     dict(type="bind", target="/workload/server-key.pem",
                                          source=str(directory / "workload" / crane / "server-key.pem"))])
                 for crane in helper.SERVICES[2:]}
    monkeypatch.setattr(kubernetes, "BASE", directory)
    monkeypatch.setattr(kubernetes, "OUT", tmp_path / "manifests")
    monkeypatch.setattr(kubernetes, "monitoring_secret", lambda: None)
    monkeypatch.setattr(kubernetes.subprocess, "run", lambda *a, **kw: SimpleNamespace(stdout=json.dumps({"services": workloads})))
    inputs = []
    monkeypatch.setattr(kubernetes, "apply_input", inputs.append)
    resources = kubernetes.render()
    runner = next(item for item in inputs if item["metadata"]["name"] == "workload-runner-tls")
    runner_key = runner["data"]["client-key.pem"]
    assert base64.b64decode(runner_key) == (directory / "workload/runner/client-key.pem").read_bytes()
    for crane in workloads:
        service = next(item for item in resources if item["kind"] == "Service" and item["metadata"]["name"] == crane)
        assert {port["port"] for port in service["spec"]["ports"]} == {50051, 50053, 9101}
        stateful_set = next(item for item in resources if item["kind"] == "StatefulSet" and item["metadata"]["name"] == crane)
        pod = stateful_set["spec"]["template"]["spec"]
        assert [v["secret"]["secretName"] for v in pod["volumes"]] == [f"{crane}-workload-tls"]
        secret = next(item for item in inputs if item["metadata"]["name"] == f"{crane}-workload-tls")
        assert set(secret["data"]) == {"ca.pem", "server.pem", "server-key.pem", "allowlist.json"}
        assert stateful_set["spec"]["volumeClaimTemplates"][0]["metadata"]["name"] == "data"
    public = (kubernetes.OUT / "resources.yaml").read_text() + json.dumps([i for i in inputs if i["kind"] == "ConfigMap"])
    assert "PRIVATE KEY" not in public and runner_key not in public
    for secret in (i for i in inputs if i["kind"] == "Secret"):
        assert all(value not in public for name, value in secret["data"].items() if name.endswith("-key.pem"))


def test_peer_tls_identities_per_crane_and_upgrade_without_overwrite(tmp_path):
    import shutil
    from cranelock.peer_tls import agent_uri, certificate_uri
    helper = load_helper()
    directory = tmp_path / "experiment"
    helper.provision(directory)
    peer = helper.require_peer_tls(directory)
    for crane in helper.CRANE_IDS:
        assert certificate_uri((peer / f"{crane}.pem").read_bytes()) == agent_uri(crane)
    assert certificate_uri((peer / "monitor.pem").read_bytes()) == "urn:cranelock:monitor"
    # An experiment provisioned before peer mTLS gains only tls/agents.
    shutil.rmtree(peer)
    with pytest.raises(ValueError, match="provision-peer-tls"):
        helper.require_peer_tls(directory)
    signing = (directory / "signing/crane-1.json").read_bytes()
    helper.provision_peer_tls(directory)
    assert helper.require_peer_tls(directory) == peer
    assert (directory / "signing/crane-1.json").read_bytes() == signing
    with pytest.raises(FileExistsError):
        helper.provision_peer_tls(directory)
