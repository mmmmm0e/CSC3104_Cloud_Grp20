"""Credential isolation and no-overwrite checks for local deployment provisioning."""
import importlib.util
import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


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
