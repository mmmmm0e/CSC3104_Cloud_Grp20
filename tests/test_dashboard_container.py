import json
from pathlib import Path
import shutil
import subprocess
import threading
from urllib.request import urlopen

import pytest

from cranelock_sim.dashboard import make_server


# checks the container listener serves the original ui and results
def test_container_listener_serves_original_assets(tmp_path):
    with make_server(tmp_path, 0, host="0.0.0.0") as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            assets = Path(__file__).resolve().parents[1] / "cranelock_sim/web"
            for route, name in (("/", "index.html"), ("/app.js", "app.js"),
                                ("/style.css", "style.css")):
                with urlopen(base + route, timeout=3) as response:
                    assert response.read() == (assets / name).read_bytes()
            with urlopen(base + "/api/catalog", timeout=3) as response:
                assert json.load(response)["runs"] == []
        finally:
            server.shutdown()
            thread.join(timeout=3)


# validates the compose model when docker is installed
@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker CLI is not installed")
def test_progress_compose_resolves_container_commands():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        ["docker", "compose", "-f", "deploy/compose.progress.json", "--profile", "tools",
         "config", "--format", "json"], cwd=root, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    services = json.loads(result.stdout)["services"]
    assert services["demo"]["command"] == ["-m", "cranelock_sim.progress_demo"]
    assert not services["demo"].get("ports")
    dashboard = services["dashboard"]
    assert dashboard["ports"][0]["host_ip"] == "127.0.0.1"
    assert dashboard["volumes"][0]["read_only"] is True
    assert services["demo"]["volumes"][0].get("read_only", False) is False
