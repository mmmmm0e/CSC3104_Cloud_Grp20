import asyncio
import json
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from cranelock_sim.dashboard import make_server
from cranelock_sim.live import LiveRuntime, LiveYard


# exercises the full operator walkthrough with native recovery workers
def test_live_yard_uses_gate_authority_and_preserves_occupancy(tmp_path):
    async def exercise():
        yard = LiveYard(tmp_path)

        async def action(name, crane=None):
            payload = dict(action=name)
            if crane:
                payload["crane"] = crane
            return await yard.action(payload)

        async def wait_for(condition):
            async with asyncio.timeout(10):
                while not condition(state := await yard.state()):
                    await asyncio.sleep(.1)
            return state

        try:
            await yard.start()
            state = await action("request", "crane-1")
            assert state["reservation"] == "crane-1" and state["occupant"] is None
            state = await action("enter", "crane-1")
            assert state["occupant"] == "crane-1" and state["gate"]["lane_state"] == "OCCUPIED"
            await action("request", "crane-3")
            await action("stop_controller")
            state = await wait_for(lambda state: state["gate"]["mode"] == "LOCAL")
            assert state["gate"]["epoch"] == 1 and state["gate"]["holder"] == "crane-1"
            assert state["occupant"] == "crane-1"
            state = await action("stale_entry")
            assert state["decision"] == "STALE_EPOCH"

            await action("stop_agent", "crane-1")
            state = await wait_for(lambda state: state["gate"]["epoch"] == 2)
            assert state["gate"]["holder"] == "crane-2"
            assert state["occupant"] == "crane-1" and state["reservation"] is None
            assert state["waiting"] == ["crane-3"]
            await action("clear")
            await wait_for(lambda state: state["reservation"] == "crane-3")
            state = await action("enter", "crane-3")
            assert state["occupant"] == "crane-3"
            state = await action("exit", "crane-3")
            assert state["completed"] == 1 and state["gate"]["lane_state"] == "CLEAR"
            await action("restore_controller")
            state = await wait_for(lambda state: state["gate"]["mode"] == "CENTRAL")
            assert state["gate"]["epoch"] == 2 and state["gate"]["holder"] == "crane-3"
        finally:
            await yard.close()

        summary = json.loads((yard.directory / "summary.json").read_text(encoding="utf-8"))
        assert summary["safety_violations"] == 0

    asyncio.run(exercise())


# verifies the live route and session checks alongside the evaluation page
def test_live_yard_http_callbacks_are_opt_in(tmp_path):
    runtime = LiveRuntime(tmp_path)
    server = make_server(tmp_path, 0, live=runtime)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urlopen(base + "/live", timeout=3) as response:
            page = response.read()
            assert b"Live crane yard" in page
            assert b'<a href="/">Evaluation</a>' in page
        with urlopen(base + "/", timeout=3) as response:
            page = response.read()
            assert b"Crane coordination tests" in page
            assert b'<a href="/live" data-live-view>Live demo</a>' in page
        with urlopen(base + "/views.css", timeout=3) as response:
            assert b".view-header" in response.read()
        with urlopen(base + "/api/live/state", timeout=3) as response:
            token = json.load(response)["session_token"]
        body = b'{"action":"request","crane":"crane-1"}'
        for origin, session in ((base, ""), ("https://other.example", token)):
            request = Request(base + "/api/live/action", body, headers={
                "Content-Type": "application/json", "Origin": origin, "X-CraneLock-Session": session,
            })
            with pytest.raises(HTTPError) as error:
                urlopen(request, timeout=3)
            assert error.value.code == 403
        request = Request(base + "/api/live/action", body, headers={
            "Content-Type": "application/json", "Origin": base, "X-CraneLock-Session": token,
        })
        with urlopen(request, timeout=3) as response:
            assert json.load(response)["reservation"] == "crane-1"
        with make_server(tmp_path, 0, live_port=server.server_port) as evaluation:
            evaluation_thread = threading.Thread(target=evaluation.serve_forever, daemon=True)
            evaluation_thread.start()
            try:
                url = f"http://127.0.0.1:{evaluation.server_port}/live"
                with urlopen(url, timeout=3) as response:
                    assert response.url == base + "/live"
            finally:
                evaluation.shutdown()
                evaluation_thread.join()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
        runtime.close()

    with make_server(tmp_path, 0) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with pytest.raises(HTTPError) as error:
                urlopen(f"http://127.0.0.1:{server.server_port}/live", timeout=3)
            assert error.value.code == 404
        finally:
            server.shutdown()
            thread.join()
