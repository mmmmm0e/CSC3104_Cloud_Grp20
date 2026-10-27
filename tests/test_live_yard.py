import asyncio
import json
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from cranelock_sim.dashboard import make_server
from cranelock_sim.failures import until
from cranelock_sim.live import LiveRuntime, LiveYard


# checks real movement, occupancy and recovery decisions behind the controls
def test_live_yard_moves_only_after_gate_acceptance(tmp_path):
    async def scenario():
        yard = LiveYard(tmp_path)
        await yard.start()
        try:
            state = await yard.action(dict(action="request", crane="crane-2"))
            assert state["reservation"] == "crane-2" and state["occupant"] is None
            assert state["gate"]["lane_state"] == "RESERVED"
            state = await yard.action(dict(action="enter", crane="crane-2"))
            assert state["occupant"] == "crane-2" and state["gate"]["lane_state"] == "OCCUPIED"
            state = await yard.action(dict(action="request", crane="crane-3"))
            assert state["waiting"] == ["crane-3"] and state["occupant"] == "crane-2"
            await yard.action(dict(action="stop_controller"))
            await until(lambda: all(agent.controller_suspected for agent in yard.cluster.agents.values()),
                        4, "controller suspicion")
            state = await yard.action(dict(action="recover", crane="crane-3"))
            assert state["gate"]["epoch"] == 1 and state["gate"]["mode"] == "LOCAL"
            assert state["occupant"] == "crane-2" and state["recovery"]["votes"] >= 2
            assert state["recovery"]["status"] == "Installed at gate"
            state = await yard.action(dict(action="stale_entry"))
            assert state["decision"] == "STALE_EPOCH" and state["occupant"] == "crane-2"
            state = await yard.action(dict(action="exit", crane="crane-2"))
            assert state["occupant"] is None and state["reservation"] == "crane-3"
            state = await yard.action(dict(action="enter", crane="crane-3"))
            assert state["occupant"] == "crane-3"
            state = await yard.action(dict(action="exit", crane="crane-3"))
            assert state["completed"] == 2 and state["gate"]["lane_state"] == "CLEAR"
            state = await yard.action(dict(action="restore_controller"))
            assert state["controller_online"] and state["gate"]["mode"] == "CENTRAL"
        finally:
            await yard.close()
        audit = [json.loads(line) for line in (yard.directory / "gate-events.jsonl").read_text().splitlines()]
        assert sum(event["operation"] == "ValidateEntry" and event["allowed"] and not event["replayed"]
                   for event in audit) == 2
    asyncio.run(scenario())


# a gate restart retains the independent physical occupant
def test_live_gate_restart_does_not_clear_the_lane(tmp_path):
    async def scenario():
        yard = LiveYard(tmp_path)
        await yard.start()
        try:
            await yard.action(dict(action="request", crane="crane-1"))
            await yard.action(dict(action="enter", crane="crane-1"))
            state = await yard.action(dict(action="gate"))
            assert not state["gate_online"] and state["occupant"] == "crane-1"
            state = await yard.action(dict(action="request", crane="crane-2"))
            assert state["reservation"] is None and state["waiting"] == ["crane-2"]
            await yard.action(dict(action="gate"))
            await asyncio.sleep(1)
            state = await yard.state()
            assert state["gate_fresh"] and state["occupant"] == "crane-1"
            assert state["gate"]["lane_state"] == "OCCUPIED"
            state = await yard.action(dict(action="enter", crane="crane-2"))
            assert state["occupant"] == "crane-1"
        finally:
            await yard.close()
    asyncio.run(scenario())


# exercises a real no-majority split and stale position evidence
def test_live_yard_minority_and_missing_observations_block_progress(tmp_path):
    async def scenario():
        yard = LiveYard(tmp_path)
        await yard.start()
        try:
            state = await yard.action(dict(action="recover", crane="crane-2"))
            assert state["recovery"]["votes"] == 0
            await yard.action(dict(action="stop_controller"))
            await yard.action(dict(action="isolate"))
            await until(lambda: all(agent.controller_suspected for agent in yard.cluster.agents.values()),
                        4, "controller suspicion")
            state = await yard.action(dict(action="recover", crane="crane-2"))
            assert state["recovery"]["votes"] == 1 and state["gate"]["epoch"] == 0
            assert state["recovery"]["status"] == "NO_QUORUM"
            await yard.action(dict(action="sensor"))
            await asyncio.sleep(yard.config.observation_timeout_seconds + 0.1)
            state = await yard.state()
            assert state["gate"]["lane_state"] == "UNKNOWN" and state["occupant"] is None
        finally:
            await yard.close()
    asyncio.run(scenario())


# only reachable members reconcile after a real peer partition
def test_live_majority_partition_fences_the_isolated_holder(tmp_path):
    async def scenario():
        yard = LiveYard(tmp_path)
        await yard.start()
        try:
            await yard.action(dict(action="request", crane="crane-1"))
            await yard.action(dict(action="enter", crane="crane-1"))
            await yard.action(dict(action="stop_controller"))
            await yard.action(dict(action="partition"))
            await until(lambda: all(agent.controller_suspected for agent in yard.cluster.agents.values()),
                        4, "controller suspicion")
            state = await yard.action(dict(action="recover", crane="crane-2"))
            assert state["gate"]["holder"] == "crane-2" and state["gate"]["epoch"] == 1
            assert state["recovery"]["votes"] == 2 and state["recovery"]["status"] == "Installed at gate"
            assert state["occupant"] == "crane-1"
            old = next(agent for agent in state["agents"] if agent["id"] == "crane-1")
            assert old["epoch"] == 0
            state = await yard.action(dict(action="stale_entry"))
            assert state["decision"] == "STALE_EPOCH" and state["occupant"] == "crane-1"
        finally:
            await yard.close()
    asyncio.run(scenario())


# stopping a crane does not remove its physical occupancy
def test_live_yard_stopped_occupant_requires_explicit_clearance(tmp_path):
    async def scenario():
        yard = LiveYard(tmp_path)
        await yard.start()
        try:
            await yard.action(dict(action="request", crane="crane-1"))
            await yard.action(dict(action="enter", crane="crane-1"))
            state = await yard.action(dict(action="stop_agent", crane="crane-1"))
            assert state["occupant"] == "crane-1"
            state = await yard.action(dict(action="exit", crane="crane-1"))
            assert state["occupant"] == "crane-1" and "running physical occupant" in state["decision"]
            state = await yard.action(dict(action="clear"))
            assert state["occupant"] is None and state["gate"]["lane_state"] == "CLEAR"
        finally:
            await yard.close()
    asyncio.run(scenario())


# protects live actions while the saved evaluation remains read only
def test_live_http_controls_require_local_session(tmp_path):
    runtime = LiveRuntime(tmp_path)
    server = make_server(tmp_path, 0, live=runtime)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urlopen(base + "/live", timeout=3) as response:
            assert b"One lane. Three cranes." in response.read()
        with urlopen(base + "/api/live/state", timeout=3) as response:
            state = json.load(response)
        body = json.dumps(dict(action="request", crane="crane-1")).encode()
        for headers in ({"Content-Type": "application/json"},
                        {"Content-Type": "application/json", "Origin": "https://untrusted.example",
                         "X-CraneLock-Session": state["session_token"]}):
            with pytest.raises(HTTPError) as error:
                urlopen(Request(base + "/api/live/action", data=body, headers=headers), timeout=3)
            assert error.value.code == 403
        headers = {"Content-Type": "application/json", "Origin": base,
                   "X-CraneLock-Session": state["session_token"]}
        with urlopen(Request(base + "/api/live/action", data=body, headers=headers), timeout=5) as response:
            assert json.load(response)["reservation"] == "crane-1"
        with urlopen(base + "/", timeout=3) as response:
            assert b"Crane coordination tests" in response.read()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        runtime.close()
