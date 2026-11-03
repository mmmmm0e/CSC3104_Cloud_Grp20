import asyncio

from cranelock_sim.failures import until
from cranelock_sim.live import LiveYard


# waits for actual gate authority without initiating recovery
async def wait_authority(yard, mode, epoch, timeout=12):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        state = await yard.state()
        if state["gate_fresh"] and state["gate"]["mode"] == mode and state["gate"]["epoch"] == epoch:
            return state
        await asyncio.sleep(0.05)
    raise AssertionError(f"Authority did not reach {mode} epoch {epoch}: {state}")


# exercises unattended recovery, holder replacement and controller return
def test_automatic_cycle_preserves_occupancy_and_waiting_work(tmp_path):
    async def scenario():
        yard = LiveYard(tmp_path, automatic=True)
        await yard.start()
        try:
            await yard.action(dict(action="request", crane="crane-1"))
            await yard.action(dict(action="enter", crane="crane-1"))
            await yard.action(dict(action="request", crane="crane-3"))
            await yard.action(dict(action="stop_controller"))
            state = await wait_authority(yard, "LOCAL", 1)
            assert state["gate"]["holder"] == "crane-1"
            assert state["occupant"] == "crane-1" and state["waiting"] == ["crane-3"]
            assert state["reservation"] is None
            state = await yard.action(dict(action="stale_entry"))
            assert state["decision"] == "STALE_EPOCH"
            await yard.action(dict(action="stop_agent", crane="crane-1"))
            state = await wait_authority(yard, "LOCAL", 2)
            assert state["gate"]["holder"] == "crane-2" and state["occupant"] == "crane-1"
            assert state["reservation"] is None
            await yard.action(dict(action="restore_controller"))
            state = await wait_authority(yard, "CENTRAL", 2)
            assert state["gate"]["holder"] == "crane-2" and state["waiting"] == ["crane-3"]
            assert state["occupant"] == "crane-1" and state["gate"]["lane_state"] == "OCCUPIED"
            await yard.action(dict(action="clear"))
            state = await yard.action(dict(action="enter", crane="crane-3"))
            assert state["occupant"] == "crane-3"
            state = await yard.action(dict(action="exit", crane="crane-3"))
            assert state["completed"] == 1
            assert yard.cluster.violation is None
            assert sum(event["event"] == "agent_event" and
                event["agent_event"]["event"] == "AUTOMATIC_RECOVERY_INSTALLED" for event in yard.events) == 2
        finally:
            await yard.close()
    asyncio.run(scenario())


# refuses replacement authority when no communicating majority exists
def test_automatic_recovery_waits_for_majority_then_heals(tmp_path):
    async def scenario():
        yard = LiveYard(tmp_path, automatic=True)
        await yard.start()
        try:
            await yard.action(dict(action="isolate"))
            await yard.action(dict(action="stop_controller"))
            await until(lambda: all(len(agent.suspected) == 2 and agent.controller_suspected
                for agent in yard.cluster.agents.values()), 5, "partition suspicion")
            await asyncio.sleep(yard.cluster.timeout * 2)
            state = await yard.state()
            assert state["gate"]["epoch"] == 0 and state["gate"]["mode"] == "CENTRAL"
            assert all(not agent.vote_state for agent in yard.cluster.agents.values())
            await yard.action(dict(action="heal"))
            state = await wait_authority(yard, "LOCAL", 1)
            assert state["recovery"]["votes"] >= 2
        finally:
            await yard.close()
    asyncio.run(scenario())


# only the majority replaces a partitioned holder and never clears occupancy
def test_automatic_majority_partition_and_gate_outage(tmp_path):
    async def scenario():
        yard = LiveYard(tmp_path, automatic=True)
        await yard.start()
        try:
            await yard.action(dict(action="request", crane="crane-1"))
            await yard.action(dict(action="enter", crane="crane-1"))
            await yard.action(dict(action="partition"))
            await yard.action(dict(action="stop_controller"))
            state = await wait_authority(yard, "LOCAL", 1)
            assert state["gate"]["holder"] == "crane-2"
            assert state["occupant"] == "crane-1" and state["reservation"] is None
            await yard.action(dict(action="gate"))
            state = await yard.action(dict(action="request", crane="crane-3"))
            assert state["reservation"] is None and not state["gate_online"]
            await yard.action(dict(action="gate"))
            await asyncio.sleep(1)
            state = await yard.state()
            assert state["occupant"] == "crane-1" and state["gate"]["lane_state"] == "OCCUPIED"
            assert state["reservation"] is None
        finally:
            await yard.close()
    asyncio.run(scenario())


# retains unentered requests across recovery revocation
def test_automatic_revoked_reservation_is_requeued(tmp_path):
    async def scenario():
        yard = LiveYard(tmp_path, automatic=True)
        await yard.start()
        try:
            before = await yard.action(dict(action="request", crane="crane-1"))
            grant = yard.reservation["grant"]["id"]
            assert before["reservation"] == "crane-1"
            await yard.action(dict(action="stop_controller"))
            await wait_authority(yard, "LOCAL", 1)
            deadline = asyncio.get_running_loop().time() + 5
            while asyncio.get_running_loop().time() < deadline:
                if yard.reservation and yard.reservation["grant"]["id"] != grant:
                    break
                await asyncio.sleep(0.05)
            assert yard.reservation and yard.reservation["grant"]["id"] != grant
            assert len(yard.rows) == 1 and yard.reservation["row"]["status"] == "waiting"
            state = await yard.action(dict(action="enter", crane="crane-1"))
            assert state["occupant"] == "crane-1"
        finally:
            await yard.close()
    asyncio.run(scenario())


# a stopped requester cannot prevent healthy queued cranes from progressing
def test_automatic_queue_skips_stopped_requester(tmp_path):
    async def scenario():
        yard = LiveYard(tmp_path, automatic=True)
        await yard.start()
        try:
            await yard.action(dict(action="request", crane="crane-1"))
            await yard.action(dict(action="request", crane="crane-3"))
            await yard.action(dict(action="stop_agent", crane="crane-1"))
            await yard.action(dict(action="stop_controller"))
            await wait_authority(yard, "LOCAL", 1)
            deadline = asyncio.get_running_loop().time() + 5
            while asyncio.get_running_loop().time() < deadline:
                state = await yard.state()
                if state["reservation"] == "crane-3":
                    break
                await asyncio.sleep(0.05)
            assert state["reservation"] == "crane-3" and state["waiting"] == ["crane-1"]
            state = await yard.action(dict(action="enter", crane="crane-3"))
            assert state["occupant"] == "crane-3"
        finally:
            await yard.close()
    asyncio.run(scenario())
