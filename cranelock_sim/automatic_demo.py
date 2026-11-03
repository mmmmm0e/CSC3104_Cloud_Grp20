import argparse
import asyncio
from datetime import datetime
import json
from pathlib import Path
import uuid

from .live import LiveYard


# waits for a committed authority transition without invoking recovery
async def wait_authority(yard, mode, epoch, timeout=15):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        state = await yard.state()
        if state["gate_fresh"] and (state["gate"]["mode"], state["gate"]["epoch"]) == (mode, epoch):
            return state
        await asyncio.sleep(0.05)
    raise RuntimeError(f"Automatic authority transition timed out: {mode}/{epoch}")


# records automatic recovery while all movement remains gate validated
async def run_automatic_demo(output, *, progress=print, peer_delay=0, peer_jitter=0, seed=20):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    yard = LiveYard(output, automatic=True)
    checks = []
    log = (output / "demo.log").open("x", encoding="utf-8")

    # reports only observed component behavior
    def check(label, condition):
        if not condition:
            raise AssertionError(label)
        checks.append(label)
        line = f"[PASS] automatic flow: {label}"
        log.write(line + "\n")
        log.flush()
        progress(line)

    try:
        await yard.start()
        yard.network.set_delay(peer_delay, peer_jitter, seed)
        await yard.action(dict(action="request", crane="crane-1"))
        state = await yard.action(dict(action="enter", crane="crane-1"))
        check("normal entry accepted", state["occupant"] == "crane-1")
        await yard.action(dict(action="request", crane="crane-3"))
        await yard.action(dict(action="stop_controller"))
        state = await wait_authority(yard, "LOCAL", 1)
        check("controller loss automatically installs majority recovery",
              state["gate"]["holder"] == "crane-1" and state["recovery"]["votes"] >= 2)
        check("recovery retains occupancy and waiting request",
              state["occupant"] == "crane-1" and state["waiting"] == ["crane-3"])
        state = await yard.action(dict(action="stale_entry"))
        check("old entry permission rejected", state["decision"] == "STALE_EPOCH")
        await yard.action(dict(action="stop_agent", crane="crane-1"))
        state = await wait_authority(yard, "LOCAL", 2)
        check("holder loss automatically installs replacement",
              state["gate"]["holder"] == "crane-2" and state["occupant"] == "crane-1")
        check("replacement receives no permission while occupied", state["reservation"] is None)
        await yard.action(dict(action="restore_controller"))
        state = await wait_authority(yard, "CENTRAL", 2)
        check("controller return reconciles automatically",
              state["gate"]["holder"] == "crane-2" and state["occupant"] == "crane-1"
              and state["waiting"] == ["crane-3"])
        await yard.action(dict(action="clear"))
        state = await yard.action(dict(action="enter", crane="crane-3"))
        check("queued crane enters only after explicit clearance", state["occupant"] == "crane-3")
        state = await yard.action(dict(action="exit", crane="crane-3"))
        check("safe crossing completes", state["completed"] == 1 and yard.cluster.violation is None)
        installs = [event for event in yard.events if event["event"] == "agent_event"
                    and event["agent_event"]["event"] == "AUTOMATIC_RECOVERY_INSTALLED"]
        stopped_controller = next(event["time"] for event in yard.events if event["event"] == "controller_stopped")
        stopped_holder = next(event["time"] for event in yard.events if event["event"] == "operator_action"
                              and event.get("action") == "stop_agent")
        restored = next(event["time"] for event in yard.events if event["event"] == "controller_restored")
        returned = next(event["time"] for event in yard.events if event["event"] == "controller_event"
                        and event["controller_event"]["event"] == "AUTOMATIC_CENTRAL_RETURN")
        summary = dict(model="automatic-lifecycle", passed=True, checks_passed=checks,
            peer_delay_seconds=peer_delay, peer_jitter_seconds=peer_jitter, seed=seed,
            final_epoch=state["gate"]["epoch"], final_mode=state["gate"]["mode"],
            safety_violations=0, evidence_directory=str(yard.directory),
            controller_failure_to_install_seconds=round(installs[0]["time"] - stopped_controller, 3),
            holder_failure_to_install_seconds=round(installs[1]["time"] - stopped_holder, 3),
            controller_restore_to_reconciliation_seconds=round(returned - restored, 3),
            limitations="Local gRPC services. Faults and physical clearance are injected; recovery and return are automatic.")
        (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary
    except BaseException as error:
        (output / "failure.json").write_text(json.dumps(dict(error=type(error).__name__,
            detail=str(error), checks_passed=checks)), encoding="utf-8")
        raise
    finally:
        await yard.close()
        log.close()


# repeats the actual automatic lifecycle under seeded peer delivery delays
async def run_delay_sweep(output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    results = []
    for delay, jitter in ((0.005, 0.015), (0.01, 0.02)):
        for seed in (7, 20, 3104):
            results.append(await run_automatic_demo(output / f"delay-{delay}-seed-{seed}",
                peer_delay=delay, peer_jitter=jitter, seed=seed))
    summary = dict(passed=True, runs=len(results), results=results,
        limitations="Local application-level peer RPC delays and jitter; not packet-level network emulation. Seeded delay draws do not make OS scheduling deterministic.")
    (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


# runs the reproducible automatic lifecycle and retains its evidence
def main():
    parser = argparse.ArgumentParser(description="Verify automatic CraneLock recovery and controller return")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--peer-delay", type=float, default=0)
    parser.add_argument("--peer-jitter", type=float, default=0)
    parser.add_argument("--seed", type=int, default=20)
    parser.add_argument("--delay-sweep", action="store_true", help="run two delay settings with three seeds each")
    args = parser.parse_args()
    output = args.output or Path("run/evaluation") / (
        "automatic-" + datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
    if args.delay_sweep:
        asyncio.run(run_delay_sweep(output))
    else:
        asyncio.run(run_automatic_demo(output, peer_delay=args.peer_delay, peer_jitter=args.peer_jitter, seed=args.seed))
    print(f"Evidence: {output.resolve()}")


if __name__ == "__main__":
    main()
