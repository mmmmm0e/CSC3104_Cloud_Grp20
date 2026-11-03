import argparse
import asyncio
import json
import time
import uuid
from collections.abc import Mapping

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import grpc
from google.protobuf.json_format import MessageToDict

from cranelock_gate.client import Client
from cranelock_gate.crypto import CERTIFICATE_DOMAIN, canonical, encode_signature

from .agent import Agent, Mode
from . import protocol_pb2 as agent_pb
from . import protocol_pb2_grpc as agent_rpc
from .reconciliation import ReconciliationClient, ReconciliationError


class ControllerError(RuntimeError):
    # Gate state cannot safely drive controller actions
    pass


class AgentStateMonitor:
    # Polls live state from configured crane-agent gRPC endpoints

    def __init__(self, agents, *, timeout=2.0):
        if not isinstance(agents, Mapping) or not agents:
            raise ValueError("agents must map agent IDs to gRPC addresses")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if any(not isinstance(agent_id, str) or not agent_id
               or not isinstance(address, str) or not address
               for agent_id, address in agents.items()):
            raise ValueError("Agent IDs and gRPC addresses must be nonempty strings")
        self.timeout = timeout
        self.channels = {agent_id: grpc.insecure_channel(address)
                         for agent_id, address in agents.items()}
        self.stubs = {agent_id: agent_rpc.CraneAgentStub(channel)
                      for agent_id, channel in self.channels.items()}

    def read_states(self):
        results = {}
        for agent_id, stub in self.stubs.items():
            try:
                response = stub.GetAgentState(agent_pb.Empty(), timeout=self.timeout)
                state = MessageToDict(
                    response,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )
                if state["agent_id"] != agent_id:
                    results[agent_id] = {
                        "status": "IDENTITY_MISMATCH",
                        "reported_agent_id": state["agent_id"],
                    }
                else:
                    results[agent_id] = {"status": "AVAILABLE", "state": state}
            except grpc.RpcError as error:
                results[agent_id] = {
                    "status": "UNAVAILABLE",
                    "error": error.code().name,
                    "details": error.details() or "",
                }
        return {"observed_at": time.time(), "agents": results}

    def close(self):
        for channel in self.channels.values():
            channel.close()


def format_snapshot(snapshot, phase):
    # Formats a monitor snapshot as a readable per-agent status report
    observed_at = time.strftime(
        "%Y-%m-%d %H:%M:%S %z", time.localtime(snapshot["observed_at"]))
    lines = [f"Agent state {phase} - {observed_at}"]
    for agent_id, result in snapshot["agents"].items():
        status = result["status"]
        if status == "AVAILABLE":
            state = result["state"]
            token = "yes" if state.get("has_token") else "no"
            lines.append(
                f"  {agent_id}: {status} | epoch {state.get('epoch', '?')} | "
                f"{state.get('mode', '?')} | holds token: {token} | "
                f"holder: {state.get('token_holder') or '-'}")
            peers = state.get("peers", [])
            if peers:
                peer_status = ", ".join(
                    f"{peer.get('agent_id', '?')}={peer.get('status', '?')}"
                    for peer in peers)
                lines.append(f"    peers: {peer_status}")
            queue = state.get("request_queue", [])
            lines.append(f"    requests waiting: {', '.join(queue) if queue else '-'}")
            transfer = state.get("pending_transfer_id")
            if transfer:
                lines.append(f"    pending transfer: {transfer}")
        elif status == "IDENTITY_MISMATCH":
            lines.append(
                f"  {agent_id}: {status} | endpoint reported "
                f"{result.get('reported_agent_id', '?')}")
        else:
            detail = result.get("details")
            error = result.get("error", "UNKNOWN")
            lines.append(
                f"  {agent_id}: {status} | {error}"
                + (f" - {detail}" if detail else ""))
    return "\n".join(lines)


class CentralController:
    # Coordinates central authority for a configured gate.

    def __init__(self, config, private_key, channel):
        if not isinstance(private_key, Ed25519PrivateKey):
            raise TypeError("Controller signing key must be an Ed25519 private key")
        expected_key = bytes.fromhex(config.service_keys[config.controller_id])
        actual_key = private_key.public_key().public_bytes_raw()
        if actual_key != expected_key:
            raise ValueError("Controller private key does not match configured identity")
        self.config = config
        self.private_key = private_key
        self.client = Client(channel, config, {config.controller_id: private_key})

    def get_state(self):
        # Returns the latest state reported by this gate.
        return self.client.state()

    def install_central(self, holder, *, request_id=None, max_attempts=3, expected_state=None):
        # Installs CENTRAL authority, refreshing snapshots after stale decisions.
        if holder not in self.config.members:
            raise ValueError("Holder must be a configured member")
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        if request_id is not None and (
                not isinstance(request_id, str) or not 1 <= len(request_id) <= 128):
            raise ValueError("request_id must contain 1 to 128 characters")

        for attempt in range(max_attempts):
            state = expected_state if expected_state is not None else self.get_state()
            epoch = state.get("epoch")
            revision = state.get("revision")
            if type(epoch) is not int or epoch < -1 or type(revision) is not int or revision < 0:
                raise ControllerError("Gate returned invalid authority state")
            if epoch == -1 and revision != 0:
                raise ControllerError("Uninitialized gate has an unexpected revision")

            payload = dict(
                version=1,
                lane_id=self.config.lane_id,
                membership_id=self.config.membership_id,
                epoch=0 if epoch == -1 else epoch,
                mode="CENTRAL",
                holder=holder,
                expected_epoch=epoch,
                expected_revision=revision,
            )
            certificate = dict(
                payload=payload,
                signatures=[dict(
                    signer_id=self.config.controller_id,
                    signature=encode_signature(
                        self.private_key.sign(CERTIFICATE_DOMAIN + canonical(payload))),
                )],
            )
            result = self.client.call(
                self.config.controller_id,
                "InstallAuthority",
                request_id=request_id if attempt == 0 and request_id else str(uuid.uuid4()),
                certificate=certificate,
                expected_epoch=epoch,
                expected_revision=revision,
            )
            if (expected_state is not None or result["allowed"]
                    or result["reason"] not in {"STALE_EPOCH", "STALE_REVISION"}):
                return result
        return result

    # reconciles remote agents without sharing agent objects
    async def reconcile_remote(self, agents, *, credentials=None, timeout=3):
        state = await asyncio.to_thread(self.get_state)
        client = ReconciliationClient(self.config, self.config.controller_id, self.private_key,
                                      credentials=credentials, timeout=timeout)
        await client.reconcile(agents, state)
        current = await asyncio.to_thread(self.get_state)
        if any(current.get(key) != state.get(key) for key in ("epoch", "revision", "holder", "mode")):
            raise ReconciliationError("Gate authority changed during reconciliation")
        return current

    async def reconcile_agents(self, agents):
        # Reconciles supplied agents from the gate's currently accepted authority.
        if not isinstance(agents, Mapping):
            raise TypeError("agents must be a mapping of member IDs to agent instances")
        for agent_id, agent in agents.items():
            if (agent_id not in self.config.members or not isinstance(agent, Agent)
                    or agent.state.agent_id != agent_id):
                raise ValueError("Agent mapping must use configured IDs matching each agent")

        state = await asyncio.to_thread(self.get_state)
        epoch = state.get("epoch")
        holder = state.get("holder")
        mode_name = state.get("mode")
        if (type(epoch) is not int or epoch < 0 or not isinstance(holder, str)
                or holder not in self.config.members
                or not isinstance(mode_name, str) or mode_name not in {"CENTRAL", "LOCAL"}):
            raise ControllerError("Gate has no valid installed authority to reconcile")

        mode = Mode[mode_name]
        for agent in agents.values():
            if agent.gate:
                if not await agent.reconcile_from_gate():
                    raise ControllerError("Agent could not reconcile from its gate")
            else:
                await agent.reconcile(epoch, holder, mode, revision=state["revision"])
        return state


class CentralControllerFleet:
    # Collects and dispatches controller operations across independent lane gates

    def __init__(self, controllers):
        if not isinstance(controllers, Mapping) or not controllers:
            raise ValueError("controllers must be a mapping of lane IDs to controller instances")
        for lane_id, controller in controllers.items():
            if (not isinstance(controller, CentralController)
                    or lane_id != controller.config.lane_id):
                raise ValueError("Each controller must be registered under its configured lane ID")
        self.controllers = dict(controllers)

    def get_all_states(self):
        # Reads every configured gate and propagates failures rather than omitting lanes.
        return {lane_id: controller.get_state()
                for lane_id, controller in self.controllers.items()}

    def install_central(self, lane_id, holder, **options):
        return self._controller(lane_id).install_central(holder, **options)

    async def reconcile_agents(self, lane_id, agents):
        # Reconciles agents for the selected lane.
        return await self._controller(lane_id).reconcile_agents(agents)

    def _controller(self, lane_id):
        try:
            return self.controllers[lane_id]
        except KeyError:
            raise ValueError(f"No gate is configured for lane {lane_id!r}") from None


def main():
    # Poll agent state and print each snapshot until interrupted
    parser = argparse.ArgumentParser(
        description="Monitor live CraneLock agent state from the central controller")
    parser.add_argument("--agent", action="append", required=True, metavar="ID=HOST:PORT")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--reconcile", action="store_true", help="send signed gate-confirmed reconciliation RPCs")
    parser.add_argument("--serve", action="store_true", help="serve controller RPCs and automatically reconcile LOCAL return")
    parser.add_argument("--listen", default="127.0.0.1:50070")
    parser.add_argument("--bootstrap-holder", help="initialize an empty gate when starting --serve")
    parser.add_argument("--gate-address")
    parser.add_argument("--gate-config")
    parser.add_argument("--gate-keys", help="controller private-key JSON file")
    args = parser.parse_args()
    if args.interval <= 0 or args.timeout <= 0:
        parser.error("--interval and --timeout must be positive")
    agents = {}
    for value in args.agent:
        if "=" not in value:
            parser.error(f"Invalid --agent {value!r}; expected ID=HOST:PORT")
        agent_id, address = value.split("=", 1)
        if not agent_id or not address or agent_id in agents:
            parser.error(f"Invalid or duplicate --agent {value!r}")
        agents[agent_id] = address

    if args.serve and args.reconcile:
        parser.error("Choose --serve or --reconcile")
    if args.bootstrap_holder and not args.serve:
        parser.error("--bootstrap-holder requires --serve")
    if args.reconcile or args.serve:
        if not all((args.gate_address, args.gate_config, args.gate_keys)):
            parser.error("Reconciliation requires --gate-address, --gate-config and --gate-keys")
        from cranelock_gate.config import Config
        from cranelock_gate.demo import load_keys
        config = Config.load(args.gate_config)
        key = load_keys(args.gate_keys)[config.controller_id]
        if args.serve and (args.listen.rsplit(":", 1)[0] not in {"127.0.0.1", "localhost", "[::1]"}
                or args.gate_address.rsplit(":", 1)[0] not in {"127.0.0.1", "localhost", "[::1]"}
                or any(address.rsplit(":", 1)[0] not in {"127.0.0.1", "localhost", "[::1]"}
                       for address in agents.values())):
            parser.error("The controller service CLI is restricted to loopback; use credentialed Python services remotely")
        with grpc.insecure_channel(args.gate_address) as channel:
            controller = CentralController(config, key, channel)
            if args.serve:
                if args.bootstrap_holder:
                    state = controller.get_state()
                    if state["epoch"] == -1:
                        result = controller.install_central(args.bootstrap_holder, expected_state=state)
                        if not result["allowed"]:
                            raise ControllerError(result["reason"])
                from .controller_service import ControllerService, ControllerReturnWorker

                # runs return reconciliation for the lifetime of the controller service
                async def serve():
                    service = ControllerService(controller, args.listen)
                    worker = ControllerReturnWorker(controller, agents, timeout=args.timeout)
                    try:
                        await worker.run(args.interval)
                    finally:
                        await worker.close()
                        await asyncio.to_thread(service.close)

                try:
                    asyncio.run(serve())
                except KeyboardInterrupt:
                    pass
                return
            state = asyncio.run(controller.reconcile_remote(agents, timeout=args.timeout))
            print(json.dumps({name: state[name] for name in ("epoch", "revision", "holder", "mode")}),
                  flush=True)
        return

    monitor = AgentStateMonitor(agents, timeout=args.timeout)
    try:
        phase = "startup"
        while True:
            snapshot = monitor.read_states()
            print(format_snapshot(snapshot, phase), flush=True)
            phase = "update"
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        monitor.close()


if __name__ == "__main__":
    main()
