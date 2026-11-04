import argparse
import asyncio
import importlib
import inspect
import json
import math
from pathlib import Path
import grpc
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from google.protobuf.json_format import MessageToDict
from .agent import Agent, Mode
from . import recovery
from . import protocol_pb2 as pb
from . import protocol_pb2_grpc as rpc


def load_recovery_key(path, agent_id, config):
    """Load only this member's key, using the existing JSON/hex key format."""
    values = json.loads(Path(path).read_text(encoding="utf-8"))
    if type(values) is not dict or set(values) != {agent_id}:
        raise ValueError("Recovery key file must contain only this agent's identity")
    key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(values[agent_id]))
    recovery.validate_identity(config, agent_id, key)
    return key


def load_controller_probe(spec):
    module, separator, name = spec.partition(":")
    if not module or not separator or not name:
        raise ValueError("Expected MODULE:ASYNC_FUNCTION")
    probe = getattr(importlib.import_module(module), name)
    if not inspect.iscoroutinefunction(probe):
        raise ValueError("Controller probe must be an async function")
    inspect.signature(probe).bind()
    return probe


async def main(argv=None):
    parser = argparse.ArgumentParser(description="CraneLock normal-operation demo")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--id", required=True)
    run.add_argument("--listen", required=True)
    run.add_argument("--peer", action="append", default=[], metavar="ID=HOST:PORT")
    run.add_argument("--holder", required=True)
    run.add_argument("--epoch", type=int, default=1)
    run.add_argument("--mode", choices=["CENTRAL", "LOCAL"], default="LOCAL")
    run.add_argument("--interval", type=float, default=0.5)
    run.add_argument("--timeout", type=float, default=2)
    run.add_argument("--state-file", required=True)
    run.add_argument("--request-after", type=float)
    run.add_argument("--gate-address")
    run.add_argument("--gate-config")
    run.add_argument("--gate-keys", help="JSON private signing keys; only this agent's key is retained")
    run.add_argument("--gate-tls-ca")
    run.add_argument("--gate-tls-cert")
    run.add_argument("--gate-tls-key")
    run.add_argument("--recovery-config", help="Existing gate Config JSON; requires gate integration")
    run.add_argument("--recovery-key", help="JSON file containing only this agent's hex Ed25519 private key")
    run.add_argument("--controller-probe", metavar="MODULE:ASYNC_FUNCTION",
                     help="Installed async controller probe returning availability; requires gate and recovery")
    state = commands.add_parser("state")
    state.add_argument("address")
    args = parser.parse_args(argv)
    if args.command == "state":
        async with grpc.aio.insecure_channel(args.address) as channel:
            result = await rpc.CraneAgentStub(channel).GetAgentState(pb.Empty(), timeout=3)
            print(json.dumps(MessageToDict(result, preserving_proto_field_name=True,
                                          always_print_fields_with_no_presence=True), indent=2))
        return
    if (not math.isfinite(args.interval) or not math.isfinite(args.timeout)
            or not 0 < args.interval < args.timeout or args.epoch < 0):
        parser.error("Require nonnegative epoch and finite 0 < interval < timeout")
    if any("=" not in value or not all(value.split("=", 1)) for value in args.peer):
        parser.error("Each --peer must be ID=HOST:PORT with nonempty ID and address")
    peers = dict(p.split("=", 1) for p in args.peer)
    if len(peers) != len(args.peer):
        parser.error("Duplicate peer IDs")
    if not args.id or args.id in peers or args.holder not in {args.id, *peers}:
        parser.error("Require an ID, peers excluding self and a holder in the membership")
    recovery_config, recovery_key, controller_probe = None, None, None
    if bool(args.recovery_config) != bool(args.recovery_key):
        parser.error("Provide --recovery-config and --recovery-key together")
    if args.recovery_config:
        if not args.gate_address:
            parser.error("Recovery requires --gate-address, --gate-config and --gate-keys")
        from cranelock_gate.config import Config
        try:
            recovery_config = Config.load(args.recovery_config)
            if set(recovery_config.members) != {args.id, *peers}:
                parser.error("Agent IDs and peers must match configured recovery membership")
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            parser.error("Recovery configuration is invalid or unreadable")
        try:
            recovery_key = load_recovery_key(args.recovery_key, args.id, recovery_config)
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            parser.error("Recovery key is unreadable, invalid, not agent-only or does not match its public key")
    if args.controller_probe:
        if recovery_config is None:
            parser.error("--controller-probe requires gate integration and recovery configuration")
        try:
            controller_probe = load_controller_probe(args.controller_probe)
        except Exception:
            parser.error("Controller probe must identify an importable async function with no required arguments")
    gate = None
    if any([args.gate_tls_ca, args.gate_tls_cert, args.gate_tls_key]) and not args.gate_address:
        parser.error("Gate TLS options require --gate-address")
    if any([args.gate_address, args.gate_config, args.gate_keys]):
        if not all([args.gate_address, args.gate_config, args.gate_keys]):
            parser.error("Provide --gate-address, --gate-config and --gate-keys together")
        from .gate_client import GateClient
        credentials = None
        tls = [args.gate_tls_ca, args.gate_tls_cert, args.gate_tls_key]
        if any(tls):
            if not all(tls):
                parser.error("Provide all three --gate-tls-* files for mutual TLS")
            try:
                credentials = grpc.ssl_channel_credentials(
                    Path(args.gate_tls_ca).read_bytes(), Path(args.gate_tls_key).read_bytes(),
                    Path(args.gate_tls_cert).read_bytes())
            except (OSError, ValueError):
                parser.error("Gate TLS files are invalid or unreadable")
        elif args.gate_address.rsplit(":", 1)[0] not in {"127.0.0.1", "localhost", "[::1]"}:
            parser.error("Gate connections outside loopback require mutual TLS")
        try:
            gate = GateClient.connect(args.gate_address, args.gate_config, args.gate_keys,
                                      args.id, timeout=args.timeout, credentials=credentials)
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            parser.error("Gate configuration or signing key is invalid or unreadable")
        if set(gate.client.config.members) != {args.id, *peers}:
            await gate.close()
            parser.error("Agent IDs and peers must match configured gate membership")
        if recovery_config is not None and recovery_config != gate.client.config:
            await gate.close()
            parser.error("Recovery and gate configuration must match")
    try:
        agent = Agent(args.id, peers, args.holder, epoch=args.epoch, mode=Mode[args.mode],
                      interval=args.interval, timeout=args.timeout, state_file=args.state_file, gate=gate,
                      recovery_config=recovery_config, recovery_private_key=recovery_key,
                      controller_probe=controller_probe)
    except (OSError, ValueError, TypeError, KeyError):
        if gate:
            await gate.close()
        parser.error("Agent state or startup configuration is invalid or unreadable")
    try:
        await agent.start(args.listen)
        if args.request_after is not None:
            await asyncio.sleep(args.request_after)
            reply = await agent.request_token()
            agent.emit("TOKEN_REQUEST_RESULT", accepted=reply.accepted, reason=reply.reason)
        await asyncio.Event().wait()
    finally:
        await agent.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
