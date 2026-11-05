"""Launch the existing controller against explicitly configured gate authority."""
import argparse
import asyncio
import datetime
import ipaddress
import math
from pathlib import Path
import re
import signal
import ssl
import sys

import grpc
from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID

from cranelock.controller import CentralController, ControllerError
from cranelock_gate.config import Config
from cranelock_gate.demo import load_keys, open_channel


def is_loopback_endpoint(address, *, listener=False):
    """Validate HOST:PORT (brackets for IPv6); plaintext is loopback-only."""
    match = re.fullmatch(r"(\[[0-9a-fA-F:.]+\]|[a-zA-Z0-9.-]+):([0-9]{1,5})", address)
    if not match:
        raise ValueError("Expected HOST:PORT or [IPv6]:PORT")
    host, port = match.groups()
    if not (0 if listener else 1) <= int(port) <= 65535:
        raise ValueError("Invalid port")
    if host.startswith("["):
        return ipaddress.IPv6Address(host[1:-1]).is_loopback
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        if any(not re.fullmatch(r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?", label)
               for label in host.split(".")):
            raise ValueError("Invalid host") from None
        return host.lower() == "localhost"


def validate_tls(ca, cert, key, *, server=False):
    """Check local PEM material before any gate mutation; never prompt for passwords."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER if server else ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cafile=ca)
    context.load_cert_chain(cert, key, password=lambda: "")
    leaf = x509.load_pem_x509_certificate(Path(cert).read_bytes())
    now = datetime.datetime.now(datetime.timezone.utc)
    if not leaf.not_valid_before_utc <= now < leaf.not_valid_after_utc:
        raise ValueError("TLS certificate is outside its validity period")
    try:
        usage = leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    except x509.ExtensionNotFound:
        return
    purpose = ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH
    if purpose not in usage and ExtendedKeyUsageOID.ANY_EXTENDED_KEY_USAGE not in usage:
        raise ValueError("TLS certificate has the wrong purpose")


def parse_options(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gate-config", required=True, help="Existing gate Config JSON")
    parser.add_argument("--controller-key", required=True,
                        help="JSON file containing only the controller's hex Ed25519 private key")
    parser.add_argument("--gate-address", required=True, metavar="HOST:PORT")
    parser.add_argument("--listen", required=True, metavar="HOST:PORT",
                        help="Controller heartbeat listener; [IPv6]:PORT also accepted")
    parser.add_argument("--gate-timeout", type=float, default=3.0,
                        help="Deadline in seconds for each Trusted Gate RPC (default: 3)")
    for prefix in ("gate-tls", "tls"):
        for name in ("ca", "cert", "key"):
            parser.add_argument(f"--{prefix}-{name}")
    args = parser.parse_args(argv)
    if not math.isfinite(args.gate_timeout) or args.gate_timeout <= 0:
        parser.error("--gate-timeout must be finite and positive")
    try:
        gate_loopback = is_loopback_endpoint(args.gate_address)
        listen_loopback = is_loopback_endpoint(args.listen, listener=True)
    except ValueError:
        parser.error("Addresses must be HOST:PORT or [IPv6]:PORT; only --listen permits port 0")
    for prefix, loopback, server in (("gate_tls", gate_loopback, False), ("tls", listen_loopback, True)):
        paths = [getattr(args, f"{prefix}_{name}") for name in ("ca", "cert", "key")]
        option = prefix.replace("_", "-")
        if any(paths):
            if not all(paths):
                parser.error(f"Provide --{option}-ca, --{option}-cert and --{option}-key together")
            try:
                validate_tls(*paths, server=server)
            except (OSError, ValueError):
                parser.error(f"--{option} files must be readable, valid PEM with a matching key and certificate")
        elif not loopback:
            parser.error(f"Non-loopback connections require all three --{option}-* files")
    try:
        config = Config.load(args.gate_config)
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        parser.error("Gate configuration is invalid or unreadable")
    try:
        keys = load_keys(args.controller_key)
        if set(keys) != {config.controller_id}:
            raise ValueError("Controller-only key file required")
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        parser.error("Signing key file must contain only the configured controller's hex Ed25519 private key")
    return parser, args, config, keys[config.controller_id]


async def main(argv=None):
    parser, args, config, private_key = parse_options(argv)
    credentials = None
    if args.tls_ca:
        try:
            credentials = grpc.ssl_server_credentials(
                [(Path(args.tls_key).read_bytes(), Path(args.tls_cert).read_bytes())],
                root_certificates=Path(args.tls_ca).read_bytes(), require_client_auth=True)
        except (OSError, ValueError):
            parser.error("Heartbeat TLS files are invalid or unreadable")
    channel, controller = None, None
    handlers = {}
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    stage = "connect to the Trusted Gate"
    try:
        channel = open_channel(args.gate_address, args.gate_tls_ca, args.gate_tls_cert, args.gate_tls_key)
        try:
            controller = CentralController(config, private_key, channel, timeout=args.gate_timeout)
        except (ValueError, TypeError, KeyError):
            parser.error("Controller signing key does not match the configured controller identity")
        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            sig = getattr(signal, name, None)
            if sig is not None:
                handlers[sig] = signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
        state = await asyncio.to_thread(controller.get_state)
        if type(state) is not dict:
            raise ControllerError("Invalid gate state")
        if state.get("epoch") == -1:
            print("Trusted Gate is uninitialized; explicitly bootstrap authority before starting the controller.",
                  file=sys.stderr)
            return 1
        if stop.is_set():
            return 0
        stage = "restore existing Trusted Gate authority"
        state = await asyncio.to_thread(controller.restore_central)
        if stop.is_set():
            return 0
        stage = "start the controller heartbeat listener"
        startup = asyncio.create_task(controller.start_heartbeat_server(args.listen, credentials=credentials))
        try:
            port = await asyncio.shield(startup)
        except asyncio.CancelledError:
            # Let server ownership reach the controller before closing it below.
            await asyncio.gather(startup, return_exceptions=True)
            raise
        if stop.is_set():
            return 0
        print(f"Controller active: epoch={state['epoch']} revision={state['revision']} "
              f"holder={state['holder']} mode={state['mode']}", flush=True)
        host = args.listen.rsplit(":", 1)[0]
        print(f"Heartbeat endpoint listening on {host}:{port}", flush=True)
        await stop.wait()
        return 0
    except (grpc.RpcError, ControllerError, OSError, RuntimeError, ValueError):
        # Do not echo remote errors, JSON/PEM contents or signing material.
        print(f"Controller startup failed while attempting to {stage}.", file=sys.stderr)
        return 1
    finally:
        try:
            if controller is not None:
                await controller.stop_heartbeat_server()
        finally:
            if channel is not None:
                channel.close()
            for sig, previous in handlers.items():
                signal.signal(sig, previous)


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass
