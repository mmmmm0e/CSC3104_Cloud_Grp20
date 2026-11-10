"""Test-only gRPC transport around the team's CentralController implementation."""
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json

import grpc

from cranelock.controller import CentralController
from cranelock_gate.client import Client


CONTROLLER_IMPLEMENTATION = "cranelock.controller.CentralController"
CONTROLLER_SOURCE = "21fac92"


def encode(value):
    return json.dumps(value).encode()


class RPCStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.counts = Counter()
        self.deadline = None

    def add(self, method):
        with self.lock:
            if self.deadline is None or time.monotonic() <= self.deadline:
                self.counts[method] += 1

    def reset(self, *, deadline=None):
        with self.lock:
            self.counts.clear()
            self.deadline = deadline

    def snapshot(self):
        with self.lock:
            return dict(self.counts)


class PeerCounter(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, stats):
        self.stats = stats

    async def intercept_unary_unary(self, continuation, details, request):
        method = details.method.decode() if isinstance(details.method, bytes) else details.method
        self.stats.add("peer." + method.rsplit("/", 1)[-1])
        return await continuation(details, request)


class MeasuredGateClient(Client):
    def __init__(self, channel, config, keys, stats):
        super().__init__(channel, config, keys)
        self.stats = stats

    def send(self, payload):
        self.stats.add("gate." + payload["operation"])
        return super().send(payload)

    def state(self):
        self.stats.add("gate.GetState")
        return super().state()


class TeamControllerService:
    def __init__(self, config, private_key, channel, stats):
        self.controller = CentralController(config, private_key, channel)
        # Instrument the same signed gate client API without replacing the
        # controller's authority construction, validation or retry decisions.
        self.controller.client = MeasuredGateClient(
            channel, config, {config.controller_id: private_key}, stats)
        self.pool = ThreadPoolExecutor(max_workers=4)
        self.server = grpc.server(self.pool)
        methods = {
            "Heartbeat": grpc.unary_unary_rpc_method_handler(lambda request, context: dict(alive=True),
                request_deserializer=json.loads, response_serializer=encode),
            "Assign": grpc.unary_unary_rpc_method_handler(self.assign,
                request_deserializer=json.loads, response_serializer=encode),
        }
        self.server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("cranelock.sim.Controller", methods),))
        self.port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.stopped = False

    def assign(self, request, context):
        return self.controller.install_central(request["holder"])

    def close(self):
        if not self.stopped:
            self.server.stop(0).wait(timeout=3)
            self.pool.shutdown(wait=True)
            self.stopped = True


class ControllerConnection:
    def __init__(self, port, stats):
        self.channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        self.stats = stats
        base = "/cranelock.sim.Controller/"
        self.heartbeat_rpc = self.channel.unary_unary(base + "Heartbeat", request_serializer=encode,
                                                     response_deserializer=json.loads)
        self.assign_rpc = self.channel.unary_unary(base + "Assign", request_serializer=encode,
                                                  response_deserializer=json.loads)

    async def probe(self):
        self.stats.add("controller.Heartbeat")
        try:
            return (await self.heartbeat_rpc({}, timeout=0.05))["alive"]
        except grpc.aio.AioRpcError:
            return False

    async def assign(self, holder):
        self.stats.add("controller.Assign")
        # Assign includes signed, durable gate writes. Use the gate client's
        # normal RPC budget rather than a heartbeat-sized disk-write deadline.
        return await self.assign_rpc(dict(holder=holder), timeout=3)

    async def close(self):
        await self.channel.close()
