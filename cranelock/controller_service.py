import asyncio
from concurrent.futures import ThreadPoolExecutor
import json

import grpc

from . import protocol_pb2 as pb
from . import protocol_pb2_grpc as rpc
from .reconciliation import ReconciliationError


# runs the team controller independently of the evaluation transport
class ControllerService:
    # exposes scheduling and liveness over the existing controller rpc contract
    def __init__(self, controller, address, *, credentials=None):
        self.controller = controller
        self.pool = ThreadPoolExecutor(max_workers=4)
        self.server = grpc.server(self.pool)
        methods = {
            "Heartbeat": grpc.unary_unary_rpc_method_handler(self.heartbeat,
                request_deserializer=json.loads, response_serializer=lambda value: json.dumps(value).encode()),
            "Assign": grpc.unary_unary_rpc_method_handler(self.assign,
                request_deserializer=json.loads, response_serializer=lambda value: json.dumps(value).encode()),
        }
        self.server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
            "cranelock.reference.Controller", methods),))
        self.port = (self.server.add_secure_port(address, credentials) if credentials
                     else self.server.add_insecure_port(address))
        if not self.port:
            self.pool.shutdown(wait=True)
            raise ValueError("Cannot bind controller service")
        self.server.start()

    # reports service liveness without implying gate availability
    def heartbeat(self, request, context):
        return dict(alive=True)

    # serializes assignments through the gate's expected state checks
    def assign(self, request, context):
        holder = request.get("holder")
        if holder not in self.controller.config.members:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Unknown holder")
        state = self.controller.get_state()
        if state["mode"] == "CENTRAL" and state["holder"] == holder:
            return dict(allowed=True, reason="UNCHANGED", state=state)
        return self.controller.install_central(holder)

    # releases the listener and worker threads
    def close(self):
        self.server.stop(0).wait(timeout=3)
        self.pool.shutdown(wait=True)


# probes the real controller service from an agent process
class ControllerProbe:
    # supports loopback experiments and credentialed remote connections
    def __init__(self, address, *, timeout=1, credentials=None):
        self.channel = (grpc.aio.secure_channel(address, credentials) if credentials
                        else grpc.aio.insecure_channel(address))
        self.timeout = timeout
        self.call = self.channel.unary_unary("/cranelock.reference.Controller/Heartbeat",
            request_serializer=lambda value: json.dumps(value).encode(), response_deserializer=json.loads)

    # treats transport loss as suspicion rather than authority
    async def probe(self):
        try:
            return (await self.call({}, timeout=self.timeout)).get("alive") is True
        except grpc.RpcError:
            return False

    # closes the agent's controller connection
    async def close(self):
        await self.channel.close()


# returns to central authority only after reading the current holder and queue
class ControllerReturnWorker:
    # polls configured endpoints without sharing agent objects
    def __init__(self, controller, addresses, *, timeout=1, credentials=None, emit=print):
        if not addresses or set(addresses) - set(controller.config.members):
            raise ValueError("Return endpoints must be configured members")
        self.controller, self.timeout, self.emit = controller, timeout, emit
        self.credentials = credentials
        self.addresses = dict(addresses)
        self.channels = {identity: (grpc.aio.secure_channel(address, credentials) if credentials
            else grpc.aio.insecure_channel(address)) for identity, address in addresses.items()}
        self.stubs = {identity: rpc.CraneAgentStub(channel) for identity, channel in self.channels.items()}

    # keeps the accepted holder and physical state during return
    async def step(self):
        state = await asyncio.to_thread(self.controller.get_state)
        active, snapshots = {}, {}
        for identity, stub in self.stubs.items():
            try:
                snapshot = await stub.GetAgentState(pb.Empty(), timeout=self.timeout)
                if snapshot.agent_id == identity:
                    active[identity], snapshots[identity] = self.addresses[identity], snapshot
            except grpc.RpcError:
                pass
        if state["holder"] not in active:
            return False
        if len(active) < self.controller.config.majority:
            return False
        if state["mode"] == "LOCAL":
            # confirms the holder adopted the accepted epoch before changing mode
            snapshot = snapshots[state["holder"]]
            if (snapshot.epoch, snapshot.revision, snapshot.token_holder) != (
                    state["epoch"], state["revision"], state["holder"]):
                return False
            queue = list(snapshot.request_queue)
            result = await asyncio.to_thread(self.controller.install_central, state["holder"], expected_state=state)
            if not result["allowed"]:
                return False
            await self.controller.reconcile_remote(active, timeout=self.timeout, credentials=self.credentials)
            self.emit(json.dumps(dict(event="AUTOMATIC_CENTRAL_RETURN", epoch=result["state"]["epoch"],
                revision=result["state"]["revision"], holder=state["holder"], pending_queue=queue,
                lane_state=result["state"]["lane_state"])))
            return True
        await self.controller.reconcile_remote(active, timeout=self.timeout, credentials=self.credentials)
        return False

    # retries partial reconciliation without reporting it as complete
    async def run(self, interval=0.5):
        while True:
            try:
                await self.step()
            except (grpc.RpcError, ReconciliationError, OSError, ValueError) as error:
                self.emit(json.dumps(dict(event="CONTROLLER_RETURN_WAITING", reason=str(error))))
            await asyncio.sleep(interval)

    # closes monitoring channels on shutdown
    async def close(self):
        await asyncio.gather(*(channel.close() for channel in self.channels.values()))
