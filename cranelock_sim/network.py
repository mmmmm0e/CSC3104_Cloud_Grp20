"""Experiment-only peer RPC partitioning; management and gate RPCs stay reachable."""
import grpc
import json
import asyncio
import math
import random


class PeerNetwork:
    def __init__(self, members, emit):
        self.members, self.emit = set(members), emit
        self.groups = {member: 0 for member in members}
        self.blocked_calls = 0
        self.delay_seconds = self.jitter_seconds = 0
        self.random = random.Random(20)

    # delays peer deliveries independently so concurrent calls can reorder
    def set_delay(self, seconds=0, jitter=0, seed=20):
        if any(not math.isfinite(value) or value < 0 for value in (seconds, jitter)):
            raise ValueError("Peer delay and jitter must be finite and nonnegative")
        self.delay_seconds, self.jitter_seconds = seconds, jitter
        self.random = random.Random(seed)
        self.emit("peer_delay_configured", delay_seconds=seconds, jitter_seconds=jitter, seed=seed)

    def partition(self, groups):
        flat = [member for group in groups for member in group]
        if len(flat) != len(set(flat)) or set(flat) != self.members or any(not group for group in groups):
            raise ValueError("Partition groups must contain each member exactly once")
        self.groups = {member: index for index, group in enumerate(groups) for member in group}
        self.emit("network_partitioned", groups=groups)

    def interceptor(self, receiver):
        network = self

        class Interceptor(grpc.aio.ServerInterceptor):
            async def intercept_service(self, continuation, details):
                handler = await continuation(details)
                method = details.method.rsplit("/", 1)[-1]
                field = {"Heartbeat": "agent_id", "TokenRequest": "agent_id",
                         "TokenTransfer": "sender_id", "VoteRecovery": "proposer_id"}.get(
                             method)
                if handler is None or (field is None and method != "ReconcileAuthority"):
                    return handler

                async def invoke(request, context):
                    sender = getattr(request, field) if field else None
                    if method == "ReconcileAuthority":
                        try:
                            payload = json.loads(request.payload_json)
                            sender = payload.get("actor_id") if type(payload) is dict else None
                            if not isinstance(sender, str):
                                sender = None
                        except (ValueError, TypeError):
                            sender = None
                    if sender in network.groups:
                        delay = network.delay_seconds + network.random.uniform(0, network.jitter_seconds)
                        if delay:
                            network.emit("peer_rpc_delayed", sender=sender, receiver=receiver,
                                         method=method, delay_seconds=delay)
                            await asyncio.sleep(delay)
                    if (sender in network.groups and
                            network.groups[sender] != network.groups[receiver]):
                        network.blocked_calls += 1
                        network.emit("peer_rpc_blocked", sender=sender, receiver=receiver,
                                     method=details.method.rsplit("/", 1)[-1])
                        await context.abort(grpc.StatusCode.UNAVAILABLE, "Injected peer-network partition")
                    return await handler.unary_unary(request, context)

                return grpc.unary_unary_rpc_method_handler(invoke,
                    request_deserializer=handler.request_deserializer,
                    response_serializer=handler.response_serializer)

        return Interceptor()
