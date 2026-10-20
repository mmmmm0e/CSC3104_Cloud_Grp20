"""Async normal-operation protocol. No recovery or gate authority lives here."""
import asyncio
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum
import json
import os
from pathlib import Path
import time
import uuid

import grpc
from . import protocol_pb2 as pb
from . import protocol_pb2_grpc as rpc


class Mode(IntEnum):
    CENTRAL = pb.CENTRAL
    LOCAL = pb.LOCAL
    REJOINING = pb.REJOINING


@dataclass
class AgentState:
    agent_id: str
    current_epoch: int
    token_holder: str
    mode: Mode = Mode.LOCAL
    has_token: bool = False
    request_queue: deque = field(default_factory=deque)


class Agent(rpc.CraneAgentServicer):
    def __init__(self, agent_id, peers, holder, *, epoch=1, mode=Mode.LOCAL,
                 interval=0.5, timeout=2.0, state_file=None, clock=time.monotonic,
                 event_sink=None):
        if not agent_id or epoch < 1 or interval <= 0 or timeout <= interval:
            raise ValueError("Require ID, positive epoch and 0 < interval < timeout")
        if agent_id in peers or not holder or holder not in {agent_id, *peers}:
            raise ValueError("Holder must be a member; peers must exclude self")
        self.peers = dict(peers)
        self.interval, self.timeout, self.clock = interval, timeout, clock
        self.path = Path(state_file) if state_file else None
        self.lock = asyncio.Lock()
        self.events = event_sink or (lambda e: print(json.dumps(e), flush=True))
        self.state = AgentState(agent_id, epoch, holder, mode, holder == agent_id)
        self.pending = None
        self.last_handoff = ""
        self.received = set()
        self.vote_state = {}  # Opaque reserved storage; Recovery owns vote policy.
        if self.path and self.path.exists():
            saved = json.loads(self.path.read_text())
            if saved["agent_id"] != agent_id:
                raise ValueError("State file belongs to another agent")
            self.state.current_epoch = max(epoch, saved["epoch"])
            self.state.token_holder = saved["holder"]
            self.state.mode = Mode.REJOINING
            self.state.has_token = False
            self.vote_state = saved.get("vote_state", {})
        self.last_seen = {peer: clock() for peer in peers}
        self.suspected = set()
        self.holder_suspicion = None
        self.channels = {p: grpc.aio.insecure_channel(a) for p, a in peers.items()}
        self.stubs = {p: rpc.CraneAgentStub(c) for p, c in self.channels.items()}
        self.tasks = []
        self.server = None
        self.persist()

    def emit(self, event, peer_id=None, **fields):
        self.events(dict(event=event, timestamp=time.time(),
                         monotonic=self.clock(), agent_id=self.state.agent_id,
                         peer_id=peer_id, epoch=self.state.current_epoch, **fields))

    def persist(self):
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(".tmp")
            with temporary.open("w") as stream:
                json.dump(dict(agent_id=self.state.agent_id, epoch=self.state.current_epoch,
                               holder=self.state.token_holder, has_token=self.state.has_token,
                               vote_state=self.vote_state), stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)

    def reply(self, accepted, reason=""):
        return pb.Reply(accepted=accepted, reason=reason, epoch=self.state.current_epoch)

    def valid(self, epoch):
        return self.state.mode != Mode.REJOINING and epoch == self.state.current_epoch

    async def reconcile(self, epoch, holder, mode=Mode.LOCAL):
        """Trusted integration hook ONLY: caller must establish authoritative state.

        Never call based on a heartbeat or a single unverified peer response.
        """
        async with self.lock:
            if epoch < self.state.current_epoch or holder not in {self.state.agent_id, *self.peers}:
                raise ValueError("Stale epoch or unknown holder")
            if mode == Mode.REJOINING:
                raise ValueError("Reconciliation requires an active mode")
            self.state.current_epoch = epoch
            self.state.token_holder = holder
            self.state.has_token = holder == self.state.agent_id
            self.state.mode = mode
            self.state.request_queue.clear()
            self.pending = None
            self.last_handoff = ""
            self.received.clear()
            self.persist()
            self.emit("MODE_CHANGED", mode=mode.name)

    async def Heartbeat(self, request, context):
        async with self.lock:
            if request.agent_id not in self.peers or request.epoch < self.state.current_epoch:
                return self.reply(False, "Unknown peer or stale epoch")
            self.last_seen[request.agent_id] = self.clock()
            self.suspected.discard(request.agent_id)
            self.emit("HEARTBEAT_RECEIVED", request.agent_id)
            # A known holder may announce its normal handoff. This never grants
            # local ownership; only TokenTransfer can do that.
            if (self.valid(request.epoch) and not self.state.has_token
                    and request.agent_id == self.state.token_holder
                    and not request.has_token and request.mode != pb.REJOINING
                    and request.outgoing_transfer_id
                    and request.token_holder in {self.state.agent_id, *self.peers}
                    and request.token_holder != request.agent_id):
                self.state.token_holder = request.token_holder
                self.persist()
                self.emit("TOKEN_HOLDER_CHANGED", holder=request.token_holder)
            return self.reply(True)

    async def TokenRequest(self, request, context):
        async with self.lock:
            if not self.valid(request.epoch) or not self.state.has_token or self.pending:
                return self.reply(False, "Not active holder at this epoch")
            if request.agent_id not in self.peers:
                return self.reply(False, "Unknown requester")
            self.emit("TOKEN_REQUEST_RECEIVED", request.agent_id, request_id=request.request_id)
            if request.agent_id not in self.state.request_queue:
                self.state.request_queue.append(request.agent_id)
                self.emit("TOKEN_QUEUED", request.agent_id, request_id=request.request_id)
            return self.reply(True)

    async def TokenTransfer(self, request, context):
        async with self.lock:
            if (not self.valid(request.epoch) or request.sender_id not in self.peers
                    or request.receiver_id != self.state.agent_id or not request.transfer_id):
                return self.reply(False, "Invalid transfer or epoch")
            key = (request.epoch, request.sender_id, request.transfer_id)
            if key in self.received:
                return self.reply(True, "Already received")
            if (self.state.has_token or self.pending
                    or self.state.token_holder != request.sender_id):
                return self.reply(False, "Sender is not known holder")
            queue = list(request.request_queue)
            if (len(queue) != len(set(queue)) or any(
                    p not in self.peers or p == self.state.agent_id for p in queue)):
                return self.reply(False, "Invalid queue")
            self.state.has_token = True
            self.last_handoff = ""
            self.state.token_holder = self.state.agent_id
            self.state.request_queue = deque(queue)
            self.received.add(key)
            self.persist()
            self.emit("TOKEN_TRANSFER_RECEIVED", request.sender_id, transfer_id=request.transfer_id)
            self.emit("TOKEN_HOLDER_CHANGED", holder=self.state.agent_id)
            return self.reply(True)

    async def GetAgentState(self, request, context):
        async with self.lock:
            s = self.state
            return pb.AgentState(agent_id=s.agent_id, epoch=s.current_epoch,
                has_token=s.has_token, token_holder=s.token_holder, mode=s.mode,
                request_queue=list(s.request_queue),
                pending_transfer_id=self.pending.transfer_id if self.pending else "",
                peers=[pb.PeerState(agent_id=p, last_seen_monotonic=t,
                    status="SUSPECTED_UNAVAILABLE" if p in self.suspected else "AVAILABLE")
                    for p, t in self.last_seen.items()])

    async def request_token(self):
        async with self.lock:
            if self.state.mode == Mode.REJOINING or self.state.has_token:
                return self.reply(False, "Rejoining or already holder")
            holder = self.state.token_holder
            message = pb.TokenRequestMessage(agent_id=self.state.agent_id,
                epoch=self.state.current_epoch, request_id=uuid.uuid4().hex)
            self.emit("TOKEN_REQUEST_SENT", holder, request_id=message.request_id)
        return await self.stubs[holder].TokenRequest(message, timeout=self.timeout)

    async def transfer_next(self):
        async with self.lock:
            if self.state.mode == Mode.REJOINING:
                return False
            if not self.pending:
                if not self.state.has_token or not self.state.request_queue:
                    return False
                receiver = self.state.request_queue.popleft()
                self.pending = pb.TokenTransferMessage(sender_id=self.state.agent_id,
                    receiver_id=receiver, epoch=self.state.current_epoch,
                    transfer_id=uuid.uuid4().hex, request_queue=list(self.state.request_queue))
                # Persist relinquishment BEFORE network I/O. Never roll back on timeout.
                self.state.has_token = False
                self.last_handoff = self.pending.transfer_id
                self.state.token_holder = receiver
                self.state.request_queue.clear()
                self.persist()
                self.emit("TOKEN_TRANSFER_STARTED", receiver, transfer_id=self.pending.transfer_id)
                self.emit("TOKEN_HOLDER_CHANGED", holder=receiver)
            message = self.pending
        try:
            result = await self.stubs[message.receiver_id].TokenTransfer(message, timeout=self.timeout)
        except grpc.aio.AioRpcError:
            return False
        async with self.lock:
            if result.accepted and self.pending is message:
                self.pending = None
            return result.accepted

    async def detect(self):
        async with self.lock:
            for peer, seen in self.last_seen.items():
                elapsed = self.clock() - seen
                if elapsed > self.timeout and peer not in self.suspected:
                    self.suspected.add(peer)
                    self.emit("PEER_SUSPECTED", peer, last_seen_monotonic=seen,
                              detection_latency_seconds=elapsed, timeout_seconds=self.timeout)
                if peer in self.suspected and peer == self.state.token_holder:
                    key = (self.state.current_epoch, peer)
                    if self.holder_suspicion != key:
                        self.holder_suspicion = key
                        self.emit("TOKEN_HOLDER_SUSPECTED", peer,
                                  detection_latency_seconds=elapsed)
            if self.state.token_holder not in self.suspected:
                self.holder_suspicion = None

    async def heartbeat_peer(self, peer, message):
        try:
            await self.stubs[peer].Heartbeat(message, timeout=self.interval)
            self.emit("HEARTBEAT_SENT", peer)
        except grpc.aio.AioRpcError:
            pass

    async def heartbeat_loop(self):
        while True:
            async with self.lock:
                message = pb.HeartbeatMessage(agent_id=self.state.agent_id,
                    epoch=self.state.current_epoch, token_holder=self.state.token_holder,
                    mode=self.state.mode, has_token=self.state.has_token,
                    outgoing_transfer_id=self.last_handoff,
                    sent_unix_seconds=time.time())
            await asyncio.gather(*(self.heartbeat_peer(p, message) for p in self.peers))
            await asyncio.sleep(self.interval)

    async def monitor_loop(self):
        while True:
            await self.detect()
            await asyncio.sleep(self.interval)

    async def transfer_loop(self):
        while True:
            await self.transfer_next()
            await asyncio.sleep(self.interval)

    async def start(self, address, auto_transfer=True):
        self.server = grpc.aio.server()
        rpc.add_CraneAgentServicer_to_server(self, self.server)
        port = self.server.add_insecure_port(address)
        if not port:
            raise RuntimeError("Cannot bind agent address")
        await self.server.start()
        self.emit("AGENT_STARTED", mode=self.state.mode.name)
        self.tasks = [asyncio.create_task(self.heartbeat_loop()),
                      asyncio.create_task(self.monitor_loop())]
        if auto_transfer:
            self.tasks.append(asyncio.create_task(self.transfer_loop()))
        return port

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.server:
            await self.server.stop(0)
        await asyncio.gather(*(c.close() for c in self.channels.values()))
