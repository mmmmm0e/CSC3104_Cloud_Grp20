import asyncio
from collections.abc import Mapping
import json
import math
import uuid

import grpc

from cranelock_gate.crypto import canonical, verify
from . import protocol_pb2 as pb, protocol_pb2_grpc as rpc


DOMAIN = b"cranelock.reconciliation.v1\x00"
FIELDS = {"version", "operation", "lane_id", "membership_id", "actor_id", "target_agent_id",
          "request_id", "epoch", "revision", "holder", "mode"}
AUTHORITY_FIELDS = ("epoch", "revision", "holder", "mode")


class ReconciliationError(RuntimeError):
    pass


# validates the signed destination and authority snapshot
def validate(raw, signature, config, agent_id):
    if len(raw) > 8192:
        raise ValueError("INVALID_REQUEST")
    payload = json.loads(raw)
    if (type(payload) is not dict or set(payload) != FIELDS or canonical(payload) != raw
            or type(payload["version"]) is not int or payload["version"] != 1
            or payload["operation"] != "ReconcileAuthority"
            or payload["lane_id"] != config.lane_id
            or payload["membership_id"] != config.membership_id
            or payload["target_agent_id"] != agent_id
            or not isinstance(payload["request_id"], str) or not 1 <= len(payload["request_id"]) <= 128
            or not isinstance(payload["holder"], str) or payload["holder"] not in config.members
            or payload["mode"] not in {"CENTRAL", "LOCAL"}
            or any(type(payload[name]) is not int or not 0 <= payload[name] <= 2**63-1
                   for name in ("epoch", "revision"))):
        raise ValueError("INVALID_REQUEST")
    actor = payload["actor_id"]
    if (not isinstance(actor, str) or actor not in config.keys
            or not verify(config.keys[actor], signature, payload, DOMAIN)):
        raise ValueError("INVALID_SIGNATURE")
    if actor != config.controller_id and not (actor in config.members and payload["mode"] == "LOCAL"):
        raise ValueError("UNAUTHORIZED")
    return payload


# signs requests and reconciles remote agent endpoints
class ReconciliationClient:
    def __init__(self, config, actor_id, private_key, *, credentials=None, timeout=3):
        if (actor_id not in config.keys or private_key.public_key().public_bytes_raw()
                != bytes.fromhex(config.keys[actor_id])):
            raise ValueError("Signing key does not match configured identity")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be finite and positive")
        self.config, self.actor_id, self.private_key = config, actor_id, private_key
        self.credentials, self.timeout = credentials, timeout

    # binds a signed request to one agent and gate snapshot
    def request(self, agent_id, state):
        payload = dict(version=1, operation="ReconcileAuthority", lane_id=self.config.lane_id,
            membership_id=self.config.membership_id, actor_id=self.actor_id,
            target_agent_id=agent_id, request_id=str(uuid.uuid4()),
            **{key: state[key] for key in AUTHORITY_FIELDS})
        raw = canonical(payload)
        signature = self.private_key.sign(DOMAIN + raw)
        validate(raw, signature, self.config, agent_id)
        return pb.AuthorityReconciliation(payload_json=raw, signature=signature)

    # never reports partial or failed acknowledgements as a completed handover
    async def reconcile(self, agents, state):
        if not isinstance(agents, Mapping) or not agents:
            raise ValueError("agents must map configured identities to addresses")
        if any(identity not in self.config.members or not isinstance(address, str) or not address
               for identity, address in agents.items()):
            raise ValueError("Unknown agent or invalid endpoint")
        requests = {identity: self.request(identity, state) for identity in agents}
        channels = {}
        try:
            for identity, address in agents.items():
                channels[identity] = (grpc.aio.secure_channel(address, self.credentials)
                    if self.credentials else grpc.aio.insecure_channel(address))
            results = await asyncio.gather(*(
                rpc.CraneAgentStub(channel).ReconcileAuthority(requests[identity], timeout=self.timeout)
                for identity, channel in channels.items()), return_exceptions=True)
            failures = []
            for identity, result in zip(channels, results):
                if isinstance(result, BaseException):
                    failures.append(f"{identity}: {result}")
                elif (not result.accepted or (result.epoch, result.revision) !=
                      (state["epoch"], state["revision"])):
                    failures.append(f"{identity}: {result.reason or 'authority mismatch'}")
            if failures:
                raise ReconciliationError("Reconciliation failed: " + "; ".join(failures))
            return state
        finally:
            await asyncio.gather(*(channel.close() for channel in channels.values()))
