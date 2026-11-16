"""Workload RPC client and explicit grant-attempt retry state."""
import json
import uuid

import grpc

from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc


class GrantAttempt:
    def __init__(self, epoch, revision):
        self.request = pb.WorkloadGrantRequest(request_id=str(uuid.uuid4()),
            expected_epoch=epoch, expected_revision=revision)
        self.uncertain = False

    def call(self, stub, timeout=2):
        try:
            response = stub.RequestGrant(self.request, timeout=timeout)
        except grpc.RpcError:
            self.uncertain = True
            raise
        if response.reason == "GATE_UNCERTAIN":
            self.uncertain = True
        return dict(request_id=response.request_id, allowed=response.allowed, reason=response.reason,
                    replayed=response.replayed, action_applied=response.action_applied,
                    grant=json.loads(response.grant_json) if response.grant_json else None)

    def unresolved(self, result):
        return self.uncertain and result["reason"] in {
            "GATE_UNCERTAIN", "GATE_UNAVAILABLE", "STATE_UNAVAILABLE", "AUTHORITY_UNCERTAIN",
            "STALE_AUTHORITY", "NOT_HOLDER", "TRANSFER_PENDING"}


def channel(address, ca, cert, key):
    credentials = grpc.ssl_channel_credentials(ca, key, cert)
    connection = grpc.secure_channel(address, credentials)
    return connection, rpc.CraneWorkloadStub(connection)
