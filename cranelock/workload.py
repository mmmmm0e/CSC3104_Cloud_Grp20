"""Authenticated, deliberately narrow workload-to-agent boundary."""
import asyncio
import json
from pathlib import Path
import uuid

import grpc
from cryptography import x509

from . import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from .agent import Mode


def load_allowlist(path, agent_id, lane_id):
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if (type(value) is not dict or set(value) != {"crane_id", "lane_id", "allowed_workload_uri_sans"}
            or value["crane_id"] != agent_id or value["lane_id"] != lane_id):
        raise ValueError("Workload allowlist does not match crane and lane")
    identities = value["allowed_workload_uri_sans"]
    if (type(identities) is not list or not identities
            or any(type(i) is not str or not i.startswith("urn:cranelock:workload:")
                   or i == "urn:cranelock:workload:"
                   or len(i) > 255 or not i.isascii() or any(c.isspace() for c in i)
                   for i in identities) or len(set(identities)) != len(identities)):
        raise ValueError("Invalid workload URI SAN allowlist")
    return frozenset(identities)


def verified_identity(context):
    auth = context.auth_context()
    if auth.get("transport_security_type") != [b"ssl"]:
        return None
    certificates = auth.get("x509_pem_cert", [])
    if len(certificates) != 1:
        return None
    try:
        certificate = x509.load_pem_x509_certificate(certificates[0])
        sans = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        identities = sans.get_values_for_type(x509.UniformResourceIdentifier)
        return identities[0] if len(identities) == 1 else None
    except (ValueError, x509.ExtensionNotFound):
        return None


class WorkloadService(rpc.CraneWorkloadServicer):
    def __init__(self, agent, allowed):
        self.agent, self.allowed = agent, allowed
        self.tasks = set()

    async def RequestGrant(self, request, context):
        identity = verified_identity(context)
        if identity not in self.allowed:
            self.agent.emit("WORKLOAD_GRANT_DENIED", workload_identity=identity,
                            request_id=request.request_id[:128], reason="UNAUTHORIZED")
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Unauthorized workload")
        try:
            value = uuid.UUID(request.request_id)
            valid = value.version == 4 and str(value) == request.request_id
        except (ValueError, AttributeError):
            valid = False
        if not valid:
            self.agent.emit("WORKLOAD_GRANT_DENIED", workload_identity=identity,
                            request_id=request.request_id[:128], reason="INVALID_REQUEST_ID")
            return pb.WorkloadGrantResponse(request_id=request.request_id[:128], reason="INVALID_REQUEST_ID")
        if not request.HasField("expected_epoch") or not request.HasField("expected_revision"):
            self.agent.emit("WORKLOAD_GRANT_DENIED", workload_identity=identity,
                            request_id=request.request_id, reason="MISSING_AUTHORITY")
            return pb.WorkloadGrantResponse(request_id=request.request_id, reason="MISSING_AUTHORITY")
        # Shield the full operation, including the authority lock, from RPC cancellation.
        task = asyncio.create_task(self._execute(request, identity))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return await asyncio.shield(task)

    async def _execute(self, request, identity):
        agent = self.agent
        reason = None
        result = None
        central_refresh = False
        async with agent.authority_lock:
            async with agent.lock:
                state = agent.state
                binding = [identity, request.expected_epoch, request.expected_revision]
                existing = agent.workload_requests.get(request.request_id)
                if existing is not None and existing != binding:
                    reason = "REQUEST_ID_CONFLICT"
                elif agent.gate is None or agent.path is None:
                    reason = "GATE_UNAVAILABLE"
                elif agent.background_failure or state.mode == Mode.REJOINING or agent.authority_refresh_needed:
                    reason = "AUTHORITY_UNCERTAIN"
                elif agent.pending or agent.gate_attempt:
                    reason = "TRANSFER_PENDING"
                elif (not state.has_token or state.token_holder != state.agent_id or
                      (state.current_epoch, state.revision) != (request.expected_epoch, request.expected_revision)):
                    if state.mode == Mode.CENTRAL:
                        central_refresh = True
                    else:
                        reason = "NOT_HOLDER" if not state.has_token else "STALE_AUTHORITY"
            if reason is None:
                authority = await agent.query_gate()
                if authority is None:
                    reason = "GATE_UNAVAILABLE"
                elif (authority.get("epoch"), authority.get("revision"), authority.get("holder"), authority.get("mode")) != (
                        request.expected_epoch, request.expected_revision, agent.state.agent_id, agent.state.mode.name):
                    reason = "STALE_AUTHORITY"
                else:
                    async with agent.lock:
                        if central_refresh and not agent.pending and not agent.gate_attempt:
                            agent.adopt_gate_state(authority)
                        if (agent.background_failure or agent.state.mode == Mode.REJOINING or agent.authority_refresh_needed
                                or agent.pending or agent.gate_attempt or not agent.state.has_token
                                or (agent.state.current_epoch, agent.state.revision) !=
                                   (request.expected_epoch, request.expected_revision)):
                            reason = "AUTHORITY_UNCERTAIN"
                        elif existing is None:
                            # Bind before sending: a restart cannot reuse this ID for different bytes.
                            agent.workload_requests[request.request_id] = binding
                            try:
                                agent.persist()
                            except OSError:
                                # A failed replace can leave the binding durable.
                                # Keep it in memory until a later write succeeds.
                                reason = "STATE_UNAVAILABLE"
                    if reason is None:
                        try:
                            result = await agent.gate.request_grant(request.request_id,
                                request.expected_epoch, request.expected_revision)
                        except (grpc.RpcError, RuntimeError, OSError):
                            reason = "GATE_UNCERTAIN"
                    if result is not None:
                        # A concurrent recovery vote can suspend the agent even while
                        # authority_lock is held. Gate authority may also change in
                        # another process, so verify both again after the I/O.
                        current = await agent.query_gate()
                        async with agent.lock:
                            safe = (current is not None and not agent.background_failure and not agent.authority_refresh_needed
                                and not agent.pending and not agent.gate_attempt
                                and agent.state.mode != Mode.REJOINING and agent.state.has_token
                                and agent.state.token_holder == agent.state.agent_id
                                and (agent.state.current_epoch, agent.state.revision) ==
                                    (request.expected_epoch, request.expected_revision)
                                and (current.get("epoch"), current.get("revision"),
                                     current.get("holder"), current.get("mode")) ==
                                    (request.expected_epoch, request.expected_revision,
                                     agent.state.agent_id, agent.state.mode.name))
                            if safe and result["allowed"] and not result["replayed"]:
                                grant = result.get("grant") or {}
                                safe = (current.get("grant") or {}).get("id") == grant.get("id") and bool(grant.get("id"))
                            if not safe:
                                agent.emit("WORKLOAD_GATE_OUTCOME_UNCERTAIN", workload_identity=identity,
                                    request_id=request.request_id, gate_allowed=result["allowed"],
                                    gate_reason=result["reason"], gate_replayed=result["replayed"],
                                    gate_action_applied=result["action_applied"])
                                # Preserve the Gate's durable outcome and binding;
                                # the caller must retry this exact request, not move.
                                result = dict(allowed=False, reason="GATE_UNCERTAIN",
                                    replayed=result["replayed"], action_applied=result["action_applied"], grant=None)
        if result is None:
            result = dict(allowed=False, reason=reason, replayed=False, action_applied=False, grant=None)
        agent.emit("WORKLOAD_GRANT_RESULT", workload_identity=identity,
                   lane_id=agent.gate.client.config.lane_id if agent.gate else None,
                   request_id=request.request_id, allowed=result["allowed"], reason=result["reason"],
                   replayed=result["replayed"], action_applied=result["action_applied"])
        return pb.WorkloadGrantResponse(request_id=request.request_id, allowed=result["allowed"],
            reason=result["reason"], replayed=result["replayed"], action_applied=result["action_applied"],
            grant_json=json.dumps(result["grant"], separators=(",", ":")).encode() if result["grant"] else b"")


async def start_workload(agent, address, credentials, allowed):
    server = grpc.aio.server()
    service = WorkloadService(agent, allowed)
    rpc.add_CraneWorkloadServicer_to_server(service, server)
    port = server.add_secure_port(address, credentials)
    if not port:
        raise RuntimeError("Cannot bind workload address")
    await server.start()
    return server, service, port
