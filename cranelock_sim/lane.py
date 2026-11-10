"""Physical boundary usable with the gate's local or gRPC client interface."""
from threading import RLock

import grpc

from cranelock_gate.client import may_enter


class SafetyViolation(RuntimeError):
    pass


class Lane:
    def __init__(self, client, clock, emit):
        self.client, self.clock, self.emit = client, clock, emit
        self.lock = RLock()
        # Only a NEW simulated yard starts empty. Restart must preserve/reconcile
        # this physical model; a gate restart must never reset it.
        self.occupant = None
        self.request_id = None
        self.used_grants = set()

    def observe(self):
        with self.lock:
            for attempt in range(3):
                state = self.client.state()
                result = self.client.call(self.client.config.simulator_id, "ReportLaneState",
                    session_id=state["session_id"],
                    observation_revision=state["observation_revision"],
                    sequence=state["observation_sequence"] + 1,
                    observed_at_ms=round(self.clock() * 1000),
                    lane_state="CLEAR" if self.occupant is None else "OCCUPIED")
                if result["allowed"] or result["reason"] not in {"STALE_OBSERVATION", "STALE_SESSION"}:
                    return result
                # Maintenance may have changed the observation phase between
                # GetState and ReportLaneState. Read again and take a NEW sample
                # of this physical model; never replay old clearance evidence.
                self.emit("lane_observation_resampled", attempt=attempt + 1)
            return result

    def enter(self, holder, request_id, grant, authority):
        with self.lock:
            try:
                result = self.client.call(self.client.config.simulator_id, "ValidateEntry",
                    epoch=authority["epoch"], revision=authority["revision"],
                    holder=holder, grant_id=grant["id"], session_id=grant["id"])
            except (grpc.RpcError, OSError, TimeoutError):
                # Ambiguous responses never cause physical movement.
                self.emit("entry_uncertain", request_id=request_id, crane=holder)
                return False
            if not may_enter(result):
                self.emit("entry_denied", request_id=request_id, crane=holder,
                          reason=result["reason"])
                return False
            if self.occupant is not None:
                raise SafetyViolation("Gate accepted entry into an occupied physical lane")
            if grant["id"] in self.used_grants:
                raise SafetyViolation("Gate accepted a grant already used for movement")
            if (result["state"]["epoch"], result["state"]["revision"],
                    result["state"]["holder"]) != (
                    authority["epoch"], authority["revision"], holder):
                raise SafetyViolation("Gate accepted entry with stale or conflicting authority")
            self.used_grants.add(grant["id"])
            self.occupant, self.request_id = holder, request_id
            self.emit("physical_entry", request_id=request_id, crane=holder,
                      epoch=authority["epoch"], revision=authority["revision"])
            return True

    def exit(self, holder, request_id):
        with self.lock:
            if (self.occupant, self.request_id) != (holder, request_id):
                raise SafetyViolation("Exit does not match the physical occupant")
            self.occupant, self.request_id = None, None
            self.emit("physical_exit", request_id=request_id, crane=holder)
            result = self.observe()
            if not result["allowed"]:
                raise RuntimeError(f"Gate refused fresh exit evidence: {result['reason']}")
