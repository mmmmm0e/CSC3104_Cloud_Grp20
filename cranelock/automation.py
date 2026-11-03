import asyncio

import grpc

from .agent import Mode
from .reconciliation import ReconciliationClient, ReconciliationError


# connects suspicion to signed recovery without granting physical clearance
class RecoveryWorker:
    # requires the existing durable voter and a live gate connection
    def __init__(self, agent, *, credentials=None):
        if agent.gate is None or agent.recovery_config is None:
            raise ValueError("Automatic recovery requires a gate and durable recovery voting")
        self.agent = agent
        self.credentials = credentials
        self.ready = None
        self.ready_since = 0
        self.last_failure = None

    # proposes only from a stable majority view with a deterministic coordinator
    async def step(self):
        agent = self.agent
        state = await agent.gate.state()
        if not await agent.reconcile_from_gate(expected={key: state[key] for key in
                ("epoch", "revision", "holder", "mode")}):
            return
        async with agent.lock:
            if agent.state.mode == Mode.REJOINING:
                return
            failed = (agent.controller_suspected if state["mode"] == "CENTRAL"
                      else state["holder"] in agent.suspected)
            reachable = sorted({agent.state.agent_id, *agent.peers} - agent.suspected)
        if not failed or len(reachable) < agent.recovery_config.majority:
            self.ready = None
            return
        holder = state["holder"] if state["holder"] in reachable else reachable[0]
        view = (state["epoch"], state["revision"], holder, tuple(reachable))
        if view != self.ready:
            self.ready, self.ready_since = view, agent.clock()
            return
        if reachable[0] != agent.state.agent_id or agent.clock() - self.ready_since < agent.timeout:
            return
        certificate = await agent.propose_recovery(state, holder)
        if certificate is None:
            return
        client = agent.gate.client
        result = await asyncio.to_thread(client.call, agent.state.agent_id, "InstallAuthority",
            certificate=certificate, expected_epoch=state["epoch"], expected_revision=state["revision"])
        if not result["allowed"]:
            agent.emit("AUTOMATIC_RECOVERY_REJECTED", reason=result["reason"])
            return
        accepted = result["state"]
        agent.emit("AUTOMATIC_RECOVERY_INSTALLED", target_epoch=accepted["epoch"],
                   votes=len(certificate["signatures"]), new_holder=accepted["holder"],
                   signers=[vote["signer_id"] for vote in certificate["signatures"]])
        await agent.reconcile_from_gate(expected={key: accepted[key] for key in
            ("epoch", "revision", "holder", "mode")})
        targets = {peer: address for peer, address in agent.peers.items() if peer in reachable}
        if targets:
            await ReconciliationClient(agent.recovery_config, agent.state.agent_id,
                agent.recovery_private_key, timeout=agent.timeout,
                credentials=self.credentials).reconcile(targets, accepted)
        self.ready = None

    # retries unavailable services while retaining durable voting rules
    async def run(self):
        while True:
            try:
                await self.step()
                self.last_failure = None
            except (grpc.RpcError, ReconciliationError, OSError, ValueError, RuntimeError) as error:
                failure = (type(error).__name__, str(error))
                if failure != self.last_failure:
                    self.agent.emit("AUTOMATION_WAITING", reason=failure[0], detail=failure[1])
                    self.last_failure = failure
            await asyncio.sleep(self.agent.interval)
