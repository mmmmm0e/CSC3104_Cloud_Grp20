"""Optional process-local metrics. Observations never grant protocol authority."""
import asyncio
import time

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server


class AgentMetrics:
    def __init__(self, agent):
        self.agent = agent
        self.registry = CollectorRegistry()
        labels = ["agent_id"]
        self.epoch = Gauge("cranelock_agent_epoch", "Cached authority epoch", labels, registry=self.registry)
        self.token = Gauge("cranelock_agent_has_token", "Cached token ownership, not lane permission", labels, registry=self.registry)
        self.queue = Gauge("cranelock_agent_queue_length", "Local pending request count", labels, registry=self.registry)
        self.mode = Gauge("cranelock_agent_mode", "Cached coordination mode", labels + ["mode"], registry=self.registry)
        self.peers = Gauge("cranelock_agent_suspected_peers", "Locally suspected peers", labels, registry=self.registry)
        self.controller = Gauge("cranelock_agent_controller_suspected", "Local controller suspicion", labels, registry=self.registry)
        self.events = Counter("cranelock_agent_events", "Selected local events; not global recovery totals", labels + ["event", "outcome"], registry=self.registry)
        self.detection = Histogram("cranelock_agent_detection_seconds", "Local last-observation-to-suspicion duration", labels + ["kind"], registry=self.registry)
        self.recovery = Histogram("cranelock_agent_recovery_seconds", "Local suspicion-to-successful-install duration, not physical entry", labels, registry=self.registry)
        self.started = None
        self.http = self.thread = self.task = None
        self.original_sink = agent.events

    def sink(self, event):
        self.original_sink(event)
        try:
            name, identity = event["event"], self.agent.state.agent_id
            if name in {"RECOVERY_CERTIFICATE_FORMED", "RECOVERY_NO_QUORUM", "RECOVERY_REJECTED",
                        "RECOVERY_AUTHORITY_INSTALL_RESULT", "CONTROLLER_SUSPECTED",
                        "CONTROLLER_RESTORED", "TOKEN_HOLDER_SUSPECTED"}:
                outcome = str(event.get("accepted", event.get("reason", "observed")))
                self.events.labels(identity, name, outcome).inc()
            if name in {"CONTROLLER_SUSPECTED", "TOKEN_HOLDER_SUSPECTED"} and self.started is None:
                self.started = event.get("monotonic", time.monotonic())
            if name in {"PEER_SUSPECTED", "CONTROLLER_SUSPECTED"} and "detection_latency_seconds" in event:
                self.detection.labels(identity, name).observe(event["detection_latency_seconds"])
            if name == "RECOVERY_AUTHORITY_INSTALL_RESULT" and event.get("accepted") and self.started is not None:
                self.recovery.labels(identity).observe(max(0, event["monotonic"] - self.started))
                self.started = None
            if name == "CONTROLLER_RESTORED":
                self.started = None
        except Exception:
            # Instrumentation must never change coordination decisions.
            pass

    async def sample(self):
        while True:
            async with self.agent.lock:
                state, identity = self.agent.state, self.agent.state.agent_id
                self.epoch.labels(identity).set(state.current_epoch)
                self.token.labels(identity).set(state.has_token)
                self.queue.labels(identity).set(len(state.request_queue))
                self.peers.labels(identity).set(len(self.agent.suspected))
                self.controller.labels(identity).set(self.agent.controller_suspected)
                for mode in ("CENTRAL", "LOCAL", "REJOINING"):
                    self.mode.labels(identity, mode).set(state.mode.name == mode)
            await asyncio.sleep(.5)

    def start(self, port, address):
        self.http, self.thread = start_http_server(port, addr=address, registry=self.registry)
        self.agent.events = self.sink
        self.task = asyncio.create_task(self.sample())

    async def close(self):
        self.agent.events = self.original_sink
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        if self.http:
            await asyncio.to_thread(self.http.shutdown)
            self.http.server_close()


class ControllerMetrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.heartbeats = Counter("cranelock_controller_heartbeats", "Successful native heartbeat responses", registry=self.registry)
        self.epoch = Gauge("cranelock_controller_epoch", "Gate epoch observed by controller", registry=self.registry)
        self.http = self.thread = None

    def observe(self, state):
        self.heartbeats.inc()
        self.epoch.set(state["epoch"])

    def start(self, port, address):
        self.http, self.thread = start_http_server(port, addr=address, registry=self.registry)

    async def close(self):
        if self.http:
            await asyncio.to_thread(self.http.shutdown)
            self.http.server_close()
