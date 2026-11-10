# Deployment and failure injection implementation

CraneLock components are packaged as Python 3.12 Docker images. A gate image runs
the trusted access service, while a shared agent image runs either a crane-agent
launcher or the native controller launcher. Compose connects one gate, one
controller and three fixed-membership agents over a private project network.
Host gRPC and dashboard ports bind only to loopback. The gate and controller use
mutual TLS; each process receives its own Ed25519 application-signing key through
a read-only mount. Development transport client certificates are shared per
service destination and do not bind certificates to individual signing identities.
Peer-agent RPCs retain the existing trusted-network plaintext transport.

Each crane stores epoch, holder, revision and durable voting information in its
own named volume. The gate uses one SQLite database volume and one active process.
Initialization is an explicit new-experiment action, separate from startup and
restart. A restart cannot clear physical occupancy, reset a vote or silently create
new token authority. Native launchers reconcile against accepted gate state. The
platform launcher supports repeatable provisioning, explicit bootstrap, endpoint
inspection, state checks and evidence export while preserving experiment storage.

A local Kubernetes deployment uses Kustomize, namespace-scoped Services,
ConfigMaps and Secrets. Three one-replica StatefulSets give fixed identities
separate volumeClaimTemplates and credential mounts. The gate Deployment uses
Recreate and a single persistent claim. Explicit initialization and bootstrap Jobs
run before normal controller/agent startup. Service-specific readiness, startup and
liveness probes measure responsiveness without treating unknown lane occupancy as
an unhealthy process. Validation uses a dedicated single-node kind cluster and
local/block-backed storage; replicated-gate availability and node-loss recovery
are not evaluated by this deployment.

Prometheus scrapes gate, controller and agent metrics. Optional agent telemetry
samples cached authority, mode, local queue and suspicions and counts selected
recovery events without participating in protocol decisions. Controller telemetry
records successful native heartbeats. Grafana data sources and dashboards are
provisioned from versioned files. Target reachability is displayed separately from
application health to avoid presenting a stale healthy sample after failure. Local
recovery-install durations exclude physical clearance and next entry; metrics
must not be mistaken for end-to-end physical-safety or throughput results.

The failure injector distinguishes graceful stop, SIGKILL and restart. Timed peer
partitions use isolated network-namespace firewall rules; netem injects packet
latency, jitter and reordering, with cleanup in finally blocks. TCP preserves stream
ordering, so application-message reordering remains a separate simulator test.
A synthetic missing-update scenario sends one empty-lane observation, suppresses
further evidence past its timeout and verifies fail-closed grant rejection. It does
not clear an existing physical model or perform a crane crossing. Fault commands
and validators retain timestamped JSON records and container logs. Fault timestamps
bracket command execution, while detection and safe-entry latency require the
corresponding component and physical events.

Deployment evidence includes real controller outage/return, LOCAL majority
replacement after holder loss, stale-permission rejection, conservative restarts,
provisioned monitoring and Kubernetes pod/PVC checks. Exact pass/fail results and
local artifact paths are recorded in Platform_Validation.md. End-to-end workload,
physical-safety and scalability evaluation must use a runner connected to these
external services, rather than the simulator's separately constructed processes.
