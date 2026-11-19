# Deployment and failure injection implementation

CraneLock components are packaged as Python 3.12 Docker images. A gate image runs
the trusted access service, while a shared agent image runs either a crane-agent
launcher or the native controller launcher. Compose connects one gate, one
controller and three fixed-membership agents over a private project network.
The integrated Compose and Kubernetes profiles use mutual TLS for Gate, controller,
workload and crane-to-crane RPCs. Each crane has its own peer certificate, and the
receiving agent verifies that the caller certificate matches the claimed crane
identity. Ed25519 application-signing keys authenticate signed protocol messages.
Published Compose gRPC and monitoring ports bind to loopback. See the
[peer TLS configuration](AGENT_LAUNCHER.md#agent-to-agent-mutual-tls).

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
an unhealthy process. A fatal agent worker failure exits the process so Kubernetes
can restart the container. Temporary persistence failures use the retry path.
Validation uses a dedicated single-node kind cluster and local storage.
Replicated-gate availability and node-loss recovery are outside its scope.

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
provisioned monitoring and Kubernetes pod/PVC checks. See the
[platform validation record](Platform_Validation.md) and
[deployed workload results](DEPLOYED_RESULTS.md) for evidence and scope. The
logical scalability runs measure simulated lane capacity. Deployment validation
uses fixed three-agent membership.
