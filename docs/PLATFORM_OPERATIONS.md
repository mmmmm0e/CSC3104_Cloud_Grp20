# Platform operations and completion scope

This platform runs a fixed three-agent CraneLock simulation, the native controller,
trusted gate and metrics services. Local Compose and single-node Kubernetes are
validated execution environments. They are separate experiments with separate
persistent databases even when they use the same development identities. Never
mix endpoints between them.

## Integrated monitoring

The integrated helper includes deploy/compose.monitoring.yaml automatically.
Prometheus runs at http://127.0.0.1:9092 and Grafana at http://127.0.0.1:3001.
For a live demo, `python scripts/integrated.py grafana` opens the CraneLock dashboard
in a new browser window, full screen without menus (press Esc to show them). Log in as `admin`
with the password in `demo-data/integrated/grafana-password`.
The original standalone monitoring ports 9091/3000 are a different project.

Grafana's Prometheus source and the CraneLock Integrated Platform dashboard are
provisioned from deploy/monitoring; no manual import or data-source selection is
required. The username is admin. Read the generated local password from
`demo-data/integrated/grafana-password`; never put it in Git or logs. Initial
credentials apply to a new Grafana volume and are not a password-rotation mechanism.

Prometheus scrapes the gate, controller and all three agents every five seconds.
The `up` panel indicates whether the latest scrape succeeded, independently of
last reported application health. Instant queries avoid displaying an old healthy
sample as current state after a target fails. Missing series show No current data.

Agent metrics are optional launcher features (`--metrics-port`, `--metrics-bind`).
Sampling takes a short state lock without network calls; metrics event handling
never grants authority and errors in instrumentation do not alter coordination.
The metrics HTTP listeners are unauthenticated and remain on the local deployment
network; do not expose them publicly.

| Metric | Interpretation |
| --- | --- |
| up | Prometheus scrape reachability, not lane safety |
| cranelock_gate_available | Gate storage/service health; UNKNOWN lane can still be healthy |
| cranelock_gate_lane_unknown | 1 means unknown clearance; 0 does not imply empty |
| cranelock_gate_grants_total / entries_total | Process-local counters, reset on restart |
| cranelock_agent_epoch / mode | Cached authority view for each agent |
| cranelock_agent_has_token | Cached token view; stale views are fenced at the gate |
| cranelock_agent_queue_length | Agent's local waiting queue |
| cranelock_agent_suspected_peers | Local failure-detector observations |
| cranelock_agent_controller_suspected | Local controller suspicion |
| cranelock_agent_events_total | Selected local recovery/controller events; not a global recovery count |
| cranelock_agent_detection_seconds | Last observation to suspicion on that agent's clock |
| cranelock_agent_recovery_seconds | Local suspicion to its successful authority install; excludes next physical entry |
| cranelock_controller_heartbeats_total | Successful native controller heartbeat responses |

Do not sum token-owner gauges across partitioned epochs as a physical-safety check.
Use accepted gate entries and an independent physical trace to check collisions.
No physical throughput, lane-use fairness or end-to-end access-latency claim follows
from this metrics dashboard alone.

## Container failure experiments

Build the dedicated tool image once:

```bash
python scripts/faults.py build
```

Single-service controls (no dependency auto-start):

```bash
python scripts/integrated.py stop controller
python scripts/integrated.py start controller
python scripts/integrated.py kill crane-1
python scripts/integrated.py start crane-1
python scripts/integrated.py restart gate
```

A stop is graceful; kill sends SIGKILL. Select the actual current holder, not always
crane-1. The validator derives it from accepted gate authority.

Timed network faults:

```bash
python scripts/faults.py partition crane-1 --seconds 10
python scripts/faults.py partition all --seconds 10
python scripts/faults.py delay crane-3 --ms 200 --jitter 20 --reorder 25 --seconds 5
python scripts/faults.py reordered-command
```

Partition drops traffic only between crane peers; gate/controller connectivity is
preserved. All-isolated removes every peer-to-peer link. The tool uses a short-lived
NET_ADMIN helper joined only to the selected project's container network namespace;
it does not edit the host firewall. Timing parameters are bounded to 60 seconds and
rules/qdiscs are removed in finally cleanup. Do not restart a target while a timed
fault is active. If the tool is force-killed or cleanup fails, inspect iptables/qdisc
state before another experiment. Do not run concurrent validators or fault commands.

Netem reorders IP packets. TCP restores byte ordering, so this is NOT a delivered
gRPC-message reordering test. The simulator's RPC delay/reorder adapter remains the
appropriate general application-message test. The reordered-command case holds an
already signed old grant, installs a legal newer controller authority revision,
then releases the old request and verifies STALE_REVISION. It is a targeted fencing
check, not a general RPC delivery-reordering scheduler. No claim is made that packet timing is
bit-for-bit deterministic across runs; workload seeds and fault schedules must be
recorded separately.

Missing occupancy updates are tested with an explicitly NEW empty synthetic lane,
not by assuming an existing real/simulated occupied lane is empty:

```bash
python scripts/faults.py lost-updates --new-empty-simulation
```

This requires UNKNOWN occupancy and no active grant and must not run beside another
physical workload runner. It sends one fresh empty-model observation, stops sending
updates beyond the configured timeout and verifies the gate rejects a holder grant
with LANE_UNKNOWN. It never moves a crane or proves full physical safety.

Serial deployment validation:

```bash
python scripts/validate_platform.py all --new-empty-simulation
```

Scenarios are controller loss/return, LOCAL holder SIGKILL and restart with stale
permission fencing, gate outage/restart, monitoring DOWN visibility, majority
recovery across a peer partition, a drained all-isolated no-majority window, packet
delay/reordering cleanup, delayed signed-request fencing, and omitted
occupancy evidence. Run each scenario individually using its CLI help if needed.
The validator restores stopped services but never erases epochs, votes or grants.
A failed scenario is retained as failed evidence, not converted into a pass.

Fault commands append to `run/platform-validation/fault-events.jsonl`.
Validators create unique JSON summaries. `integrated.py logs` exports cumulative
container logs; export before recreation removes the old container history. No
fixed seed or exact recovery-latency benchmark is asserted by these smoke checks.

## Kubernetes first setup

Install kind and kubectl from their official sources. This repository pins the
context kind-cranelock and keeps its kubeconfig in ignored demo-data/kubeconfig;
it does not use a production or default context. Commands assume the Compose
configuration/credentials have already been provisioned and images built.

```bash
python scripts/kubernetes.py cluster-create
python scripts/kubernetes.py load-images
python scripts/kubernetes.py init
python scripts/kubernetes.py up
python scripts/kubernetes.py status
python scripts/kubernetes.py check
python scripts/validate_kubernetes.py
```

The image-loader exports the local architecture before importing to avoid Docker
Desktop multi-platform provenance-content errors. The first image import may take
several minutes. The image archive stays in ignored demo-data.

The generator writes public Kustomize resources to deploy/kubernetes/integrated.
Runtime public configuration uses a ConfigMap; private signing/TLS files are sent
to namespace Secrets over kubectl stdin, never written to tracked manifests or
placed on the command line. Agent sets have one replica each so each identity can
receive its own signing Secret. Headless Services provide stable peer endpoints.
Each set has a distinct volumeClaimTemplate, producing data-crane-1-0 etc.

The gate uses one replica, Recreate and a single PVC. Initial database creation and
signed authority bootstrap are separate Jobs while other replicas are zero. Neither
normal startup nor pod replacement automatically initializes a database. Init
refuses existing jobs/storage; an interrupted init must be investigated, not reset.
The existing filesystem lease prevents concurrent gate ownership on the same
filesystem; this is not a highly available replicated gate.

Services use the same internal names/ports as Compose (gate:50051,
controller:50052, crane-1/2/3:50051 for mTLS peers and crane-1/2/3:50053 for mTLS workload).
Monitoring uses prometheus:9090 and grafana:3000.

On an existing experiment, run `python scripts/integrated.py provision-workload`
once if the workload bundle is missing. Use `kubernetes.py load-images` and `up`
to upgrade the dedicated cluster; do not rerun `init` on its existing PVCs.
The renderer mounts each `crane-N-workload-tls` Secret read-only in its own StatefulSet.
An in-cluster runner can mount `workload-runner-tls` at `/workload-client` and
`workload-runner-config` at `/workload-config`, then pass
`--workload-config /workload-config/config.json` to its workload client. That
config contains paths and internal endpoints only; it carries no keys. These
objects provision credentials and endpoints, not a runner Pod. A local auth check
can use loopback port-forwards to each `service/crane-N` workload port and a host
runner config whose endpoint ports match the forwards. No workload Service is
published outside the cluster by default.
These ClusterIP/headless endpoints are not directly host-accessible. For a temporary
local dashboard connection, in a separate terminal:

```bash
kubectl --kubeconfig demo-data/kubeconfig --context kind-cranelock -n cranelock port-forward service/grafana 3002:3000
```

Then open http://127.0.0.1:3002; stop forwarding with Ctrl+C. Do not bind forwarding
to all interfaces. The same local Grafana password initializes this separate volume.

Readiness/startup/liveness probes check service responsiveness. UNKNOWN occupancy
and REJOINING are not reasons to restart a process repeatedly. Probes do not prove
physical safety or quorum availability. StatefulSet headless Services publish
not-yet-ready peers so startup does not deadlock on mutual discovery.

Kubernetes validation checks agent pod replacement with the same PVC and rejoin,
exact gate authority persistence with peers paused for planned maintenance, new gate
session/invalidated clearance, then restored peer convergence and five live metrics
targets. A single local kind node does not validate node-loss resilience or production
storage. Kubernetes NetworkPolicy partition enforcement is not claimed for kind's
default CNI; the tested network faults are in Compose containers.

Resume an existing cluster (never init again):

```bash
python scripts/kubernetes.py cluster-start
python scripts/kubernetes.py up
python scripts/kubernetes.py check
```

## Shutdown without losing evidence

```bash
python scripts/integrated.py logs
python scripts/integrated.py stop
python scripts/kubernetes.py logs
python scripts/kubernetes.py stop
python scripts/kubernetes.py cluster-stop
```

If the old standalone monitoring project is still running:

```bash
docker compose -f deploy/compose.yaml stop
```

Persistent volumes and the stopped kind node are retained. Do not delete the kind
cluster, remove PVCs, use down -v or delete databases to resume an experiment.

## External workload starting point

```bash
python scripts/external_smoke.py --new-empty-simulation --workload-config demo-data/integrated/workload/config.json
```

This uses the actual host-published peer, workload and mutual-TLS gate endpoints. It creates
one new empty physical model, requests an actual token handoff, uses separate
crane/simulator signing identities to request and consume a grant, serializes entry
and exit through the existing Lane lock, and leaves clearance UNKNOWN after its
last evidence expires. The test refuses an existing known lane/grant and must not
run beside another physical runner. It writes physical-entry/exit and authority
events to a unique local JSON record. It is a minimal integration example for
Darius, not the final workload generator or scalability benchmark.

## Workload validation scope

Use `scripts/external_workload.py` with the generated workload config for sustained
tests on the integrated Docker deployment. The `cranelock_sim` CLI modes create
their own services and do not automatically use these endpoints. Run the auth probe
first, then exercise normal traffic and each fault separately, allowing occupancy
to expire to UNKNOWN and all agents to converge before each test. An authentic grant
still requires a fresh `ValidateEntry`. These local experiments use synthetic lane
movement and three agents; they do not measure larger deployments or node loss.
