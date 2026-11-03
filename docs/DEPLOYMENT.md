# Deployment, persistence and testing handoff

## Local standalone validation

Install from the project root and run the README commands. The tests use temporary
databases and ephemeral signing keys, not the saved demo identities. No other member's
service is required. The demo uses real gRPC on loopback and does not bypass the gate.

The local validation covers quorum/signature failures, concurrent admission, normal
handoff, recovery during occupancy, grant expiry, lost/delayed observations, restart,
clock rollback, disk-commit failure, controller reconciliation, mutual TLS, metrics,
health status and deterministic physical-occupancy fault sequences.

`demo setup` provisions development public/private keys and explicitly initialises a
NEW database. `serve` never creates a missing database. Reinitialisation refuses an
existing file; deleting a database is not a recovery procedure. In WAL mode a running
gate also keeps `gate.sqlite-wal` and `gate.sqlite-shm` beside the database: they hold
committed state. Never copy or delete `gate.sqlite` alone; stop the gate first, or use
SQLite's backup API, so the copy includes every committed transaction. If persistent state is
lost, stop the experiment, confirm physical clearance, and start a separately named
new experiment under an explicit provisioning process.

## Docker Compose (isolated development)

Start Docker Desktop with Linux containers enabled. Then, from the repository root:

```powershell
docker compose -f deploy/compose.yaml build
docker compose -f deploy/compose.yaml run --rm gate init --config /config/config.json --database /data/gate.sqlite
docker compose -f deploy/compose.yaml up -d
docker compose -f deploy/compose.yaml ps
.\.venv\Scripts\python.exe -m cranelock_gate.demo run --directory demo-data --target 127.0.0.1:50051
```

The host `demo-data/config.json` must already exist (use demo setup). The Compose named
volume uses its own database, distinct from the host demo database. Run container init
once only, before starting the container. A later init attempt must fail rather than
reset state. `up` refuses to serve if the volume is empty or corrupt.

In this workspace, container validation already created and initialised the named
volume. Skip the `run ... init` command and use `up -d` to reuse it.

The Compose ports are published to host loopback. The image's development listener is
plaintext and command-authenticated; do not expose it to an untrusted network.
Private demo signing keys stay on the host and are not included in the image or gate
configuration mount. The container runs as UID/GID 10001; `/data` is writable.

Restart test:

```powershell
docker compose -f deploy/compose.yaml restart gate
docker compose -f deploy/compose.yaml logs gate
```

After restart, the gate retains epoch/holder/revision but marks occupancy UNKNOWN and
invalidates grants. The next simulator must read the new session and provide fresh
clearance. No position update means no admission. The standalone demo does this.

Export the container's ordered trace:

```powershell
docker compose -f deploy/compose.yaml exec -T gate python -m cranelock_gate.server export-events --config /config/config.json --database /data/gate.sqlite
```

Exporting is read-only and does not restart the gate or invalidate grants. The database
uses SQLite WAL mode, so a slow export cannot block the gate's commits. When stopping
the experiment, `docker compose -f deploy/compose.yaml down` retains its data volume.
Do not remove that volume as part of a recovery test.

## Kubernetes template (Member 6)

Files in `deploy/kubernetes` are an isolated-simulation template, not an already
deployed cluster. Provide an image registry/tag or load the image into the local cluster.
Use storage that supports SQLite locking and durable writes; an ordinary local/block
filesystem is expected. Do not use a shared network filesystem without validating its
locking/durability semantics. Do not clone the database or run a second gate for this lane.

Initial provisioning:

```powershell
kubectl create configmap cranelock-gate-config --from-file=config.json=demo-data/config.json
kubectl apply -f deploy/kubernetes/gate.yaml
kubectl scale deployment cranelock-gate --replicas=0
kubectl wait --for=delete pod -l app=cranelock-gate --timeout=120s
kubectl apply -f deploy/kubernetes/init-job.yaml
kubectl wait --for=condition=complete job/cranelock-gate-init --timeout=120s
kubectl scale deployment cranelock-gate --replicas=1
kubectl rollout status deployment/cranelock-gate
```

The initially created Deployment may fail to start before provisioning; this is the
intended missing-state protection. Ensure it is fully stopped before running init.
Only use the init Job for a new PVC/experiment. Normal deployment and pod recovery
must never include automatic database initialization.

Use `replicas: 1`, `Recreate`, and the same persistent volume across pod restarts. The
database process lease rejects concurrent ownership on the same filesystem. These
mechanisms are not a replicated-gate consensus algorithm. Member 6 must preserve the
single-active-gate assumption, including during node/volume recovery.

Readiness/liveness use the standard gRPC health service. UNKNOWN lane state is healthy:
restarting a gate cannot create physical clearance. Storage failure sets NOT_SERVING;
the process permanently stops accepting commands until restarted with valid storage.

The simulation manifest includes a Prometheus scrape annotation. Member 6 should
adapt it to the cluster's scrape configuration or ServiceMonitor. No Grafana dashboard
or cluster-specific chaos system is included. Gate metrics are:

- `cranelock_gate_decisions_total{operation,reason,replayed}`
- `cranelock_gate_grants_total` and `cranelock_gate_entries_total`
- `cranelock_gate_lane_unknown` and `cranelock_gate_available`
- `cranelock_gate_safety_stop_seconds` (cumulative UNKNOWN duration, including restart)

Counters reset with process restart; the durable event table remains the source for
whole-experiment totals. Normal occupied time is not counted as a safety stop.
Serve/scrape metrics only on a trusted network; their HTTP endpoint is unauthenticated.
It listens on the `--bind` host (loopback by default; `0.0.0.0` in the container images)
unless `--metrics-bind` names another address.

## Mutual TLS outside isolated development

Provide PEM server certificate/key and a trusted client CA. The server certificate's
SAN must match the client's destination hostname. Use:

```powershell
.\.venv\Scripts\python.exe -m cranelock_gate.server serve --config demo-data/config.json --database demo-data/gate.sqlite --bind 0.0.0.0:50051 --tls-cert config/server.pem --tls-key config/server-key.pem --tls-ca config/ca.pem
```

The three TLS arguments must be supplied together; client certificates are mandatory.
Command signatures still determine the authorised actor and role. The gate does not
hold the cranes' private signing keys. Deliver one private signing key only to its
corresponding actor; the demo's combined private-key file is a development fixture.

Member clients construct a secure channel and reuse the supplied Client class:

```python
from pathlib import Path
import grpc
from cranelock_gate.client import Client

credentials = grpc.ssl_channel_credentials(
    root_certificates=Path("ca.pem").read_bytes(),
    private_key=Path("client-key.pem").read_bytes(),
    certificate_chain=Path("client.pem").read_bytes(),
)
with grpc.secure_channel("gate.example:50051", credentials) as channel:
    client = Client(channel, config, own_signing_keys)
    current_state = client.state()
```

For Kubernetes TLS deployment, mount TLS files from Secrets, replace the insecure
argument with TLS arguments, and replace the native plaintext gRPC probes with
authenticated checks. A kubelet native gRPC probe cannot use the client certificate
required by this listener; use the bundled health check as an exec probe instead,
with a client certificate the gate's CA trusts:

```yaml
readinessProbe:
  exec:
    command: [python, -m, cranelock_gate.healthcheck,
              --tls-cert, /tls/probe.pem, --tls-key, /tls/probe-key.pem,
              --tls-ca, /tls/ca.pem, --server-name, cranelock-gate]
  initialDelaySeconds: 2
```

Use the same command for `livenessProbe`. `--server-name` must match a SAN in the gate's
server certificate when that certificate does not name `127.0.0.1`. Without TLS
arguments the health check probes the plaintext development listener, as Compose does. The supplied default manifest intentionally targets the
isolated plaintext simulation. Do not deploy it unchanged as an authenticated-network
configuration.

Configuration is bound to the database with a digest of public keys, identities and
timing settings. Changing these requires an explicit new-experiment/migration design;
the v1 gate refuses silent membership or policy changes. Keep clocks synchronised.
Clock rollback blocks commands and invalidates grants rather than extending authority.
The gate uses wall-clock time so expiry survives restarts. Even a few-millisecond backward
step therefore stops the lane until the clock passes its previous value: configure time
synchronisation to slew (for example chrony without `makestep` after startup), not step.

Storage schema version 2 adds the observation phase revision. Earlier development
schema-1 databases upgrade transactionally at startup without resetting authority,
request history or audit records; the normal restart reconciliation still applies.
Unknown storage schema versions are rejected. This migration does not change fixed
membership or the timing policy.

## Failure-injection integration

Member 6 can kill/restart the process or pod, interrupt the gRPC connection, delay
signed messages and suppress simulator updates. The gate has no remote failure-injection
API. Member 2/1 own majority recovery and token state after their own failures.

Expected checks for the integrated project:

| Failure | Gate outcome |
| --- | --- |
| Holder stops before entry | Valid recovery fences its grant; uncertain reservation stays blocked |
| Holder stops during occupancy | New authority installed, physical lane remains occupied/unknown |
| Minority partition | Certificate rejected; no replacement authority |
| Old entry after recovery/handoff | Old epoch/revision rejected |
| Lost position updates | UNKNOWN, then require fresh clearance |
| Gate unavailable | Simulator stops; no bypass or cached-grant entry |
| Gate restart | Persisted authority; new session; no old grants or observations |
| Storage commit failure | No success response; gate stops serving new commands |
| Controller returns with old snapshot | Stale update rejected; reconcile first |

Member 4 must verify physical-entry traces alongside gate events and run performance
experiments. This repository's standalone tests do not establish the full distributed
protocol's safety, Kubernetes fault behaviour or scalability results.

Member 4 must implement the physical-boundary serialisation described in CONTRACT.md:
admission, application of movement, position sampling, exit and operator clearance
must share the simulator's lane lock. The gate cannot atomically update another
process's physical model by itself.
