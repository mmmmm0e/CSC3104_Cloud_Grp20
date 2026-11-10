# Integrated container handoff

This deployment runs the actual gate, controller and three recovery-enabled agents
on one Docker Compose network. It provides endpoints for an external workload
runner and repeatable service stop/start/kill controls. It does not itself run the
yard simulator or generate physical lane movements by itself.

For provisioned monitoring, fault experiments, Kubernetes and shutdown, see
[platform operations](PLATFORM_OPERATIONS.md). A minimal external crossing example
is available as `python scripts/external_smoke.py --new-empty-simulation`.
Submission instructions are in `output/pdf/GROUP20_INSTRUCTIONS.pdf`; report material
is in `docs/PLATFORM_IMPLEMENTATION.md`.

Run commands from the repository root with Docker Desktop running and Python 3.12+
in the project virtual environment. The helper also works from another directory
when invoked by its absolute path. These are local development identities, fixed
three-agent membership and one active gate, not a production port deployment.

## First setup on another machine

Create and activate a virtual environment using the platform's normal commands,
then install the project:

```bash
python -m pip install -e ".[dev]"
python scripts/integrated.py provision
python scripts/integrated.py init
python scripts/integrated.py up
python scripts/integrated.py status
python scripts/integrated.py state
python scripts/integrated.py check
```

Provisioning creates ignored files under `demo-data/integrated`: public configuration,
development signing keys, one signing file per actor, and separate gate/controller
TLS sets. It refuses any existing experiment directory. Never commit these private
files. Local setup generates new identities on each teammate's machine; do not
combine agents from independently provisioned experiments.

`init` builds images, explicitly initializes a NEW Docker gate volume, starts the
TLS gate and installs controller-signed CENTRAL authority at epoch 0/revision 1
with holder crane-1. It refuses an existing database. It does not clear the lane.
If initialization succeeded but bootstrap was interrupted, start the gate using
Compose and run `python scripts/integrated.py bootstrap` once; this independently
refuses an already initialized authority. Do not rerun initialization or delete
state as a recovery procedure.

`up` builds current code and starts services; it does not initialize or reset
storage. Gate readiness is checked; process startup of agents/controller is not
proof of protocol convergence. Use `state` to verify all agents agree with the gate,
peers are AVAILABLE and only the authoritative holder has the token. Initial peer
DNS connections can take time to settle. UNKNOWN occupancy is healthy but blocks entry.

## Existing experiment on Arif's machine

Configuration and volumes already exist. Skip `provision`, `init` and `bootstrap`:

```bash
python scripts/integrated.py up
python scripts/integrated.py state
```

Keep the `cranelock-integrated` project name: it identifies the existing volumes.
Never substitute the old standalone agents or their crane-a/b/c configuration.
The helper's `up` applies the new host ports and can recreate containers while
retaining volumes. Agents must reconcile after recreation.

## Endpoints and container names

| Service | Host endpoint | Compose-network endpoint | Container name |
| --- | --- | --- | --- |
| Gate gRPC | localhost:50061 | gate:50051 | cranelock-integrated-gate-1 |
| Controller heartbeat | localhost:50062 | controller:50052 | cranelock-integrated-controller-1 |
| Crane 1 gRPC | 127.0.0.1:50101 | crane-1:50051 | cranelock-integrated-crane-1-1 |
| Crane 2 gRPC | 127.0.0.1:50102 | crane-2:50051 | cranelock-integrated-crane-2-1 |
| Crane 3 gRPC | 127.0.0.1:50103 | crane-3:50051 | cranelock-integrated-crane-3-1 |
| Gate metrics | http://127.0.0.1:9190/metrics | http://gate:9090/metrics | cranelock-integrated-gate-1 |

Host ports bind to loopback only. Gate and controller require mutual TLS. Agent
RPCs use plaintext within the isolated local simulation. Service names work only
inside this Compose network, not from the host. Each agent and gate has a separate
named persistent volume. Controller authority resides at the gate, not a separate
controller database. The gate/controller TLS client identities are shared
development identities per destination; signing keys remain actor-specific. This
does not implement per-member certificate-to-signing-identity authorization.

Machine-readable endpoint and credential-path information (no key contents):

```bash
python scripts/integrated.py endpoints
```

`check` verifies the host-published agent RPCs, gate mutual TLS, controller mutual-TLS
heartbeat and consistent CENTRAL authority with available peers. Run it only at a
healthy CENTRAL baseline, not during a LOCAL fault experiment. Its native controller
heartbeat has the same restoration semantics as agent heartbeats; it is not a
passive TCP probe. It generates no requests for physical entry or clearance.

Gate clients use `tls/gate/ca.pem`, `client.pem` and `client-key.pem` under
`demo-data/integrated`. Controller clients use the corresponding controller TLS
directory. Server certificates cover service names, localhost and 127.0.0.1.
Development certificates expire after 30 days; certificate rotation requires a
coordinated procedure, not rerunning provision over existing state.

## Workload runner integration

The current `cranelock_sim --mode ...` commands create their own services. Running
them beside this deployment is NOT a container workload/recovery experiment.
Darius must adapt the runner to connect to the external endpoints above instead
of constructing Agent/Gate/RunningServer instances.

- Use the generated public config and the simulator-only signing file
  `demo-data/integrated/signing/simulator.json` for simulator-signed gate actions.
  Use only the appropriate actor's key for any other signed action. The endpoint
  manifest exposes paths, never private key contents.
- Use the shared proto clients for agent token requests/state and the existing
  signed gate Client for grant/entry/occupancy operations. Respect actor permissions
  in CONTRACT.md; simulator credentials alone do not authorize crane grant requests.
- Serialize admission, movement, sampling, exit and clearance through the physical
  lane lock. An RPC grant or has_token=true never replaces actual entry validation.
- Start with UNKNOWN occupancy. Supply fresh, phase/session-aware simulator evidence
  after independently establishing physical clearance. No launcher automatically
  clears the lane or simulates operator clearance.
- The agent CLI coordinates digital authority but does not expose a complete remote
  crane-workload API. Agree the grant-request boundary with agent/gate owners; do
  not bypass it or distribute all crane private keys to a single crane container.
- Keep state queries separate from traffic generation: `state` is read-only.
  The simulator playback dashboard shows saved experiments, not live container state.

## Failure controls and evidence

```bash
python scripts/integrated.py stop controller
python scripts/integrated.py start controller
python scripts/integrated.py kill crane-1
python scripts/integrated.py start crane-1
python scripts/integrated.py restart gate
python scripts/integrated.py logs
```

`stop` uses graceful Compose shutdown; `kill` uses SIGKILL and is a distinct crash
experiment. Select the CURRENT gate holder when testing holder loss; crane-1 is
only the initial holder. In CENTRAL mode, a holder stop does not by itself exercise
LOCAL recovery. First verify controller-loss fallback if testing the LOCAL path.
Restarting a failed service uses its existing state. No command resets epochs,
votes or physical occupancy.

Fault actions append start/end wall-clock and host-monotonic timestamps and command
success to `run/platform-validation/fault-events.jsonl`. Those bracket command
execution, not the exact instant the service became unavailable. Use component
events to measure detection and recovery. The `logs` command creates a unique
cumulative container-log export in that directory; exports contain prior runs too.
Container recreation may remove old container logs, so export before recreating.
Do not subtract unrelated host/container monotonic clocks. Preserve workload seed,
run boundaries, authority versions, gate decisions and physical traces together.

Network partitions, delay/reordering and lost occupancy updates are not implemented
by this helper. The single-process simulator scenarios remain separate evidence.

## Shutdown and resume

```bash
python scripts/integrated.py logs
python scripts/integrated.py stop
```

This stops only the integrated project and retains all volumes. The older
`deploy/compose.yaml` monitoring/standalone project, if running, is separate:

```bash
docker compose -f deploy/compose.yaml stop
```

Quit Docker Desktop after all wanted services are stopped. Resume the integrated
experiment with `python scripts/integrated.py up` and verify `state`. Do not use
`down -v`, database deletion or new identities to resume an existing experiment.

## Validation scope

The previous manual deployment verified mutual TLS, agent convergence, controller
fallback and stable controller return after bounded reconnect retries. One
external synthetic crossing is now validated; a complete workload experiment is
still shared evaluation work. The host port mappings and helper must be exercised on the target
machine before claiming this handoff is operational there. The latest platform
validation also covers optional component telemetry, provisioned monitoring,
container faults, Kustomize deployment and a single external synthetic crossing.
See Platform_Validation.md for evidence and scope. The original standalone
Grafana/Prometheus project still monitors its old gate independently.
