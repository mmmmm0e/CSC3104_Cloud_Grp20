# CraneLock

CraneLock is a CSC3104 Cloud & Distributed Computing coursework prototype for
coordinating multiple simulated cranes that share a single lane. The research
problem is how to retain coordinated access when a central scheduler or token
holder fails, while preventing a change in digital authority from being mistaken
for physical clearance. The system schedules centrally in **CENTRAL** mode and
can recover to peer coordination in **LOCAL** mode.

## Architecture and behavior

- **Crane Agents** exchange gRPC token requests, handoffs and heartbeats. Requests
  are coordinated through a FIFO queue; agents persist coordination state and
  retry incomplete handoffs conservatively.
- **Controller and recovery**: the controller schedules CENTRAL authority. If
  controller heartbeats fail, configured agents can automatically fall back to
  LOCAL coordination. On LOCAL holder failure, agents can collect signed votes
  from a strict majority to install a new recovery epoch. A returning controller
  reconciles authority and can restore CENTRAL mode.
- **Trusted Gate**: one Gate enforces authority using epoch and revision fencing,
  validates entry, and persists authority and occupancy state. A new epoch or
  token does not clear an occupied or unknown lane. Entry requires valid Gate
  authorization and fresh trusted physical clearance.
- **Physical lane simulator**: an independent simulated lane models entry,
  occupancy and exit, and supplies trusted observations to the Gate. It is not
  a real crane or sensor integration.
- **Evaluation and deployment**: workload runners record events and metrics;
  a local dashboard displays saved results. Docker Compose runs the integrated
  Gate, controller and three Agents, with monitoring; Kubernetes manifests and
  operations are also provided.

Agents restart conservatively in `REJOINING` and reconcile with the Gate before
participating. The Gate retains durable state and treats occupancy conservatively
on restart. Recovery and controller return depend on the required services,
quorum and physical evidence; progress is not guaranteed in every failure.

## Setup and developer commands

Use Python 3.12 or later and pip. Docker Desktop with Linux containers is needed
for the integrated deployment; Kubernetes setup has additional prerequisites in
[platform operations](docs/PLATFORM_OPERATIONS.md). From the repository root,
for example in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest -q
```

On Linux/macOS use `.venv/bin/python` in place of the Windows executable. The
last command runs the full Python test suite. Generated protobuf bindings are
checked in; after changing a protocol, regenerate them with
`python scripts/generate_proto.py` from the activated environment. Optional
plotting dependencies are available with `python -m pip install -e ".[dev,evaluation]"`.

For a new integrated deployment, follow the [platform handoff](docs/PLATFORM_HANDOFF.md)
for provisioning, initialization, startup and convergence checks. Existing
installations must retain their identities and volumes; do not reinitialize them
as a recovery procedure. [Deployment](docs/DEPLOYMENT.md) covers persistence,
TLS and the standalone Gate; [platform operations](docs/PLATFORM_OPERATIONS.md)
covers monitoring, fault controls and Kubernetes.

## Evaluation

### Integrated Docker workload

After completing the platform handoff and confirming a healthy CENTRAL baseline,
run a single normal workload against the existing integrated services:

```powershell
.\.venv\Scripts\python.exe scripts/integrated.py state
.\.venv\Scripts\python.exe scripts/integrated.py check
.\.venv\Scripts\python.exe scripts/external_workload.py --new-empty-simulation --workload-config demo-data/integrated/workload/config.json --scenario normal --requests 18 --seed 20
```

The workload uses a newly declared empty **simulated** lane and must run without
another active workload. For controller-outage and holder-failure commands, their
prerequisites, and checks between runs, see [integrated container testing](docs/EVALUATION.md#test-the-integrated-containers).
Those fault scenarios stop and restart the affected services and retain their
volumes; they should be run only on a suitable test deployment. Results are saved
under `run/deployed-evaluation`.

### Logical scalability and dashboard

The logical experiment varies simulated crane count and demand through a FIFO
scheduler and the Gate. For the controlled 18-configuration matrix, run:

```powershell
foreach ($rate in 2, 5) {
    foreach ($cranes in 3, 5, 10) {
        foreach ($seed in 7, 20, 3104) {
            .\.venv\Scripts\python.exe -m cranelock_sim --mode logical --cranes $cranes --duration 60 --arrival-rate $rate --crossing-time 0.5 --seed $seed
        }
    }
}
```

Arrival rates are aggregate requests per simulated second. Each run creates a new
results directory under `run/evaluation`. This logical model does not measure
networked Agent performance or recovery. To browse saved results, run
`.\.venv\Scripts\python.exe -m cranelock_sim.dashboard` and open
`http://127.0.0.1:8765`; see the [evaluation guide](docs/EVALUATION.md) for
other scenarios, dashboard scope and comparison commands.

### Validation evidence

During the **17–18 November 2026** validation period, the reported full Python
suite result was **443 passed, 1 skipped, 0 failed**. The integrated CraneLock-only
workload completed **9/9 experiments** (normal, controller outage and holder
failure, each with seeds 7, 20 and 3104): **162/162 simulated crossings** completed
with zero detected lane overlaps. The controlled logical matrix completed
**18/18 experiments**, using 3, 5 and 10 cranes, aggregate arrival rates of 2 and
5 requests per second, and the same three seeds. These results were gathered across
the validation period; they were not all rerun on commit `8390b36`. Raw newer
workload and logical experiment artifacts are local under the ignored `run/`
directory and are not included in the repository.

An **earlier, separate 27-case Docker comparison** tested central lock, basic
token and CraneLock across three scenarios and three seeds. It used a distinct
experiment deployment, not the newer integrated CraneLock-only workload. Its
provenance and results, together with an earlier integrated dataset, are in
[Docker evaluation results](docs/DEPLOYED_RESULTS.md); do not pool the datasets.
Observed results are bounded simulation evidence, not a general safety proof or
an availability guarantee.

## Scope and limitations

- Membership is fixed for a run. The integrated Docker tests use three deployed
  Agent processes; the 5- and 10-crane experiments are logical simulations only.
- The design assumes one trusted Gate and trusted simulated physical observations.
  Gate unavailability, missing quorum or uncertain clearance can prevent progress.
- Agent-to-agent RPCs use mutual TLS with per-agent identity binding in the
  integrated Compose and Kubernetes deployments (see the
  [agent launcher guide](docs/AGENT_LAUNCHER.md)). Standalone launches and the
  isolated loopback test harnesses may still run peers in plaintext.
- Agent and Gate state is persistent, but the handoff queue is not replicated.
  Permanent loss of an Agent's local storage does not permit full queue recovery;
  controller reassignment can also leave previously accepted requests unresolved.
  See [handoff durability](docs/HANDOFF_DURABILITY_DESIGN.md).
- This is a coursework simulation, with no industrial safety certification or
  real crane, sensor or actuator integration. The safety evidence depends on the
  assumptions documented in [Gate safety evidence](docs/SAFETY_PROOF.md).

## Standalone demos and documentation

The standalone Gate demo is separate from the integrated deployment. For a new
local demo directory, run:

```powershell
.\.venv\Scripts\python.exe -m cranelock_gate.demo setup --directory demo-data
.\.venv\Scripts\python.exe -m cranelock_gate.demo run --directory demo-data
```

Skip setup if that directory is already provisioned. The demo uses local
development identities; see [standalone deployment](docs/DEPLOYMENT.md#local-standalone-validation).
The [agent launcher guide](docs/AGENT_LAUNCHER.md) and
[agent/Gate integration guide](docs/AGENT_GATE.md) describe additional standalone
and integration examples.

- [Architecture review](docs/ARCHITECTURE_REVIEW.md) — historical design review
  and context; its milestone-status observations predate the integrated baseline.
- [Safety evidence](docs/SAFETY_PROOF.md) and [recovery safety](docs/RECOVERY_SAFETY.md).
- [Evaluation and dashboard](docs/EVALUATION.md) and [historical Docker results](docs/DEPLOYED_RESULTS.md).
- [Deployment and TLS](docs/DEPLOYMENT.md), [platform handoff](docs/PLATFORM_HANDOFF.md)
  and [platform operations](docs/PLATFORM_OPERATIONS.md).
- [Handoff durability](docs/HANDOFF_DURABILITY_DESIGN.md) and [recovery retries](docs/RECOVERY_RETRY.md).
