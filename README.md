# CraneLock | CSC3104 Group 20

CraneLock coordinates simulated cranes sharing a single lane. This repository
contains the Python/gRPC source, tests, workload runners, Docker Compose files,
and Kubernetes manifests for the CSC3104 Cloud and Distributed Computing project.

The research article (`GROUP20.pdf`), detailed testing instructions
(`GROUP20_INSTRUCTIONS.pdf`), and presentation video are submitted separately.

## Research contribution

CraneLock combines majority-approved token recovery with a trusted Gate that
checks lane occupancy independently. This lets agents restore coordination
after controller or token-holder failure while keeping an occupied or unknown
lane blocked until trusted clearance arrives. The evaluation compares CraneLock
with central locking and basic token passing under identical seeded workloads.
See the [Docker comparison](docs/DEPLOYED_RESULTS.md) and the
[recovery safety argument](docs/RECOVERY_SAFETY.md).

## How it works

1. **Normal operation.** The controller schedules turns in `CENTRAL` mode. The
   current holder requests a grant, and the lane simulator submits it to the Gate
   for one-shot entry validation.
2. **Controller outage.** Agents detect missed heartbeats and collect matching
   Ed25519-signed votes from a strict majority of configured members. The Gate
   verifies the certificate and installs the next epoch in `LOCAL` mode,
   retaining the current holder. Agents then pass the token peer to peer.
3. **Holder outage in LOCAL mode.** The same quorum process selects the next
   configured agent as holder. A partition needs a strict majority and access
   to the Gate to install recovery. Votes are saved before signing and can be
   replaced only after verifying a strictly newer Gate revision in the same
   epoch. The Gate checks the expected epoch and revision atomically.
4. **Admission and return.** The Gate rejects stale authority after a transition.
   The holder needs a fresh grant and trusted clearance for entry. A returning
   controller reconciles with the Gate before restoring `CENTRAL` mode.

The integrated Compose and Kubernetes deployments use mutual TLS, including
identity-bound peer RPCs. Restarted agents reconcile with the Gate before
participating. Fixed membership, one trusted Gate, and trusted simulated occupancy
are assumptions of the prototype. Same-snapshot vote conflicts can stall
recovery, and permanent loss of a sender's storage can leave queued work
unresolved. See [handoff durability](docs/HANDOFF_DURABILITY_DESIGN.md).

## Before you start

| Tool | Needed for |
| --- | --- |
| Python 3.12 or later | Everything: tests, simulator, dashboard, deployment scripts |
| Docker Desktop (Linux containers) | The integrated deployment and the Docker comparison |
| kind and kubectl | The Kubernetes deployment only |
| Microsoft Edge or Google Chrome | The optional dashboard browser test only |

The install, tests, simulator and dashboard below need only Python.

## Quick start

Run these commands from the repository root in PowerShell. On Linux or macOS,
use `.venv/bin/python` as the Python executable after creating the environment.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest -q
```

Two optional tests are skipped with this install: the chart test needs
matplotlib and the dashboard browser test needs Playwright. To include them,
install the extras and run the tests again:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev,evaluation,ui-test]"
.\.venv\Scripts\python.exe -m pytest -q
```

## Try the simulator without Docker

This runs CraneLock, central locking and basic token passing side by side as
local processes, under a healthy run, a controller outage and a holder outage.
It takes about one minute.

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim --mode benefits --seeds 7 --loads 6 --no-plots --output run/evaluation/quick-start
.\.venv\Scripts\python.exe -m cranelock_sim.dashboard
```

Open `http://127.0.0.1:8765` and select the `quick-start` results. Stop the
dashboard with Ctrl+C. The output folder must not already exist; choose a new
name to run it again.

## Interactive demo

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.live
```

Stop the standalone evaluation server first if it is already using port 8765.
Open [the live yard](http://127.0.0.1:8765/live) and select **Start guided demo**.
Use **Live demo** and **Evaluation** in the header to switch between the two views.
One live-yard server provides both views.
Advance through turn-taking, a no-quorum refusal, LOCAL recovery, stale-grant
rejection, holder replacement, explicit clearance, and CENTRAL restoration.
Each step runs live simulator actions and waits for the observed result.
The manual controls are also available for individual experiments.

The demo starts a fresh local yard with simulated occupancy and writes evidence
under `run/evaluation/live-*`. Docker is optional for the separate integrated
deployment below.

## Integrated deployment

For the integrated deployment, start Docker Desktop in Linux-container mode.
Use `provision` and `init` for a fresh deployment. For an existing deployment,
keep its credentials and volumes and start at `up`.

```powershell
.\.venv\Scripts\python.exe scripts/integrated.py provision
.\.venv\Scripts\python.exe scripts/integrated.py init
.\.venv\Scripts\python.exe scripts/integrated.py up
.\.venv\Scripts\python.exe scripts/integrated.py check
```

The check verifies a healthy CENTRAL baseline, agreement on the Gate's authority,
and token ownership by the current holder. Use
`.\.venv\Scripts\python.exe scripts/integrated.py stop` to stop the services
and retain their volumes. See the [platform handoff](docs/PLATFORM_HANDOFF.md)
and [Kubernetes setup](docs/PLATFORM_OPERATIONS.md#kubernetes-first-setup).

## Evaluation and dashboard

Use the [evaluation guide](docs/EVALUATION.md) to reproduce normal, outage,
partition, restart, and logical scalability tests. To replay saved results:

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.dashboard
```

Open `http://127.0.0.1:8765`. The dashboard reads saved experiments from
`run/evaluation`. Results are not included in the repository, so the dashboard
is empty until you run an experiment (see the simulator step above). To open live
metrics for the integrated deployment, run
`.\.venv\Scripts\python.exe scripts/integrated.py grafana`.

- The recorded 20 November 2026 Python suite result was **459 passed and 2 skipped**. The two skipped tests are the
  optional chart and browser checks; both pass with the optional extras installed.
- The nine integrated Docker workloads completed **162/162 simulated crossings**
  with zero detected overlap. The **18 logical runs** exercised 3, 5, and 10
  simulated cranes. A separate single-agent kind check observed one container
  restart and a return to Ready after a fatal worker failure.
- In the separate 27-case Docker comparison, CraneLock completed **53/54**
  requests in each outage scenario. Central locking completed **18/54** in the
  controller-outage scenario and basic token passing completed **0/54** in the
  holder-outage scenario. See the [results and measurement scope](docs/DEPLOYED_RESULTS.md).

## Repository layout

| Path | Contents |
| --- | --- |
| `cranelock/` | Agent, controller, token handoff, and recovery |
| `cranelock_gate/` | Durable authority, grants, and entry validation |
| `cranelock_sim/` | Simulated lane, experiment runners, and dashboard |
| `proto/` | Protobuf service definitions |
| `deploy/`, `scripts/` | Deployment manifests and operational tools |
| `tests/`, `docs/` | Automated tests and project documentation |
