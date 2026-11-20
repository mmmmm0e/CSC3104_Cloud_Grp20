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

## Quick start

Use Python 3.12 or later. Run these commands from the repository root in
PowerShell. On Linux or macOS, use `.venv/bin/python` as the Python executable
after creating the environment.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest -q
```

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

Open `http://127.0.0.1:8765`. The dashboard reads saved experiments. To open live
metrics for the integrated deployment, run
`.\.venv\Scripts\python.exe scripts/integrated.py grafana`.

- The recorded 20 November 2026 Python suite result was **459 passed and 2 skipped**.
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
