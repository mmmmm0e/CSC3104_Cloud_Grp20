# Short project demonstration

Start a fresh yard:

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.live --automatic --port 8767
```

Open `http://127.0.0.1:8767/live`. Allow the startup heartbeats to appear. Keep the
evidence-directory link visible and record the demonstration as backup.

| Time | Action | Explain |
| --- | --- | --- |
| 0:00–0:20 | Crane 1: Request turn, then Enter lane. Crane 3: Request turn. | The controller schedules access; the gate validates entry; other requests wait. Digital ownership and physical occupancy are separate. |
| 0:20–0:40 | Stop controller. Wait for automatic LOCAL epoch installation. | Agents detect missing responses, persist signed matching votes, and require a strict majority. Recovery keeps the healthy holder and does not clear the lane. |
| 0:40–0:50 | Try earlier permission. | The gate rejects the old epoch; the old permission cannot authorize movement. |
| 0:50–1:10 | Stop Crane 1. Wait for automatic replacement. | A majority replaces the failed holder, but Crane 1 remains physically inside and new entry stays blocked. |
| 1:10–1:25 | Restore controller. Wait for CENTRAL mode. | Return reconciles accepted authority and retains waiting work and occupancy. The controller does not declare the lane empty. |
| 1:25–1:45 | Operator clear lane; Crane 3: Enter lane, then Exit lane. | This is an explicit simulated clearance action. Only after clearance can a fresh grant produce accepted entry. |

Do not click Propose recovery in automatic mode. Failure injection is manual; the
recovery and return decisions are automatic. Entry, exit and operator clearance remain
explicit physical-simulator actions. This demonstrates a local prototype, not a
Kubernetes deployment or industrial crane control.

For a repeatable console backup with retained logs:

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.automatic_demo
```

Each member can explain their component while it appears: agent queue/heartbeats,
recovery votes, gate fencing, physical simulator, controller return and platform
packaging. Show performance comparisons separately in the original saved evaluation
dashboard and state their reference-controller scope.
