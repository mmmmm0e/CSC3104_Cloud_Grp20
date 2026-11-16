# Testing and results (Darius)

This guide covers running the simulator, reading the dashboard and interpreting
the comparisons. Commands below run from the repository root in PowerShell.

## Open the dashboard

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.dashboard
```

Open **http://127.0.0.1:8765**. The viewer reads `run/evaluation`; it does not start
experiments or control cranes. Use **Refresh results** after generating results
and **Ctrl + F5** after changing the interface. Stop the server with Ctrl+C.

The separate **Integrated CraneLock deployment checks** section reads `run/deployed-evaluation`
automatically. It shows median timings, observed ranges and per-run evidence
downloads for the deployed CraneLock services. Expand **Inspect Docker runs and
download evidence** for individual results. These results are not pooled with
the three-system comparisons below. Restart the server after updating its code.
Use `--deployed-root PATH` if the Docker evidence is saved elsewhere.

The recommended set is the Docker comparison when available; otherwise use
`benefits-autonomous-validated` for local-process results. Select a scenario in the
main dropdown. Request count and sequence filters are inside **Test settings and
saved experiments**. A sequence (seed) repeats the same requests; it is not a crane.

- The summary explains why each system continues or stalls.
- Bars compare the median across selected repeated sequences. Missing recovery
  time is never shown as zero seconds.
- On failure pages, waiting uses that failure test, including requests before and
  after the fault. Entry counts show how many requests have a measured wait;
  requests that never entered are excluded, not assigned zero. A short average
  alongside many unserved requests does not establish better performance. Healthy
  waiting is shown on the healthy-operation page.
- The progress chart shows cumulative completed requests for **one selected
  sequence**, using recorded exit times. A flat line means no new completions.
  Its failure marker spans the recorded fault times across systems.
- Playback follows one saved run. **Next event** steps through it; guided play
  holds each important event for four seconds. The clock shows recorded time.
  **Jump to controller stopping** visibly marks the controller stopped. CraneLock
  can subsequently enter LOCAL mode while that controller remains stopped.
- Saved evidence exposes request CSVs, summaries and recorded events. Advanced
  settings also expose older or failed experiments; these are not fresh results.

The dashboard uses local HTML, CSS and JavaScript, system fonts and no npm build
or external assets. Its server is read-only and bound to loopback. Artifact
downloads are allowlisted and bounded to the configured results directory.

## Run the comparisons

Install optional plotting dependencies if needed:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev,evaluation]"
.\.venv\Scripts\python.exe -m cranelock_sim --mode benefits --duration 5
```

This creates a new timestamped results folder. By default it tests three systems,
three scenarios, seeds 7/20/3104 and request counts 6/18: 54 individual runs.
For a smaller test:

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim --mode benefits --seed 20 --loads 6 --scenario controller-outage --duration 5 --no-plots
```

`--output` chooses a destination; it must be a **new** directory. `--no-plots`
keeps CSV/JSON output without requiring matplotlib. Legacy `--mode compare`
retains the earlier fixed-window healthy/holder-failure comparison.

| System | Healthy operation | Controller stops | Token holder stops |
| --- | --- | --- | --- |
| Central lock | The team's CentralController installs access authority | Cannot assign new access | Can reassign while its coordinator is available |
| Basic token | Cranes pass one token | Continues locally; no controller recovery needed | Stalls because it cannot replace the missing token |
| CraneLock | Starts centrally in the benefits experiment | Detects missing heartbeats, obtains majority votes and installs LOCAL authority | Can replace the token with majority-approved recovery |

Central authority uses `cranelock/controller.py`, now integrated with platform main
snapshot `02da358`. Saved `benefits-autonomous-validated` results still record
`21fac92`; they have not been relabelled or converted into container results.
Future local comparisons record `02da358`. The `benefits-v4` runner starts five independent processes:
a controller, a gate and three crane agents. Agents use the supported CLI with
gate integration, their own signing keys and automatic recovery enabled for CraneLock.

The controller process exposes the real controller heartbeat service. A small
Assign transport wraps `CentralController.install_central` for workload scheduling.
The test runner submits demand, models lane occupancy and kills the controller or
token-holder process. Agent workers detect failure, collect signed majority votes,
install replacement authority and pass the LOCAL token; the runner does not call
recovery or token-transfer methods. Only initial LOCAL bootstrap uses a fixture
certificate. The independent gate still validates every physical entry.

These are separate processes on **one computer using loopback gRPC**. The physical
cranes and lane remain simulated. This evaluation does not demonstrate Docker,
Kubernetes, a remote network or actual Tuas port hardware.

## Read the measurements

**Healthy batches** submit all requests at time zero and run until every request
finishes. Total completion time excludes setup and teardown. Average waiting
includes every request's request-to-entry delay. Lower values are better. A
separate watchdog fails a stalled batch; it never reports partial work as a fast
successful completion. New autonomous runs use `benefits-v4` and
`measurement_policy=complete_all_requests`.

**Failure tests** keep requests arriving over a fixed observation window (five
seconds in the saved evaluation, three by CLI default). This lets a stalled system
finish its test. Completed requests must both enter and exit within that window.
Controller-outage entry counts include only new entries after the fault. Time to
resume spans fault injection to the next new entry, including detection,
permission agreement and waiting for demand or clearance. A system that never
resumes has no successful recovery time. Basic token needs no controller-failure
recovery because it is already local.

Compare only identical request sequences, counts, workload hashes and timing
parameters. Pairing also requires the same evaluation version, recovery driver,
execution mode and controller revision. RPC counts are unmeasured in the independent
process runner and are not presented as a performance benefit.

Generate a fresh full matrix (the directory must not already exist):

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim --mode benefits --duration 5 --output run/evaluation/benefits-autonomous-validated --no-plots
```

Old saved comparison sets have been removed after validating their replacement.
Running pytest verifies behavior; it does not populate the dashboard. Teammates
must run the experiment command to generate their own local results.

## Test the integrated containers

`scripts/external_workload.py` connects to Member 6's existing Docker deployment.
It never starts private gate/controller/agent instances. Docker Desktop and the
first-time setup in `docs/PLATFORM_HANDOFF.md` are prerequisites. The runner checks
the five running container identities, uses mutual TLS to the gate, and retains
container IDs/image IDs in its manifest. Run `integrated.py state` and `check` at a
healthy CENTRAL baseline before each workload.

```powershell
.\.venv\Scripts\python.exe scripts/external_workload.py --new-empty-simulation --scenario normal --requests 18 --seed 20
.\.venv\Scripts\python.exe scripts/external_workload.py --new-empty-simulation --scenario controller-outage --requests 18 --seed 20 --duration 30
.\.venv\Scripts\python.exe scripts/external_workload.py --new-empty-simulation --scenario holder-failure --requests 18 --seed 20 --duration 30
```

Run these individually, waiting for CENTRAL convergence, AVAILABLE peers and
UNKNOWN lane occupancy before the next run. A new empty physical model must be
explicitly declared, and no other workload may be active. The runner refuses to
reset an existing occupied/clear/reserved model or outstanding grant. At the end,
fresh synthetic clearance expires naturally to UNKNOWN; it is never reset by a
database operation. A failed run can retain an unresolved synthetic occupant in
its failure evidence; investigate it before declaring a new model.

Healthy traffic runs until the batch finishes or its watchdog fails. Controller
outage kills only the controller one-third through the observation window. The
holder test first stops the controller and waits for agent-originated LOCAL
authority, then kills the current holder and sends demand only from survivors.
Agents perform all token transfers and majority recovery. The host test driver
uses separate actor-only signing clients for crane grant requests, matching the
platform's `external_smoke.py` boundary; it is not a new remote crane-workload API.
Only services stopped by the test are restarted, retaining their volumes.
`restoration_succeeded` records successful start commands; use `state`/`check` to
verify eventual protocol convergence before another run.

Each run saves requests, events, a summary, restoration outcomes and cumulative
container logs under `run/deployed-evaluation`. Fault-to-entry times are measured
from command start and command completion, bracketing Docker command overhead;
they are not exact crash detection or certificate-install durations. Host and
container monotonic clocks are never subtracted. Missing entries are not zero wait.

These runs test deployed **CraneLock only**. They are separate from the dashboard's
three-system local comparisons; do not mix their timings to claim a performance
advantage. Nine Docker runs have now been verified on this Windows machine:
three scenarios and seeds 7/20/3104, with all 162 requests completed and no detected
overlap. See [deployed results](DEPLOYED_RESULTS.md) for medians, ranges and raw
evidence locations. This runtime evidence is separate from the runner's signed-gate
contract tests and from the existing local-comparison dashboard dataset.

## Compare all three systems in Docker

With Docker Desktop running, from the repository root:

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.docker_comparison --build --seeds 7 20 3104 --requests 18 --duration 30
```

This creates 27 experiments: three models, three scenarios and three seeds.
Each experiment starts a fresh gate, controller and three crane-agent containers
on its own bridge network, then removes only those experiment containers and
network. Your integrated deployment and its state volumes remain intact.
Use a fresh `--output` directory to retain each complete matrix.

All models use the same image, 1 CPU/256 MiB limit per service, 0.5-second
heartbeat interval, 2-second detection timeout and identical seeded requests.
Healthy traffic submits all 18 requests together and measures batch completion;
failure tests observe scheduled requests for 30 seconds. Model order is shuffled
per seed. Run these experiments without other test workloads competing for CPU.

- Central lock depends on the team controller for each assignment.
- Basic token starts in LOCAL mode; its automatic replacement recovery is disabled.
- CraneLock enables the team's autonomous voting, fencing and LOCAL handoff.

Controller failure kills that experiment's controller at one-third of the window.
Holder failure kills crane-1 before any crossing and sends demand only from B/C;
CraneLock and basic token start in LOCAL mode for that scenario, while central
lock retains its controller. The runner never manufactures recovery certificates.

CENTRAL assignment uses the existing test RPC adapter around the team's
`CentralController`, running in its own container. Lane movement and demand remain
synthetic. This is a containerised evaluation deployment, separate from the
integrated monitored stack. Gate and controller-heartbeat links use development
mutual TLS; peer and assignment links use plaintext on the private test bridge.
The common host driver serves requests in FIFO order. Timing includes its polling,
signing and observation costs; this is not a concurrent-load stress test or a
measurement of unconstrained production throughput.

Results appear as a separate comparison collection under `run/evaluation` and
are not pooled with older local-process measurements or the nine integrated
CraneLock-only runs. Failure timing uses host kill-command completion to next
entry; cross-container monotonic clocks are not subtracted. No observed resumption
has a missing time, not zero seconds. A baseline correctly stalling can pass its
expected-behavior check; compare completed work to assess availability.

## Other safety tests

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim --mode agents
.\.venv\Scripts\python.exe -m cranelock_sim --mode recovery
.\.venv\Scripts\python.exe -m cranelock_sim --mode partition
.\.venv\Scripts\python.exe -m cranelock_sim --mode restart
.\.venv\Scripts\python.exe -m cranelock_sim --mode logical --seed 20
```

- Agents: real local gRPC token requests and handoffs through A, B and C.
- Recovery: holder failure before entry or during occupancy; majority replacement
  must reject old permissions and wait for confirmed physical clearance.
- Partition: A is disconnected from B/C, or all peers are disconnected. Two peers
  can approve recovery; without a majority recovery must be refused. Disconnection
  alone does not revoke existing authority or prove an occupied lane is empty.
- Restart: A reloads stale state in REJOINING, stays blocked until reconciliation
  against accepted gate authority, then needs a fresh token before entering.
- Logical: seeded traffic through the actual gate with simulated time and FIFO
  scheduling. It does not measure distributed protocol performance.

## Evidence and code layout

Each run keeps its own summary, requests, events, gate decisions and state files
under `run/evaluation`. Separate run directories prevent different seeds, systems
or faults from overwriting each other's evidence. Root CSVs and plots aggregate
these runs. State databases and private keys must not be published.

| Location | Purpose |
| --- | --- |
| `cranelock_sim/__main__.py` | Command entry point for all simulator modes |
| `experiment.py`, `agents.py`, `failures.py` | Workload, live-agent and failure scenarios |
| `autonomous.py`, `benefits.py` | Independent service processes, measured automatic recovery and paired results |
| `comparison.py` | Earlier focused comparison adapter and shared export helpers |
| `lane.py`, `network.py`, `controller.py` | Shared physical lane, network faults and test transport for the team controller |
| `dashboard.py` | Local results server |
| `web/index.html`, `web/style.css`, `web/app.js` | Page structure, appearance and interaction |
| `tests/test_*.py` | Checks grouped by the behavior they test |

The benefits matrix uses three agents and fixed membership. Physical occupancy
is modeled independently of digital authority: changing holder or epoch never
clears the lane. Zero detected safety violations is a bounded observation, not a
proof of safety for every failure or a production performance benchmark.

Legacy focused modes (`recovery`, `partition`, `restart`, `compare`) remain useful
protocol/safety fixtures. They use harness orchestration and are distinct from the
autonomous `benefits-v4` measurements. They are not evidence of deployed recovery.

Independent runs retain raw service logs, per-agent durable votes, the gate audit,
process IDs and endpoints. The browser serves only allowlisted evidence; private
keys and SQLite state must stay local. No replacement certificate is fabricated
by the runner for an output artifact.

## Verify changes

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Optional real-browser checks use `.[ui-test]` and an installed Edge or Chrome;
they skip if the browser is unavailable. Dashboard checks cover filters, safe
artifact access, missing measurements, recorded progress, event ordering, guided
playback, controller stopping and mobile layout.

If Windows locks a previous pytest temporary directory, choose a fresh
`--basetemp` outside the repository. Generated test folders, caches, screenshots
and development runs are disposable; validated experiment evidence is retained.
