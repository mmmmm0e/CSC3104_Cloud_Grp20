# CraneLock

CraneLock coordinates access to one shared lane using a controller, crane agents,
token passing and a trusted gate. Signed majority recovery replaces authority when
failures occur. The simulator models physical occupancy separately from permission
to enter, so changing the token holder never implies that the lane is clear.

## Run the automatic application

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.live --automatic --port 8767
```

Open **http://127.0.0.1:8767/live**. Request a turn and enter the lane, then stop
the controller. Agents detect the outage, collect durable signed votes and install
LOCAL authority automatically. Stopping the local holder triggers replacement
recovery. Restore the controller service to trigger automatic CENTRAL return.
Physical entry/exit and explicit operator clearance remain separate controls.
There is no need to click **Propose recovery** in automatic mode.

For a bounded demonstration with saved checks and logs:

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.automatic_demo
```

For tests, all progress scenarios, six delayed automatic cycles, 54 reference
comparison runs and 27 logical scaling runs in one evidence directory:

```powershell
.\.venv\Scripts\python.exe scripts/validate_project.py
```

Use `--skip-evaluation` for a quicker functional check. Each invocation uses a new
directory under `run/`; logs and `validation.json` retain actual exit statuses.
See [automatic service setup and limitations](docs/AUTOMATION.md) for independent
agent/controller processes. The original manual UI and evaluation remain available.
See [current validation results and remaining platform work](docs/FINAL_STATUS.md).

## Logical flow

1. **Start in CENTRAL mode.** The controller schedules a crane. The gate stores the
   accepted epoch, holder and revision; agents reconcile against that authority.
2. **Request a turn.** Requests wait while the lane is unavailable. Only the accepted
   holder can obtain a grant, and only when the gate has fresh clearance evidence.
   A grant reserves the lane; it does not move the crane.
3. **Validate entry.** At the lane boundary, the simulator submits the grant to the
   gate. The gate checks the holder, epoch, revision and grant validity. Only an
   accepted entry changes physical occupancy to that crane.
4. **Exit and confirm clearance.** Physical exit and fresh position evidence allow
   the gate to mark the lane CLEAR. The next waiting crane can then proceed.
5. **Suspect controller failure.** Agents detect missing controller responses. A
   recovery proposer reads the current gate state and proposes LOCAL authority in
   epoch `e + 1`, retaining the current holder when it is healthy and reachable.
6. **Collect a strict majority.** Each voter saves its vote before replying and
   signs the exact proposal using library Ed25519. Conflicting votes for the same
   target epoch are rejected. A certificate requires `floor(N / 2) + 1` distinct
   configured members: two votes for three cranes.
7. **Install and reconcile.** The caller submits the certificate to the gate. The
   gate verifies the signatures and expected epoch/revision before installing LOCAL
   authority. Old grants are revoked. Member-signed `ReconcileAuthority` RPCs ask
   reachable agents to adopt the state after checking it against the live gate.
   OCCUPIED or UNKNOWN lane state remains blocked throughout recovery.
8. **Continue locally.** Agents queue requests and pass the token without controller
   scheduling. Gate-aware handoff records the new holder and revision before token
   notification. If the holder also fails, another majority certificate is needed
   for a replacement in the next epoch.
9. **Return to CENTRAL mode.** The controller reads the accepted authority and
   synchronizes waiting work. It installs CENTRAL authority with the current epoch
   and a fresh revision, then sends controller-signed reconciliation RPCs. The local
   lifecycle preserves the holder, queue and physical occupancy during this return;
   old permissions fail the gate's revision check.

The lifecycle runner orchestrates these steps through the actual local services.
The live UI exposes operator controls for failures, recovery and controller return.
The default agent CLI remains manual. Opt-in `--auto-recover` connects suspicion,
certificate collection, gate installation and reconciliation; the controller's
`--serve` mode handles automatic return. The live `--automatic` mode enables both.
Holder replacement, partitions and restart are also exercised in separate scenarios.

### Network failures and safety

| Failure | Program behavior |
| --- | --- |
| Controller connection fails | Agents can approve LOCAL recovery with a strict majority and a reachable gate. |
| Peers split into groups | Only a strict-majority group can form replacement authority; it must also reach the gate. |
| No group has a majority | No replacement certificate is formed and authority is unchanged. |
| Gate is unreachable | No new entry is accepted, including attempts using a cached grant. |
| Position evidence stops | Once evidence expires, the gate treats clearance as UNKNOWN and blocks new entry. |
| Holder stops inside the lane | Recovery may replace authority, but physical occupancy continues to block admission. |
| Communication recovers | Agents must reconcile with accepted gate state before using authority. |

Two strict majorities overlap in at least one member. Because that member durably
refuses conflicting votes for the same epoch, conflicting recovery certificates
cannot both form under the configured crash-failure model. The gate accepts only
the next epoch against its current revision and fences older epochs/revisions.
A minority cannot create replacement authority. An isolated holder's existing
permission remains subject to gate validation; after replacement is installed its
old permission is rejected. These guarantees assume the configured membership,
uncompromised signing keys, durable votes and one authoritative gate.

## Program structure

| File or directory | Responsibility |
| --- | --- |
| `cranelock/agent.py` | Peer heartbeats, suspicion, request queues, token handoff and agent RPC handlers. |
| `cranelock/recovery.py` | Recovery proposals, durable voting, signature verification and majority certificates. |
| `cranelock/automation.py` | Stable-view recovery coordinator and automatic certificate installation. |
| `cranelock/controller_service.py` | Controller RPC service, heartbeat probe and automatic return worker. |
| `cranelock/reconciliation.py` | Signed network requests to adopt authority checked against the gate. |
| `cranelock/controller.py` | Central scheduling authority, agent monitoring and controller reconciliation. |
| `cranelock/gate_client.py` | Agent adapter for signed gate operations. |
| `cranelock_gate/` | Gate RPC service, permission checks, cryptography and persistent SQLite state. |
| `proto/` | Protobuf message definitions and service contracts for gRPC communication. |
| `cranelock_sim/lane.py` | Independent physical lane model and entry/exit validation. |
| `cranelock_sim/agents.py`, `network.py` | Local gRPC agent cluster and simulated peer partitions. |
| `cranelock_sim/lifecycle.py` | Integrated CENTRAL → LOCAL → CENTRAL demonstration. |
| `cranelock_sim/live.py`, `web/live.*` | Interactive crane yard, failure controls and live evidence. |
| `cranelock_sim/progress_demo.py` | Combined scenario runner that saves presentation evidence. |
| `cranelock_sim/experiment.py`, `comparison.py`, `benefits.py` | Traffic experiments, reference comparisons and exported metrics. |
| `cranelock_sim/dashboard.py`, `web/` | Saved evaluation charts, playback and evidence downloads. |
| `tests/` | Component, RPC, safety and integration tests. |
| `deploy/` | Container and deployment configuration; runtime validation is separate from local tests. |

### Choose a program

Run these from the repository root after installing dependencies using the setup
instructions below. Each command is an alternative entry point.

| Purpose | PowerShell command |
| --- | --- |
| Interactive crane simulation | `.\.venv\Scripts\python.exe -m cranelock_sim.live` |
| Complete controller lifecycle | `.\.venv\Scripts\python.exe -m cranelock_sim.lifecycle` |
| Combined progress scenarios, including controller return | `.\.venv\Scripts\python.exe -m cranelock_sim.progress_demo --with-controller` |
| Holder failure scenarios | `.\.venv\Scripts\python.exe -m cranelock_sim --mode recovery` |
| Majority/minority partition scenarios | `.\.venv\Scripts\python.exe -m cranelock_sim --mode partition` |
| Restart and reconciliation scenarios | `.\.venv\Scripts\python.exe -m cranelock_sim --mode restart` |
| Benefits evaluation without optional plotting dependencies | `.\.venv\Scripts\python.exe -m cranelock_sim --mode benefits --duration 5 --no-plots` |
| Read-only saved evaluation dashboard | `.\.venv\Scripts\python.exe -m cranelock_sim.dashboard` |
| Test suite | `.\.venv\Scripts\python.exe -m pytest -q` |
| Regenerate protobuf bindings | `.\.venv\Scripts\python.exe scripts/generate_proto.py` |

The live UI opens at **http://127.0.0.1:8765/live**; the saved evaluation dashboard
opens at **http://127.0.0.1:8765/**. To run both servers concurrently, give one a
different port, for example `cranelock_sim.live --port 8767`. Live runs and saved
scenario evidence are separate from the original evaluation's comparison metrics.

## Interactive crane yard

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.live
```

Open **http://127.0.0.1:8765/live**. Stop an existing dashboard on that port first,
or use `--port 8767`. The interactive page starts three actual local gRPC agents,
the controller, trusted gate and physical lane model. It animates a crane only after
the gate accepts entry. The original saved evaluation remains available at `/`.

Use a crane's **Request turn**, then **Enter lane**. Use **Exit lane** to release
physical occupancy. Requests from other cranes wait. The failure lab can stop/restart
cranes, stop/restore the controller, partition/heal peer links, pause position evidence,
stop/restore the gate and propose signed majority recovery from a selected crane.
**Try earlier permission** sends the recorded old grant to the actual gate.
**Operator clear lane** explicitly removes the physical simulated occupant; it is not
automatic recovery or a statement about a real yard.

For a short demonstration, enter Crane 1, request Crane 2, stop the controller and
wait for the suspicion event. Propose recovery from Crane 2. Observe the new epoch
while Crane 1 remains inside, then try the earlier permission and see its rejection.
Exit Crane 1; Crane 2 can obtain a reservation and enter. With all peers isolated,
a recovery attempt cannot form the required majority. With position updates paused,
the gate becomes UNKNOWN after its observation timeout, even if the UI knows where
the simulated crane is. A stopped crane never disappears from the physical lane.

Live events, gate audit, state, request CSV and a closing summary are retained in a
fresh `run/evaluation/live-...` directory. This is an operator-driven local simulator,
not a performance benchmark or a multi-container deployment. The live action API is
enabled only by this command, bound to loopback and protected by a same-origin session
token. The ordinary `cranelock_sim.dashboard` server remains read-only.

This repository contains the crane agent and recovery quorum component (`cranelock`)
and the trusted access gate (`cranelock_gate`), plus Member 4's simulator and evaluation
tools (`cranelock_sim`). Local demo runners connect agent gRPC, recovery certificates,
gate validation and the simulator's physical lane. Gate-authorized token handoff and
gate-based restart reconciliation are available in opt-in gate mode.
See [agent/gate integration and exact local demo](docs/AGENT_GATE.md) for setup,
authority rules, failure behavior, validation and team integration decisions.
The separate protobuf services and original standalone baseline are preserved.

## Full local controller integration

Controller-to-agent state synchronization now uses the signed `ReconcileAuthority`
gRPC method. The controller supplies endpoint addresses, not Python agent objects.
Each agent verifies the caller's Ed25519 signature and checks the exact epoch,
revision, holder and mode against its live gate before adopting them. A recovery
member can request LOCAL reconciliation; CENTRAL reconciliation requires the controller.
Unreachable or mismatched authority is never reported as a completed handover.

The lifecycle runner uses member-signed RPCs after LOCAL recovery and controller-signed
RPCs on return. The old in-process helper remains only for compatible tests/adapters.
To reconcile separately running agents from already accepted gate authority:

```powershell
.\.venv\Scripts\python.exe -m cranelock.controller --reconcile --agent crane-1=127.0.0.1:50051 --agent crane-2=127.0.0.1:50052 --gate-address 127.0.0.1:50060 --gate-config demo-data/config.json --gate-keys demo-data/private-keys.json
```

Use the actual addresses and matching identities for your experiment. This command
does not install authority or certify physical clearance. The CLI example is for an
isolated loopback setup. The Python RPC client accepts TLS credentials, and the agent
server accepts server credentials; signatures authenticate requests, not plaintext
transport responses. The separate-process test verifies real network reconciliation.

Network-failure coverage includes controller-link-only loss with the controller still
running, majority/minority peer partitions, gate unavailability, missing occupancy
observations and cached-grant rejection when services are unreachable. These are
bounded local RPC/service-fault tests, not Kubernetes or physical-network validation.

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.lifecycle
```

This uses Kendrick's gate-aware agents, Moe's quorum certificates, Max's gate,
Darius's physical lane and Kirby's `CentralController`. It verifies CENTRAL operation,
actual controller-service loss, majority-approved LOCAL fallback, gate-authorized token
handoff and CENTRAL return. Return preserves the current epoch, holder, waiting queue
and physical occupancy; old grants are fenced by revision. Waiting work resumes only
after the simulator confirms exit. The caller paces the handoffs and return; the
default agent CLI does not supply the entire orchestration policy. The opt-in
automatic workers and their independent-process commands are described above.

To include this cycle with all existing progress scenarios:

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.progress_demo --with-controller
.\.venv\Scripts\python.exe -m cranelock_sim.dashboard
```

The extended suite runs nine scenarios and saves fresh evidence under `run/evaluation`.
In the unchanged dashboard layout, select the saved controller-cycle experiment under
**Other saved experiments** for playback. Controller restoration and CENTRAL return
are reflected in playback. Existing baseline calculations and reference-controller
comparison runs are preserved; those graphs are not measurements of this new cycle.
Docker Compose's `demo` tool includes this cycle by default. Container execution and
Kubernetes/monitoring validation remain separate platform checks.

Install both components and test dependencies with `python -m pip install -e ".[dev]"`
(or `python -m pip install -r requirements.txt`), then run `python -m pytest -q`.
Run `python scripts/generate_proto.py` to regenerate bindings for both protocols.

Member 4's first evaluation milestone is available with
`python -m cranelock_sim --cranes 3 --duration 60 --seed 20`.
It runs seeded normal traffic through the signed gate core and exports physical
traces, request CSV and summary metrics. See [evaluation instructions and scope](docs/EVALUATION.md).
Its logical FIFO scheduler does not yet measure the distributed CraneLock protocol.
Run `python -m cranelock_sim --mode agents` for the three-agent normal-handoff
integration: real loopback gRPC token requests/transfers, gate entry checks and
physical lane movements, with two rounds of A/B/C crossings.
Run `python -m cranelock_sim --mode recovery` for holder failure before entry and
during occupancy. It prints live `[PASS]` checks and measures actual heartbeat
detection, quorum recovery, old-permission fencing and safe replacement entry.
Run `python -m cranelock_sim --mode partition` to keep all agents alive while
blocking peer RPCs across a majority/minority split or three isolated agents.
The live checks verify majority recovery, stale-holder fencing, occupancy blocking
and refusal to install replacement authority without a quorum.
Run `python -m cranelock_sim --mode restart` to restart the failed holder from
its old state after recovery, verify REJOINING blocks participation, then reconcile
from accepted gate authority and complete a new token-authorized crossing.
Run `python -m cranelock_sim --mode compare` for paired normal/holder-failure
comparisons of a central reference scheduler, basic-token agents and CraneLock.
Install `.[evaluation]` for PNG/SVG graphs. See [baseline comparison scope and metrics](docs/EVALUATION.md#run-the-comparisons).
View saved experiments with `python -m cranelock_sim.dashboard`, then open
`http://127.0.0.1:8765`. The local dashboard includes model charts, run filters,
recorded lane playback and evidence downloads. See [dashboard instructions](docs/EVALUATION.md#open-the-dashboard).

Both demos default to port 50051. Run them separately or configure distinct ports
when running them together.

## Members 1 to 4 combined progress demo

For the container-packaged version using the platform's Docker approach, see
[Docker progress demo](docs/DEPLOYMENT.md#container-packaged-progress-demo-members-1-to-4-and-platform).
It adds a demo/evaluation image, persistent results volume and dashboard container.
The existing standalone agent/gate deployments remain available separately.

Run the complete local presentation sequence with one command:

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.progress_demo
```

This reuses Member 4's actual simulator, physical lane safety checks and scenario
runners with Members 1 to 3's agents, quorum recovery and trusted gate. It runs eight
independent scenarios, each with fresh state:

| Stage | What to show |
| --- | --- |
| Normal operation | Six physical crossings over two token-handoff rounds |
| Holder failure, before entry and during occupancy | Heartbeat detection, signed majority recovery, stale-permission rejection and clearance blocking |
| Majority partition, before entry and during occupancy | Agents remain alive while cross-partition peer RPCs are blocked; only the majority recovers |
| No majority | All peers are isolated; the gate refuses replacement authority and no physical entry occurs |
| Restart, before entry and during occupancy | The old holder reloads stale state, stays REJOINING, reconciles and crosses only after a fresh token handoff |

Every scenario compares recorded physical entries with accepted gate entry decisions.
No-quorum success means safely refusing recovery, not restoring progress or clearing
the lane. Logs use UTC timestamps, while scenario timing uses host monotonic seconds.

The command prints a new evidence directory under `run/evaluation/progress-...`.
It contains `demo.log`, a combined `summary.json`, `scenarios.csv`, and separate
scenario directories with physical/agent events, gate audit logs and request CSVs.
A failed scenario exits with an error and retains a failed root summary and logs.
Existing output directories are refused.

To choose a new evidence directory and view it in Member 4's dashboard:

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.progress_demo --output run/evaluation/group-progress
.\.venv\Scripts\python.exe -m cranelock_sim.dashboard --root run/evaluation/group-progress
```

Open `http://127.0.0.1:8765`. The dashboard reads saved evidence; it does not run live
crane controls. Select the saved normal, recovery, partition or restart experiments
under **Test settings and saved experiments** to inspect the recorded state and events.
The demo does not generate baseline-comparison charts; those use the separate
evaluation commands documented in [docs/EVALUATION.md](docs/EVALUATION.md).

Presentation scope: a connected Members 1 to 4 prototype on one host using real gRPC,
an independent simulated physical lane and runner-controlled failures/reconciliation.
It does not claim Kubernetes deployment or Member 5's controller integration.

## Members 1 to 3 progress demo

Run the connected local demo from the repository root:

```powershell
.\.venv\Scripts\python.exe -m cranelock.progress_demo
```

The command starts three real gRPC agents and a real gRPC gate on temporary local
ports, then checks token queueing and handoff, accepted entry, heartbeat failure
detection, minority rejection, majority recovery, agent reconciliation, old-holder
fencing, and occupancy blocking until explicit simulated clearance. Every step prints
`PASS`; a failed check exits with an error. No external services are needed.

Each invocation creates a new `run/progress-<timestamp>-<id>/` directory and prints
its absolute path. Evidence includes:

| File | Contents |
| --- | --- |
| `demo.log` | Timestamped, readable demo steps and results |
| `agents.jsonl` | Actual agent heartbeat, token, suspicion and recovery events |
| `gate.jsonl` | Actual durable gate audit events, including rejected requests |
| `summary.json` | Overall result, individual checks and demo scope |

To select a new output directory:

```powershell
.\.venv\Scripts\python.exe -m cranelock.progress_demo --output run/presentation-demo
Get-Content run/presentation-demo/demo.log
Get-Content run/presentation-demo/summary.json
```

The selected directory must not already exist, so earlier evidence is preserved.
`run/` is ignored by Git and includes development identities and databases; share
only the evidence files above. Timestamps inside logs use UTC.

For the presentation, explain that this runner supplies the orchestration and uses
signed development controller, simulator and operator commands. It stops agents to
model unavailable members, restarts one from its persistent state, and explicitly
reconciles it from the gate. It also sends an old signed request to model a delayed
holder command. These are local component integration checks, not Kubernetes failure
injection or a complete controller/yard-simulator implementation. The normal agent
CLI stays manual unless the automatic recovery options above are enabled.

## CraneLock — normal crane-agent milestone

The crane agent provides normal-operation token coordination and its shared contract.

Python 3.12+ is intended; validation here used CPython 3.14 on Windows. No existing
formatter, linter, or type checker was configured. Dependencies use pip and a venv.

### Setup (PowerShell, repository root)

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe scripts/generate_proto.py
.\.venv\Scripts\python.exe -m pytest -q
```

On this workstation the default MSYS Python has no pip. To create the environment:

```powershell
& "$env:LOCALAPPDATA\Python\pythoncore-3.14-64\python.exe" -m venv .venv
```

Generated modules are checked in; regenerate them after changing the proto. Do not
edit generated code. The schema uses package `cranelock.v1` and stable numbered
fields; add future recovery/gate/controller services without replacing this API.

### Standalone baseline two-process demo (without the gate)

Use a **fresh demo directory** for each new simulated cluster. All agents must agree
on initial epoch, holder and complete membership. Only one configured initial
holder is allowed. These commands use epoch 1 and LOCAL mode; CENTRAL is also
represented, but controller decisions and automatic mode transitions are deferred.

Terminal 1:

```powershell
.\.venv\Scripts\python.exe -m cranelock run --id crane-a --listen 127.0.0.1:50051 --peer crane-b=127.0.0.1:50052 --holder crane-a --state-file run/demo1/a.json
```

Terminal 2 (B requests after 15 seconds):

```powershell
.\.venv\Scripts\python.exe -m cranelock run --id crane-b --listen 127.0.0.1:50052 --peer crane-a=127.0.0.1:50051 --holder crane-a --state-file run/demo1/b.json --request-after 15
```

Terminal 3, before and after the 15-second delay:

```powershell
.\.venv\Scripts\python.exe -m cranelock state 127.0.0.1:50051
.\.venv\Scripts\python.exe -m cranelock state 127.0.0.1:50052
```

Initially A reports `has_token: true`. B sends TokenRequest, A queues B, and the
automatic transfer worker hands off the token. Afterwards A reports false and B
true. Logs contain request/transfer IDs and holder-change events.

Press **Ctrl+C in Terminal 2** to stop B. After roughly 2–2.5 seconds since its last
received heartbeat, A emits `PEER_SUSPECTED` and `TOKEN_HOLDER_SUSPECTED`. State shows
`SUSPECTED_UNAVAILABLE`; A never regenerates the token. Change timing with
`--interval 0.5 --timeout 2`. Repeat `--peer ID=HOST:PORT` for arbitrary membership.

In standalone mode, restarting with an existing state file intentionally enters REJOINING with
`has_token=false`. There is no CLI bypass. `Agent.reconcile()` is a trusted Python
integration hook whose caller must supply authoritative epoch/holder information.
Do not delete state files to rejoin an existing cluster. For a separate fresh demo,
stop both agents and use a new path such as `run/demo2/a.json` and `run/demo2/b.json`.

### Files and behavior

| File | Purpose |
| --- | --- |
| `cranelock/agent.py` | State model, async lock, FIFO, heartbeats, suspicion, transfer and persistence |
| `cranelock/__main__.py` | Agent process, delayed token request and state query CLI |
| `cranelock/__init__.py` | Package marker |
| `proto/cranelock/protocol.proto` | Shared Heartbeat, TokenRequest, TokenTransfer, GetAgentState contract |
| `cranelock/protocol_pb2.py` | Generated protobuf messages |
| `cranelock/protocol_pb2_grpc.py` | Generated gRPC bindings |
| `scripts/generate_proto.py` | Reproducible protobuf generation |
| `tests/test_agent.py` | Ownership, FIFO, concurrent requests, timing, epochs, restart and retry tests |
| `tests/test_process_demo.py` | Two real CLI processes, handoff, peer termination and suspicion |
| `requirements.txt` | Pinned direct runtime/build/test dependencies |
| `.gitignore` | Exclude local environment, caches and demo state |
| `README.md` | Modified setup, architecture, demo and integration notes |

One asyncio lock protects mutable coordination state. RPC network waits happen
outside it. The queue deduplicates by requesting agent and moves with the token.
The sender persists relinquishment before sending; retry uses the same transfer
ID. The receiver handles duplicate deliveries. An uncertain transfer can leave
zero usable owners; safety takes priority over availability until recovery.

Epochs must match for requests/transfers. Higher heartbeat epochs do not
automatically grant authority; older heartbeats are rejected. Heartbeats from the
known holder can announce an outgoing handoff, but never set local `has_token`.
Only receipt of a valid TokenTransfer grants local ownership.

Persistence uses flushed JSON plus atomic file replacement for epoch/holder/token
and reserved opaque vote storage. Restart always blocks ownership. Queue, transfer
retry and deduplication history are in-memory for this milestone. Recovery votes are
durable and exposed through `VoteRecovery`; a member will reproduce an identical vote
but will not sign a conflicting proposal for the same target epoch. Use one process
and one state file per agent.

Events are JSON lines with Unix and monotonic timestamps, agent/peer IDs, epoch,
and operation IDs where relevant. `Agent(event_sink=...)` is the recovery/evaluation
hook; callbacks run synchronously and must be fast and nonthrowing. Failure events
include last-seen time and observed detection latency. Monotonic clocks measure
durations on one host; do not subtract monotonic timestamps from different hosts.

### Limits and team integration decisions

- **Moe (Recovery):** recovery proposals, persistent votes and Ed25519 majority
  certificates are implemented. LOCAL mode requires holder suspicion; CENTRAL
  fallback requires controller suspicion. Gate-enabled agents reconcile accepted
  recovery authority through their signed gate client.
- **Max (Gate):** normal handoff and restart integration are documented in
  [the gate integration guide](docs/AGENT_GATE.md). Grant/entry integration remains deferred.
- **Darius (Evaluation):** confirm JSON event fields, operation IDs, clock alignment
  and detection-latency definition. Current heartbeat fanout is O(N²) per round.
- **Kirby (Controller):** `CentralController` is connected in the local lifecycle
  scenario, including outage, LOCAL fallback, queue reconciliation and CENTRAL return.
  Distributed deployment and scheduler policy remain integration work.
- **Arif (Platform):** confirm ports, peer configuration, persistent storage path,
  shutdown behavior and eventual authentication. This is plaintext trusted-peer
  local demo gRPC; IDs are validated against configured membership, not authenticated.

Normal holder announcements can be missed across multiple rapid handoffs or
partitions. A stale holder view then safely rejects transfers; robust discovery
and reconciliation need a later agreed protocol. Persistence is conservative,
not a durable distributed transfer transaction. No claim is made that this alone
solves partitions, crash recovery or gate safety. Initial bootstrap configuration
must be consistent; it is not a consensus algorithm.

Tests cover the eleven requested behaviors across scenario tests, plus uncertain
transfer retry, duplicate reception and restart blocking.

The next milestone after the standalone baseline is now implemented in opt-in gate
mode; see [the integration guide](docs/AGENT_GATE.md) for its remaining boundaries.
Recovery and gate authority remain separate components, connected through their
existing signed contracts.

## CraneLock: Member 2 recovery and quorum

Recovery creates a proposal for epoch `e+1` using the gate's current epoch and
authority revision. Members sign the same canonical payload using Ed25519 from the
`cryptography` library. A certificate requires `floor(N/2)+1` distinct member votes;
a minority partition cannot form one.

Each voter validates the proposer signature, checks its current epoch and failure
suspicion, and persists its vote before returning a signature. A member cannot sign
a conflicting proposal for the same target epoch, including after restart. The gate
independently verifies the certificate and fences the previous holder when it installs
the new authority. Recovery does not establish physical lane clearance.

| File | Purpose |
| --- | --- |
| `cranelock/recovery.py` | Proposal validation, Ed25519 signatures, vote policy and certificate assembly |
| `cranelock/agent.py` | `VoteRecovery` RPC and `propose_recovery` vote collection |
| `proto/cranelock/protocol.proto` | Recovery proposal and vote messages |
| `tests/test_recovery.py` | Majority/minority partition, old-holder fencing and restart vote tests |
| [docs/RECOVERY_SAFETY.md](docs/RECOVERY_SAFETY.md) | Safety argument and assumptions |

### Test recovery (PowerShell, repository root)

```powershell
$testTemp = Join-Path ([IO.Path]::GetTempPath()) ("cranelock-tests-" + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_recovery.py -v -p no:cacheprovider --basetemp="$testTemp"
```

Expected result: **4 passed**. Majority recovery is tested both with standalone and
gate-aware agents; the other tests verify minority rejection and persistent votes.

Run the complete suite with a fresh temporary directory:

```powershell
$testTemp = Join-Path ([IO.Path]::GetTempPath()) ("cranelock-tests-" + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest -v -p no:cacheprovider --basetemp="$testTemp"
```

Temporary test files stay in the Windows temporary directory. The TLS rejection
test may print a handshake error while still passing.

### Integration API

Construct recovery-enabled agents with `recovery_config`, their own
`recovery_private_key` and a durable `state_file`. Membership must match the configured
peers. Call `await agent.propose_recovery(gate_state, holder)` after the appropriate
failure suspicion. It returns a gate-compatible certificate or `None` if the attempt
cannot gather a majority or its local vote is rejected. Submit the certificate through
`InstallAuthority` with its signed expected epoch/revision. Gate-aware agents then use
`reconcile_from_gate()`; standalone adapters may use the trusted `reconcile()` hook.
The default CLI does not orchestrate the complete recovery workflow. With
`--auto-recover`, the recovery worker installs and reconciles certificates after
failure suspicion. Without that option, a certificate
alone never authorizes entry.

## CraneLock: Member 3 trusted access gate

Standalone Python service for a **single simulated lane**, with signed gRPC requests,
strict-majority recovery certificates, fencing, durable SQLite state and fail-closed
occupancy handling. The agent package now produces quorum certificates; controller and
yard-simulator integration are still represented by development clients.

### Run on Windows / PowerShell

From this directory:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m cranelock_gate.demo setup --directory demo-data
.\.venv\Scripts\python.exe -m cranelock_gate.demo run --directory demo-data
```

Skip `setup` when `demo-data` already exists. It refuses to overwrite identities
or a database. The demo can be repeated with the same directory: each run starts a new
authority epoch and demonstrates unknown-lane blocking, valid admission, stale-holder
rejection, recovery during occupancy and explicit clearance.

On Linux/macOS, replace `.\.venv\Scripts\python.exe` with `.venv/bin/python`.

### Run as a persistent service

```powershell
.\.venv\Scripts\python.exe -m cranelock_gate.server serve --config demo-data/config.json --database demo-data/gate.sqlite
```

In another terminal:

```powershell
.\.venv\Scripts\python.exe -m cranelock_gate.demo run --directory demo-data --target 127.0.0.1:50051
```

The gRPC port is `50051`; Prometheus metrics are at `http://127.0.0.1:9090/metrics`.
The server prints a listening message. Press Ctrl+C to stop it; Windows Ctrl+Break is
also supported. Shutdown prints `Stopping gate...` and `Gate stopped.`.
The development demo uses plaintext loopback. Network listeners require mutual TLS
unless explicitly enabled for an isolated simulation.

### Team handoff

For the evaluation role, [benefits and costs](docs/EVALUATION.md#read-the-measurements) compares actual
reference-controller outage, lost-token recovery and healthy-operation overhead.
Run `python -m cranelock_sim --mode benefits --output run/evaluation/benefits-completion-validated`,
then `python -m cranelock_sim.dashboard`. The dashboard explains paired gains and
costs; the teammate controller remains an outstanding integration boundary.

- [Contract and signing format](docs/CONTRACT.md): Member 1's proposed shared proto,
  Member 2's certificate format, and Member 4/5 integration rules.
- [Deployment and failure testing](docs/DEPLOYMENT.md): Docker, Kubernetes, persistence,
  health checks, TLS and Member 6 handoff.
- [Report draft](docs/GATE_REPORT.md): gate Design and Implementation sections,
  with tested outcomes separated from outstanding integration claims.
- [Protobuf service](proto/cranelock_gate/v1/gate.proto): seven RPC methods.

Bindings are checked in. After changing the proto, regenerate them:

```powershell
.\.venv\Scripts\python.exe scripts/generate_proto.py
```

### Safety boundary

Run exactly **one active gate with one persistent database**. A quorum certificate
changes digital authority; it never proves that the physical lane is empty. Only the
trusted simulator may accept entry, and only fresh simulator evidence or a trusted
operator may establish clearance. Every physical entry must pass this gate. Clients
must stop on transport failure, denial, or replay acknowledgements.

This is a coursework simulator, not an industrial crane safety controller. End-to-end
CraneLock safety and performance remain unverified until the real member components
are integrated. The Docker image and restart behaviour were validated locally;
Kubernetes execution remains unverified. See [validation results](docs/VALIDATION.md).
