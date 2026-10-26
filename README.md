# CraneLock

This repository contains the crane agent and recovery quorum component (`cranelock`)
and the trusted access gate (`cranelock_gate`). Tests exercise recovery votes through
the agent's gRPC service and install the resulting certificate at the gate. Automatic
failure-to-recovery orchestration and deployment integration are still pending.

Install both components and test dependencies with `python -m pip install -e ".[dev]"`
(or `python -m pip install -r requirements.txt`), then run `python -m pytest -q`.
Run `python scripts/generate_proto.py` to regenerate bindings for both protocols.

Both demos default to port 50051. Run them separately or configure distinct ports
when running them together.

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
CLI still requires team integration for automatic recovery.

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

### Two-process demo

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

Restarting with an existing state file intentionally enters REJOINING with
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

- **Moe (Recovery):** implemented by the optional recovery configuration on each
  agent. `TOKEN_HOLDER_SUSPECTED` enables voting; `propose_recovery` gathers a
  strict majority of durable Ed25519 votes into the gate's certificate format.
  The certificate must still be installed at the trusted gate before agents call
  the authoritative `reconcile` hook.
- **Max (Gate):** confirm holder/epoch/request correlation and the future gate
  client boundary. The separate gate service provides lane-entry and occupancy/fencing APIs; agent integration remains pending.
- **Darius (Evaluation):** confirm JSON event fields, operation IDs, clock alignment
  and detection-latency definition. Current heartbeat fanout is O(N²) per round.
- **Kirby (Controller):** confirm CENTRAL command semantics and who authorizes
  LOCAL transitions and rejoining. Automatic controller-failure/return logic is deferred.
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

Recommended next milestone: integrate the returned recovery certificate with the
controller/gate client and reconcile every rejoining agent from the accepted gate
state. Never reconcile from a heartbeat or an uninstalled proposal.

## CraneLock: Member 2 recovery and quorum

Recovery creates a proposal for epoch `e+1` using the gate's current epoch and
authority revision. Members sign the same canonical payload using Ed25519 from the
`cryptography` library. A certificate requires `floor(N/2)+1` distinct member votes;
a minority partition cannot form one.

Each voter validates the proposer signature, checks its current epoch and holder
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

Use a fresh temporary directory and disable the pytest cache if Windows denies access
to its default temporary or cache directories:

```powershell
$testTemp = Join-Path (Get-Location) ("pytest-temp-" + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_recovery.py -v -p no:cacheprovider --basetemp="$testTemp"
```

Expected result: **3 passed**. These tests exercise a two-of-three majority over gRPC,
install its certificate at the gate, reject an old holder with `STALE_EPOCH`, deny a
one-of-three minority a certificate, and reject a conflicting vote after restart.

Run the complete suite with a new temporary directory:

```powershell
$testTemp = Join-Path (Get-Location) ("pytest-temp-" + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest -v -p no:cacheprovider --basetemp="$testTemp"
```

The current suite has **68 tests**. Directories matching `pytest-temp-*/` are ignored
by Git. The TLS rejection test may print a handshake error while still passing.

### Integration API

Construct each recovery-enabled `Agent` with `recovery_config` (the gate's `Config`),
its own `recovery_private_key`, and a durable `state_file`. The configured membership
must match the agent and its peers exactly; each process should hold only its own
private key. Agents without these options retain normal-operation behavior.

After holder suspicion, call `await agent.propose_recovery(gate_state, holder)`.
It returns a gate-compatible certificate or `None` when the attempt cannot gather
a majority or its local vote is rejected. It does not install authority or change
token ownership. The integration caller submits the certificate through
`InstallAuthority`, using its signed `expected_epoch` and `expected_revision`, then
reconciles agents from the gate's accepted state. A certificate alone never authorizes
entry. The existing agent CLI does not automatically start this workflow.

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
