# CraneLock — normal crane-agent milestone

The repository originally contained only this README heading. There was no agent,
schema, dependency/build configuration, test suite, logging, or teammate code to
reuse. This milestone adds only the normal-operation agent and its shared contract.

Python 3.12+ is intended; validation here used CPython 3.14 on Windows. No existing
formatter, linter, or type checker was configured. Dependencies use pip and a venv.

## Setup (PowerShell, repository root)

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

## Two-process demo

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

## Files and behavior

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
retry and deduplication history are in-memory for this milestone. There is no vote
algorithm or public voting API. Use one process and one state file per agent.

Events are JSON lines with Unix and monotonic timestamps, agent/peer IDs, epoch,
and operation IDs where relevant. `Agent(event_sink=...)` is the recovery/evaluation
hook; callbacks run synchronously and must be fast and nonthrowing. Failure events
include last-seen time and observed detection latency. Monotonic clocks measure
durations on one host; do not subtract monotonic timestamps from different hosts.

## Limits and team integration decisions

- **Moe (Recovery):** confirm authoritative reconciliation API, persistent vote
  format, higher-epoch handling, and consumption of TOKEN_HOLDER_SUSPECTED. No
  replacement token, certificate, majority vote or signing is implemented.
- **Max (Gate):** confirm holder/epoch/request correlation and the future gate
  client boundary. No lane-entry API or occupancy/fencing decision is implemented.
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
transfer retry, duplicate reception and restart blocking. No pre-existing tests
were present. No Git commit/push/history changes are part of this milestone.

Recommended next milestone: agree with Recovery and Controller on authoritative
epoch/rejoin integration, then test restart and interrupted-handoff scenarios
against that contract. Do not add quorum or gate algorithms inside this agent.
