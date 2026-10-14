# CraneLock: Member 3 trusted access gate

Standalone Python service for a **single simulated lane**, with signed gRPC requests,
strict-majority recovery certificates, fencing, durable SQLite state and fail-closed
occupancy handling. The crane algorithm, quorum producer, controller and yard simulator
are represented by development clients; their real implementations are not included.

## Run on Windows / PowerShell

From this directory:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m cranelock_gate.demo setup --directory demo-data
.\.venv\Scripts\python.exe -m cranelock_gate.demo run --directory demo-data
```

The workspace already has a virtual environment and `demo-data` generated during
validation. Skip `setup` when that directory exists. It refuses to overwrite identities
or a database. The demo can be repeated with the same directory: each run starts a new
authority epoch and demonstrates unknown-lane blocking, valid admission, stale-holder
rejection, recovery during occupancy and explicit clearance.

On Linux/macOS, replace `.\.venv\Scripts\python.exe` with `.venv/bin/python`.

## Run as a persistent service

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

## Team handoff

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

## Safety boundary

Run exactly **one active gate with one persistent database**. A quorum certificate
changes digital authority; it never proves that the physical lane is empty. Only the
trusted simulator may accept entry, and only fresh simulator evidence or a trusted
operator may establish clearance. Every physical entry must pass this gate. Clients
must stop on transport failure, denial, or replay acknowledgements.

This is a coursework simulator, not an industrial crane safety controller. End-to-end
CraneLock safety and performance remain unverified until the real member components
are integrated. The Docker image and restart behaviour were validated locally;
Kubernetes execution remains unverified. See [validation results](docs/VALIDATION.md).
