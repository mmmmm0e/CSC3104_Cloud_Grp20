# Automatic recovery and controller return

The optional automatic application connects the existing agents, durable voters,
Ed25519 certificates, gate and signed reconciliation RPCs. Failure controls inject
faults; they do not call recovery. Physical movement still requires a separate
gate-accepted entry, and removing a stopped occupant requires explicit clearance.

## Interactive and recorded runs

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.live --automatic --port 8767
.\.venv\Scripts\python.exe -m cranelock_sim.automatic_demo
.\.venv\Scripts\python.exe -m cranelock_sim.automatic_demo --delay-sweep
```

Open `http://127.0.0.1:8767/live` for the first command. The second command is an
independent bounded run: CENTRAL entry, controller failure, automatic LOCAL recovery,
holder failure, automatic replacement, controller return and safe queued entry after
explicit clearance. It writes verified checks, component logs, gate audit, SQLite
state and request CSV. Neither command changes the reference evaluation calculations.

## Independent service processes

These commands use an isolated loopback development setup. Use a fresh directory
for setup; existing keys and databases are deliberately not overwritten.

Create identities and the gate database once:

```powershell
.\.venv\Scripts\python.exe -m cranelock_gate.demo setup --directory demo-data/automatic
```

Start the gate in its own terminal:

```powershell
.\.venv\Scripts\python.exe -m cranelock_gate.server serve --config demo-data/automatic/config.json --database demo-data/automatic/gate.sqlite --bind 127.0.0.1:50060
```

Start the controller in another terminal:

```powershell
.\.venv\Scripts\python.exe -m cranelock.controller --serve --listen 127.0.0.1:50070 --bootstrap-holder crane-1 --agent crane-1=127.0.0.1:50051 --agent crane-2=127.0.0.1:50052 --agent crane-3=127.0.0.1:50053 --gate-address 127.0.0.1:50060 --gate-config demo-data/automatic/config.json --gate-keys demo-data/automatic/private-keys.json
```

Start each agent in a separate terminal:

```powershell
.\.venv\Scripts\python.exe -m cranelock run --id crane-1 --listen 127.0.0.1:50051 --peer crane-2=127.0.0.1:50052 --peer crane-3=127.0.0.1:50053 --holder crane-1 --epoch 0 --mode CENTRAL --state-file demo-data/automatic/crane-1.json --gate-address 127.0.0.1:50060 --gate-config demo-data/automatic/config.json --gate-keys demo-data/automatic/private-keys.json --controller-address 127.0.0.1:50070 --auto-recover
.\.venv\Scripts\python.exe -m cranelock run --id crane-2 --listen 127.0.0.1:50052 --peer crane-1=127.0.0.1:50051 --peer crane-3=127.0.0.1:50053 --holder crane-1 --epoch 0 --mode CENTRAL --state-file demo-data/automatic/crane-2.json --gate-address 127.0.0.1:50060 --gate-config demo-data/automatic/config.json --gate-keys demo-data/automatic/private-keys.json --controller-address 127.0.0.1:50070 --auto-recover
.\.venv\Scripts\python.exe -m cranelock run --id crane-3 --listen 127.0.0.1:50053 --peer crane-1=127.0.0.1:50051 --peer crane-2=127.0.0.1:50052 --holder crane-1 --epoch 0 --mode CENTRAL --state-file demo-data/automatic/crane-3.json --gate-address 127.0.0.1:50060 --gate-config demo-data/automatic/config.json --gate-keys demo-data/automatic/private-keys.json --controller-address 127.0.0.1:50070 --auto-recover
```

Wait for controller/peer heartbeat events before injecting a fault. Stop the
controller with Ctrl+C: the elected recovery coordinator collects votes and installs
LOCAL authority. Stop the holder: the surviving majority can replace it. Restart the
controller with the same command: it preserves the accepted holder and epoch, advances
the revision and reconciles reachable agents. Inspect any agent with:

```powershell
.\.venv\Scripts\python.exe -m cranelock state 127.0.0.1:50052
```

This service setup has no physical simulator attached: the gate correctly remains
UNKNOWN until trusted position evidence arrives. Use the live application to exercise
physical movement. The fixture key file contains all development identities; each
agent retains only its own key. The CLI controller service/probe is loopback-only;
remote service instances require configured authenticated transport using the Python
credential hooks. This is not an independently validated container deployment.

## Coordination policy and limits

- An agent refreshes authority from the live gate before deciding to recover.
- CENTRAL recovery requires controller suspicion; LOCAL recovery requires holder
  suspicion. A communicating majority and a stable view lasting one detection timeout
  are required before proposing.
- The lowest ID in the observed reachable set coordinates. A healthy reachable holder
  is retained; otherwise the lowest reachable ID is proposed as replacement.
- Votes remain one durable digest per target epoch. All signatures and expected gate
  state are checked by the existing gate. Partial reconciliation is retried through
  live gate reads rather than reported as a completed handover.
- Controller return requires a reachable majority and a responding accepted holder.
  It reads that holder's queue, pins the expected gate snapshot and preserves the
  holder/epoch. Same-epoch reconciliation preserves its queue; gate occupancy is not
  changed. If the holder is unavailable, return waits rather than inventing its state.
- The live application retains client-side waiting requests across revoked grants and
  resubmits them. Arbitrary external clients must retain and retry their own requests.

The deterministic proposer and settling delay reduce competing proposals. They do
not implement a general consensus protocol. Divergent failure views can split durable
votes across conflicting proposals, leaving no certificate even after links recover.
The protocol then remains safely stopped; it does not reset votes to force progress.
Changing membership, replacing the gate, Byzantine faults and recovery from lost vote
storage are outside the model. These are availability limits, not permissions to bypass
majority approval or physical clearance.

## Validation

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_automation.py tests/test_automation_process.py -v
.\.venv\Scripts\python.exe scripts/validate_project.py
```

Peer delay experiments use two application-level delivery settings: 5 ms plus
uniform 0–15 ms jitter, and 10 ms plus uniform 0–20 ms jitter. Concurrent RPCs can
arrive out of order; a separate test deliberately reverses two requests over real
gRPC and verifies queue order and unique ownership. These are not packet-level
network faults. Seeded random draws do not make host scheduling deterministic.

The process test launches three independent agent CLI processes and a controller
process, kills the controller and holder, and verifies automatic gate transitions,
occupancy blocking and queue preservation on controller return. The full validation
runner executes tests, the automatic demo, six delayed cycles, nine existing progress scenarios, 54
reference comparison runs and 27 logical scaling runs. Comparisons use seeds
7/20/3104, loads 6/18 and five-second failure windows. Scaling varies logical membership
3/5/10 and arrival rates 2/5/10; it excludes gRPC and distributed recovery costs.
