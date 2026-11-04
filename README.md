# CraneLock

## Demo the controller heartbeat and return from LOCAL

This local development demo uses the checked-in Python modules and temporary
development identities. It starts a gate, a controller heartbeat service and
three gate-connected agents. Stopping the controller lets the agents install
LOCAL authority with a strict-majority certificate; restarting it restores
CENTRAL authority on the gate's current holder. Do not use the generated private
keys outside this local demo.

Open six terminals at the repository root. For a first run, start with a new
demo directory:

```bash
python3 -m cranelock_gate.demo setup --directory run/controller-demo
python3 -m cranelock_gate.server serve \
  --config run/controller-demo/config.json \
  --database run/controller-demo/gate.sqlite \
  --bind 127.0.0.1:50060 --metrics-port 0
```

In a second terminal, bootstrap the gate's initial authority:

```bash
python3 -m cranelock.gate_demo bootstrap \
  --directory run/controller-demo --target 127.0.0.1:50060
```

Bootstrap is only for a newly created, uninitialized demo gate (epoch `-1`).
If you have run this demo before, do not bootstrap again: start the existing
gate with the same config and database paths, then check its state:

```bash
python3 -m cranelock.gate_demo state \
  --directory run/controller-demo --target 127.0.0.1:50060
```

If the output shows an epoch of `0` or higher and a holder, the gate is already
initialized; continue with the controller and agent steps below. If you want a
separate fresh demo, use a different directory for `setup`, `serve`, and all
subsequent commands.

Run the existing controller service from the third terminal. It restores CENTRAL
from the gate at startup and serves heartbeats on `127.0.0.1:50052` until stopped
with Ctrl+C:

```bash
python3 -m cranelock.run_controller
```

In the fourth terminal, start the agents. Each command names all three
configured members and uses a distinct listen port and state file:

```bash
python3 -m cranelock run \
  --id crane-1 --listen 127.0.0.1:50101 \
  --peer crane-2=127.0.0.1:50102 --peer crane-3=127.0.0.1:50103 \
  --holder crane-1 --epoch 0 --interval 0.2 --timeout 1 \
  --state-file run/controller-demo/agent-1.json \
  --gate-address 127.0.0.1:50060 \
  --gate-config run/controller-demo/config.json \
  --gate-keys run/controller-demo/private-keys.json \
  --controller-address 127.0.0.1:50052
```

In a fifth terminal, start crane-2:

```bash
python3 -m cranelock run \
  --id crane-2 --listen 127.0.0.1:50102 \
  --peer crane-1=127.0.0.1:50101 --peer crane-3=127.0.0.1:50103 \
  --holder crane-1 --epoch 0 --interval 0.2 --timeout 1 \
  --state-file run/controller-demo/agent-2.json \
  --gate-address 127.0.0.1:50060 \
  --gate-config run/controller-demo/config.json \
  --gate-keys run/controller-demo/private-keys.json \
  --controller-address 127.0.0.1:50052
```

In a sixth terminal, start crane-3:

```bash
python3 -m cranelock run \
  --id crane-3 --listen 127.0.0.1:50103 \
  --peer crane-1=127.0.0.1:50101 --peer crane-2=127.0.0.1:50102 \
  --holder crane-1 --epoch 0 --interval 0.2 --timeout 1 \
  --state-file run/controller-demo/agent-3.json \
  --gate-address 127.0.0.1:50060 \
  --gate-config run/controller-demo/config.json \
  --gate-keys run/controller-demo/private-keys.json \
  --controller-address 127.0.0.1:50052
```

Check the state from another terminal:

```bash
python3 -m cranelock state 127.0.0.1:50101
python3 -m cranelock.gate_demo state \
  --directory run/controller-demo --target 127.0.0.1:50060
```

The agents should report `CENTRAL`; the gate's holder should initially be
`crane-1`. Stop the controller with Ctrl+C. Once the configured timeout passes,
agents vote for a LOCAL fallback; check the gate until its mode is `LOCAL` and
its epoch has advanced. Restart `python3 -m cranelock.run_controller`: the next successful
agent heartbeat restores CENTRAL using the latest gate holder, epoch and
revision. Stop services with Ctrl+C when finished. The gate starts with lane
state `UNKNOWN`, so token grants remain denied until a trusted physical
observation confirms the lane is clear.

