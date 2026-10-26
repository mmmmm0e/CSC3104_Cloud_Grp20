# CraneLock: Member 5 Centralized Controller

The central controller manages one configured gate/lane. Create one instance per
gate, each with that gate's `Config`, controller Ed25519 private key, and gRPC
channel, then pass the instances to
`CentralControllerFleet({"lane-1": lane_1, "lane-2": lane_2})`. The fleet's
`get_all_states()` reads each gate's latest snapshot and fails explicitly if any
gate cannot be reached. A per-gate `install_central(holder)` bootstraps CENTRAL
authority at epoch zero or updates current authority from a fresh epoch/revision;
stale updates are retried from a new snapshot. `reconcile_agents(agents)` is an
async trusted integration hook that reconciles supplied local `Agent` instances
from that gate's current accepted authority. It does not change lane occupancy or
establish physical clearance.

To monitor agent processes directly, start the controller after the agents with:

```sh
python -m cranelock.controller \
  --agent crane-a=127.0.0.1:50051 \
  --agent crane-b=127.0.0.1:50052 \
  --interval 1
```

It immediately prints a readable `startup` snapshot, then prints updated snapshots
at the chosen interval. Each available agent shows its status, epoch, mode, token
ownership, holder, peers, and request queue. Offline agents appear with `UNAVAILABLE`
status and the connection error instead of being omitted. This captures each agent's
current state at controller startup; agents do not expose historical state from before
that snapshot. Press Ctrl+C to stop monitoring.