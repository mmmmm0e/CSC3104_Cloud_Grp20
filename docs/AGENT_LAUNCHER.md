# Standalone Crane Agent startup for Arif

The launcher is `python -m cranelock run`. Existing invocations without recovery
or controller options retain their existing behavior. Gate integration alone
does not enable voting or automatic recovery.

## Recovery configuration and credentials

Enable voting and automatic LOCAL holder recovery with both
`--recovery-config PATH` and `--recovery-key PATH`, together with the existing
`--gate-address`, `--gate-config`, and `--gate-keys` options. The recovery config
uses the existing `cranelock_gate.config.Config` JSON format, including public
member keys, lane ID, membership ID, service identities/keys and gate settings.
Point `--recovery-config` and `--gate-config` at the same public config file.
The launcher rejects different configurations, including different public keys.

`--id` plus every `--peer ID=HOST:PORT` must exactly match the configured member
IDs. Include temporarily offline members; do not shrink membership to obtain a
quorum. The initial `--holder` must be a member. Use the installed gate epoch
(`--epoch 0` for the standard bootstrap); the gate supplies the current revision
and authoritative holder/mode during reconciliation. Keep IDs consistent across
agent arguments, public configuration, gate deployment and signing keys.

The recovery key file uses the existing private-key JSON/hex format but must
contain **only this agent's entry**:

```json
{"crane-1": "<64 hexadecimal characters encoding this agent's 32-byte Ed25519 private key>"}
```

The public half must match `members["crane-1"]` in the config. The same agent-only
file can supply `--gate-keys`. Other cranes' keys and controller/simulator/operator
keys must not be distributed to this process. The combined `private-keys.json`
created by the development demo is rejected by `--recovery-key`.

Provision private files through each agent's own read-only secret mount; restrict
host access with the deployment's Windows ACLs or Unix permissions. Supply paths,
never private-key material, in command arguments. The launcher neither generates
production identities nor rewrites permissions or key files, and configuration
errors do not print their contents. The Agent retains one validated recovery key.

Example from the repository root on Windows (directories are deployment inputs):

```powershell
.\.venv\Scripts\python.exe -m cranelock run `
  --id crane-1 --listen 127.0.0.1:50051 `
  --peer crane-2=127.0.0.1:50052 --peer crane-3=127.0.0.1:50053 `
  --holder crane-1 --epoch 0 --mode LOCAL `
  --interval 0.5 --timeout 2 --state-file run/crane-1/state.json `
  --gate-address 127.0.0.1:50060 --gate-config config/gate.json `
  --gate-keys secrets/crane-1.json `
  --recovery-config config/gate.json --recovery-key secrets/crane-1.json
```

Use distinct IDs, peer lists, listen endpoints, secret files and state volumes
for the other members. The Agent requires a durable writable state file for
votes and restart reconciliation: retain it across restarts, run one process per
state file, and do not reset it to bypass conflicting votes. Gate outage keeps
the agent fail-closed; it does not switch to ungated operation. Certificate
acceptance does not clear lane occupancy or grant physical entry.

Gate connections outside loopback still require all three existing options:
`--gate-tls-ca`, `--gate-tls-cert`, and `--gate-tls-key`. Provision the matching
server/client identities and trust files as described in [DEPLOYMENT.md](DEPLOYMENT.md).
This launcher change does not add encryption/authentication to peer-agent RPCs;
retain the existing network isolation requirements.

## Controller probe adapter: interface still needed from Kirby

The Agent's existing controller interface is an async no-argument callable
returning `True` for an available controller and `False` for an unavailable one.
The launcher can import it with `--controller-probe MODULE:ASYNC_FUNCTION`.
For example, `--controller-probe kirby_controller.agent_probe:probe` is a proposed
deployment adapter name, **not a module supplied by this repository**. Install
the actual agreed adapter and use its real name. Invalid imports, synchronous
functions and functions requiring arguments are rejected before Agent startup.
Only load trusted adapter code; importing a module executes its initialization.

This option requires gate and recovery configuration. The Agent invokes the probe
with a deadline equal to `--interval` and marks the controller suspected only
after `--timeout` without a successful observation. Errors/timeouts count as
failed observations; shutdown cancellation propagates. It then uses the existing
quorum protocol for CENTRAL-to-LOCAL fallback, preserving the gate holder.
Controller return does not itself install CENTRAL authority.

`cranelock/controller.py` currently provides controller operations and an agent
monitor, but no production controller heartbeat server/API. The simulator's
reference `ControllerConnection` is not that deployment contract. No new
controller RPC or `--controller-address` option is introduced here. Without an
adapter, omit `--controller-probe`: LOCAL recovery works, but automatic
CENTRAL-to-LOCAL fallback remains disabled even if `--mode CENTRAL` is supplied.

Kirby must supply the production health RPC/service name, request/response
contract, controller endpoint, TLS/authentication policy, and an adapter that
checks that controller within the Agent's probe budget. Endpoint/credentials
configuration belongs to that adapter until the team agrees its interface.
TCP reachability or the gate's health is not a substitute for controller health.

Arif needs the fixed public membership/config, one private key per agent,
reachable peer/gate endpoints, the initial installed authority, TLS files for
remote gate connections, persistent writable state volumes and shutdown/restart
policy. Add the controller endpoint, credentials, installed adapter module and
its configuration once Kirby supplies them. Existing manifests are not changed
automatically by these launcher options.

Missing, malformed or mismatched configuration exits with an argparse error
(status 2), before serving agent RPCs. A gate client created during validation is
closed on a later validation failure. The existing one-vote-per-target-epoch
rule and stale-snapshot availability limitation are unchanged.
