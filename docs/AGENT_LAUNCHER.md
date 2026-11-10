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

## Controller heartbeat and return

Use `--controller-address HOST:PORT` for Kirby's existing
`cranelock.v1.CraneController/Heartbeat` RPC. It requires the gate and recovery
options above so the agent can complete CENTRAL-to-LOCAL-to-CENTRAL operation.
Outside loopback, also provide `--controller-tls-ca`, `--controller-tls-cert`
and `--controller-tls-key`. The launcher owns and closes this heartbeat client
on shutdown and startup failure.

The Agent's existing controller interface is an async no-argument callable
returning `True` for an available controller and `False` for an unavailable one.
As an alternative, the launcher can import it with
`--controller-probe MODULE:ASYNC_FUNCTION`; the two controller options are mutually
exclusive. Imported functions are not closed by the launcher.
For example, `--controller-probe kirby_controller.agent_probe:probe` is a proposed
deployment adapter name, **not a module supplied by this repository**. Install
the actual agreed adapter and use its real name. Invalid imports, synchronous
functions and functions requiring arguments are rejected before Agent startup.
Only load trusted adapter code; importing a module executes its initialization.

Both options require gate and recovery configuration. The Agent invokes the probe
with a deadline equal to `--timeout` and marks the controller suspected only
after `--timeout` without a successful observation. Errors/timeouts count as
failed observations; shutdown cancellation propagates. It then uses the existing
quorum protocol for CENTRAL-to-LOCAL fallback, preserving the gate holder.
On a successful native heartbeat, the controller restores CENTRAL using the
holder, epoch and revision from one gate snapshot. Concurrent handoffs or recovery
invalidate that snapshot; retries read the latest holder. Agents record controller
liveness immediately, then reconcile fresh gate authority after pending local
handoffs are resolved. A plain probe returning `True` does not install CENTRAL;
its controller must perform that gate operation. Heartbeat response fields and
peer reports never grant ownership.

Without either controller option, LOCAL recovery works but automatic
CENTRAL-to-LOCAL fallback remains disabled even if `--mode CENTRAL` is supplied.
The controller service launcher is configured separately as described below.
Certificate-to-member authorization and service deployment still need integration.
The simulator's reference controller is separate from this heartbeat service.

## Controller service launcher

Run `python -m cranelock.run_controller` with these required options:

- `--gate-config FILE`: the existing public gate configuration, loaded with `Config.load`.
- `--controller-key FILE`: the existing JSON/hex key format, containing exactly one
  entry under the configured `controller_id`. Its Ed25519 public key must match
  `service_keys[controller_id]`. Combined development key files are rejected.
- `--gate-address HOST:PORT`: the Trusted Gate endpoint.
- `--listen HOST:PORT`: the controller heartbeat endpoint. IPv6 uses `[IPv6]:PORT`;
  listening on port `0` requests an ephemeral port, which is printed at startup.

`--gate-timeout SECONDS` sets each gate RPC's deadline; the default is `3` seconds.
Values must be finite and positive. Plaintext connections and listeners are
restricted to loopback. For remote operation, configure both TLS directions:

- `--gate-tls-ca FILE --gate-tls-cert FILE --gate-tls-key FILE` authenticates the
  gate and presents the controller's TLS client identity. The gate address must
  match the server certificate; there is no hostname-verification bypass.
- `--tls-ca FILE --tls-cert FILE --tls-key FILE` configures the heartbeat listener's
  server identity and CA for client certificates. Client certificates are required.
  Agents connect using their existing `--controller-tls-*` options.

Each TLS group must be complete. Files must contain valid PEM, the private key must
match the certificate, and the leaf certificate must be currently valid and have
the appropriate client/server purpose when extended key usage is present.
Password-encrypted TLS keys are rejected without an interactive password prompt.
Provision signing keys and TLS keys as private files; only their paths belong on
the command line. Startup diagnostics do not print file contents or remote errors.

For example, in PowerShell with already provisioned credentials:

```powershell
python -m cranelock.run_controller `
  --gate-config C:\CraneLock\config.json `
  --controller-key C:\CraneLock\controller.key.json `
  --gate-address gate.example:50051 --listen 0.0.0.0:50052 `
  --gate-tls-ca C:\CraneLock\gate-ca.pem `
  --gate-tls-cert C:\CraneLock\controller-client.pem `
  --gate-tls-key C:\CraneLock\controller-client-key.pem `
  --tls-ca C:\CraneLock\agent-ca.pem `
  --tls-cert C:\CraneLock\controller-server.pem `
  --tls-key C:\CraneLock\controller-server-key.pem
```

Startup reads the Trusted Gate and uses the existing `restore_central()` operation.
An already CENTRAL gate keeps its authority unchanged. LOCAL authority returns to
CENTRAL with the current holder and epoch, incrementing the revision; concurrent
transfers or recovery cause fresh-snapshot retries. Physical occupancy is preserved.
The launcher never resets a gate, clears a lane, or chooses a replacement holder.

An uninitialized gate exits with status `1` and explicit bootstrap guidance. Install
initial authority through the established signed `InstallAuthority` contract first:
controller-signed CENTRAL authority names an explicitly selected configured holder
at epoch `0` and matches the uninitialized gate's epoch/revision. The existing
majority-signed LOCAL bootstrap is also supported. This launcher provides no
automatic bootstrap or initial-holder option.

Invalid local configuration exits with status `2`; an unavailable/rejecting gate or
listener startup failure exits with status `1`. The listener is announced only after
successful authority restoration and server startup. SIGINT, SIGTERM and Windows
SIGBREAK stop the heartbeat server and close the gate channel, with status `0` on
normal shutdown. Task cancellation also closes both resources. If startup has
already restored authority before a later failure, it does not roll back that gate
decision or overwrite subsequent authority.

Arif needs the fixed public membership/config, one private key per agent,
reachable peer/gate endpoints, the initial installed authority, TLS files for
remote gate connections, persistent writable state volumes and shutdown/restart
policy. Add the controller endpoint and credentials, or a trusted adapter and
its configuration. Existing manifests are not changed
automatically by these launcher options.

Missing, malformed or mismatched configuration exits with an argparse error
(status 2), before serving agent RPCs. A gate client created during validation is
closed on a later validation failure. The existing one-vote-per-target-epoch
rule and stale-snapshot availability limitation are unchanged.

## Process lifecycle integration test

`tests/test_controller_process.py` starts the real Trusted Gate, real controller
launcher and all three agent launchers as independent OS processes. It uses
temporary Ed25519 identities, one recovery key/state file per agent, isolated
ports, and mutual TLS for gate/controller connections. Peer RPCs retain the current
loopback transport. The driver submits workload through the existing TokenRequest
RPC and signs physical lane observations; actual agent processes perform holder
transfers, peer token delivery, voting and fallback installation.

The cases kill the controller with all three agents alive, kill it with only two
agents surviving, and verify refusal to install LOCAL with only one survivor.
They restart the real controller, verify holder-preserving CENTRAL restoration,
reject stale epochs/revisions/holders and delayed certificates/token transfers,
preserve occupied-lane blocking, and admit a valid new entry after trusted exit
confirmation. All child processes are stopped and reaped, including on failure.

Run with a fresh Windows-local pytest directory:

```powershell
$testTemp = Join-Path $PWD ("run/pytest-controller-process-" + [guid]::NewGuid().ToString("N"))
.\.venv\Scripts\python.exe -B -m pytest tests/test_controller_process.py --basetemp $testTemp -q
```

Each case records `controller-process-results.json`, individual service logs and
`gate-audit.jsonl` under its pytest temporary directory. The JSON records per-agent
failure detection, observed LOCAL installation, controller restart-to-CENTRAL and
restart-to-agent-convergence latency, authority snapshots, vote/handoff evidence,
PIDs and exit codes. Detection uses the same host's monotonic clocks; gate phase
times are observed through RPC polling, and restoration includes process startup.
These are local observations rather than production latency guarantees. Credentials
and runtime artifacts stay in temporary directories and must not be committed.
