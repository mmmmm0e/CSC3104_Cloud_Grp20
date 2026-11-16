# Agent workload grant interface

Each gate-enabled Agent can serve a separate `cranelock.v1.CraneWorkload` gRPC
listener. It exposes only `RequestGrant`; the existing CraneAgent peer listener
and its methods are unchanged. Configure all five options together:

```
--workload-listen 0.0.0.0:50053
--workload-tls-ca /workload/ca.pem
--workload-tls-cert /workload/server.pem
--workload-tls-key /workload/server-key.pem
--workload-allowlist /workload/allowlist.json
```

The server requires a client certificate signed by the configured CA. Each
client certificate must have exactly one URI SAN, matched exactly against this
Agent's allowlist. For the deployed runner the approved identity is
`urn:cranelock:workload:deployed-runner`. Server certificates must contain DNS
or IP SANs matching the hostnames or IPs the clients actually connect to.

For each crane provide a separate read-only JSON allowlist, for example:

```json
{"crane_id":"crane-1","lane_id":"lane-1","allowed_workload_uri_sans":["urn:cranelock:workload:deployed-runner"]}
```

The crane and lane must match the Agent ID and Gate configuration. Empty or
invalid allowlists fail startup. Keep private keys private and mount the
allowlist and certificates read-only; the Agent still needs its existing durable
state volume and actor-only Gate signing key. Bind and publish the new workload
port separately from the existing peer port. Protect the existing peer network
according to its current trust assumptions: this change does not authenticate
peer RPCs. For platform setup see the [Compose handoff](PLATFORM_HANDOFF.md) and
[dedicated kind instructions](PLATFORM_OPERATIONS.md).

For `scripts/external_workload.py` and `scripts/external_smoke.py`, provide
`--workload-config PATH` pointing to a host-readable JSON file:

```json
{
  "ca":"/path/to/ca.pem",
  "cert":"/path/to/client.pem",
  "key":"/path/to/client-key.pem",
  "agents": {
    "crane-1":"localhost:50201",
    "crane-2":"localhost:50202",
    "crane-3":"localhost:50203"
  }
}
```

These ports are examples; platform wiring must supply the actual endpoints.
The runner still needs its simulator signing key and Gate TLS credentials for
physical observations and entry validation, but does not load crane signing
keys for grant requests.

`RequestGrant` accepts a canonical lowercase UUIDv4 `request_id`,
`expected_epoch`, and `expected_revision`; both authority fields must be present,
including when their value is zero. The response includes `request_id`,
`allowed`, `reason`, `replayed`, `action_applied`, and `grant_json` when a
grant exists. The request ID is durably bound to identity and expected
authority before Gate I/O. Retry an ambiguous result with the same ID and
snapshot; make a new attempt after a definite denial. Gate replay is historical,
so an approval with `replayed=true` or `action_applied=false` is not a movement
instruction. The simulator must successfully perform a fresh `ValidateEntry`
before movement. The persisted request bindings currently grow with the number
of attempts; operators must retain the state file and plan for its growth.

If the Agent becomes unsafe after Gate I/O, it returns `allowed=false`,
`reason=GATE_UNCERTAIN` and no grant. `action_applied` and `replayed` retain
the Gate's actual flags; an applied Gate operation is not rolled back. Retry
the same ID and authority snapshot, and never move on a replayed approval.

Local evaluation provisions credentials in a separate setup process, then
the workload driver loads only simulator signing and workload mTLS keys.
Docker comparison services receive read-only per-service credential mounts
and separate writable state directories. The credentials are stored under the
ignored `.credentials` directory in each evaluation output; do not share it as
evidence. Local processes run under the same host account, which can still
read all these files. This layout does not provide same-host OS-user isolation.
