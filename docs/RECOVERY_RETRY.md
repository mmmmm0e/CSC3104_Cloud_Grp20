# Recovery retries and failure validation

The retry contract below describes the current implementation. The validation
record at the end describes the 3 November 2026 run.

## Finding

A vote signs the entire canonical recovery proposal, including `expected_epoch`
and `expected_revision`. Suppose an agent saves a vote for target epoch 1 against
gate snapshot `(0, 1)`. A normal handoff or CENTRAL authority change then advances
the gate to `(0, 2)`. The original certificate cannot be installed because its
expected revision is stale. A proposal against revision 2 needs fresh votes
because the expected revision is part of the signed payload.

Previously this was a permanent stop: the old vote blocked every later proposal for
that target epoch. Because CENTRAL handoffs advance the revision without changing the
epoch, a single abandoned fallback attempt could disable controller fallback for good.

**Current behaviour:** a Gate-enabled member may replace its vote for target epoch
`e+1` with one for a *strictly newer* revision of the same epoch `e`, after a live
Gate read confirms that newer snapshot. The Gate can then never accept a certificate
for the older revision, so this does not allow two installable certificates for one
target epoch; see [recovery safety](RECOVERY_SAFETY.md). Same-revision proposals
naming another holder, older revisions, standalone voters without a Gate, and legacy
digest-only votes stay locked.

## Implemented retry contract

The implementation retains one active SHA-256 proposal digest per target epoch.
Only the live-verified revision advance described above can replace it.
New state files additionally retain the exact proposal in `vote_proposals`; both are
written by the same atomic, fsynced state-file update before returning a signature.
Restart validates that saved proposals match their digests. Legacy digest-only files
remain supported; malformed records fail closed, and matching retries can add the
missing proposal metadata without replacing the digest.
Gate-enabled proposers and receivers query the live gate before persisting a vote.
Standalone voters without a gate rely on the integration caller's snapshot and the
gate's eventual installation checks. Legacy locks without proposal metadata may
report generic `CONFLICTING_VOTE` instead of the revision-specific blocked reason.

| Condition | Implemented behavior |
| --- | --- |
| No vote yet; supplied gate snapshot is already stale | Reject before consuming a vote. |
| Same proposal, unchanged gate authority, temporary peer loss | Retry identical bytes; signatures remain valid and a returning voter may complete the quorum. |
| Another holder is proposed for the same Gate snapshot | Reject `CONFLICTING_VOTE`; retain the original vote. |
| Same target epoch, strictly newer live-verified gate revision (Gate-enabled) | Replace the vote with the newer proposal and continue collecting; the older certificate can no longer install. |
| Same target epoch, changed revision without Gate verification, or older revision | Emit `RECOVERY_RETRY_BLOCKED` with `STALE_VOTE_REVISION`; retain the original vote. |
| Gate changes while votes are being collected | Discard the local collection result and emit `GATE_CHANGED_DURING_VOTING`; a later attempt re-votes for the newer snapshot. |
| Gate changes after the final read but before installation | The gate's atomic expected epoch/revision check rejects installation. |
| Identical certificate submission after a lost reply | Reuse a request ID derived from actor and exact certificate; a committed command returns historical acknowledgement without another authority transition. |
| Gate unavailable or installation outcome uncertain | Query the live gate; never infer ownership from votes, a timeout or a historical acknowledgement. |
| A separately valid certificate advances accepted epoch | Reconcile, then a vote for the following target epoch is allowed; older vote history remains unchanged. |

The automatic proposer reuses its saved holder when the saved proposal still matches
the current authority snapshot. A rejected changed proposal does not send a fresh vote
round to peers. The original `RECOVERY_REJECTED`/`CONFLICTING_VOTE` event remains for
compatibility; the additional blocked event explains the stale revision specifically.

`GateClient.install_recovery(certificate)` reuses the command ID for an identical
certificate. After a *known rejection*, a caller may explicitly supply a fresh
`request_id` to resubmit the unchanged certificate where the rejection condition is
recoverable. This changes only command deduplication. It never changes the signed
proposal, unlocks votes or makes a stale certificate acceptable. Automatic recovery
does not manufacture new proposal revisions to bypass a rejection.

## Why changed-revision retries were blocked (historical)

This section records the original reasoning. It has been superseded by the
monotonic-revision supersession described above, which relies on the Gate's atomic
expected-snapshot check rather than one immutable vote per target epoch.

The original implementation locked one vote per target epoch. A controller return
at the same epoch could restore CENTRAL service while leaving those votes locked
for a later LOCAL recovery attempt.

Consequently, a quorum with stale or split locked votes may remain unable to recover
even after all peers reconnect. The implementation guarantees fail-closed behavior;
it does not guarantee eventual progress in this condition. An independently valid
next-epoch certificate may resolve it if sufficient unconflicted votes exist, but
such a certificate is not guaranteed to exist.

The current implementation permits the live-verified revision advance described
above. Same-revision conflicts can still stall recovery.

## Failure tests

The new tests cover:

- A stale snapshot before either proposer or remote-voter persistence.
- Gate revision changing after the proposer's vote but before collection completes.
- Revision-locked retries after restart, including legacy state.
- Conflicting holder proposals, corrupt saved metadata and malformed legacy votes.
- Identical install retries and historical acknowledgements after commit.
- A separately accepted epoch allowing a new target-epoch vote without deleting history.
- Five configured members with two unavailable: three real gRPC voters can recover.
- Five configured members with three unavailable: two cannot recover; a returning
  third voter completes the identical proposal while existing digests remain unchanged.
- Stale-holder rejection and UNKNOWN-lane blocking after recovery.
- Independent agent CLI processes using `--enable-recovery` to collect a real quorum.

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_recovery_retry.py tests/test_recovery_multiple_failures.py tests/test_recovery_cli.py tests/test_auto_recovery.py tests/test_controller_fallback.py -v
```

The five-member RPC tests control the proposed healthy replacement explicitly.
The automatic policy chooses a fixed successor independent of local failure views:
the member *after* the failed holder in sorted, wrapping order. Previously it chose
the lowest other ID, so with crane-1 and crane-2 both down recovery alternated
between them forever. Rotation walks the whole membership, so a live member is
reached after one further epoch per unavailable successor
(`tests/test_review_fixes.py`).

## Real container validation

```powershell
.\.venv\Scripts\python.exe scripts/validate_recovery_containers.py
```

The harness creates a uniquely named Compose project, builds the existing agent
image and starts five agent containers plus a gate with separate persistent volumes.
An initialization container creates the gate database. Each agent receives only its
own private signing key. The harness verifies these phases:

1. Provision initial authority, warm up all five services and report occupied state.
2. Stop cranes 1 and 5; cranes 2/3/4 must install majority recovery naming crane 2.
3. Check durable vote payloads, stale-holder fencing and occupancy blocking.
4. Stop crane 2; the remaining two members cannot install replacement authority.
5. Restart crane 1 from its persistent volume; the restored majority completes the
   identical pending proposal naming crane 3 (the next member after failed holder
   crane 2) and still cannot admit a crane into the occupied lane.

All agent containers share the gate's network namespace and communicate over loopback
on separate ports. They are distinct processes/filesystems with persistent volumes;
this is not bridge-network partition testing or remote mutual-TLS validation. Host
ports are dynamically allocated and published only on loopback. Initial provisioning
uses fixture signatures and is not counted as agent recovery.

The harness saves container logs, gate audit and `summary.json`, then removes only
its uniquely named project and volumes. Generated development private keys remain
inside the ignored evidence directory; do not publish that directory unchanged.

To prepare fixtures on a machine without Docker:

```powershell
.\.venv\Scripts\python.exe scripts/validate_recovery_containers.py --prepare-only
```

Preparation records `runtime_validated: false` and `passed: false`. The original
3 November run prepared and inspected the Compose file. See the
[Docker evaluation results](DEPLOYED_RESULTS.md) for separate three-agent runtime
evidence.

## Validation record

The final branch regression run on 3 November 2026 completed with **239 passed,
1 skipped** in 98.84 seconds. The optional browser check was skipped. Test XML is
retained at `run/recovery-review-20261103/tests-final.xml`. The prepared container
fixtures are under `run/recovery-review-20261103/container-fixtures/`, with runtime
validation explicitly false. No container execution is claimed by these results.
