# Recovery safety argument

## Protocol

For a gate snapshot `(e, r)`, a proposer constructs exactly one canonical authority
payload naming epoch `e+1`, the replacement holder, lane, fixed membership, and the
expected snapshot `(e, r)`. The proposer signs it with Ed25519. A member votes only
when it is active at epoch `e`, locally suspects the current holder, authenticates the
proposer, and has not durably voted for a different payload at target epoch `e+1`.
The vote is fsynced before its signature is returned. A certificate contains distinct
valid member signatures and is usable only with at least `floor(N/2)+1` signatures.
In CENTRAL mode, voting instead requires suspicion of the configured controller;
fallback retains the current holder. LOCAL mode requires holder suspicion.

The gate independently verifies every signature, the strict-majority count, the
fixed membership/lane identity, the exact `e+1` step, and the expected `(e, r)` against
its durable current state. Installing it advances the epoch and revision and revokes
all existing grants. A replacement holder must then obtain a new grant; physical
occupancy remains blocking until trusted clearance arrives.

## Why a minority cannot recover

Let `q = floor(N/2)+1`. Any partition with fewer than `q` configured members can
produce at most that many distinct signatures, so the gate rejects its certificate
with `NO_QUORUM`. Private keys are distinct and Ed25519 signatures are verified by a
library, so an absent member cannot be counted and duplicate identities do not help.
Only a partition containing a strict majority can reach `q`. Failure suspicion is a
permission to vote, not proof by itself and not authority to enter the lane.

## Why conflicting recoveries cannot both be accepted

Any two strict majorities intersect in at least one member. An honest member durably
records one proposal digest per target epoch before releasing its signature and will
reject a different proposal for that epoch after retries or restart. Therefore two
different payloads for the same `e+1` cannot both collect valid strict majorities.
Even if a delayed proposal was valid when created, the gate's atomic expected
epoch/revision comparison permits only the proposal matching its current snapshot.
The first accepted authority change makes every competing or delayed certificate
stale.

## Why an old holder cannot obtain an accepted grant

Every grant request is signed and binds both epoch and authority revision. Recovery
atomically advances the epoch and revokes the old grant. A request from the former
holder using its old values is rejected as `STALE_EPOCH`; changing the request to the
new epoch does not help because the authenticated actor is not the new holder and is
rejected as `WRONG_HOLDER`. A normal same-epoch handoff advances the revision, so its
delayed requests are likewise rejected as `STALE_REVISION` or `WRONG_HOLDER`.

Finally, a certificate or grant is not entry permission. Only the trusted simulator's
fresh, one-shot `ValidateEntry` transition can accept movement, and it rechecks the
current holder, epoch, revision, grant, and physical session. Thus a minority, stale
holder, delayed packet, or replay cannot turn obsolete digital authority into an
accepted lane entry. The claim assumes fixed membership, uncompromised member keys,
honest non-equivocating voters, one durable gate, and all entry paths passing through
that gate.

## Retry safety and availability

Identical proposals may be retried without changing the durable vote. The exact
proposal is retained alongside its digest and validated after restart. A proposal
for another holder or expected revision at the same target epoch remains conflicting.
Live gate checks before voting and after collection reduce stale attempts, while the
gate's atomic expected-snapshot check closes the remaining installation race.

A changed gate revision can permanently invalidate already signed proposals while
their target-epoch locks remain. A returning majority alone does not remove this
limitation. The system retains votes and blocks rather than signing another proposal
for that target epoch. See [recovery retry contract and failure tests](RECOVERY_RETRY.md)
for the chosen safe strategy, container validation command and future protocol options.
