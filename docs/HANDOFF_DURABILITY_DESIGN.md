# Gate handoff durability and limits

The Gate remains the only authority for the holder, epoch and revision. A peer
heartbeat is a refresh signal and a peer transfer is never sufficient authority.
The sender must persist accepted requests and the exact signed-command fields,
request ID, destination and remaining FIFO before sending a Gate command. It
must remain suspended while the command may be in flight, including after a
restart, and retry the identical command to obtain the Gate's durable decision.
The Gate's current state still fences any delivery; a historical success alone
must never authorize a stale transfer.

New state files also record that they belong to a Gate-enabled agent. Starting
one without Gate configuration is rejected before its outbox can be rewritten.
Legacy files without this marker cannot always be distinguished from standalone
state, so deployments must keep the documented one-file-per-agent configuration.

After a committed command, the sender retains the original notification and
queue durably until the receiver has durably recorded them and acknowledged it.
The receiver must not forward the token before that record exists. The Gate
now durably includes an optional `authority_transition` in its state response,
written in the same transaction as a transfer or installation. It identifies
the operation, authority tuple, actor and request ID. A receiver waits for a
notification matching a Gate-recorded transfer, even when its configured holder
already names itself. Controller installations are distinguishable and do not
require a peer notification unless an earlier delivery was already unresolved.
The Gate also retains the most recent handoff identity across later authority
installations, so an agent which missed both events still waits for that receipt.

Legacy Gate states and older servers may omit these fields; no database reset is
required, and a new transition populates them. For those responses,
an agent that observes a same-epoch holder change to itself conservatively waits;
some controller-directed changes can consequently stall. The fallback cannot
detect an unseen handoff when the configured or saved holder already names the
receiver. The marker does not bind the queue contents, and peer RPC identities
are not authenticated by this protocol. A complete barrier needs Gate-verified
queue identity or a separately reviewed authenticated/replicated protocol.
Deploy the updated Gate and all agents together: older agents do not enforce the
receiver barrier and can forward a token before the queue is delivered.
An epoch-changing recovery is independently certified and does not by itself
prove delivery of an earlier queue. A still-current holder may preserve its own
already-durable queue across such a change.

If Gate authority moves past an unresolved command or notification, the sender
must not replay a stale notification or silently discard its saved requests. It
retains an observable, blocked handoff for operator/protocol resolution. This
is an availability tradeoff, not automatic reconciliation of the old queue.

This local outbox only protects an agent that returns with its state file. It
does **not** provide an end-to-end durable queue if the sender and its storage
are permanently unavailable. A controller installation can also reassign the
holder without transferring its previously accepted queue; the Gate does not
record that queue. That stronger guarantee requires a separately
reviewed protocol: for example, replicate the queue and handoff decision before
acknowledging requests, or make requesters durably retry stable request IDs
until completion. Requester retry alone does not automatically preserve global
FIFO or exactly-once workload execution. A Gate-owned durable queue/receipt
barrier is another option, but requires an explicit Gate contract and migration;
it is not implemented by the local outbox.
