# Platform validation

Date: 21 October 2026
Environment: macOS, Python 3.12.14

- Automated tests: 64 passed.
- Gate Docker image: built successfully.
- Container demo: all 11 expected outcomes observed.
- Container health after restart: healthy.
- Restart preserved epoch 3 and holder crane-2.
- Restart changed session, invalidated the grant and marked the lane UNKNOWN.

Evidence: gate-events.jsonl and gate-events-after-restart.jsonl.

These initial standalone results are supplemented by the milestones below.

## Containerised crane agents

- Built cranelock-agent:0.1.0.
- Started three agents with separate persistent volumes.
- Verified peer heartbeat communication.
- Verified token transfer from crane-a to crane-b.
- Gracefully stopped crane-b while it held the token.
- crane-a suspected crane-b approximately 2.32 seconds after
  its last heartbeat.
- crane-a retained epoch 1 and did not create a replacement token.
- Majority recovery and restart reconciliation were not exercised in this initial
  standalone milestone.

## Standalone agent restart

- Restarted crane-b using its existing persistent volume.
- Retained epoch 1 and remembered holder crane-b.
- Entered REJOINING with has_token=false, preventing reuse of saved ownership.
- Peer communication recovered after initial suspicions.
- This check did not exercise authoritative reconciliation or resumed lane use.

## Gate monitoring

- Added Prometheus and Grafana to the gate Compose deployment.
- Verified the Prometheus gate target was UP and gate availability reported 1.
- Created panels for availability, unknown occupancy, grants and accepted entries.
- Ran the development demo against the monitored gate; Grafana displayed two
  grants and one accepted entry.
- Exported the dashboard to deploy/grafana/cranelock-monitoring.json.
- Data-source and dashboard provisioning remain manual. Monitoring currently
  covers the gate, not the complete system.

## Integrated Docker deployment and controller lifecycle

Validation date: 10 November 2026. Environment: macOS, Python 3.12.14,
Docker Desktop and Docker Compose.

- Created a separate cranelock-integrated Compose project with fresh gate and
  agent volumes, preserving earlier experiment storage.
- Configured the trusted gate, native controller and three gate-enabled,
  recovery-enabled agents with matching crane-1/2/3 membership.
- Mounted an individual signing-key file for each agent and the controller.
  Development TLS client identities are shared per destination service; this
  deployment does not establish per-member TLS identity binding. Peer-agent RPCs
  remain plaintext on the Compose network.
- Verified mutual-TLS connections to the gate and controller heartbeat service.
- Explicitly bootstrapped CENTRAL authority at epoch 0, revision 1, holder crane-1.
  Lane occupancy remained UNKNOWN; bootstrap did not grant entry or clearance.
- Verified all agents converged to CENTRAL authority, reported their peers
  AVAILABLE and assigned usable token ownership only to crane-1.

### Launcher and test compatibility fixes

- Removed an access to the undefined enable_recovery launcher argument.
- Updated the recovery CLI test to use recovery-config, recovery-key and
  per-agent signing files.
- Separated subprocess stdout JSON events from stderr diagnostics in the
  gate/agent restart test.
- Full automated suite before the reconnect change: 330 passed, 2 skipped.

### Controller return instability and reconnect change

The first controller stop triggered LOCAL fallback at epoch 1, revision 2 while
preserving crane-1 as holder. After controller return, agents reconnected at
different times. Connection-refused errors coincided with repeated LOCAL/CENTRAL
transitions before authority settled at epoch 67, revision 135. The controller's
current and DNS-resolved addresses both matched the failed address, so an address
change was not established. This initial run is not a clean controller-return pass.

Bounded the controller heartbeat client's gRPC reconnect settings to an initial
backoff of 500 ms, a minimum reconnect setting of 1000 ms and a maximum backoff of
2000 ms, including explicitly recreated channels. Delayed reconnect backoff was a
working diagnosis, not a conclusively isolated root cause.

- After the change, the controller, fallback, controller-launcher and
  agent-launcher tests reported 112 passed.
- Rebuilt the shared agent image and recreated the three agent containers while
  retaining their persistent volumes. Agents reconciled to epoch 67, revision 135.
- Repeated a controller outage lasting more than 60 seconds. All agents adopted
  LOCAL mode at epoch 68, revision 136, preserving holder crane-1.
- Started the controller again. All agents adopted CENTRAL mode at epoch 68,
  revision 137, with only crane-1 owning the token.
- A subsequent observation roughly 55 seconds later showed unchanged authority
  and all peers AVAILABLE. No further authority churn was observed in that interval.
- No precise controller-return latency was measured. These observations support
  stable restoration in this retest, not a general guarantee for every network fault.

Local evidence files:

- run/platform-validation/controller-return.log
- run/platform-validation/controller-return-bounded-retry.log

These logs are retained locally under the ignored run directory. The later export
contains cumulative container history, including earlier events, so use event
timestamps and authority versions to distinguish the retest.

## Reproducible container handoff

Validation date: 11 November 2026.

- Added scripts/integrated.py for non-overwriting development provisioning,
  explicit new-volume initialization/bootstrap, build/start, state inspection,
  endpoint discovery, evidence export and service stop/start/restart/SIGKILL.
- Added docs/PLATFORM_HANDOFF.md with first-time and existing-experiment commands,
  TLS/signing paths, actor boundaries, workload integration and shutdown steps.
- Published loopback-only controller port 50062 and agent ports 50101/50102/50103,
  alongside gate port 50061 and metrics port 9190.
- Saved existing logs before recreating controller/agent containers; retained
  the existing configuration and named state volumes. The gate was not reset.
- Verified host agent RPCs for all three agents, gate mutual TLS and native
  controller heartbeat mutual TLS using scripts/integrated.py check.
- All agents agreed with gate authority at epoch 69, revision 139, CENTRAL mode,
  holder crane-1, with all peers AVAILABLE and only crane-1 owning the token.
  This is a new deployment observation, not a replay of the epoch-68 return test.
- The provisioning test verifies actor-only private signing files match public
  configuration and rejects overwriting an existing experiment.
- Final full automated suite, including the reconnect change and handoff test:
  331 passed, 2 skipped in 69.50 seconds. Socket-dependent tests required execution
  outside the restricted sandbox; sandbox-only failures were not protocol results.
- Current helper/host-port checks used the existing local experiment. The entire
  first-time provisioning-to-launch path has not been exercised on another machine.

Final local logs: run/platform-validation/integrated-1794344006591403000.log.
No physical lane crossings were exercised. Darius's current simulator modes still
create their own services; an external-endpoint workload adapter is required.

## Platform completion validation

Validation date: 11 November 2026.

- Added optional agent gauges for epochs, ownership, modes, queues and suspicions,
  local recovery event counters and local detection/install histograms; added
  native controller heartbeat metrics. Instrumentation does not grant authority.
- Added the integrated monitoring Compose overlay and versioned Grafana data source,
  dashboard provider and dashboard. All five Prometheus targets were UP. Verified
  the provisioned dashboard through the Grafana API and observed controller DOWN
  when its container stopped.
- Added service-specific stop/start/restart/SIGKILL controls. Corrected a discovery
  from validation: Compose start could start dependencies, invalidating a LOCAL
  restart experiment. Fault controls now target exactly one container via Docker.
- Added timed peer firewall partitions and netem packet delay/jitter/reordering,
  with bounded duration and cleanup. Packet reordering is distinct from application
  delivery ordering through TCP.
- All-isolated peer fault check passed: after draining in-flight proposals, no
  authority change occurred in a five-second fully disconnected observation
  window; all agents suspected both peers. Network rules were removed afterward.
- Added a targeted application-level message-reordering check: hold a signed grant
  request, install a newer legal controller authority revision, then release the
  old signed request. The gate returned STALE_REVISION.
- Added an explicit new-empty-model missing-occupancy-update test; after one fresh
  observation and omission past the configured timeout, a new grant was denied
  with LANE_UNKNOWN. No real occupied lane was cleared by this test.
- Final serial container validation passed controller loss/return, LOCAL holder
  SIGKILL, majority replacement, stale-epoch fencing, old-holder restart/rejoin,
  gate outage/session invalidation, monitoring DOWN visibility, majority recovery
  across an actual peer network partition, netem cleanup and missing updates.
- Added a dedicated local kind cluster, public Kustomize resources, ConfigMap/Secret
  provisioning, three one-replica StatefulSets with independent volumeClaimTemplates,
  Services, gate PVC/Recreate, explicit initialization/bootstrap Jobs and probes.
- Kubernetes validation replaced crane-2's pod, retained its PVC UID and observed
  REJOINING followed by reconciliation. With peers paused for planned maintenance,
  gate pod replacement preserved the exact authority tuple, changed its session,
  invalidated grants and retained UNKNOWN occupancy. Resumed services converged;
  five Kubernetes scrape targets were UP and Grafana was healthy.
- The Kubernetes check used a single local node and local/block-backed storage.
  It does not establish node-loss resilience, network-policy enforcement, or a
  replicated-gate availability guarantee.
- A minimal external-endpoint smoke runner completed an actual container token
  handoff (crane-1 to crane-2) followed by one synthetic physical entry and exit.
  It used the existing serialized Lane boundary, ended at epoch 104/revision 205
  and left occupancy UNKNOWN with no grant after observation expiry. This is one
  scripted crossing, not a full workload/scalability result.
- Isolated an older manual-certificate unit test from its automatic recovery worker:
  two independent installers had raced the same certificate/snapshot. Automatic
  recovery remains covered by its own tests and the container validation.
- Final full regression result: **332 passed, 2 skipped in 68.71 seconds**.
- Added platform implementation/report text and a five-page deployment instructions
  PDF; every rendered page was visually inspected.

Evidence retained locally (the ignored run directory is not a shared artifact store):

- run/platform-validation/container-validation-1794345207771367000.json
- run/platform-validation/container-validation-1794345310799556000.json
- run/platform-validation/kubernetes-validation-1794344940111758000.json
- run/platform-validation/external-smoke-1794345535728272000.json
- run/platform-validation/container-validation-1794345787483397000.json
- run/platform-validation/fault-events.jsonl
- run/platform-validation/final-pytest.log

Successful JSON summaries and the final test result are also copied into
`docs/evidence/platform/` for team review; they contain no credentials.

After evidence export, both integrated and older standalone Compose projects were
stopped. Kubernetes workloads were scaled to zero and the dedicated kind node was
stopped. No Docker containers remained running; experiment volumes/PVCs and the
local node filesystem were retained. Docker Desktop itself was not uninstalled.

Earlier failed validation attempts remain in that directory and are not counted
as passes. Historical logs include multiple attempts; use timestamps, authority
versions and the named successful summaries above to identify the final evidence.

## Remaining shared project work

Merge verification on 11 November 2026: preserved main's --enable-recovery alias
alongside explicit recovery-config/recovery-key, and tested both CLI styles.
The two-process normal-operation demo now allows three seconds for initial peer
connection backoff before its one-shot request; handoff and failure-detection
assertions are unchanged. The merged main suite reported **342 passed, 2 skipped
in 89.21 seconds**. The earlier 332-test result above describes the pre-merge
platform branch. See docs/evidence/platform/merged-main-test-results.txt.

The platform deliverables now have local implementation and validation. A teammate
must still follow the first-time instructions on their own machine for handoff
acceptance. Darius and component owners must connect the full workload generator,
collect physical traces under sustained demand and produce performance/scalability
results. The existing cranelock_sim modes continue to create their own services;
use external_smoke.py as an endpoint-integration example, not a replacement for
those experiments. Submission report/video assembly and peer review remain team
work. The isolated deployment results are not an industrial safety certification.


## Evaluation adapter handoff: 12 November 2026

The Darius branch now preserves its independent-process comparison runner while
integrating this platform work. `scripts/external_workload.py` supplies sustained
healthy, controller-outage and LOCAL holder-loss demand to the external endpoints.
Its signed-gate contract and credential-isolation checks do not establish Docker
runtime performance. The tester workstation has no Docker installation, so run
the [deployed workload commands](EVALUATION.md#test-the-integrated-containers) on a
Docker-enabled machine and retain the resulting evidence separately. The existing
54 dashboard comparisons still describe their original local-process environment.
