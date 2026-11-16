# Docker evaluation results

## Integrated deployment: CraneLock-only checks

Validation date: 12 November 2026 (Asia/Singapore).

Nine Docker workload runs: three scenarios, seeds 7/20/3104, 18 requests each. All 162 requests completed; no overlapping lane entries were detected. CSV counts, physical entry/exit events, fault timestamps, epoch changes, container identities and restart command outcomes were checked. The final live check passed in CENTRAL mode at epoch 10, revision 114, with peers AVAILABLE.

| Scenario | Runs passed | Requests completed | Reported timing | Median | Observed range |
| --- | ---: | ---: | --- | ---: | ---: |
| Healthy traffic | 3/3 | 54/54 | Entire batch completion | 6.427 s | 6.221 to 6.564 s |
| Controller failure | 3/3 | 54/54 | Kill-command completion to next entry | 0.504 s | 0.354 to 0.600 s |
| Token-holder failure | 3/3 | 54/54 | Kill-command completion to next entry | 2.323 s | 1.978 to 5.463 s |

The failure timing includes demand/clearance waiting and is not pure detection or certificate-install time. Holder-failure observations vary from roughly 1.98 to 5.46 seconds; three successful repeats do not establish a fixed recovery guarantee. Healthy requests arrive together; failure requests are spread across 30 seconds. Their waiting averages are not directly comparable.

The controller is stopped before each holder-loss experiment to establish LOCAL mode. The current holder is then killed, and surviving agents recover without a runner-generated replacement. Each experiment restarts only the containers it stopped. State volumes and epochs are retained between runs; initial holder/epoch and request sequence can therefore differ. These are repeated functional observations, not independent fresh-deployment trials.

The gate/controller/agents are actual containers; lane movement remains synthetic and grant requests are driven by actor-specific host clients. These nine runs verify deployed CraneLock only. The separate paired Docker evaluation below compares the baselines; these two datasets and the historical local-process comparisons are never pooled.

## Individual results

| Seed | Scenario | Completed | Batch time | Next entry after command | Evidence folder |
| ---: | --- | ---: | ---: | ---: | --- |
| 7 | Healthy traffic | 18/18 | 6.564 s | n/a | `run/deployed-evaluation/repeated-20261112-222840/seed-7/normal` |
| 20 | Healthy traffic | 18/18 | 6.427 s | n/a | `run/deployed-evaluation/20261112-222105-839498` |
| 3104 | Healthy traffic | 18/18 | 6.221 s | n/a | `run/deployed-evaluation/repeated-20261112-222840/seed-3104/normal` |
| 7 | Controller failure | 18/18 | n/a | 0.600 s | `run/deployed-evaluation/repeated-20261112-222840/seed-7/controller-outage` |
| 20 | Controller failure | 18/18 | n/a | 0.504 s | `run/deployed-evaluation/20261112-222221-859421` |
| 3104 | Controller failure | 18/18 | n/a | 0.354 s | `run/deployed-evaluation/repeated-20261112-222840/seed-3104/controller-outage` |
| 7 | Token-holder failure | 18/18 | n/a | 5.463 s | `run/deployed-evaluation/repeated-20261112-222840/seed-7/holder-failure` |
| 20 | Token-holder failure | 18/18 | n/a | 1.978 s | `run/deployed-evaluation/20261112-222340-989536` |
| 3104 | Token-holder failure | 18/18 | n/a | 2.323 s | `run/deployed-evaluation/repeated-20261112-222840/seed-3104/holder-failure` |

## Evidence and reproduction

The aggregate JSON/CSV and summary hashes are in `run/deployed-evaluation/repeated-20261112-222840`. Each original folder retains its manifest, request CSV, physical/fault events, cumulative container logs and restoration outcomes. These raw artifacts are local under ignored `run/`; this report does not publish them or credentials.

See [deployed workload commands](EVALUATION.md#test-the-integrated-containers). The original seed-20 runs were retained unchanged; seeds 7 and 3104 were run sequentially afterward, with a live CENTRAL/idle check before each case. No experiment was reinitialised to obtain these results.

## Paired Docker comparison

Validation date: 13 November 2026 (Asia/Singapore). All **27 expected-behavior checks passed**: three models, three scenarios and seeds 7/20/3104. Expected stalls pass their check but remain unsuccessful service outcomes in the graphs. Across 486 requests, 393 completed within the applicable measurement policy; no overlapping physical entries were detected.

Every case used five fresh containers, the same pinned image, 1 CPU/256 MiB per service, 0.5-second heartbeats, a 2-second detection timeout, 18 identical seeded requests and 0.1-second synthetic crossings. Healthy batches ran to completion; failures used a 30-second window. Model order was shuffled per seed. Gate/controller-heartbeat traffic used development mutual TLS; peer and test-assignment RPCs used the private test bridge.

| Scenario | Model | Completed across three runs | Median reported time | Observed time range |
| --- | --- | ---: | ---: | --- |
| Healthy batch | Central lock | 54/54 | 7.838 s | 4.801 to 8.190 s |
| Healthy batch | Basic token | 54/54 | 11.109 s | 8.342 to 11.544 s |
| Healthy batch | CraneLock | 54/54 | 5.261 s | 5.091 to 8.271 s |
| Controller stopped | Central lock | 18/54 | Did not resume | No next entry |
| Controller stopped | Basic token | 53/54 | 0.332 s | 0.164 to 0.596 s |
| Controller stopped | CraneLock | 53/54 | 1.677 s | 1.573 to 1.953 s |
| Token holder stopped | Central lock | 54/54 | 0.593 s | 0.262 to 0.873 s |
| Token holder stopped | Basic token | 0/54 | Did not resume | No next entry |
| Token holder stopped | CraneLock | 53/54 | 4.137 s | 2.630 to 4.483 s |

Healthy timing is full-batch completion. Failure timing is kill-command completion to the next entry, including remaining detection, coordination and demand/clearance waiting. Basic token's controller-outage time measures continuing service, not recovery: it was already LOCAL. No cross-container monotonic clocks were subtracted.

CraneLock admitted **11 to 12 more requests per run** after controller-kill initiation than central lock, and completed **17 to 18 more requests per run** after holder failure than basic token. Both token models had occasional requests finish after the 30-second cutoff; those late exits remain in the CSV and are excluded from the table's failure completion totals.

The healthy median bars alone are misleading about relative speed. Comparing each matching seed, CraneLock minus central-lock batch time was +0.081 s, -2.577 s and +0.290 s. The median paired difference was **+0.081 s**, with a wide observed range. These three repeats do not establish a consistent healthy speed advantage. The demonstrated benefit is continued coordination across both tested failure types.

CENTRAL scheduling uses the team's `CentralController` through the existing test-only assignment RPC adapter in its own container. The actual agents handle LOCAL token transfer and majority recovery. The host supplies FIFO demand, signatures and synthetic occupancy, so measurements include driver overhead and do not establish concurrent-load throughput or physical-port safety. This experiment deployment is separate from the integrated monitoring stack.

Evidence is retained at `run/evaluation/docker-comparison-validated`: per-case manifests/container IDs, workload hashes, request CSVs, physical/protocol events, gate audit events, logs, persisted votes and cleanup outcomes. `verification.json` checks all 27 cases, matched inputs/image/resources, CSV/event counts, entry/exit ordering, fault-to-entry calculations and majority vote records. Original integrated services remained healthy in CENTRAL epoch 10, revision 114; no experiment containers or networks remained.

Initial setup failures and the earlier 0.5-second RPC-deadline failure are retained under `run/evaluation/docker-development-*` and excluded from the final matched matrix. The final matrix used the same 2-second host RPC deadline and deployed heartbeat interval for every model. The dashboard selects this Docker comparison by default and keeps development and historical datasets separate.

Reproduce with [Docker comparison commands](EVALUATION.md#compare-all-three-systems-in-docker), or verify the saved matrix with `python -m cranelock_sim.docker_comparison --audit run/evaluation/docker-comparison-validated`. Raw evidence remains local under ignored `run/`; this report contains no credentials.
