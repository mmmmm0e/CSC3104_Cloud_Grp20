# CraneLock integration and evaluation status

Date: 31 October 2026, Asia/Singapore.

The local prototype implements the proposal's central scheduling, token coordination,
strict-majority signed recovery, epoch/revision fencing, independent physical occupancy
checks and safe controller return. Opt-in background workers now connect suspicion to
certificate installation and signed reconciliation. The live yard retains outstanding
requests across revoked grants and skips stopped requesters while preserving their
requests for restart.

## Validation evidence

Evidence directory: `run/validation-20261031-194129-4f1be27c/`.
The initial batch used `scripts/validate_project.py --skip-tests`; full regression
tests and the delay sweep were run separately in the same evidence directory.
`validation.json` describes the initial batch, `tests-final.xml` records the final
test run, and `delays/summary.json` records the additional delay sweep.

| Check | Observed result |
| --- | --- |
| Final regression suite | 249 passed, 2 skipped in 149.88 s. Skips: unavailable Docker CLI and optional browser test dependency. |
| Automatic lifecycle | Nine checks passed: controller outage, LOCAL recovery, holder replacement, stale permission rejection, occupancy blocking, CENTRAL return and queued entry after clearance. |
| Existing combined progress demo | All nine scenarios passed, with zero detected safety violations. |
| Independent processes | Real agent CLI and controller processes recover and return without direct integration-caller recovery calls. |
| Reference comparisons | 54 runs across three systems, three scenarios, two request loads and three seeds; zero detected safety violations. |
| Logical scaling | 27 runs across 3/5/10 cranes, arrival rates 2/5/10 and three seeds; zero detected safety violations. |
| Delayed automatic cycles | Six runs with peer delivery delay and jitter; all passed with zero detected safety violations. |
| Delivery reordering | A test reverses two concurrent real gRPC requests and checks the resulting queue and unique ownership. |

Raw evidence is retained locally and ignored by Git. Re-run validation on the target
machine to create a fresh portable evidence directory; the test XML includes skipped
checks. A passing scenario means its assertions held, not that all requests completed
inside every finite observation window.

## Evaluation findings

Reference experiments used seeds 7, 20 and 3104, request loads 6 and 18, five-second
failure windows, three agents and a shared gate on one Windows host. Healthy batches
ran to completion. Paired differences below are medians over the three seeded pairs.

| Paired observation | 6 requests | 18 requests |
| --- | ---: | ---: |
| Extra new entries after controller outage, CraneLock vs central lock | 4 | 12 |
| Extra completed crossings after holder failure, CraneLock vs basic token | 6 | 17 |
| Healthy mean wait difference, CraneLock minus central lock | +21.56 ms | +36.45 ms |
| Healthy instrumented RPC attempts per completion difference, CraneLock minus central lock | +4.00 | +5.28 |

The basic token baseline completed no requests after holder failure in these traces.
The central lock baseline issued no new entries after its controller outage. Basic
token coordination already operates locally and therefore also continues through a
controller outage; CraneLock's additional benefit is recovery from holder failure.
Some requests remain unfinished at the fixed failure-window boundary; they are not
silently counted as completed or treated as healthy-batch failures.

The six actual-controller automatic cycles used 5 ms + uniform 0–15 ms peer jitter,
or 10 ms + uniform 0–20 ms jitter, with the same three seed values. Observed ranges:

| Automatic transition | Observed elapsed time |
| --- | ---: |
| Controller failure to LOCAL installation | 1.709–1.855 s |
| Holder failure to replacement installation | 2.060–2.094 s |
| Controller restoration to return reconciliation | 0.525–1.113 s |

These timings include detection and the recovery worker's settling period. They
are six bounded local observations, not a service-level guarantee. Delay draws are
seeded, but OS scheduling and real RPC/disk timing vary.

The reference comparison uses its existing experimental controller and adapter-driven
fallback. Its metrics are not measurements of the automatic team-controller cycle.
Logical scaling exercises the signed gate and workload queue with simulated time;
it excludes distributed token, network and recovery overhead. Increasing crane count
in those runs does not establish distributed protocol scalability.

## Remaining acceptance work

- Run the complete deployment on the team's Docker/Kubernetes environment, including
  separate service identities, persistent volumes, probes, service discovery and
  Prometheus/Grafana. Docker and kubectl were unavailable in this session. Existing
  historical gate/container results do not validate the new automatic integration.
- Validate authenticated remote transport and real container/network fault injection.
  The new independent-process CLI example is restricted to loopback.
- Complete distributed scaling and any unmeasured evaluation requirements, including
  holder-failure RPC overhead and aggregate safety-stop/availability measurements.
- Incorporate these results and the [safety argument](RECOVERY_SAFETY.md) into the
  group's final report, then record the [short demonstration](DEMO_SCRIPT.md).

The protocol uses one durable vote per target epoch. Divergent failure views can
split votes and prevent progress even after communication recovers. It remains
fail-closed in that case; it does not erase votes. This availability limit must be
stated in the report. The implementation combines established mechanisms for the
crane-lane model; these observations do not establish a world-first algorithm.

## Reproduce

```powershell
.\.venv\Scripts\python.exe scripts/validate_project.py
```

This creates fresh logs, test XML, summaries, request CSVs and gate audits. To run only
functional checks, use `--skip-evaluation`. To show the automatic application, use:

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.live --automatic --port 8767
```

Open `http://127.0.0.1:8767/live`. No new commits, pushes or merges are performed by
the validation commands.
