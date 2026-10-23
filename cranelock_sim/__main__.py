import argparse
import asyncio
import json
from datetime import datetime

from .experiment import Experiment, run_experiment


def main():
    parser = argparse.ArgumentParser(description="Run lane workload, agent or recovery experiments")
    parser.add_argument("--cranes", type=int, default=3)
    parser.add_argument("--mode", choices=["logical", "agents", "recovery", "partition", "restart", "compare", "benefits"], default="logical",
                        help="Logical workload, normal agents, holder failure, partition or restart")
    parser.add_argument("--scenario", choices=["all", "before-entry", "during-occupancy",
                        "majority-before-entry", "majority-during-occupancy", "no-majority", "normal", "holder-failure", "controller-outage"],
                        default=None, help="Recovery/partition/restart mode only (default all)")
    parser.add_argument("--detection-timeout", type=float, default=None,
                        help="Recovery mode: heartbeat timeout in seconds (default 0.5)")
    parser.add_argument("--clearance-delay", type=float, default=None,
                        help="Recovery mode: explicit removal delay after blocked entry (default 0.1)")
    parser.add_argument("--rounds", type=int, default=None,
                        help="Agent mode only: A/B/C crossings per round (default 2)")
    parser.add_argument("--duration", type=float, default=None,
                        help="Logical: simulated seconds (default 60); compare: window (default 2); benefits: failure window only (default 3), healthy batches run to completion")
    parser.add_argument("--arrival-rate", type=float, default=2,
                        help="Aggregate requests/second across all cranes")
    parser.add_argument("--crossing-time", type=float, default=None)
    seeds = parser.add_mutually_exclusive_group()
    seeds.add_argument("--seed", type=int, default=None)
    seeds.add_argument("--seeds", default=None, help="Compare mode: comma-separated integer seeds (default 7,20,3104)")
    parser.add_argument("--requests", type=int, default=None, help="Compare mode: requests per trace (default 6)")
    parser.add_argument("--loads", default=None, help="Benefits mode: comma-separated request counts (default 6,18)")
    parser.add_argument("--no-plots", action="store_true", help="Compare mode: CSV/JSON only")
    parser.add_argument("--output", default=None, help="New directory; existing paths are refused")
    args = parser.parse_args()
    try:
        output = args.output or datetime.now().strftime("run/evaluation/%Y%m%d-%H%M%S-%f")
        if args.mode not in {"compare", "benefits"} and (args.seeds is not None or args.requests is not None or args.no_plots):
            parser.error("seeds, requests and no-plots apply only to compare mode")
        if args.loads is not None and args.mode != "benefits":
            parser.error("loads applies only to benefits mode")
        if args.mode not in {"recovery", "partition", "restart", "compare", "benefits"} and any(value is not None for value in
                (args.scenario, args.detection_timeout, args.clearance_delay)):
            parser.error("scenario, detection-timeout and clearance-delay apply only to recovery/partition/restart mode")
        if args.mode == "benefits":
            from .benefits import run_benefits
            if args.cranes != 3 or args.arrival_rate != 2 or args.rounds is not None or args.clearance_delay is not None or args.requests is not None:
                parser.error("Benefits uses three agents and --loads; rounds, arrival-rate, clearance-delay and requests do not apply")
            summary = asyncio.run(run_benefits(output,
                seeds=tuple(int(v.strip()) for v in args.seeds.split(",")) if args.seeds else (args.seed,) if args.seed is not None else (7,20,3104),
                loads=tuple(int(v.strip()) for v in args.loads.split(",")) if args.loads else (6,18),
                scenario=args.scenario or "all", duration=args.duration if args.duration is not None else 3,
                crossing_time=args.crossing_time if args.crossing_time is not None else .03,
                detection_timeout=args.detection_timeout if args.detection_timeout is not None else .25,
                plots=not args.no_plots,
                progress=lambda message: print(message, flush=True)))
        elif args.mode == "compare":
            from .comparison import run_comparison
            if args.cranes != 3 or args.arrival_rate != 2 or args.rounds is not None or args.clearance_delay is not None:
                parser.error("Compare uses three agents and fixed-count burst traces; rounds, arrival-rate and clearance-delay do not apply")
            comparison_seeds = (tuple(int(value.strip()) for value in args.seeds.split(","))
                                if args.seeds is not None else (args.seed,) if args.seed is not None else (7, 20, 3104))
            summary = asyncio.run(run_comparison(output, seeds=comparison_seeds,
                scenario=args.scenario or "all", requests=args.requests if args.requests is not None else 6,
                duration=args.duration if args.duration is not None else 2,
                crossing_time=args.crossing_time if args.crossing_time is not None else 0.03,
                detection_timeout=args.detection_timeout if args.detection_timeout is not None else 0.25,
                plots=not args.no_plots, progress=lambda message: print(message, flush=True)))
        elif args.mode in {"recovery", "partition", "restart"}:
            from .failures import run_recovery_suite, run_partition_suite, run_restart_suite
            if (args.cranes != 3 or args.duration not in (None, 60) or args.arrival_rate != 2
                    or args.seed not in (None, 20) or args.rounds is not None):
                parser.error("Recovery/partition/restart mode uses a fixed three-agent scenario; "
                             "workload settings and rounds do not apply")
            runner = {"recovery": run_recovery_suite, "partition": run_partition_suite,
                      "restart": run_restart_suite}[args.mode]
            summary = asyncio.run(runner(output, scenario=args.scenario or "all",
                detection_timeout=args.detection_timeout if args.detection_timeout is not None else 0.5,
                clearance_delay=args.clearance_delay if args.clearance_delay is not None else 0.1,
                crossing_time=args.crossing_time if args.crossing_time is not None else 0.05,
                progress=lambda message: print(message, flush=True)))
        elif args.mode == "agents":
            from .agents import run_agent_demo
            if args.cranes != 3 or args.duration not in (None, 60) or args.arrival_rate != 2 or args.seed not in (None, 20):
                parser.error("Agent mode uses exactly 3 cranes and a scripted workload; "
                             "duration, arrival-rate and seed apply only to logical mode")
            summary = asyncio.run(run_agent_demo(output,
                rounds=args.rounds if args.rounds is not None else 2,
                crossing_time=args.crossing_time if args.crossing_time is not None else 0.1))
        else:
            if args.rounds is not None:
                parser.error("--rounds applies only to agent mode")
            settings = Experiment(args.cranes, args.duration if args.duration is not None else 60, args.arrival_rate,
                                  args.crossing_time if args.crossing_time is not None else 0.5,
                                  args.seed if args.seed is not None else 20)
            summary = run_experiment(settings, output)
    except (ValueError, FileExistsError) as error:
        parser.error(str(error))
    print(json.dumps({key: value for key, value in summary.items() if key != "results"}, indent=2))
    print(f"Results saved to {output}")


if __name__ == "__main__":
    main()
