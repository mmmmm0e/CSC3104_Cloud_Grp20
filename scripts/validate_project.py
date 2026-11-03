import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid


# runs checks sequentially and retains commands, results and failure logs
def validate(output, *, tests=True, evaluation=True):
    root = Path(__file__).resolve().parents[1]
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    checks = []
    record = dict(created_at=datetime.now(timezone.utc).isoformat(), passed=False,
        tests_requested=tests, evaluation_requested=evaluation, checks=checks,
        scope="Local services and reference comparisons. Logical scaling excludes network and distributed recovery costs.",
        docker_cli_available=shutil.which("docker") is not None,
        kubectl_available=shutil.which("kubectl") is not None, deployment_validated=False)

    # preserves each native command's actual exit status
    def run(label, arguments):
        command = [sys.executable, *map(str, arguments)]
        started = time.monotonic()
        print(f"Running {label}", flush=True)
        with (output / f"{label}.log").open("x", encoding="utf-8") as log:
            process = subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace")
            try:
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    print(line, end="", flush=True)
                code = process.wait()
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=10)
                process.stdout.close()
        checks.append(dict(name=label, command=command, exit_code=code,
                           elapsed_seconds=round(time.monotonic() - started, 3)))
        if code:
            raise RuntimeError(f"{label} failed; inspect {output / (label + '.log')}")

    try:
        if tests:
            run("tests", ["-m", "pytest", "-q", "-p", "no:cacheprovider",
                f"--basetemp={output / 'pytest-temp'}", f"--junitxml={output / 'tests.xml'}"])
        run("automatic", ["-m", "cranelock_sim.automatic_demo", "--output", output / "automatic"])
        run("progress", ["-m", "cranelock_sim.progress_demo", "--with-controller", "--output", output / "progress"])
        if evaluation:
            run("delays", ["-m", "cranelock_sim.automatic_demo", "--delay-sweep", "--output", output / "delays"])
            run("benefits", ["-m", "cranelock_sim", "--mode", "benefits", "--seeds", "7,20,3104",
                "--loads", "6,18", "--duration", "5", "--no-plots", "--output", output / "benefits"])
            for count in (3, 5, 10):
                for rate in (2, 5, 10):
                    for seed in (7, 20, 3104):
                        label = f"logical-{count}-{rate}-{seed}"
                        run(label, ["-m", "cranelock_sim", "--mode", "logical", "--cranes", count,
                            "--arrival-rate", rate, "--seed", seed, "--duration", "30", "--output", output / label])
        record["passed"] = True
    finally:
        (output / "validation.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
        print(f"Evidence: {output}", flush=True)
    return record


# provides one command for the reproducible local validation package
def main():
    parser = argparse.ArgumentParser(description="Validate CraneLock and retain all local evidence")
    parser.add_argument("--skip-tests", action="store_true")
    parser.add_argument("--skip-evaluation", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or Path("run") / ("validation-" + datetime.now().strftime("%Y%m%d-%H%M%S")
        + "-" + uuid.uuid4().hex[:8])
    validate(output, tests=not args.skip_tests, evaluation=not args.skip_evaluation)


if __name__ == "__main__":
    main()
