"""Loopback results viewer with optional controls for an explicitly started live yard."""
import argparse
import csv
import json
import math
import mimetypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


ARTIFACTS = {
    "summary.json", "experiment.json", "comparison.csv", "requests.csv", "events.jsonl",
    "gate-events.jsonl", "recovery-certificate.json", "gate-config.json", "failure.json",
    "completed.png", "completed.svg", "p95_wait_seconds.png", "p95_wait_seconds.svg",
    "recovery_latency_seconds.png", "recovery_latency_seconds.svg", "benefits.csv",
    "benefit_summary.png", "benefit_summary.svg", "workload.json", "restoration.json",
}
MAX_FILE_BYTES = 25 * 1024 * 1024


def family(model):
    if model in {"central-lock", "basic-token", "cranelock"}:
        return "comparison"
    if model == "holder-restart-rejoin":
        return "restart"
    if model == "peer-network-partition":
        return "partition"
    if model == "holder-service-stop-recovery":
        return "recovery"
    if model == "three-agent-grpc-normal-handoff":
        return "agents"
    return "logical" if model == "local-gate-normal-traffic" else "other"


class Results:
    def __init__(self, root):
        self.root = Path(root).resolve()

    def directory(self, identity):
        if not identity:
            raise ValueError("Choose a saved result")
        path = (self.root / identity).resolve()
        if not path.is_relative_to(self.root) or not path.is_dir():
            raise ValueError("Result is outside the results folder or does not exist")
        return path

    def safe_file(self, directory, name):
        path = (directory / name).resolve()
        if not path.is_relative_to(self.root) or not path.is_file():
            raise FileNotFoundError(name)
        if path.stat().st_size > MAX_FILE_BYTES:
            raise ValueError("This artifact is too large for the viewer (25 MB limit)")
        return path

    def read_json(self, directory, name):
        try:
            path = self.safe_file(directory, name)
        except FileNotFoundError:
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"{name} must contain a JSON object")
        # Reject non-standard NaN/Infinity before these reach the browser.
        json.dumps(value, allow_nan=False)
        return value

    def detail(self, identity):
        path = self.directory(identity)
        experiment = self.read_json(path, "experiment.json")
        if experiment is None:
            raise ValueError("This directory is not an individual experiment")
        summary = self.read_json(path, "summary.json")
        failure = self.read_json(path, "failure.json")
        status = ("failed" if failure or (summary and summary.get("passed") is False)
                  or (summary and summary.get("safety_violations", 0) > 0)
                  else "passed" if summary and (summary.get("passed") is True
                       or summary.get("safety_violations") == 0) else "incomplete")
        return dict(id=path.relative_to(self.root).as_posix(), experiment=experiment,
                    summary=summary, failure=failure, status=status,
                    artifacts=self.artifacts(path))

    def artifacts(self, path):
        available = []
        for name in sorted(ARTIFACTS):
            try:
                self.safe_file(path, name)
                available.append(name)
            except (FileNotFoundError, ValueError):
                pass
        return available

    def completion_timing(self, path, summary):
        """Read recorded exits; an unfinished-at-cutoff count alone is not failure."""
        window, requested = summary.get("duration_seconds"), summary.get("requested")
        if type(window) not in (int, float) or not math.isfinite(window) or window <= 0:
            return None
        try:
            with self.safe_file(path, "requests.csv").open(newline="", encoding="utf-8") as stream:
                reader = csv.DictReader(stream)
                if "exited_at" not in (reader.fieldnames or []):
                    return None
                exits = [float(row["exited_at"]) if row["exited_at"] else None for row in reader]
            if len(exits) != requested or any(value is not None and (not math.isfinite(value) or value < 0) for value in exits):
                return None
            before = sum(value is not None and value <= window for value in exits)
            if before != summary.get("completed"):
                return None
            late = [value for value in exits if value is not None and value > window]
            return dict(finished_by_cutoff=before, finished_after_cutoff=len(late),
                        no_recorded_completion=sum(value is None for value in exits),
                        late_finish_seconds=late)
        except (OSError, ValueError, TypeError, KeyError, csv.Error):
            return None

    def progress(self, identity):
        """Validated cumulative completions from the same exits used in totals."""
        path = self.directory(identity)
        item = self.detail(identity)
        summary = item["summary"] or {}
        window = (summary.get("total_completion_seconds")
                  if summary.get("measurement_policy") == "complete_all_requests"
                  else summary.get("duration_seconds"))
        unavailable = dict(available=False, reason="Recorded completion timestamps are unavailable or inconsistent.")
        if type(window) not in (int, float) or not math.isfinite(window) or window <= 0:
            return unavailable
        try:
            with self.safe_file(path, "requests.csv").open(newline="", encoding="utf-8") as stream:
                reader = csv.DictReader(stream)
                if "exited_at" not in (reader.fieldnames or []):
                    return unavailable
                exits = [float(row["exited_at"]) if row["exited_at"] else None for row in reader]
            if len(exits) != summary.get("requested") or any(
                    v is not None and (not math.isfinite(v) or v < 0) for v in exits):
                return unavailable
            completed = sorted(v for v in exits if v is not None and v <= window)
            if len(completed) != summary.get("completed"):
                return unavailable
            fault = summary.get("fault_at_seconds")
            if fault is None:
                fault = next((event.get("time") for event in self.events(identity)["events"]
                              if event.get("event") in {"holder_stopped", "controller_stopped"}), None)
            if type(fault) not in (int, float) or not math.isfinite(fault) or not 0 <= fault <= window:
                fault = None
            return dict(available=True, exits=completed, window=window, fault_at=fault)
        except (OSError, ValueError, TypeError, KeyError, csv.Error):
            return unavailable

    def catalog(self):
        runs, warnings = [], []
        if not self.root.is_dir():
            return dict(runs=[], collections=[], warnings=["The results folder does not exist yet."])
        for manifest in self.root.rglob("experiment.json"):
            try:
                path = self.directory(manifest.parent.relative_to(self.root).as_posix())
                item = self.detail(path.relative_to(self.root).as_posix())
                experiment, summary = item["experiment"], item["summary"] or {}
                model = experiment.get("model") or summary.get("model") or (
                    "local-gate-normal-traffic" if "arrival_rate" in experiment else "unknown")
                identity = item["id"]
                collection = identity.split("/")[0]
                runs.append(dict(id=identity, collection=collection,
                    family=family(model), model=model,
                    scenario=experiment.get("scenario", "normal"), seed=experiment.get("seed"),
                    status=item["status"], summary=summary, failure=item["failure"],
                    completion_timing=self.completion_timing(path, summary)
                        if family(model) == "comparison" and experiment.get("scenario") == "normal" else None,
                    modified=manifest.stat().st_mtime,
                    development="development" in collection))
            except (ValueError, TypeError, OSError) as error:
                warnings.append(f"Skipped {manifest.parent.name}: {error}")
        runs.sort(key=lambda item: (-item["modified"], item["id"]))
        collections = []
        for name in dict.fromkeys(run["collection"] for run in runs):
            path = self.directory(name)
            members = [run for run in runs if run["collection"] == name]
            collections.append(dict(id=name, count=len(members), family=members[0]["family"],
                                    artifacts=self.artifacts(path)))
        return dict(runs=runs, collections=collections, warnings=warnings)

    def events(self, identity):
        directory = self.directory(identity)
        self.safe_file(directory, "experiment.json")
        try:
            path = self.safe_file(directory, "events.jsonl")
        except FileNotFoundError:
            return dict(events=[], total=0, truncated=False, malformed=0)
        events, total, malformed = [], 0, 0
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict):
                        raise ValueError("Expected an event object")
                    json.dumps(event, allow_nan=False)
                    total += 1
                    if len(events) < 10000:
                        events.append(event)
                except ValueError:
                    malformed += 1
        return dict(events=events, total=total, truncated=total > len(events), malformed=malformed)


def make_server(root, port=8765, deployed_root=None, *, live=None, live_port=None):
    results = Results(root)
    deployed = Results(deployed_root if deployed_root is not None else Path(root).resolve().parent / "deployed-evaluation")
    assets = files("cranelock_sim").joinpath("web")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query)
            try:
                identity = query.get("id", [""])[0]
                if live is None and live_port is not None and parsed.path == "/live":
                    self.send_response(307)
                    self.send_header("Location", f"http://127.0.0.1:{live_port}/live")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                if live is not None and parsed.path == "/api/live/state":
                    if not self.local_host():
                        return self.send_json(dict(error="Local host required"), 403)
                    return self.send_json(live.submit(live.state()))
                if parsed.path == "/api/catalog":
                    return self.send_json(results.catalog())
                if parsed.path == "/api/deployed":
                    return self.send_json(deployed.catalog())
                if parsed.path == "/api/run":
                    return self.send_json(results.detail(identity))
                if parsed.path == "/api/events":
                    return self.send_json(results.events(identity))
                if parsed.path == "/api/progress":
                    return self.send_json(results.progress(identity))
                if parsed.path in {"/api/artifact", "/api/deployed/artifact"}:
                    source = deployed if parsed.path == "/api/deployed/artifact" else results
                    name = query.get("name", [""])[0]
                    if name not in ARTIFACTS:
                        raise ValueError("Artifact is not available for download")
                    path = source.safe_file(source.directory(identity), name)
                    return self.send_bytes(path.read_bytes(), mimetypes.guess_type(name)[0]
                                           or "application/octet-stream", download=name)
                asset = {"/": "index.html", "/app.js": "app.js", "/style.css": "style.css",
                         "/views.css": "views.css"}.get(parsed.path)
                if live is not None:
                    asset = {"/live": "live.html", "/live.js": "live.js", "/live.css": "live.css"}.get(
                        parsed.path, asset)
                if asset:
                    data = assets.joinpath(asset).read_bytes()
                    if asset == "index.html" and live is None and live_port is None:
                        data = data.replace(b' data-live-view', b' hidden', 1)
                    return self.send_bytes(data, mimetypes.guess_type(asset)[0] or "text/plain")
                self.send_json(dict(error="Not found"), 404)
            except FileNotFoundError:
                self.send_json(dict(error="Artifact not found"), 404)
            except (ValueError, TypeError, OSError) as error:
                self.send_json(dict(error=str(error)), 400)

        # restricts browser controls to the local server
        def local_host(self):
            try:
                host = urlsplit("http://" + self.headers.get("Host", ""))
                return host.hostname in {"localhost", "127.0.0.1"} and not host.username and not host.path
            except ValueError:
                return False

        # accepts bounded same-origin operator actions on the live server only
        def do_POST(self):
            if live is None:
                self.send_error(501, "Unsupported method")
                return
            if urlsplit(self.path).path != "/api/live/action":
                return self.send_json(dict(error="Not found"), 404)
            origin = "http://" + self.headers.get("Host", "")
            token = self.headers.get("X-CraneLock-Session", "")
            if not self.local_host() or self.headers.get("Origin") != origin:
                return self.send_json(dict(error="Same-origin session required"), 403)
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 1 <= length <= 4096 or self.headers.get_content_type() != "application/json":
                    raise ValueError("A bounded JSON action is required")
                payload = json.loads(self.rfile.read(length))
                if type(payload) is not dict:
                    raise ValueError("Action must be an object")
                return self.send_json(live.submit(live.action(payload, token)))
            except PermissionError as error:
                return self.send_json(dict(error=str(error)), 403)
            except (ValueError, TypeError, RuntimeError, OSError) as error:
                self.send_json(dict(error=str(error)), 400)

        def send_json(self, value, status=200):
            self.send_bytes(json.dumps(value, allow_nan=False).encode(), "application/json", status=status)

        def send_bytes(self, data, mime, status=200, download=None):
            try:
                self.send_response(status)
                self.send_header("Content-Type", mime + ("; charset=utf-8" if mime.startswith("text/") else ""))
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
                if download:
                    self.send_header("Content-Disposition", f'attachment; filename="{download}"')
                self.end_headers()
                self.wfile.write(data)
            except ConnectionError:
                # Refreshing or closing a tab can abort either write. Do not try
                # to send an error response over the same disconnected socket.
                self.close_connection = True

        def log_message(self, format, *args):
            pass

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def main():
    parser = argparse.ArgumentParser(description="View saved CraneLock results locally")
    parser.add_argument("--root", default="run/evaluation")
    parser.add_argument("--deployed-root", help="Docker evidence directory (default: deployed-evaluation beside --root)")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--live-port", type=int, help="Port of a separately running live yard")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("port must be between 0 and 65535")
    if args.live_port is not None and (not 1 <= args.live_port <= 65535 or args.live_port == args.port):
        parser.error("live-port must be a different port between 1 and 65535")
    with make_server(args.root, args.port, args.deployed_root, live_port=args.live_port) as server:
        print(f"CraneLock results: http://127.0.0.1:{server.server_port}", flush=True)
        print(f"Reading saved experiments from {Path(args.root).resolve()}", flush=True)
        print("Press Ctrl+C to stop.", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
