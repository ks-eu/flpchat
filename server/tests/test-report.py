"""Format streamed Cargo test output using the test catalog."""

import argparse
import json
import math
import os
import re
import signal
import subprocess
import sys
from collections import Counter
from pathlib import Path


PHASES = {
    "Running regular tests": "regular",
    "Running performance tests": "performance",
}
TEST_LINE = re.compile(r"^test ([\w:]+) \.\.\.\s*(.*)$")
STATUS_LINE = re.compile(r"^(ok|FAILED|ignored)(?:,.*)?$")
RUN_LINE = re.compile(r"^running (\d+) tests?$")
SUMMARY_LINE = re.compile(
    r"^test result: .+\. (\d+) passed; (\d+) failed; (\d+) ignored;"
)
FAILURE_HEADER = re.compile(r"^---- ([\w:]+) (?:stdout|stderr) ----$")
NUMBER = r"(?:[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|nan|inf(?:inity)?)"
TIMING_LINE = re.compile(
    rf"(?P<case>perf-[\w-]+): (?P<count>\d+) samples, min (?P<minimum>{NUMBER}) ms, "
    rf"median (?P<median>{NUMBER}) ms, max (?P<maximum>{NUMBER}) ms",
    re.IGNORECASE,
)
STATUS_LABELS = {"ok": "PASS", "FAILED": "FAIL", "ignored": "SKIP"}
COLOR = sys.stdout.isatty() and "NO_COLOR" not in os.environ


def no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def load_catalog(path):
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=no_duplicate_keys)


def validate_catalog(catalog):
    errors = []
    if not isinstance(catalog, dict) or set(catalog) != {"components", "cases"}:
        return ["Catalog must contain components and cases objects."], {}, {}
    raw_components = catalog["components"]
    raw_cases = catalog["cases"]
    if not isinstance(raw_components, dict) or not isinstance(raw_cases, dict):
        return ["Catalog components and cases must be objects."], {}, {}

    components = {}
    required_component = {"title", "inputs"}
    for component_id, component in raw_components.items():
        if not isinstance(component, dict) or not required_component <= component.keys():
            errors.append(f"Invalid component metadata: {component_id}.")
        elif not all(isinstance(component[key], str) for key in required_component):
            errors.append(f"Invalid component title or inputs: {component_id}.")
        else:
            components[component_id] = component

    ids = set()
    used_components = set()
    cases = {}
    required_case = {"id", "component", "suite", "alias", "input", "expected"}
    for name, case in raw_cases.items():
        if not isinstance(name, str) or "::" not in name:
            errors.append(f"Invalid harness test name: {name!r}.")
        if not isinstance(case, dict) or not required_case <= case.keys():
            errors.append(f"Invalid case metadata: {name}.")
            continue
        if not all(isinstance(case[key], str) and case[key] for key in required_case):
            errors.append(f"Case metadata fields must be nonempty strings: {name}.")
            continue
        case_id = case["id"]
        if case_id in ids:
            errors.append(f"Duplicate catalog case ID: {case_id}.")
            continue
        ids.add(case_id)
        if case["component"] not in components:
            errors.append(f"Case {name} references missing component {case['component']}.")
            continue
        if case["suite"] not in {"regular", "performance"}:
            errors.append(f"Invalid suite for {name}: {case['suite']}.")
            continue
        cases[name] = case
        used_components.add(case["component"])

    stale = components.keys() - used_components
    errors.extend(f"Unused catalog component: {component}." for component in sorted(stale))
    return errors, components, cases


class OutputParser:
    def __init__(self, cases, live=False):
        self.cases = cases
        self.live = live
        self.interactive = live and sys.stdout.isatty()
        self.statuses = {}
        self.test_suites = {}
        self.timings = []
        self.run_counts = {"regular": 0, "performance": 0}
        self.summary_counts = {"regular": Counter(), "performance": Counter()}
        self.phases = []
        self.current_phase = None
        self.pending = None
        self.failure_name = None
        self.failure_details = {}
        self.diagnostics = []
        self.data_errors = []
        self.diagnostic_seen = set()
        self.progress_active = False
        self.case_suites = {name: case["suite"] for name, case in cases.items()}

    def _diagnostic(self, line):
        if line.strip() and line not in self.diagnostic_seen:
            self.diagnostic_seen.add(line)
            self.diagnostics.append(line)

    def _record_timing(self, line, harness_name):
        for match in TIMING_LINE.finditer(line):
            self.timings.append(
                {
                    "id": match["case"],
                    "count": match["count"],
                    "minimum": match["minimum"],
                    "median": match["median"],
                    "maximum": match["maximum"],
                    "harness": harness_name,
                }
            )

    def _progress(self, suite):
        if not self.interactive:
            return
        total = sum(case["suite"] == suite for case in self.cases.values())
        completed = sum(
            status is not None and self.test_suites.get(name) == suite
            for name, status in self.statuses.items()
        )
        width = 24
        filled = int(width * completed / total) if total else width
        bar = "=" * filled + "." * (width - filled)
        sys.stdout.write(f"\r{suite.title()} [{bar}] {completed}/{total}")
        sys.stdout.flush()
        self.progress_active = True
        if total and completed >= total:
            sys.stdout.write("\n")
            sys.stdout.flush()
            self.progress_active = False

    def _finish_status(self, name, status):
        if name in self.statuses:
            self.data_errors.append(f"Duplicate harness result for {name}.")
        else:
            self.statuses[name] = status
        suite = self.test_suites.get(name) or self.case_suites.get(name)
        if suite:
            self.test_suites[name] = suite
            self._progress(suite)
        self.pending = None

    def consume(self, raw_line):
        line = raw_line.rstrip("\r\n")
        if line in PHASES:
            self.current_phase = PHASES[line]
            self.phases.append(self.current_phase)
            self.pending = None
            if self.live:
                print(line, flush=True)
            return

        if match := RUN_LINE.fullmatch(line):
            if self.current_phase:
                self.run_counts[self.current_phase] += int(match[1])
            return

        if match := SUMMARY_LINE.match(line):
            if self.current_phase:
                counts = dict(zip(("ok", "FAILED", "ignored"), map(int, match.groups())))
                self.summary_counts[self.current_phase].update(counts)
            return

        if match := FAILURE_HEADER.fullmatch(line):
            self.failure_name = match[1]
            self.failure_details.setdefault(self.failure_name, [])
            return
        if self.failure_name:
            if line == "failures:" or line.startswith("test result:"):
                self.failure_name = None
            else:
                self.failure_details[self.failure_name].append(line)
                return

        if match := TEST_LINE.fullmatch(line):
            name, trailing = match.groups()
            self.pending = name
            if self.current_phase:
                self.test_suites[name] = self.current_phase
            self._record_timing(line, name)
            if status_match := STATUS_LINE.fullmatch(trailing.strip()):
                self._finish_status(name, status_match[1])
            return

        self._record_timing(line, self.pending)
        if self.pending:
            if status_match := STATUS_LINE.fullmatch(line.strip()):
                self._finish_status(self.pending, status_match[1])
                return
            if line.strip() and not TIMING_LINE.search(line):
                self.failure_details.setdefault(self.pending, []).append(line)

        if self.live:
            self._stream_line(line)

        if line.startswith(("warning:", "error:", "Error:")):
            self._diagnostic(line)
        elif self.current_phase is None or self.run_counts[self.current_phase] == 0:
            if not line.startswith(("Finished ", "     Running ")):
                self._diagnostic(line)

    def _stream_line(self, line):
        if not line.strip() or line.startswith("test ") or RUN_LINE.fullmatch(line):
            return
        if SUMMARY_LINE.match(line) or STATUS_LINE.fullmatch(line.strip()):
            return
        if TIMING_LINE.search(line) or self.pending:
            return
        print(line, flush=True)

    def finish_live(self):
        if self.interactive and self.progress_active:
            sys.stdout.write("\n")
            sys.stdout.flush()
            self.progress_active = False


def validate_output(parser, catalog_errors, runner_status):
    errors = list(catalog_errors) + list(parser.data_errors)
    if parser.phases != ["regular", "performance"]:
        errors.append(
            "Expected one regular phase followed by one performance phase; "
            f"observed {parser.phases or 'none'}."
        )

    for suite in ("regular", "performance"):
        summary = parser.summary_counts[suite]
        total = sum(summary.values())
        if parser.run_counts[suite] != total:
            errors.append(
                f"{suite.title()} harness count mismatch: running {parser.run_counts[suite]}, "
                f"summary {total}."
            )
        observed = Counter(
            status for name, status in parser.statuses.items() if parser.test_suites.get(name) == suite
        )
        for status in STATUS_LABELS:
            if observed[status] != summary[status]:
                errors.append(
                    f"{suite.title()} {status} result count mismatch: parsed {observed[status]}, "
                    f"summary {summary[status]}."
                )

    for name, status in parser.statuses.items():
        case = parser.cases.get(name)
        if case is None:
            errors.append(f"Harness test has no catalog entry: {name}.")
        elif parser.test_suites.get(name) != case["suite"]:
            errors.append(
                f"Harness suite mismatch for {name}: expected {case['suite']}, "
                f"observed {parser.test_suites.get(name, 'unknown')}."
            )
        if status == "FAILED":
            errors.append(f"Test failed: {name}.")

    for name in parser.cases.keys() - parser.statuses.keys():
        errors.append(f"Catalog case was not run: {name}.")

    performance_by_id = {
        case["id"]: (name, case)
        for name, case in parser.cases.items()
        if case["suite"] == "performance"
    }
    timings_by_id = {}
    for timing in parser.timings:
        case_id = timing["id"]
        if case_id in timings_by_id:
            errors.append(f"Duplicate timing record for {case_id}.")
            continue
        timings_by_id[case_id] = timing
        target = performance_by_id.get(case_id)
        if target is None:
            errors.append(f"Unassociated timing record: {case_id}.")
            continue
        harness_name, _ = target
        if timing["harness"] != harness_name:
            errors.append(
                f"Timing record {case_id} belongs to {timing['harness']}, expected {harness_name}."
            )
        try:
            count = int(timing["count"])
            values = [float(timing[key]) for key in ("minimum", "median", "maximum")]
            valid_values = all(math.isfinite(value) for value in values)
        except ValueError:
            count, valid_values, values = 0, False, []
        if count != 20:
            errors.append(f"Timing record {case_id} must contain 20 samples, got {count}.")
        if not valid_values or any(value < 0 for value in values) or values != sorted(values):
            errors.append(f"Timing record {case_id} must contain finite ordered values.")

    for case_id, (name, _) in performance_by_id.items():
        if parser.statuses.get(name) == "ok" and case_id not in timings_by_id:
            errors.append(f"Passing benchmark has no timing record: {case_id}.")

    if runner_status not in (None, 0):
        errors.append(f"Raw test runner exited with status {runner_status}.")
    return list(dict.fromkeys(errors)), performance_by_id, timings_by_id


def status_label(status):
    if status is None:
        return "NOT RUN"
    return STATUS_LABELS[status]


def styled_status(status):
    label = status_label(status)
    color = {"PASS": 32, "FAIL": 31, "SKIP": 33, "NOT RUN": 33}.get(label)
    return f"\033[{color}m{label}\033[0m" if COLOR and color else label


def render_progress(parser, catalog):
    completed = len(parser.statuses)
    total = len(catalog)
    phases = ", ".join(
        f"{phase.title()} {sum(parser.test_suites.get(name) == phase for name in parser.statuses)}/"
        f"{sum(case['suite'] == phase for case in catalog.values())}"
        for phase in ("regular", "performance")
    )
    print(f"Progress: {completed}/{total} tests completed ({phases})")


def render_diagnostics(parser, errors):
    print("\nDiagnostics")
    lines = list(parser.diagnostics)
    lines.extend(f"Report data error: {error}" for error in errors)
    if not lines:
        print("(none)")
    else:
        for line in lines:
            print(line)


def render_regular(components, cases, parser):
    print("\nRegular tests")
    for component_id, component in components.items():
        entries = [
            (name, case)
            for name, case in cases.items()
            if case["component"] == component_id and case["suite"] == "regular"
        ]
        if not entries:
            continue
        print(f"\n{component['title']}")
        print(f"inputs: {component['inputs']}")
        for name, case in entries:
            status = parser.statuses.get(name)
            print(f"{styled_status(status)} {case['alias']}")
            print(f"     Input: {case['input']}")
            print(f"     Expected: {case['expected']}")
            if status == "FAILED":
                details = parser.failure_details.get(name, [])
                if details:
                    for line in details:
                        print(f"     {line}")
                else:
                    print("     (failure details were not captured)")


def timing_boundary(case):
    description = f"{case['alias']} {case['input']}"
    if "postgresql execution time" in description.lower():
        return "PostgreSQL Execution Time"
    if "decoded tcp reply" in description.lower():
        return "Handler call through decoded TCP reply"
    return case["input"]


def render_performance(cases, parser, performance_by_id, timings_by_id):
    print("\nPerformance tests (milliseconds)")
    for name, case in cases.items():
        if case["suite"] != "performance":
            continue
        case_id = case["id"]
        status = parser.statuses.get(name)
        timing = timings_by_id.get(case_id)
        print(f"\n{styled_status(status)} {case_id} — {case['alias']}")
        print(f"     Boundary: {timing_boundary(case)}")
        if timing:
            print(
                f"     {timing['count']} samples, min {timing['minimum']} ms, "
                f"median {timing['median']} ms, max {timing['maximum']} ms"
            )
        else:
            print("     Timing: not recorded")
        print(f"     Input: {case['input']}")
        print(f"     Expected: {case['expected']}")
        if status == "FAILED":
            for line in parser.failure_details.get(name, []):
                print(f"     {line}")

    for timing in parser.timings:
        if timing["id"] not in performance_by_id:
            print(f"\nTIMING {timing['id']} — unassociated record")
            print(
                f"     {timing['count']} samples, min {timing['minimum']} ms, "
                f"median {timing['median']} ms, max {timing['maximum']} ms"
            )


def report(lines, catalog_path, runner_status=None, live=False, process=None):
    try:
        raw_catalog = load_catalog(catalog_path)
        catalog_errors, components, cases = validate_catalog(raw_catalog)
    except (OSError, ValueError, TypeError) as error:
        catalog_errors = [f"Could not read test catalog: {error}."]
        components, cases = {}, {}

    parser = OutputParser(cases, live=live)
    for line in lines:
        parser.consume(line)
    if process is not None:
        process.wait()
        runner_status = process.returncode
        if process.stdout:
            process.stdout.close()
    parser.finish_live()
    errors, performance_by_id, timings_by_id = validate_output(
        parser, catalog_errors, runner_status
    )
    render_progress(parser, cases)
    render_diagnostics(parser, errors)
    render_regular(components, cases, parser)
    render_performance(cases, parser, performance_by_id, timings_by_id)
    if runner_status not in (None, 0):
        return runner_status if runner_status > 0 else 128 - runner_status
    return int(bool(errors))


def run_combined(runner, catalog_path):
    process = None
    pending_signal = []

    def forward_signal(signum, _frame):
        if process is None:
            pending_signal.append(signum)
            return
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            pass

    old_handlers = {
        signum: signal.signal(signum, forward_signal)
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    }
    try:
        try:
            process = subprocess.Popen(
                [runner, "--combined"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                start_new_session=True,
            )
        except OSError as error:
            return report(
                [f"Error: Could not start raw test runner: {error}"],
                catalog_path,
                runner_status=1,
                live=True,
            )
        for signum in pending_signal:
            forward_signal(signum, None)
        try:
            return report(
                process.stdout,
                catalog_path,
                live=True,
                process=process,
            )
        finally:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
    finally:
        for signum, handler in old_handlers.items():
            signal.signal(signum, handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner")
    args = parser.parse_args()
    catalog_path = Path(__file__).with_name("test-catalog.json")
    if args.runner:
        return run_combined(args.runner, catalog_path)
    return report(sys.stdin, catalog_path)


if __name__ == "__main__":
    sys.exit(main())
