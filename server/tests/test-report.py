"""Format the Cargo test output captured by test-report.sh."""

import os
import re
import sys
from pathlib import Path


GROUPS = (
    "Database setup",
    "Registration",
    "Credential verification",
    "Self-removal",
    "User worker",
    "Other",
)
TEST_LINE = re.compile(r"^test ([\w:]+) \.\.\.\s*(.*)$")
STATUS_LINE = re.compile(r"^(ok|FAILED|ignored)(?:,.*)?$")
SUMMARY_LINE = re.compile(
    r"^test result: .+\. (\d+) passed; (\d+) failed; (\d+) ignored;"
)
RUN_LINE = re.compile(r"^running \d+ tests?$")
TIMING_LINE = re.compile(
    r"^(.+): (\d+) samples, min ([\d.]+) ms, median ([\d.]+) ms, max ([\d.]+) ms$"
)
STATUS_LABELS = {"ok": "PASS", "FAILED": "FAIL", "ignored": "SKIP"}
COLOR = sys.stdout.isatty() and "NO_COLOR" not in os.environ


def short_name(path):
    return path.rsplit("::", 1)[-1]


def feature(path):
    name = short_name(path)
    if "::create_pool_tests::" in path or name.startswith("create_pool_"):
        return "Database setup"
    if name.startswith(("verify_credentials", "get_hashed_password")):
        return "Credential verification"
    if "::user_handler_tests::" in path:
        return "User worker"
    if "::remove::" in path or "::removeself_user_tests::" in path:
        return "Self-removal"
    if name.startswith(("handle_removeself", "removeself_user")):
        return "Self-removal"
    if "users::registration::" in path or name.startswith(("handle_register", "insert_user")):
        return "Registration"
    return "Other"


def status_text(status):
    label = STATUS_LABELS.get(status, status)
    color = {"ok": 32, "FAILED": 31, "ignored": 33}.get(status)
    return f"\033[{color}m{label}\033[0m" if COLOR and color else label


def parse(lines):
    tests = {}
    timings = {}
    expected = 0
    pending = None
    for line in lines:
        if match := SUMMARY_LINE.match(line):
            expected += sum(int(value) for value in match.groups())
        if match := TEST_LINE.fullmatch(line):
            pending, line = match.groups()
        if pending and (match := STATUS_LINE.fullmatch(line)):
            tests[pending] = match[1]
            pending = None
        elif match := TIMING_LINE.fullmatch(line):
            label, *values = match.groups()
            timings[label] = (pending, values)
    return tests, timings, expected


def timing_key(name):
    return re.sub(r"_(?:query_)?execution_time", "", name).replace(" ", "_")


def grouped(tests):
    groups = {group: [] for group in GROUPS}
    for path, status in sorted(tests.items(), key=lambda item: short_name(item[0])):
        groups[feature(path)].append((path, status))
    return groups


def print_tests(tests):
    print("\nRegular tests")
    for group, entries in grouped(tests).items():
        if not entries:
            continue
        statuses = [status for _, status in entries]
        counts = ", ".join(
            f"{statuses.count(status)} {label}"
            for status, label in (("ok", "passed"), ("FAILED", "failed"), ("ignored", "skipped"))
            if status in statuses
        )
        print(f"\n  {group} ({counts})")
        for path, status in entries:
            print(f"    {status_text(status)}  {short_name(path)}")


def print_performance(tests, timings):
    print("\nPerformance (milliseconds)")
    tests_by_key = {timing_key(short_name(path)): path for path in tests}
    by_test = {}
    unmatched = {}
    for label, (path, values) in timings.items():
        key = timing_key(label)
        # This existing benchmark uses a prose label that differs from its test name.
        if key == "removeself_user_with_wrong_hash_and_message_history":
            key = "removeself_user_wrong_hash_with_message_history"
        path = path if path in tests else tests_by_key.get(key)
        if path:
            by_test[path] = values
        else:
            unmatched[label.replace(" ", "_")] = "TIMING"
            by_test[label.replace(" ", "_")] = values

    for group, entries in grouped(tests | unmatched).items():
        if not entries:
            continue
        width = max(len("Benchmark"), *(len(short_name(path)) for path, _ in entries))
        print(f"\n  {group}")
        print(f"    {'Benchmark':<{width}}  {'Samples':>7}  {'Min':>9}  {'Median':>9}  {'Max':>9}  Status")
        for path, status in entries:
            samples, minimum, median, maximum = by_test.get(path, ("-", "-", "-", "-"))
            print(
                f"    {short_name(path):<{width}}  {samples:>7}  {minimum:>9}  "
                f"{median:>9}  {maximum:>9}  {status_text(status)}"
            )


def print_diagnostics(title, lines, exit_status, parsed, shown_build_output):
    if not parsed or (exit_status and "failures:" not in lines):
        print(f"\n{title} diagnostics")
        print("\n".join(lines).strip() or "(no output)")
        return
    preamble = []
    for line in lines:
        if RUN_LINE.fullmatch(line):
            break
        if not line.startswith(("    Finished ", "     Running ")):
            preamble.append(line)
    details = "\n".join(preamble).strip()
    if details and details not in shown_build_output:
        shown_build_output.add(details)
        print(f"\n{title} build and setup output")
        print(details)
    if exit_status:
        print(f"\n{title} failure details")
        print("\n".join(lines[lines.index("failures:") + 1:]).strip())


def main(regular_path, regular_exit, performance_path, performance_exit):
    regular_lines = Path(regular_path).read_text(errors="replace").splitlines()
    performance_lines = Path(performance_path).read_text(errors="replace").splitlines()
    regular, _, regular_expected = parse(regular_lines)
    performance, timings, performance_expected = parse(performance_lines)
    regular_parsed = bool(regular) and len(regular) == regular_expected
    performance_parsed = bool(performance) and len(performance) == performance_expected

    print_tests(regular)
    print_performance(performance, timings)
    shown_build_output = set()
    for title, lines, exit_status, parsed in (
        ("Regular tests", regular_lines, int(regular_exit), regular_parsed),
        ("Performance tests", performance_lines, int(performance_exit), performance_parsed),
    ):
        print_diagnostics(title, lines, exit_status, parsed, shown_build_output)

    failed = int(regular_exit) or int(performance_exit) or not (regular_parsed and performance_parsed)
    print(f"\nOverall  {status_text('FAILED' if failed else 'ok')}")
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
