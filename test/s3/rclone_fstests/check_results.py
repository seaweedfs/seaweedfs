#!/usr/bin/env python3
"""Compare rclone fstests results (go test2json events) with expected_failures.txt.

Only leaf subtests are judged: a parent test fails whenever one of its children
does, so its own status says nothing new. Fails on any leaf failure that is not
listed, on any listed test that passed, was skipped or did not run, on a test
that started but never finished, and on a failure outside any test (panic,
-test.timeout), so the list only ever shrinks.
"""
import json
import sys

FINAL_ACTIONS = ("pass", "fail", "skip")


def load_expected(path):
    expected = set()
    with open(path) as f:
        for line in f:
            fields = line.split("#", 1)[0].split()
            if fields:
                expected.add(fields[0])
    return expected


def load_events(path):
    events = []
    with open(path) as f:
        for line in f:
            try:
                events.append(json.loads(line))
            except ValueError:
                continue
    return events


def evaluate(events, expected, exit_code=None):
    started, status = set(), {}
    package_failed = False
    for e in events:
        action, test = e.get("Action"), e.get("Test")
        if not test:
            package_failed = package_failed or action == "fail"
            continue
        if action == "run":
            started.add(test)
        elif action in FINAL_ACTIONS:
            status[test] = action

    names = started | set(status)
    if not names:
        return ["no test results: the suite could not start against the server"]

    problems = []
    # test2json fails the package whenever a test failed; only a package failure
    # with no failed test behind it points at something outside the tests.
    if package_failed and "fail" not in status.values() and all(n in status for n in names):
        problems.append("the test binary failed outside any test (panic or -test.timeout?)")
    for name in sorted(names - set(status)):
        problems.append(f"{name}: started but never finished (panic or -test.timeout?)")
    # A failing run exits 1. A panic exits 2 after reporting the panicking test and
    # its parents as failed, so every test has a status, yet the rest never ran.
    if exit_code not in (None, 0, 1):
        problems.append(f"the test binary ended with exit code {exit_code} (panic or -test.timeout?); later tests did not run")

    leaves = {n for n in names if not any(o.startswith(n + "/") for o in names)}
    for name in sorted(leaves):
        if status.get(name) == "fail" and name not in expected:
            problems.append(f"{name}: unexpected failure")
    # A parent fails whenever a child does; one with no failed descendant failed on
    # its own (an assertion after its subtests, its cleanup).
    for name in sorted(names - leaves):
        if status.get(name) == "fail" and not any(
            o.startswith(name + "/") and status.get(o) == "fail" for o in names
        ):
            problems.append(f"{name}: unexpected failure (no failed subtest)")
    for name in sorted(expected):
        result = status.get(name)
        if result == "pass":
            problems.append(f"{name}: XPASS, remove it from expected_failures.txt")
        elif result != "fail":
            problems.append(f"{name}: listed as an expected failure but did not fail ({result or 'not run'})")
    return problems


def main():
    if len(sys.argv) not in (3, 4):
        sys.exit("usage: check_results.py <results.jsonl> <expected_failures.txt> [<exit code file>]")
    events = load_events(sys.argv[1])
    expected = load_expected(sys.argv[2])
    exit_code = None
    if len(sys.argv) == 4:
        with open(sys.argv[3]) as f:
            exit_code = int(f.read().strip())
    problems = evaluate(events, expected, exit_code)
    if problems:
        print("rclone fstests do not match expected_failures.txt:", file=sys.stderr)
        for p in problems:
            print("  " + p, file=sys.stderr)
        sys.exit(1)
    leaves = sum(1 for e in events if e.get("Action") == "pass" and e.get("Test"))
    print(f"rclone fstests OK: {leaves} passed tests, {len(expected)} expected failures still fail")


if __name__ == "__main__":
    main()
