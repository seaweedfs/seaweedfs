#!/usr/bin/env python3
"""Compare rclone fstests results (go test2json events) with the expected lists.

A test fails CI when its result differs from what the lists say:
- a leaf subtest that failed must be in expected_failures.txt;
- a parent test fails whenever one of its children does, so its status alone
  says nothing new. It counts as failing on its own when it has no failed
  subtest or when testify reported an assertion under it ("Error Trace:" in
  the output test2json attributes to it, e.g. a deferred cleanup check), and
  then it must be listed too;
- a skipped test must be in expected_skips.txt, so a feature that silently
  turns into a skip (Object Lock, versioning) is noticed;
- a listed test that passed, was skipped instead of failing, ran instead of
  being skipped, or did not run at all fails CI as well, so the lists only
  ever shrink together with fixes.
Tests that started but never finished, and a test binary exit code other than
0 or 1 (panic, -test.timeout), fail CI too.
"""
import argparse
import json
import sys

FINAL_ACTIONS = ("pass", "fail", "skip")
OWN_ASSERTION_MARKER = "Error Trace:"


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


def evaluate(events, expected, exit_code=None, expected_skips=None):
    """Return the problems found; skips are only checked when expected_skips is given."""
    started, status, asserted = set(), {}, set()
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
        elif action == "output" and OWN_ASSERTION_MARKER in e.get("Output", ""):
            asserted.add(test)

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

    def has_children(name):
        return any(o.startswith(name + "/") for o in names)

    def failed_on_its_own(name):
        if status.get(name) != "fail":
            return False
        if name in asserted or not has_children(name):
            return True
        return not any(o.startswith(name + "/") and status.get(o) == "fail" for o in names)

    for name in sorted(names):
        if failed_on_its_own(name) and name not in expected:
            kind = "unexpected failure" if not has_children(name) else "unexpected failure of the test itself, not of its subtests"
            problems.append(f"{name}: {kind}")
    for name in sorted(expected):
        result = status.get(name)
        if result == "pass":
            problems.append(f"{name}: XPASS, remove it from expected_failures.txt")
        elif result == "fail" and not failed_on_its_own(name):
            problems.append(f"{name}: now fails only through its subtests, remove it from expected_failures.txt")
        elif result != "fail":
            problems.append(f"{name}: listed as an expected failure but did not fail ({result or 'not run'})")

    if expected_skips is not None:
        for name in sorted(n for n in names if status.get(n) == "skip" and n not in expected_skips):
            problems.append(f"{name}: unexpected skip")
        for name in sorted(expected_skips):
            result = status.get(name)
            if result != "skip":
                problems.append(f"{name}: listed as an expected skip but {result or 'did not run'}")
    return problems


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("results", help="test2json output (JSON lines)")
    ap.add_argument("expected_failures")
    ap.add_argument("--skips", help="expected_skips.txt; without it skips are not checked")
    ap.add_argument("--exit-code", help="file holding the test binary's exit status")
    args = ap.parse_args()

    events = load_events(args.results)
    expected = load_expected(args.expected_failures)
    skips = load_expected(args.skips) if args.skips else None
    exit_code = None
    if args.exit_code:
        with open(args.exit_code) as f:
            exit_code = int(f.read().strip())
    problems = evaluate(events, expected, exit_code, skips)
    if problems:
        print("rclone fstests do not match the expected lists:", file=sys.stderr)
        for p in problems:
            print("  " + p, file=sys.stderr)
        sys.exit(1)
    passed = sum(1 for e in events if e.get("Action") == "pass" and e.get("Test"))
    print(f"rclone fstests OK: {passed} passed (parents included), "
          f"{len(expected)} expected failures still fail, {len(skips or ())} expected skips")


if __name__ == "__main__":
    main()
