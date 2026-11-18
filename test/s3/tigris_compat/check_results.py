#!/usr/bin/env python3
"""Compare an s3-api-compat-tests JSON report with expected_failures.txt.

Fails on any test that did not pass and is not listed with the status it
ended with, and on any listed test that now passes or did not run, so the list
only ever shrinks.
"""
import json
import sys


def load_expected(path):
    expected = {}
    with open(path) as f:
        for line in f:
            fields = line.split("#", 1)[0].split()
            if fields:
                expected[fields[0]] = fields[1] if len(fields) > 1 else "fail"
    return expected


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: check_results.py <results.json> <expected_failures.txt>")
    with open(sys.argv[1]) as f:
        report = json.load(f)
    expected = load_expected(sys.argv[2])

    providers = report.get("providers") or []
    results = providers[0].get("results") if providers else None
    if not results:
        sys.exit("no test results in the report: the suite could not start against the server")

    seen = set()
    regressions, fixed = [], []
    for r in results:
        name, status = r["operation"], r["status"]
        seen.add(name)
        detail = r.get("error") or r.get("notes") or ""
        if status == "pass":
            if name in expected:
                fixed.append(name)
        elif name not in expected:
            regressions.append(f"{name} [{status}]: {detail}")
        elif status != expected[name]:
            regressions.append(f"{name} [{status}, listed as {expected[name]}]: {detail}")
        else:
            print(f"expected failure: {name} [{status}]")

    missing = sorted(set(expected) - seen)
    print(f"{len(results)} tests, {len(regressions)} unexpected failures, "
          f"{len(fixed)} unexpected passes, {len(missing)} listed but not run")
    for line in regressions:
        print(f"UNEXPECTED FAILURE: {line}")
    for name in fixed:
        print(f"UNEXPECTED PASS: {name} (remove it from expected_failures.txt)")
    for name in missing:
        print(f"NOT RUN: {name} (remove it from expected_failures.txt)")
    if regressions or fixed or missing:
        sys.exit(1)


if __name__ == "__main__":
    main()
