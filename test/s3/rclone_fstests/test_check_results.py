#!/usr/bin/env python3
"""Unit tests for check_results.py. Run: python3 -m unittest -v test_check_results"""
import os
import tempfile
import unittest

import check_results


def ev(action, test=None):
    event = {"Action": action, "Package": "rclone"}
    if test is not None:
        event["Test"] = test
    return event


def finished(test, action):
    return [ev("run", test), ev(action, test)]


class EvaluateTest(unittest.TestCase):
    def test_all_pass_ok(self):
        events = finished("TestIntegration", "pass") + finished("TestIntegration/A", "pass")
        self.assertEqual(check_results.evaluate(events, set()), [])

    def test_unexpected_leaf_failure(self):
        events = finished("TestIntegration/A", "fail") + finished("TestIntegration", "fail")
        problems = check_results.evaluate(events, set())
        self.assertEqual(len(problems), 1)
        self.assertIn("TestIntegration/A", problems[0])

    def test_parent_of_expected_failure_ignored(self):
        events = (
            finished("TestIntegration/FsMkdir/X", "fail")
            + finished("TestIntegration/FsMkdir", "fail")
            + finished("TestIntegration", "fail")
        )
        self.assertEqual(check_results.evaluate(events, {"TestIntegration/FsMkdir/X"}), [])

    def test_expected_now_passes_fails(self):
        events = finished("T", "pass") + finished("T/A", "pass")
        problems = check_results.evaluate(events, {"T/A"})
        self.assertEqual(len(problems), 1)
        self.assertIn("XPASS", problems[0])

    def test_listed_skip_or_missing_fails(self):
        events = finished("T", "pass") + finished("T/Skipped", "skip")
        problems = check_results.evaluate(events, {"T/Skipped", "T/Missing"})
        self.assertEqual(len(problems), 2)
        self.assertTrue(any("T/Skipped" in p for p in problems))
        self.assertTrue(any("T/Missing" in p for p in problems))

    def test_unlisted_skip_ok(self):
        events = finished("T", "pass") + finished("T/A", "skip")
        self.assertEqual(check_results.evaluate(events, set()), [])

    def test_package_failure_fails(self):
        events = [ev("run", "T"), ev("run", "T/A"), ev("output"), ev("fail")]
        self.assertNotEqual(check_results.evaluate(events, set()), [])

    def test_package_fail_from_expected_failures_ok(self):
        # go test2json reports the package as failed whenever any test failed.
        events = finished("T/A", "fail") + finished("T", "fail") + [ev("fail")]
        self.assertEqual(check_results.evaluate(events, {"T/A"}), [])

    def test_package_fail_without_failed_test_fails(self):
        events = finished("T", "pass") + [ev("fail")]
        self.assertNotEqual(check_results.evaluate(events, set()), [])

    def test_parent_failing_on_its_own_fails(self):
        # All children pass, but the parent fails (e.g. its own cleanup).
        events = finished("T/P/A", "pass") + finished("T/P", "fail") + finished("T", "fail") + [ev("fail")]
        problems = check_results.evaluate(events, set())
        self.assertEqual(len(problems), 1)
        self.assertIn("T/P", problems[0])

    def test_panic_in_expected_test_fails(self):
        # A panicking test and its parents all report fail, then the binary exits 2.
        events = finished("T/A", "fail") + finished("T", "fail") + [ev("fail")]
        problems = check_results.evaluate(events, {"T/A"}, exit_code=2)
        self.assertEqual(len(problems), 1)
        self.assertIn("exit code 2", problems[0])

    def test_failing_run_exit_code_one_ok(self):
        events = finished("T/A", "fail") + finished("T", "fail") + [ev("fail")]
        self.assertEqual(check_results.evaluate(events, {"T/A"}, exit_code=1), [])

    def test_no_results_fails(self):
        self.assertNotEqual(check_results.evaluate([], set()), [])


class LoadExpectedTest(unittest.TestCase):
    def test_load_expected_strips_comments(self):
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            f.write("# c\n\nT/A  # why\nT/B\n")
        try:
            self.assertEqual(check_results.load_expected(f.name), {"T/A", "T/B"})
        finally:
            os.unlink(f.name)


if __name__ == "__main__":
    unittest.main()
