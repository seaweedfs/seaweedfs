import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class AptInstallTests(unittest.TestCase):
    def run_helper(self, sources, fail_action="update", timeout_azure=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            apt = root / "apt"
            (apt / "sources.list.d").mkdir(parents=True)
            (apt / "sources.list").write_text("")
            for name, content in sources.items():
                (apt / name).write_text(content)
            helper = root / "apt-install"
            helper.write_text(Path(__file__).with_name("apt-install").read_text()
                              .replace("/etc/apt", str(apt))
                              .replace("/var/lib/apt", str(root / "state")))
            commands = root / "bin"
            commands.mkdir()
            fake_apt = commands / "apt-get"
            fake_apt.write_text(r"""#!/usr/bin/env python3
import json, os, pathlib, sys
root = pathlib.Path(os.environ["VECTOR_APT_TEST_ROOT"])
apt = root / "apt"
sources = [apt / "sources.list", *apt.glob("sources.list.d/*.list"), *apt.glob("sources.list.d/*.sources")]
content = "".join(p.read_text() for p in sources)
action = next(a for a in sys.argv[1:] if a in ("update", "install", "clean"))
with (root / "calls").open("a") as output:
    output.write(json.dumps({"action": action, "sources": content, "args": sys.argv[1:]}) + "\n")
if "azure.archive.ubuntu.com" in content and action == "update" and os.environ["VECTOR_APT_FAIL_ACTION"] == "partial":
    sys.exit(100 if "APT::Update::Error-Mode=any" in sys.argv else 0)
if "azure.archive.ubuntu.com" in content and action == os.environ["VECTOR_APT_FAIL_ACTION"]:
    sys.exit(100)
""")
            fake_apt.chmod(0o755)
            fake_timeout = commands / "timeout"
            fake_timeout.write_text(r"""#!/usr/bin/env python3
import json, os, pathlib, sys
root = pathlib.Path(os.environ["VECTOR_APT_TEST_ROOT"])
apt = root / "apt"
sources = [apt / "sources.list", *apt.glob("sources.list.d/*.list"), *apt.glob("sources.list.d/*.sources")]
content = "".join(p.read_text() for p in sources)
with (root / "calls").open("a") as output:
    output.write(json.dumps({"action": "timeout", "args": sys.argv[1:]}) + "\n")
if os.environ["VECTOR_APT_TIMEOUT_AZURE"] and "azure.archive.ubuntu.com" in content:
    sys.exit(124)
os.execvp(sys.argv[3], sys.argv[3:])
""")
            fake_timeout.chmod(0o755)
            result = subprocess.run(["/bin/sh", str(helper), "fuse"],
                                    env={**os.environ, "PATH": str(commands) + os.pathsep + os.environ["PATH"],
                                         "VECTOR_APT_TEST_ROOT": str(root), "VECTOR_APT_FAIL_ACTION": fail_action,
                                         "VECTOR_APT_TIMEOUT_AZURE": "1" if timeout_azure else ""},
                                    text=True, capture_output=True, timeout=5)
            calls = [json.loads(line) for line in (root / "calls").read_text().splitlines()] if (root / "calls").exists() else []
            return result, calls

    def test_fallback_rewrites_all_source_formats(self):
        for protocol in ("http", "https"):
            for layout in ("legacy", "deb822", "mixed"):
                with self.subTest(protocol=protocol, layout=layout):
                    sources = {}
                    if layout in ("legacy", "mixed"):
                        sources["sources.list"] = f"deb {protocol}://azure.archive.ubuntu.com/ubuntu noble main\n"
                    if layout in ("deb822", "mixed"):
                        sources["sources.list.d/ubuntu.sources"] = f"Types: deb\nURIs: {protocol}://azure.archive.ubuntu.com/ubuntu\nSuites: noble\nComponents: main\n"
                    result, calls = self.run_helper(sources)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    installs = [call for call in calls if call["action"] == "install"]
                    self.assertEqual(len(installs), 1)
                    self.assertNotIn("azure.archive.ubuntu.com", installs[0]["sources"])
                    self.assertIn(protocol + "://archive.ubuntu.com", installs[0]["sources"])

    def test_install_failure_also_tries_the_next_mirror(self):
        result, calls = self.run_helper({"sources.list.d/ubuntu.sources":
                                        "Types: deb\nURIs: http://security.ubuntu.com/ubuntu\nSuites: noble-security\nComponents: main\n"},
                                       fail_action="install")
        self.assertEqual(result.returncode, 0, result.stderr)
        installs = [call for call in calls if call["action"] == "install"]
        self.assertEqual(len(installs), 2)
        self.assertIn("azure.archive.ubuntu.com", installs[0]["sources"])
        self.assertNotIn("azure.archive.ubuntu.com", installs[1]["sources"])

    def test_partial_update_does_not_install_stale_indexes(self):
        result, calls = self.run_helper({"sources.list": "deb http://azure.archive.ubuntu.com/ubuntu noble main\n"},
                                       fail_action="partial")
        self.assertEqual(result.returncode, 0, result.stderr)
        installs = [call for call in calls if call["action"] == "install"]
        self.assertEqual(len(installs), 1)
        self.assertNotIn("azure.archive.ubuntu.com", installs[0]["sources"])

    def test_timeout_advances_to_the_next_mirror(self):
        result, calls = self.run_helper({"sources.list": "deb http://azure.archive.ubuntu.com/ubuntu noble main\n"},
                                       fail_action="none", timeout_azure=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        timeouts = [call for call in calls if call["action"] == "timeout"]
        self.assertEqual(len(timeouts), 3)
        for call in timeouts:
            self.assertEqual(call["args"][0], "--kill-after=5s")
            self.assertLessEqual(int(call["args"][1].removesuffix("s")), 180)
        installs = [call for call in calls if call["action"] == "install"]
        self.assertNotIn("azure.archive.ubuntu.com", installs[0]["sources"])


if __name__ == "__main__":
    unittest.main()
