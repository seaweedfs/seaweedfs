import importlib.util
from pathlib import Path
import unittest


def load():
    path = Path(__file__).with_name("release_notes_by_component.py")
    spec = importlib.util.spec_from_file_location("release_notes_by_component", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rn = load()


def no_files(num):
    raise AssertionError("files of #%d should not be needed" % num)


class ClassifyTest(unittest.TestCase):
    def check(self, title, section, author="someone", files=None):
        got = rn.classify(title, author, (lambda: files) if files is not None else (lambda: no_files(0)))
        self.assertEqual(got, section, title)

    def test_title_prefixes(self):
        cases = {
            "s3: keep serving for -preStopSeconds after SIGTERM": "S3 API",
            "s3api: keep a multipart upload's state on the object's owner filer": "S3 API",
            "fix(s3api): reject an invalid storage class before uploading the body": "S3 API",
            "s3 lifecycle: count filer entries a walk reads but skips as progress": "S3 API",
            "iam/oidc: accept a list audience": "S3 API",
            "fix(s3tables,s3vectors): bind DeleteNamespace to the authorized records": "S3 Tables and Iceberg",
            "iceberg maintenance: expire snapshots": "S3 Tables and Iceberg",
            "s3vectors: support TagResource": "Vectors",
            "s3 vectors: page ListVectors": "Vectors",
            "vector: compact segments": "Vectors",
            "filer: fail health check when absent from lock ring": "Filer",
            "filer.sync: skip a rewritten source version": "Filer",
            "filer.remote.sync: retry": "Filer",
            "webdav: honor locks": "Filer",
            "remote.uncache, remote.meta.sync, remote.cache: keep local writes": "Filer",
            "mount: cache dir": "Mount",
            "fix(mount): flush on close": "Mount",
            "volume: keep reads over the download limit local": "Volume Server",
            "[Volume] Preserve request semantics in replica proxy reads": "Volume Server",
            "operation: pad the marginal gzip test": "Volume Server",
            "fix(storage): update deletion metrics": "Volume Server",
            "rust volume: bulk-load needle maps": "Rust Volume Server",
            "seaweed-volume: pin the status": "Rust Volume Server",
            "volume(rust): fix": "Rust Volume Server",
            "ec: mount shards through the encode's .vif": "Erasure Coding",
            "ec.rebuild: pick the right disk": "Erasure Coding",
            "ec_balance: spread shards": "Erasure Coding",
            "master: unregister a volume server whose heartbeats stop": "Master and Topology",
            "topology: never report negative free volume slots": "Master and Topology",
            "wdclient: let master-pushed adds replace locations": "Master and Topology",
            "admin: manage filer webhook targets": "Admin UI and Workers",
            "Fix/admin cluster submenu": "Admin UI and Workers",
            "weed-worker: retry": "Admin UI and Workers",
            "shell: fix s3.circuitBreaker help examples": "Shell",
            "volume.fsck: skip deleted": "Shell",
            "fix(fsck): inspect references to deleted volumes": "Shell",
            "security: optional dialed-host check": "Security",
            "helm: drop the 4.53 vector metrics render guard": "Kubernetes, Helm and Deployment",
            "install.sh: add --enterprise": "Kubernetes, Helm and Deployment",
            "mq/kafka: commit offsets": "Message Queue",
            "server: add -grpc.localSocketDir": "Core and CLI",
            "pb: recover when a local gRPC socket file is deleted": "Core and CLI",
            "docs: explain the 3-filer recommendation": "Documentation",
            "test(fuse): two mounts staging the same files": "Tests and CI",
            "test/s3vectors: a hardened cluster": "Tests and CI",
            "s3vectors test: cover tags": "Tests and CI",
            "ci: run the Python 3.9 jobs on 3.10": "Tests and CI",
            "build(deps): bump actions/setup-go from 6 to 7": "Dependencies",
        }
        for title, section in cases.items():
            self.check(title, section)

    def test_dependabot_author(self):
        self.check("Bump x from 1 to 2", "Dependencies", author="dependabot[bot]")

    def test_no_prefix_uses_files(self):
        self.check("Fix s3 cancelled read relookup", "S3 API",
                   files=["weed/s3api/s3api_object_handlers.go", "weed/s3api/s3api_object_handlers_test.go"])
        self.check("Retry transient failures in star history generation", "Tests and CI",
                   files=[".github/scripts/star_history.py"])
        self.check("fix: handle empty chunk list", "Filer",
                   files=["weed/filer/filechunks.go", "weed/storage/needle/needle.go", "weed/filer/reader_at.go"])
        self.check("Improve everything", "Other", files=[])

    def test_test_files_count_only_when_alone(self):
        self.check("Make it faster", "Mount",
                   files=["weed/mount/weedfs.go", "test/fuse/a_test.go", "test/fuse/b_test.go"])
        self.check("Make it right", "S3 API",
                   files=["weed/s3api/auth.go", "test/s3/iam/test_a.py", "test/s3/iam/test_b.py"])
        self.check("Cover more", "Tests and CI", files=["test/s3/iam/test_a.py"])


NOTES = """## What's Changed
* filer: fail health check when absent from lock ring by @a in https://github.com/seaweedfs/seaweedfs/pull/12709
* s3: answer ClientDisconnected by @b in https://github.com/seaweedfs/seaweedfs/pull/12707
* build(deps): bump actions/setup-go from 6 to 7 by @dependabot[bot] in https://github.com/seaweedfs/seaweedfs/pull/12685
* filer.sync: skip a rewritten source version by @a in https://github.com/seaweedfs/seaweedfs/pull/12366
* Fix s3 cancelled read relookup by @c in https://github.com/seaweedfs/seaweedfs/pull/12600

## New Contributors
* @c made their first contribution in https://github.com/seaweedfs/seaweedfs/pull/12600

**Full Changelog**: https://github.com/seaweedfs/seaweedfs/compare/4.53...4.54
"""


class RegroupTest(unittest.TestCase):
    def files(self, num):
        self.assertEqual(num, 12600)
        return ["weed/s3api/s3api_object_handlers.go"]

    def test_regroup(self):
        out = rn.regroup(NOTES, self.files)
        self.assertEqual(out, """## What's Changed

### S3 API
* s3: answer ClientDisconnected by @b in https://github.com/seaweedfs/seaweedfs/pull/12707
* Fix s3 cancelled read relookup by @c in https://github.com/seaweedfs/seaweedfs/pull/12600

### Filer
* filer: fail health check when absent from lock ring by @a in https://github.com/seaweedfs/seaweedfs/pull/12709
* filer.sync: skip a rewritten source version by @a in https://github.com/seaweedfs/seaweedfs/pull/12366

<details><summary>Dependencies (1 update)</summary>

* build(deps): bump actions/setup-go from 6 to 7 by @dependabot[bot] in https://github.com/seaweedfs/seaweedfs/pull/12685

</details>

## New Contributors
* @c made their first contribution in https://github.com/seaweedfs/seaweedfs/pull/12600

**Full Changelog**: https://github.com/seaweedfs/seaweedfs/compare/4.53...4.54
""")

    def test_idempotent(self):
        once = rn.regroup(NOTES, self.files)
        self.assertEqual(rn.regroup(once, self.files), once)

    def test_every_pr_kept(self):
        out = rn.regroup(NOTES, self.files)
        for n in (12709, 12707, 12685, 12366, 12600):
            self.assertEqual(out.count("/pull/%d\n" % n), 2 if n == 12600 else 1)

    def test_changelog_kept_without_new_contributors(self):
        body = NOTES.replace(
            "## New Contributors\n* @c made their first contribution in https://github.com/seaweedfs/seaweedfs/pull/12600\n\n", "")
        out = rn.regroup(body, self.files)
        self.assertTrue(out.endswith("</details>\n\n**Full Changelog**: https://github.com/seaweedfs/seaweedfs/compare/4.53...4.54\n"), out)
        self.assertEqual(rn.regroup(out, self.files), out)

    def test_unknown_line_refused(self):
        body = NOTES.replace("## New Contributors", "* a hand-written note\n\n## New Contributors")
        with self.assertRaises(ValueError):
            rn.regroup(body, self.files)
        with self.assertRaises(ValueError):
            rn.regroup(NOTES.replace("## What's Changed\n", "## What's Changed\n### Known issue\n"), self.files)

    def test_hand_written_component_heading_refused(self):
        with self.assertRaises(ValueError):
            rn.regroup(NOTES.replace("## What's Changed\n", "## What's Changed\n### Security\n"), self.files)

    def test_author_from_the_by_clause(self):
        body = "## What's Changed\n* Bump a by @dependabot[bot] in docs by @alice in https://github.com/seaweedfs/seaweedfs/pull/7\n"
        self.assertIn("### Documentation", rn.regroup(body, lambda n: ["README.md"]))

    def test_notes_without_prs_unchanged(self):
        body = "Hand-written notes\n\n**Full Changelog**: x"
        self.assertEqual(rn.regroup(body, no_files), body)


if __name__ == "__main__":
    unittest.main()
