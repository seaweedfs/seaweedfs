#!/usr/bin/env python3
"""Regroup a GitHub release's generated notes by component.

GitHub's generated notes are one flat list of PRs in merge order. This keeps every
line as GitHub wrote it, but sorts the PRs under component headings (S3 API, Filer,
Mount, Volume Server, ...), with dependency bumps folded at the end.

The component comes from the PR title prefix, which nearly every PR has
("s3api: ...", "fix(mount): ...", "filer.sync: ...", "rust volume: ..."). A PR with
no recognised prefix is placed by the files it changes.

Usage:
  release_notes_by_component.py --tag 4.54            # print the regrouped notes
  release_notes_by_component.py --tag 4.54 --write    # also update the release

Running it again on notes it already regrouped gives the same result.
"""

import argparse
import json
import re
import subprocess
import sys

# Display order. Each PR lands in exactly one section.
SECTIONS = [
    "S3 API",
    "S3 Tables and Iceberg",
    "Vectors",
    "Filer",
    "Mount",
    "Volume Server",
    "Rust Volume Server",
    "Erasure Coding",
    "Master and Topology",
    "Admin UI and Workers",
    "Shell",
    "Security",
    "Message Queue",
    "Kubernetes, Helm and Deployment",
    "Core and CLI",
    "Documentation",
    "Tests and CI",
    "Other",
    "Dependencies",
]

# Title scope -> section, first match wins. Matched against each lower-cased scope
# token, e.g. "filer.sync", "s3 lifecycle", "weed/s3api" (a "weed/" prefix is
# dropped first). Order matters: specific scopes come before the general ones
# they start with (s3tables before s3, volume.fsck before volume).
SCOPE_RULES = [
    (r"(build\(deps\)|deps|dependabot)\b", "Dependencies"),
    (r"(test|tests|e2e|integration)\b|.*\btests?$|test/", "Tests and CI"),
    (r"(rust[ -]?volume|seaweed-volume|volume\(rust\)|rust)\b", "Rust Volume Server"),
    (r"(s3 ?vectors?|vectors?|seaweed-vector|weed-vector|lance)\b", "Vectors"),
    (r"(s3 ?tables|iceberg)\b", "S3 Tables and Iceberg"),
    (r"(s3 sink|remote|remote_storage|filer|webdav|sftp|ftp|notification|replication|"
     r"redis\d*|leveldb\d*|backup)\b", "Filer"),
    (r"(s3|s3api|iam|oidc|sts|credential|sse|cors|policy|object ?lock)\b", "S3 API"),
    (r"(mount|fuse)\b", "Mount"),
    (r"(ec|ec_\w+|ec\.\w+|erasure[ _-]?coding|worker/ec)\b", "Erasure Coding"),
    (r"(master|topology|wdclient|cluster|raft|placement)\b", "Master and Topology"),
    (r"(admin|worker|weed-worker|plugin|plugin worker|maintenance)\b",
     "Admin UI and Workers"),
    (r"(shell|fsck|volume\.\w+|fs\.\w+|collection\.\w+|bucket\.\w+)", "Shell"),
    (r"(volume|volume_\w+|storage|vacuum|operation|compression|needle)\b", "Volume Server"),
    (r"(security|tls|mtls|jwt|grpc|kms)\b", "Security"),
    (r"(mq|kafka|messaging)\b", "Message Queue"),
    (r"(helm|k8s|kubernetes|operator|csi|docker|grafana|install\.sh|chart|snap|terraform)\b",
     "Kubernetes, Helm and Deployment"),
    (r"(docs?|readme|wiki)\b", "Documentation"),
    (r"(ci|release|build|github|workflow|actions)\b", "Tests and CI"),
    (r"(server|command|mini|weed-db|weed|util|pb|cmd|glog|stats|telemetry|query|config)\b",
     "Core and CLI"),
]

# Changed path prefix -> section, for PRs whose title has no usable prefix.
# Longest prefix wins.
PATH_RULES = {
    "weed/s3api/s3tables/": "S3 Tables and Iceberg",
    "weed/s3api/iceberg/": "S3 Tables and Iceberg",
    "weed/s3api/s3vectors/": "Vectors",
    "weed/s3api/lance/": "Vectors",
    "seaweed-vector/": "Vectors",
    "weed/s3api/": "S3 API",
    "weed/iam/": "S3 API",
    "weed/iamapi/": "S3 API",
    "weed/credential/": "S3 API",
    "weed/filer/": "Filer",
    "weed/filer_client/": "Filer",
    "weed/server/filer": "Filer",
    "weed/server/webdav": "Filer",
    "weed/sftpd/": "Filer",
    "weed/replication/": "Filer",
    "weed/notification/": "Filer",
    "weed/remote_storage/": "Filer",
    "weed/mount/": "Mount",
    "weed/storage/erasure_coding/": "Erasure Coding",
    "weed/ec/": "Erasure Coding",
    "weed/worker/tasks/erasure_coding/": "Erasure Coding",
    "weed/storage/": "Volume Server",
    "weed/server/volume": "Volume Server",
    "weed/operation/": "Volume Server",
    "seaweed-volume/": "Rust Volume Server",
    "weed/topology/": "Master and Topology",
    "weed/server/master": "Master and Topology",
    "weed/wdclient/": "Master and Topology",
    "weed/cluster/": "Master and Topology",
    "weed/placement/": "Master and Topology",
    "weed/admin/": "Admin UI and Workers",
    "weed/worker/": "Admin UI and Workers",
    "weed/plugin/": "Admin UI and Workers",
    "seaweed-worker/": "Admin UI and Workers",
    "weed/shell/": "Shell",
    "weed/security/": "Security",
    "weed/kms/": "Security",
    "weed/mq/": "Message Queue",
    "k8s/": "Kubernetes, Helm and Deployment",
    "docker/": "Kubernetes, Helm and Deployment",
    "snap/": "Kubernetes, Helm and Deployment",
    "terraform/": "Kubernetes, Helm and Deployment",
    "weed/": "Core and CLI",
    "test/": "Tests and CI",
    ".github/": "Tests and CI",
}

PR_LINE = re.compile(r"^\* (?P<title>.+?) by @(?P<author>\S+) in https://github\.com/\S+/pull/(?P<num>\d+)\s*$")
# Lines regroup() writes between the PRs, so a second run accepts them.
SCAFFOLD = re.compile(r"^$|^### (%s)$|^<details><summary>Dependencies \(\d+ updates?\)</summary>$|^</details>$"
                      % "|".join(re.escape(s) for s in SECTIONS))
# "fix(s3tables,s3vectors): ...", "s3 lifecycle: ...", "[Volume] ...", "Fix/admin ..."
CONVENTIONAL = re.compile(r"^(?P<type>[a-z]+)\((?P<scope>[^)]*)\)!?$")
TYPE_ONLY = {"fix", "feat", "feature", "perf", "refactor", "chore", "style", "revert", "improvement"}


def scope_of(title):
    """Return the raw scope text of a PR title, or ""."""
    t = title.strip()
    m = re.match(r"^\[([^\]]{1,40})\]", t)
    if m:
        return m.group(1)
    m = re.match(r"^(?:fix|feat|feature)/(\S+)", t, re.I)
    if m:
        return m.group(1)
    head, sep, _ = t.partition(":")
    if not sep or len(head) > 50:
        return ""
    head = head.strip()
    m = CONVENTIONAL.match(head.lower())
    if m:
        kind, scope = m.group("type"), m.group("scope")
        if kind in ("test", "tests", "ci"):
            return "test"
        if kind == "docs":
            return "docs"
        if kind == "build" and scope == "deps":
            return "build(deps)"
        return scope
    if head.lower() in TYPE_ONLY:
        return ""
    return head


def section_by_scope(scope):
    if scope.strip().lower() == "build(deps)":
        return "Dependencies"
    for token in re.split(r"\s*[,+&]\s*|\s+and\s+", scope.lower()):
        token = token.strip()
        token = re.sub(r"^weed[/ ]", "", token)
        if not token:
            continue
        for pattern, section in SCOPE_RULES:
            if re.match(pattern, token):
                return section
    return ""


def section_by_paths(paths):
    votes = {}
    code = [p for p in paths if not p.endswith(("_test.go", "_test.rs")) and not p.startswith("test/")]
    for p in code or paths:
        if p.endswith(".md"):
            section = "Documentation"
        else:
            best = max((pre for pre in PATH_RULES if p.startswith(pre)), key=len, default="")
            section = PATH_RULES.get(best, "Other")
        votes[section] = votes.get(section, 0) + 1
    if not votes:
        return "Other"
    # Most files wins; ties go to the earlier section in display order.
    return max(votes, key=lambda s: (votes[s], -SECTIONS.index(s)))


def classify(title, author, pr_files):
    if author.startswith("dependabot") or title.lower().startswith("build(deps)"):
        return "Dependencies"
    section = section_by_scope(scope_of(title))
    if section:
        return section
    return section_by_paths(pr_files())


def split_notes(body):
    """Split generated notes into (PR lines, the text before them, the text after,
    the non-blank lines of What's Changed)."""
    lines = body.replace("\r\n", "\n").split("\n")
    start = next((i for i, l in enumerate(lines) if l.strip() == "## What's Changed"), None)
    if start is None:
        return [], body, "", []
    end = next((i for i in range(start + 1, len(lines))
                if lines[i].startswith(("## ", "**Full Changelog**"))), len(lines))
    prs, stray = [], []
    block = [l.rstrip() for l in lines[start + 1:end] if l.strip()]
    for line in lines[start + 1:end]:
        if PR_LINE.match(line):
            prs.append(line)
        elif not SCAFFOLD.match(line.strip()):
            stray.append(line)
    if prs and stray:
        raise ValueError("What's Changed has lines that are not PRs: %r" % stray[:3])
    before = "\n".join(lines[:start]).rstrip()
    after = "\n".join(lines[end:]).strip()
    return prs, before, after, block


def regroup(body, pr_files):
    prs, before, after, block = split_notes(body)
    if not prs:
        return body
    groups = {s: [] for s in SECTIONS}
    for line in prs:
        m = PR_LINE.match(line)
        num = int(m.group("num"))
        groups[classify(m.group("title"), m.group("author"), lambda: pr_files(num))].append(line)

    out = [before, ""] if before else []
    out.append("## What's Changed")
    for section in SECTIONS:
        items = groups[section]
        if not items:
            continue
        out.append("")
        if section == "Dependencies":
            out.append("<details><summary>Dependencies (%d update%s)</summary>" % (len(items), "" if len(items) == 1 else "s"))
            out.append("")
            out.extend(items)
            out.append("")
            out.append("</details>")
        else:
            out.append("### " + section)
            out.extend(items)
    if after:
        out.extend(["", after])
    notes = "\n".join(out).strip() + "\n"
    # Headings are accepted only as this script's own grouping: a hand-written
    # "### Security" over PRs of other components would otherwise be dropped.
    if len(block) != len(prs) and split_notes(notes)[3] != block:
        raise ValueError("What's Changed has headings that do not match the component grouping")
    return notes


def gh(*args, stdin=None):
    return subprocess.run(["gh", *args], input=stdin, capture_output=True, text=True, check=True).stdout


def pr_files(repo, num):
    return gh("pr", "view", str(num), "-R", repo, "--json", "files", "--jq", ".files[].path").splitlines()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo", default="seaweedfs/seaweedfs")
    ap.add_argument("--tag", required=True)
    ap.add_argument("--write", action="store_true", help="update the release notes on GitHub")
    args = ap.parse_args()

    body = json.loads(gh("release", "view", args.tag, "-R", args.repo, "--json", "body"))["body"]

    failed = []

    def files_of(num):
        try:
            return pr_files(args.repo, num)
        except subprocess.CalledProcessError as e:
            print("warning: files of #%d: %s" % (num, e.stderr.strip()), file=sys.stderr)
            failed.append(num)
            return []

    try:
        notes = regroup(body, files_of)
    except ValueError as e:
        sys.exit("release %s: %s" % (args.tag, e))
    sys.stdout.write(notes)
    if notes.strip() == body.replace("\r\n", "\n").strip():
        print("release %s: notes already grouped, nothing to change" % args.tag, file=sys.stderr)
        return
    if failed and args.write:
        sys.exit("release %s: not writing, could not read the files of %s"
                 % (args.tag, ", ".join("#%d" % n for n in failed)))
    if args.write:
        gh("release", "edit", args.tag, "-R", args.repo, "--notes-file", "-", stdin=notes)
        print("release %s: notes updated" % args.tag, file=sys.stderr)


if __name__ == "__main__":
    main()
