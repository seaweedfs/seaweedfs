#!/usr/bin/env python3
"""Run the s3-listing-study tool capsules and ListObjects reference probes against SeaweedFS.

Tool cases: every mode of every capsule under the study's tools/ is compiled by
the capsule's own command.py (benchmark.adapters.compile_command, with the
custom-endpoint wiring the study's replay-endpoint pilot established), run
against SeaweedFS with the pinned tool build, normalized by the capsule's own
normalize.py, and the normalized listing is compared exactly against the keys
that were written.

Reference probes: raw, signed ListObjectsV2/ListObjects requests implementing
the strategies the study classifies tools by (serial pagination, delimiter
prefix discovery, start-after range splitting) plus the request semantics in
docs/s3-reference.md, checked against the byte-ordered key set.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

REGION = "us-east-1"
S3NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def canary_keys() -> dict[str, int]:
    """benchmark/fixtures/replay-canary/generate.sql: 2,048 keys in 16 prefixes."""
    return {f"group-{i // 128:02d}/object-{i:06d}.dat": 1024 + i for i in range(2048)}


EDGE_KEYS = [
    # docs/smoke-bucket.md: a root key beside the prefixes, for delimiter checks.
    "index.html",
    # docs/s3-reference.md examples.
    "test_file(3).png",
    "photos/2006/February/sample.jpg",
    "photos/2006/January/sample.jpg",
    "photos/2007/sample.jpg",
    "notes/summer/july",
    "notes/summer/august",
    "notes/winter",
    # Byte order around the '/' (0x2f) delimiter.
    "order/a",
    "order/a b",
    "order/a!b",
    "order/a-b",
    "order/a.b",
    "order/a/b",
    "order/a/c/d",
    "order/a0",
    "order/aa",
    "order/a~",
    "order/A",
    "order/b",
    # Zero-byte directory markers.
    "markers/dir/",
    "markers/dir/child",
    "markers/empty/",
    # Characters that need XML or URL escaping.
    "chars/space key",
    "chars/plus+key",
    "chars/percent%20key",
    "chars/amp&key",
    "chars/lt<gt>key",
    'chars/quote"key',
    "chars/apos'key",
    "chars/hash#key",
    "chars/question?key",
    "chars/equals=key",
    "chars/semicolon;key",
    "chars/colon:key",
    "chars/at@key",
    "chars/comma,key",
    "chars/star*key",
    "chars/bang!key",
    "chars/dollar$key",
    "chars/caret^key",
    "chars/backslash\\key",
    "chars/brace{key}",
    "chars/pipe|key",
    "chars/backtick`key",
    "chars/tilde~key",
    # UTF-8 byte order, which differs from UTF-16 order above U+FFFF.
    "unicode/café-nfc",
    "unicode/café-nfd",
    "unicode/日本語.txt",
    "unicode/｡-halfwidth",
    "unicode/\U0001f600-emoji",
    "unicode/z-ascii",
    "deep/a/b/c/d/e/f/g/h/i/j/k.txt",
    "long/" + "/".join(["y" * 200] * 4),
]


def edge_keys() -> dict[str, int]:
    return {k: (0 if k.endswith("/") else 100 + i) for i, k in enumerate(EDGE_KEYS)}


def body_for(key: str, size: int) -> bytes:
    seed = hashlib.sha256(key.encode()).digest()
    return (seed * (size // len(seed) + 1))[:size]


def utf8_sorted(keys) -> list[str]:
    return sorted(keys, key=lambda k: k.encode("utf-8"))


# ---------------------------------------------------------------- signed HTTP


class S3:
    def __init__(self, endpoint: str, access: str, secret: str) -> None:
        from botocore.auth import S3SigV4Auth
        from botocore.credentials import Credentials

        self.endpoint = endpoint.rstrip("/")
        self.auth = S3SigV4Auth(Credentials(access, secret), "s3", REGION)

    def request(
        self, method: str, path: str, params: list[tuple[str, str]] | None = None, body: bytes = b""
    ) -> tuple[int, bytes]:
        from botocore.awsrequest import AWSRequest

        query = "&".join(
            f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}"
            for k, v in (params or [])
        )
        url = self.endpoint + urllib.parse.quote(path, safe="/-_.~") + (f"?{query}" if query else "")
        req = AWSRequest(method=method, url=url, data=body)
        req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
        self.auth.add_auth(req)
        prepared = req.prepare()
        http = urllib.request.Request(prepared.url, data=body or None, method=method)
        for k, v in prepared.headers.items():
            http.add_header(k, v)
        try:
            with urllib.request.urlopen(http, timeout=60) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as err:
            return err.code, err.read()

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        # The first writes to a new bucket can get a retryable 500 while its
        # volumes grow; SDK clients retry those the same way.
        for attempt in range(10):
            status, out = self.request("PUT", f"/{bucket}/{key}", body=body)
            if status < 500:
                break
            time.sleep(0.5 * (attempt + 1))
        if status != 200:
            raise RuntimeError(f"PUT {bucket}/{key!r}: {status} {out[:300]!r}")

    def delete_object(self, bucket: str, key: str) -> None:
        status, out = self.request("DELETE", f"/{bucket}/{key}")
        if status != 204:
            raise RuntimeError(f"DELETE {bucket}/{key!r}: {status} {out[:300]!r}")

    def create_bucket(self, bucket: str) -> None:
        status, out = self.request("PUT", f"/{bucket}")
        if status not in (200, 409):
            raise RuntimeError(f"create bucket {bucket}: {status} {out[:300]!r}")

    def list_page(self, bucket: str, params: list[tuple[str, str]]) -> ET.Element:
        status, out = self.request("GET", f"/{bucket}", params)
        if status != 200:
            raise AssertionError(f"list {params}: HTTP {status} {out[:300]!r}")
        return ET.fromstring(out)


def text(el: ET.Element, name: str) -> str | None:
    child = el.find(S3NS + name)
    return None if child is None else (child.text or "")


@dataclass
class Page:
    entries: list[tuple[str, str]]  # ("key"|"prefix", value) in response order
    truncated: bool
    token: str | None
    next_marker: str | None
    key_count: str | None
    raw: ET.Element


def check_byte_order(values: list[str], what: str) -> None:
    for a, b in zip(values, values[1:]):
        assert a.encode() <= b.encode(), f"{what} out of byte order: {a!r} before {b!r}"


def parse_page(root: ET.Element, url_encoded: bool) -> Page:
    dec = urllib.parse.unquote_plus if url_encoded else (lambda s: s)
    entries: list[tuple[str, str]] = []
    for child in root:
        if child.tag == S3NS + "Contents":
            entries.append(("key", dec(text(child, "Key") or "")))
        elif child.tag == S3NS + "CommonPrefixes":
            entries.append(("prefix", dec(text(child, "Prefix") or "")))
    nm = text(root, "NextMarker")
    # XML carries Contents and CommonPrefixes as two lists, each in byte order;
    # their merged byte order is what pagination has to respect.
    for kind in ("key", "prefix"):
        check_byte_order([v for k, v in entries if k == kind], f"{kind} list")
    entries.sort(key=lambda e: e[1].encode())
    return Page(
        entries,
        text(root, "IsTruncated") == "true",
        text(root, "NextContinuationToken"),
        dec(nm) if nm is not None else None,
        text(root, "KeyCount"),
        root,
    )


# ---------------------------------------------------------------- expectations


def direct_view(keys: list[str], prefix: str, delimiter: str) -> list[tuple[str, str]]:
    """What a delimiter listing of prefix returns, in byte order."""
    out: dict[str, str] = {}
    for k in keys:
        if not k.startswith(prefix):
            continue
        rest = k[len(prefix) :]
        idx = rest.find(delimiter) if delimiter else -1
        if idx >= 0:
            out[prefix + rest[: idx + len(delimiter)]] = "prefix"
        else:
            out[k] = "key"
    return [(out[k], k) for k in utf8_sorted(out)]


# ---------------------------------------------------------------- reference probes


class Probe:
    def __init__(self, s3: S3, bucket: str, keys: list[str]) -> None:
        self.s3, self.bucket, self.keys = s3, bucket, utf8_sorted(keys)

    def v2_all(self, base: list[tuple[str, str]], max_keys: int, url: bool = False) -> list[Page]:
        pages: list[Page] = []
        token = None
        while True:
            params = [("list-type", "2"), ("max-keys", str(max_keys)), *base]
            if url:
                params.append(("encoding-type", "url"))
            if token:
                params.append(("continuation-token", token))
            page = parse_page(self.s3.list_page(self.bucket, params), url)
            pages.append(page)
            n = len(page.entries)
            assert n <= max_keys, f"page holds {n} > max-keys {max_keys}"
            assert page.key_count == str(n), f"KeyCount {page.key_count} != {n} entries"
            if not page.truncated:
                assert not page.token, "NextContinuationToken on an untruncated page"
                return pages
            assert page.token, "IsTruncated without NextContinuationToken"
            assert len(pages) < 10000, "pagination does not terminate"
            token = page.token

    def v1_all(self, base: list[tuple[str, str]], max_keys: int, delimiter: str) -> list[Page]:
        pages: list[Page] = []
        marker = None
        while True:
            params = [("max-keys", str(max_keys)), *base]
            if delimiter:
                params.append(("delimiter", delimiter))
            if marker is not None:
                params.append(("marker", marker))
            page = parse_page(self.s3.list_page(self.bucket, params), False)
            pages.append(page)
            assert len(page.entries) <= max_keys
            if not page.truncated:
                return pages
            if delimiter:
                assert page.next_marker, "truncated delimiter page without NextMarker"
                marker = page.next_marker
            else:
                marker = page.next_marker or page.entries[-1][1]
            assert len(pages) < 10000, "pagination does not terminate"

    @staticmethod
    def flat(pages: list[Page]) -> list[tuple[str, str]]:
        return [e for p in pages for e in p.entries]

    def expect_eq(self, got, want, what: str) -> None:
        if got != want:
            gs, ws = set(got), set(want)
            missing = [w for w in want if w not in gs][:5]
            extra = [g for g in got if g not in ws][:5]
            first = next((i for i, (a, b) in enumerate(zip(got, want)) if a != b), min(len(got), len(want)))
            raise AssertionError(
                f"{what}: got {len(got)} want {len(want)}; missing {missing} extra {extra}; "
                f"first difference at {first}: got {got[first:first + 3]} want {want[first:first + 3]}"
            )

    # Serial pagination (aws-cli, mc, s3kor, s5cmd ls).
    def serial(self, prefix: str, max_keys: int) -> None:
        want = [("key", k) for k in self.keys if k.startswith(prefix)]
        got = self.flat(self.v2_all([("prefix", prefix)], max_keys))
        self.expect_eq(got, want, f"serial prefix={prefix!r} max-keys={max_keys}")

    # Delimiter view and page boundaries inside it.
    def delimiter(self, prefix: str, delimiter: str, max_keys: int, v1: bool = False) -> None:
        want = direct_view(self.keys, prefix, delimiter)
        if v1:
            got = self.flat(self.v1_all([("prefix", prefix)], max_keys, delimiter))
        else:
            got = self.flat(self.v2_all([("prefix", prefix), ("delimiter", delimiter)], max_keys))
        self.expect_eq(got, want, f"{'v1' if v1 else 'v2'} delimiter={delimiter!r} prefix={prefix!r} max-keys={max_keys}")

    # Prefix discovery (rclone walk, s7cmd, s4cmd): recurse through CommonPrefixes.
    def walk(self, max_keys: int, v1: bool = False) -> None:
        found: list[str] = []
        queue = [""]
        while queue:
            prefix = queue.pop()
            if v1:
                entries = self.flat(self.v1_all([("prefix", prefix)], max_keys, "/"))
            else:
                entries = self.flat(self.v2_all([("prefix", prefix), ("delimiter", "/")], max_keys))
            for kind, value in entries:
                (queue.append if kind == "prefix" else found.append)(value)
        self.expect_eq(utf8_sorted(found), self.keys, f"{'v1' if v1 else 'v2'} delimiter walk max-keys={max_keys}")
        assert len(found) == len(set(found)), "walk returned a key twice"

    # Range splitting (S3P, s3-fast-list, Swath): start-after cut points.
    def ranges(self, cuts: list[str], max_keys: int) -> None:
        cuts = sorted(set(cuts), key=lambda c: c.encode())
        bounds = [None, *cuts, None]
        found: list[str] = []
        for lo, hi in zip(bounds, bounds[1:]):
            token = None
            done = False
            while not done:
                params = [("list-type", "2"), ("max-keys", str(max_keys))]
                if token:
                    params.append(("continuation-token", token))
                elif lo is not None:
                    params.append(("start-after", lo))
                page = parse_page(self.s3.list_page(self.bucket, params), False)
                for _, key in page.entries:
                    if lo is not None:
                        assert key.encode() > lo.encode(), f"start-after {lo!r} returned {key!r}"
                    if hi is not None and key.encode() > hi.encode():
                        done = True
                        break
                    found.append(key)
                if not page.truncated:
                    break
                token = page.token
        self.expect_eq(found, self.keys, f"start-after ranges ({len(cuts)} cuts, max-keys={max_keys})")

    def start_after_with_delimiter(self, prefix: str, start_after: str) -> None:
        want = [
            e
            for e in direct_view(self.keys, prefix, "/")
            if e[1].encode() > start_after.encode()
        ]
        got = self.flat(
            self.v2_all([("prefix", prefix), ("delimiter", "/"), ("start-after", start_after)], 1000)
        )
        self.expect_eq(got, want, f"start-after={start_after!r} delimiter=/ prefix={prefix!r}")

    def token_overrides_start_after(self) -> None:
        first = parse_page(self.s3.list_page(self.bucket, [("list-type", "2"), ("max-keys", "5")]), False)
        assert first.truncated and first.token
        params = [("list-type", "2"), ("max-keys", "5"), ("continuation-token", first.token)]
        plain = parse_page(self.s3.list_page(self.bucket, params), False)
        mixed = parse_page(
            self.s3.list_page(self.bucket, [*params, ("start-after", self.keys[len(self.keys) // 2])]),
            False,
        )
        self.expect_eq(mixed.entries, plain.entries, "continuation-token with a later start-after")

    def versions(self, prefix: str, delimiter: str, max_keys: int, url: bool) -> None:
        """ListObjectVersions paging (key-marker/version-id-marker) over unversioned keys."""
        want = direct_view(self.keys, prefix, delimiter) if delimiter else [("key", k) for k in self.keys if k.startswith(prefix)]
        got: list[tuple[str, str]] = []
        key_marker = version_marker = None
        dec = urllib.parse.unquote_plus if url else (lambda v: v)
        for _ in range(10000):
            params = [("versions", ""), ("max-keys", str(max_keys)), ("prefix", prefix)]
            if delimiter:
                params.append(("delimiter", delimiter))
            if url:
                params.append(("encoding-type", "url"))
            if key_marker is not None:
                params += [("key-marker", key_marker), ("version-id-marker", version_marker or "")]
            root = self.s3.list_page(self.bucket, params)
            page: list[tuple[str, str]] = []
            for child in root:
                if child.tag in (S3NS + "Version", S3NS + "DeleteMarker"):
                    page.append(("key", dec(text(child, "Key") or "")))
                elif child.tag == S3NS + "CommonPrefixes":
                    page.append(("prefix", dec(text(child, "Prefix") or "")))
            assert len(page) <= max_keys, f"{len(page)} entries > max-keys {max_keys}"
            for kind in ("key", "prefix"):
                check_byte_order([v for k, v in page if k == kind], f"version {kind} list")
            got += sorted(page, key=lambda e: e[1].encode())
            if text(root, "IsTruncated") != "true":
                break
            nk = text(root, "NextKeyMarker")
            assert nk, "truncated versions page without NextKeyMarker"
            key_marker, version_marker = dec(nk), text(root, "NextVersionIdMarker")
        self.expect_eq(got, want, f"versions prefix={prefix!r} delimiter={delimiter!r} max-keys={max_keys} url={url}")

    def url_encoding(self, prefix: str) -> None:
        want = [("key", k) for k in self.keys if k.startswith(prefix)]
        got = self.flat(self.v2_all([("prefix", prefix)], 7, url=True))
        self.expect_eq(got, want, f"encoding-type=url prefix={prefix!r}")
        root = self.s3.list_page(
            self.bucket,
            [("list-type", "2"), ("encoding-type", "url"), ("prefix", prefix), ("delimiter", "/"), ("start-after", prefix + "a b")],
        )
        for name, value in (("Prefix", prefix), ("Delimiter", "/"), ("StartAfter", prefix + "a b")):
            raw = text(root, name)
            assert raw is not None and urllib.parse.unquote_plus(raw) == value, f"encoded {name} {raw!r} != {value!r}"
        assert text(root, "EncodingType") == "url", "EncodingType not echoed"


def deleted_prefixes(s3: S3, bucket: str) -> None:
    """A prefix whose last key was deleted is no longer a CommonPrefix."""
    s3.create_bucket(bucket)
    keys = ["keep", "gone/a/b", "gone/c", "half/x", "half/y/z"]
    for k in keys:
        s3.put_object(bucket, k, b"x")
    for k in ("gone/a/b", "gone/c", "half/y/z"):
        s3.delete_object(bucket, k)
    p = Probe(s3, bucket, ["keep", "half/x"])
    p.delimiter("", "/", 1000)
    p.delimiter("half/", "/", 1000)
    p.delimiter("gone/", "/", 1000)


def run_probes(
    s3: S3, buckets: dict[str, dict[str, int]], churn_bucket: str, only: re.Pattern | None
) -> list[tuple[str, str | None]]:
    results: list[tuple[str, str | None]] = []

    def check(name: str, fn, *a) -> None:
        if only and not only.search(name):
            return
        try:
            fn(*a)
            results.append((name, None))
        except Exception as exc:  # noqa: BLE001
            results.append((name, f"{type(exc).__name__}: {exc}"))

    check(f"reference:{churn_bucket}:deleted-prefixes", deleted_prefixes, s3, churn_bucket)
    for bucket, keymap in buckets.items():
        p = Probe(s3, bucket, list(keymap))
        keys = p.keys
        tag = f"reference:{bucket}"
        for mk in (1, 7, 100, 1000):
            check(f"{tag}:serial:max-keys={mk}", p.serial, "", mk)
        check(f"{tag}:url-encoding", p.url_encoding, "")
        check(f"{tag}:token-overrides-start-after", p.token_overrides_start_after)
        for mk in (2, 1000):
            check(f"{tag}:walk-v2:max-keys={mk}", p.walk, mk)
            check(f"{tag}:walk-v1:max-keys={mk}", p.walk, mk, True)
        prefixes = sorted({k[: k.index("/") + 1] for k in keys if "/" in k})
        scopes = ["", *prefixes[:: max(1, len(prefixes) // 4)]]
        if bucket.endswith("edge"):
            scopes += ["order/", "order/a", "chars/", "unicode/", "markers/", "markers/dir/", "photos/2006/", "o"]
        for prefix in scopes:
            for mk in (1, 3, 1000):
                check(f"{tag}:delimiter-v2:prefix={prefix}:max-keys={mk}", p.delimiter, prefix, "/", mk)
                check(f"{tag}:delimiter-v1:prefix={prefix}:max-keys={mk}", p.delimiter, prefix, "/", mk, True)
        for prefix in ("", scopes[-1]):
            for delim in ("", "/"):
                for mk in (2, 1000):
                    check(f"{tag}:versions:prefix={prefix}:delimiter={delim}:max-keys={mk}", p.versions, prefix, delim, mk, False)
            check(f"{tag}:versions-url:prefix={prefix}", p.versions, prefix, "/", 3, True)
        for d in ("-", "a", "ou"):
            check(f"{tag}:delimiter={d}", p.delimiter, "", d, 1000)
        check(f"{tag}:serial:prefix-no-slash", p.serial, keys[len(keys) // 3][:4], 5)
        # Cut points that are existing keys, absent keys between keys, prefixes,
        # and a common prefix with and without its trailing delimiter.
        step = max(1, len(keys) // 9)
        existing = keys[step::step]
        absent = [k + "\x7f" for k in existing[::2]] + [k[:-1] for k in existing[1::2] if len(k) > 1]
        dirs = [k[: k.rfind("/") + 1] for k in existing if "/" in k]
        dirs += [d.rstrip("/") for d in dirs]
        for name, cuts in (("existing", existing), ("absent", absent), ("dirs", dirs), ("mixed", existing + absent + dirs)):
            for mk in (3, 1000):
                check(f"{tag}:ranges-{name}:max-keys={mk}", p.ranges, cuts, mk)
        for sa in (keys[len(keys) // 2], prefixes[len(prefixes) // 2], prefixes[len(prefixes) // 2].rstrip("/")):
            check(f"{tag}:start-after-delimiter:{sa}", p.start_after_with_delimiter, "", sa)
        if bucket.endswith("edge"):
            for sa in ("order/a", "order/a/", "order/a/b", "order/a.b", "order/a0"):
                check(f"{tag}:start-after-delimiter:{sa}", p.start_after_with_delimiter, "order/", sa)
            check(f"{tag}:url-encoding:chars/", p.url_encoding, "chars/")
    return results


# ---------------------------------------------------------------- tool capsules


@dataclass
class Case:
    tool: str
    mode: str
    bucket: str
    prefix: str
    status: str = ""
    detail: str = ""
    argv: list[str] = field(default_factory=list)


class ToolRunner:
    def __init__(self, args, study: Path, work: Path, endpoint: str) -> None:
        sys.path.insert(0, str(study / "benchmark" / "src"))
        from benchmark import adapters  # noqa: PLC0415
        from benchmark.runtime.command_adapter import CommandAdapterError, Stated  # noqa: PLC0415

        self.adapters, self.refusal, self.stated = adapters, CommandAdapterError, Stated
        self.study, self.work, self.endpoint = study, work, endpoint
        tools = Path(args.tools_dir).resolve()
        self.fixture_dir = work / "fixtures"
        self.path_map = {
            "/usr/local/bin/aws": str(tools / "aws-cli-bin" / "aws"),
            "/usr/bin/mc": str(tools / "bin" / "mc"),
            "/usr/local/bin/pS3": str(tools / "bin" / "pS3"),
            "/usr/local/bin/rclone": str(tools / "bin" / "rclone"),
            "/usr/bin/s3-fast-list": str(tools / "bin" / "s3-fast-list"),
            "/usr/bin/ks-tool": str(tools / "bin" / "ks-tool"),
            "/usr/local/bin/s3kor": str(tools / "bin" / "s3kor"),
            "/usr/local/bin/s3p": str(tools / "bin" / "s3p"),
            "/usr/local/bin/s4cmd": str(tools / "bin" / "s4cmd"),
            "/s5cmd": str(tools / "bin" / "s5cmd"),
            "/usr/local/bin/s7cmd": str(tools / "bin" / "s7cmd"),
            "/opt/java/openjdk/bin/java": str(tools / "swath" / "openjdk" / "bin" / "java"),
            "/opt/swath/swath.jar": str(tools / "swath" / "swath.jar"),
            "/usr/bin/python3": sys.executable,
            "/opt/benchmark/tools/": str(study / "tools") + "/",
        }
        os.environ["PYTHONPATH"] = str(study / "benchmark" / "src")
        self.env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "AWS_ACCESS_KEY_ID": args.access_key,
            "AWS_SECRET_ACCESS_KEY": args.secret_key,
            "AWS_REGION": REGION,
            "AWS_DEFAULT_REGION": REGION,
            "AWS_EC2_METADATA_DISABLED": "true",
            "TZ": "UTC",
            "LC_ALL": "C.UTF-8",
            "PYTHONPATH": str(study / "benchmark" / "src"),
        }

    def localize(self, argv, bucket: str) -> list[str]:
        out = []
        path_map = {**self.path_map, "/fixtures/source/": f"{self.fixture_dir / bucket}/"}
        for token in argv:
            for src, dst in path_map.items():
                if token == src or (src.endswith("/") and token.startswith(src)):
                    token = dst + token[len(src) :]
                    break
            out.append(token)
        return out

    def adapter_dir(self, tool: str) -> Path:
        return self.study / "tools" / tool / "adapter"

    def write_fixture_companions(self, bucket: str, keys: list[str]) -> None:
        """The staged-fixture companions, built the way fixture_bundle.py builds them."""
        import duckdb  # noqa: PLC0415

        out = self.fixture_dir / bucket
        shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True)
        shards = sorted({k.split("/", 1)[0] for k in keys})
        if all(re.fullmatch(r"[A-Za-z0-9._/-]+", s) for s in shards):
            (out / "s5cmd-shards.input").write_text("".join(f"{s}\n" for s in shards))
        parts = out / "parquet"
        parts.mkdir()
        with duckdb.connect() as con:
            con.execute("CREATE TABLE f (key BLOB, row_type VARCHAR)")
            con.executemany("INSERT INTO f VALUES (?, 'OBJECT')", [(k.encode(),) for k in keys])
            con.execute(f"COPY f TO '{parts / 'part-00000.parquet'}' (FORMAT parquet)")
        sys.path.insert(0, str(self.study / "tools" / "s3-fast-list" / "adapter"))
        from fixture_hints import FixtureHintsError, generate_hints  # noqa: PLC0415

        try:
            generate_hints(parts, out / "s3-fast-list-hints.input", segments=4)
        except FixtureHintsError as exc:
            print(f"{bucket}: no s3-fast-list fixture hints: {exc}", flush=True)

    def inject_endpoint(self, tool: str, argv: list[str], env: dict) -> tuple[list[str], dict]:
        """Add the endpoint the way the replay-endpoint pilot (run.sh) did, for modes
        the capsule refuses only because the replay server lacks the API."""
        ep = self.endpoint
        if tool == "rclone":
            return [a.replace(f"region={REGION}", f'region={REGION},endpoint="{ep}",force_path_style=true') for a in argv], env
        if tool == "minio-mc":
            return argv, {**env, "MC_HOST_s3": ep, "MC_REGION": REGION}
        if tool == "s3kor":
            return [argv[0], argv[1], "--custom-endpoint-url", ep, *argv[2:]], env
        if tool == "s7cmd":
            return [*argv[:-1], "--target-endpoint-url", ep, "--target-force-path-style", argv[-1]], env
        if tool == "s5cmd":
            return [argv[0], "--endpoint-url", ep, *argv[1:]], env
        return [*argv, "--endpoint-url", ep], env

    def config_for(self, adapter, case: Case, mode: str, keys: list[str]) -> dict[str, object]:
        config: dict[str, object] = {}
        for name, axis in adapter.modes[mode].axes.items():
            if isinstance(axis, self.stated):
                config[name] = 4
        if adapter.tool == "s5cmd" and mode == "fanout-with-dirs":
            initials = sorted({k[len(case.prefix)] for k in keys if k.startswith(case.prefix) and len(k) > len(case.prefix)})
            if not all(re.fullmatch(r"[A-Za-z0-9._-]", c) for c in initials):
                raise NotApplicable(f"key initials {initials} are outside the capsule's safe shard set")
            config["shard_initials"] = "".join(initials)
        return config

    def execute(self, adapter, case: Case, mode: str, sink: Path, keys: list[str], artifact_path: str = "") -> tuple[Path | None, dict]:
        signed = adapter.supports_signed
        inject = case.tool in INJECT_ENDPOINT and mode in INJECT_ENDPOINT[case.tool]
        config = self.config_for(adapter, case, mode, keys)
        argv, env = self.adapters.compile_command(
            self.adapter_dir(case.tool),
            case.tool,
            mode=mode,
            bucket=case.bucket,
            region=REGION,
            prefix=case.prefix,
            signed=signed,
            config=config,
            sink_dir=str(sink),
            artifact_path=artifact_path,
            visible_memory_gb=2.0,
            endpoint_url="" if inject else self.endpoint,
        )
        if inject:
            argv, env = self.inject_endpoint(case.tool, list(argv), env)
            case.detail = "endpoint injected; "
        argv = self.localize(argv, case.bucket)
        case.argv = argv
        sink.mkdir(parents=True, exist_ok=True)
        home = sink / "home"
        home.mkdir(exist_ok=True)
        manifest = adapter.modes[mode]
        stdout_path = sink / (manifest.product_file if manifest.product_channel == "stdout" and manifest.product_artifact else "stdout")
        with stdout_path.open("wb") as out, (sink / "stderr").open("wb") as err:
            proc = subprocess.run(
                argv,
                stdout=out,
                stderr=err,
                env={**self.env, **env, "HOME": str(home)},
                cwd=sink,
                timeout=300,
                check=False,
            )
        if proc.returncode != 0:
            tail = (sink / "stderr").read_bytes()[-600:].decode(errors="replace")
            raise RuntimeError(f"exit {proc.returncode}: {tail}")
        product = sink / manifest.product_file if manifest.product_artifact else None
        return product, config

    def run_case(self, case: Case, expected: dict[str, tuple[int, str]]) -> None:
        adapter = self.adapters.load_adapter(self.adapter_dir(case.tool), case.tool)
        manifest = adapter.modes[case.mode]
        if manifest.purpose_ceiling == "preparation" or not manifest.product_artifact or (case.tool, case.mode) in NOT_A_LISTING:
            case.status = "skip"
            case.detail = f"{manifest.purpose_ceiling} mode, not a compared listing"
            return
        if (case.tool, case.mode) in ROOT_ONLY and case.prefix:
            case.status, case.detail = "skip", "mode lists the bucket root whatever the prefix"
            return
        fixture = self.fixture_dir / case.bucket
        needs = {"fanout-fixture-with-dirs": "s5cmd-shards.input", "list-hinted-fixture": "s3-fast-list-hints.input"}.get(case.mode)
        if needs and not (fixture / needs).exists():
            case.status, case.detail = "skip", f"fixture_bundle.py would not build {needs} for this key set"
            return
        keys = list(expected)
        sink = self.work / "cases" / case.tool / case.mode / (case.bucket + "~" + (case.prefix.replace("/", "_") or "root"))
        shutil.rmtree(sink, ignore_errors=True)
        try:
            artifact_path = ""
            for step in adapter.requires.get(case.mode, ()):
                producer_sink = sink / f"requires-{step.mode}"
                self.execute(adapter, case, step.mode, producer_sink, keys)
                artifact_path = str(producer_sink / adapter.modes[step.mode].artifacts[step.artifact])
            if manifest.inline:
                inline_sink = sink / f"inline-{manifest.inline}"
                self.execute(adapter, case, manifest.inline, inline_sink, keys, artifact_path)
                inline_manifest = adapter.modes[manifest.inline]
                artifact_path = str(inline_sink / next(iter(inline_manifest.artifacts.values())))
            product, config = self.execute(adapter, case, case.mode, sink, keys, artifact_path)
        except NotApplicable as exc:
            case.status, case.detail = "skip", str(exc)
            return
        except self.adapters.AdapterError as exc:
            if "custom endpoint" in str(exc) or "replay endpoint" in str(exc) or "no --prefix" in str(exc) or "requires HeadBucket" in str(exc):
                case.status, case.detail = "skip", f"capsule refuses: {str(exc).split(': ', 2)[-1][:160]}"
                return
            raise
        normalized = sink / "normalized.tsv"
        self.adapters.normalize_to_path(
            self.adapter_dir(case.tool),
            case.tool,
            case.mode,
            case.prefix,
            normalized,
            input_path=None if manifest.product_channel == "dataset" else product,
            dataset=product if manifest.product_channel == "dataset" else None,
            config=config,
        )
        compare(case, normalized.read_bytes(), expected, manifest.fields)


INJECT_ENDPOINT = {
    "aws-cli": {"s3api-v1-text", "s3api-versions-text"},
    "minio-mc": {"versions-json", "find", "find-json"},
    "rclone": {"listv1"},
    "s3kor": {"list-versions"},
    "s4cmd": {"recursive", "shallow", "show-directory", "du"},
    "s5cmd": {"listv1", "allversions"},
    "s7cmd": {"all-versions"},
}
"""Capsule modes refused only because the study's replay server serves
ListObjectsV2 alone; SeaweedFS serves ListObjects, ListObjectVersions and
HeadBucket too."""

NOT_A_LISTING = {
    ("rclone", "debug"),
    ("rclone", "walk-debug"),
    ("s3p", "summarize"),
    ("s4cmd", "du"),
    ("s4cmd", "show-directory"),
}
"""Modes with no per-key listing: rclone's normalizer has no debug modes, summarize
and du print one aggregate, and `ls -d` prints the directory itself."""


class NotApplicable(Exception):
    pass


ROOT_ONLY = {
    ("aws-cli", "s3api-v2-remainder"),
    ("s5cmd", "fanout-fixture-with-dirs"),
    ("s3-fast-list", "list-hinted-fixture"),
}
"""Modes whose capsule lists the root (remainder) or a staged root-scope fixture."""

SHALLOW = re.compile(r"shallow|delimiter|rootkeys|remainder|max-depth")
KEYS_ONLY = re.compile(r"rootkeys|remainder")
"""Shallow modes whose normalizer keeps only the keys, dropping CommonPrefixes."""


def compare(case: Case, normalized: bytes, expected: dict[str, tuple[int, str]], fields) -> None:
    rows = []
    for line in normalized.split(b"\n"):
        if not line:
            continue
        f = line.split(b"\t")
        rows.append((f[0].decode("utf-8", "surrogateescape"), f[1].decode(), f[2].decode()))
    scope = {k: v for k, v in expected.items() if k.startswith(case.prefix)}
    keys = utf8_sorted(scope)
    shallow = bool(SHALLOW.search(case.mode))
    if shallow:
        view = direct_view(keys, case.prefix, "/")
        want_objects = [k for kind, k in view if kind == "key"]
        allowed_dirs = {k for kind, k in view if kind == "prefix"}
    else:
        want_objects = keys
        allowed_dirs = {k[: i + 1] for k in keys for i, c in enumerate(k) if c == "/" and i >= len(case.prefix)}
    problems: list[str] = []
    objects, dirs = [], []
    for key, size, etag in rows:
        is_dir = key.endswith("/") and size == "-" and etag == "-" and key not in scope
        (dirs if is_dir else objects).append((key, size, etag))
    got_keys = [k for k, _, _ in objects]
    dup = {k for k in got_keys if got_keys.count(k) > 1} if len(got_keys) != len(set(got_keys)) else set()
    if dup:
        problems.append(f"duplicate keys {sorted(dup)[:5]}")
    missing = [k for k in want_objects if k not in set(got_keys)]
    extra = [k for k in set(got_keys) if k not in set(want_objects)]
    if missing:
        problems.append(f"missing {len(missing)}: {missing[:5]}")
    if extra:
        problems.append(f"unexpected {len(extra)}: {sorted(extra)[:5]}")
    bad_dirs = [d for d, _, _ in dirs if d not in allowed_dirs]
    if bad_dirs:
        problems.append(f"unexpected directory rows {bad_dirs[:5]}")
    if shallow and not KEYS_ONLY.search(case.mode) and {d for d, _, _ in dirs} != allowed_dirs:
        problems.append(f"common prefixes {sorted(d for d, _, _ in dirs)[:8]} != {sorted(allowed_dirs)[:8]}")
    for key, size, etag in objects:
        if key not in scope:
            continue
        want_size, want_etag = scope[key]
        if size != "-" and size != str(want_size):
            problems.append(f"{key!r} size {size} != {want_size}")
        if etag != "-" and etag != want_etag:
            problems.append(f"{key!r} etag {etag} != {want_etag}")
    case.status = "fail" if problems else "pass"
    case.detail += "; ".join(problems)[:1500] if problems else f"{len(objects)} objects, {len(dirs)} prefixes"


# ---------------------------------------------------------------- main


def load_expected_failures(path: Path | None) -> dict[str, str]:
    out: dict[str, str] = {}
    if path and path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                name, _, reason = line.partition("#")
                out[name.strip()] = reason.strip()
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--study", required=True)
    ap.add_argument("--tools-dir", required=True)
    ap.add_argument("--work-dir", required=True)
    ap.add_argument("--tools", default="aws-cli,minio-mc,ps3,rclone,s3-fast-list,s3kor,s3p,s4cmd,s5cmd,s7cmd,swath")
    ap.add_argument("--bucket-prefix", default="study")
    ap.add_argument("--expected-failures", type=Path)
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--only", default="", help="regex over case names")
    ap.add_argument("--jobs", type=int, default=4)
    args = ap.parse_args()

    study = Path(args.study).resolve()
    work = Path(args.work_dir).resolve()
    work.mkdir(parents=True, exist_ok=True)
    s3 = S3(args.endpoint, args.access_key, args.secret_key)
    buckets = {f"{args.bucket_prefix}-canary": canary_keys(), f"{args.bucket_prefix}-edge": edge_keys()}
    expected: dict[str, dict[str, tuple[int, str]]] = {}
    for bucket, keymap in buckets.items():
        expected[bucket] = {k: (size, hashlib.md5(body_for(k, size)).hexdigest()) for k, size in keymap.items()}
        if args.skip_upload:
            continue
        s3.create_bucket(bucket)
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(16) as pool:
            list(pool.map(lambda k: s3.put_object(bucket, k, body_for(k, keymap[k])), keymap))
        print(f"uploaded {len(keymap)} objects to {bucket} in {time.time() - start:.1f}s", flush=True)

    only = re.compile(args.only) if args.only else None
    results: list[tuple[str, str, str]] = []
    for name, err in run_probes(s3, buckets, f"{args.bucket_prefix}-churn", only):
        results.append((name, "fail" if err else "pass", err or ""))

    runner = ToolRunner(args, study, work, args.endpoint)
    cases: list[Case] = []
    for bucket in buckets:
        runner.write_fixture_companions(bucket, list(buckets[bucket]))
        scopes = ["", "group-07/"] if bucket.endswith("canary") else ["", "order/", "chars/"]
        for tool in filter(None, args.tools.split(",")):
            adapter = runner.adapters.load_adapter(runner.adapter_dir(tool), tool)
            for mode in adapter.modes:
                for prefix in scopes:
                    case = Case(tool, mode, bucket, prefix)
                    if only and not only.search(case_name(case)):
                        continue
                    cases.append(case)

    def run(case: Case) -> Case:
        start = time.time()
        try:
            runner.run_case(case, expected[case.bucket])
        except Exception as exc:  # noqa: BLE001
            case.status = "fail"
            case.detail = f"{type(exc).__name__}: {exc}"[:1500]
            if os.environ.get("STUDY_DEBUG"):
                traceback.print_exc()
        print(f"{case.status:5} {case_name(case)} ({time.time() - start:.1f}s) {case.detail[:300]}", flush=True)
        return case

    with concurrent.futures.ThreadPoolExecutor(args.jobs) as pool:
        for case in pool.map(run, cases):
            results.append((case_name(case), case.status, case.detail))

    xfail = load_expected_failures(args.expected_failures)
    regressions, fixed = [], []
    counts: dict[str, int] = {}
    for name, status, detail in results:
        if status == "fail" and name in xfail:
            status = "xfail"
        elif status == "pass" and name in xfail:
            fixed.append(name)
        elif status == "fail":
            regressions.append((name, detail))
        counts[status] = counts.get(status, 0) + 1
    (work / "results.json").write_text(json.dumps(results, indent=1, ensure_ascii=False))
    print("\n== summary:", ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    for name in fixed:
        print(f"XPASS (remove from expected failures): {name}")
    for name, detail in regressions:
        print(f"FAIL {name}: {detail}")
    return 1 if regressions or fixed else 0


def case_name(case: Case) -> str:
    return f"{case.tool}:{case.mode}:{case.bucket.rsplit('-', 1)[-1]}:{case.prefix or '/'}"


if __name__ == "__main__":
    raise SystemExit(main())
