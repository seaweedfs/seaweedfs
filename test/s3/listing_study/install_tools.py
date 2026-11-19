#!/usr/bin/env python3
"""Install the listing tools pinned by the s3-listing-study checkout.

Every artifact locator and sha256 comes from the study's own
tools/<tool>/build/image.json, so bumping STUDY_REV bumps the tools with it.
Tools land under TOOLS_DIR at the paths listed in run_study.PATH_MAP.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tarfile
import urllib.request
import zipfile
from pathlib import Path

ALL_TOOLS = (
    "aws-cli",
    "minio-mc",
    "ps3",
    "rclone",
    "s3-fast-list",
    "s3kor",
    "s3p",
    "s4cmd",
    "s5cmd",
    "s7cmd",
    "swath",
)

MC_COMMIT = "d6541ea280b73a834b64d4097e21f2be77676104"
"""github.com/minio/mc tag RELEASE.2025-08-13T08-35-41Z."""

SOURCE_PATCHES = {
    "s3-fast-list": (
        "s3-fast-list/src/data_map.rs",
        (
            (
                "        match ctx.data_map_channel.try_recv() {",
                "        let list_tasks_running = ctx.all_list_tasks_is_running();\n"
                "        match ctx.data_map_channel.try_recv() {",
            ),
            (
                "!ctx.all_list_tasks_is_running() && !has_more_in_queue",
                "!list_tasks_running && !has_more_in_queue",
            ),
        ),
    ),
}
"""Source fixes applied before a build. s3-fast-list's data map reads the channel
and then the list-task state, so a last page sent and completed between the two
reads is dropped and the run exits 0 with a short or empty listing. Reading the
state first means a finished task's pages are already in the channel."""


def patch_source(tool: str, work: Path) -> None:
    rel, edits = SOURCE_PATCHES[tool]
    path = work / rel
    text = path.read_text()
    for old, new in edits:
        if text.count(old) != 1:
            raise SystemExit(f"{tool}: {rel} no longer matches its source patch: {old!r}")
        text = text.replace(old, new)
    path.write_text(text)


def fetch(url: str, sha256: str, dest: Path) -> Path:
    if dest.exists() and hashlib.sha256(dest.read_bytes()).hexdigest() == sha256:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    request = urllib.request.Request(url, headers={"User-Agent": "curl/8"})
    with urllib.request.urlopen(request, timeout=300) as response, tmp.open("wb") as out:
        shutil.copyfileobj(response, out)
    digest = hashlib.sha256(tmp.read_bytes()).hexdigest()
    if digest != sha256:
        tmp.unlink()
        raise SystemExit(f"{url}: sha256 {digest} != pinned {sha256}")
    tmp.rename(dest)
    return dest


def executable(path: Path) -> None:
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def wrapper(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"#!/bin/sh\n{body}\n")
    executable(path)


def tar_member(archive: Path, member: str, dest: Path) -> None:
    with tarfile.open(archive) as tar:
        source = tar.extractfile(member)
        if source is None:
            raise SystemExit(f"{archive}: no member {member}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(source.read())
    executable(dest)


def install(tool: str, study: Path, tools_dir: Path) -> None:
    image = json.loads((study / "tools" / tool / "build" / "image.json").read_text())
    artifact = image["tool_artifact"]
    url, sha = artifact["locator"], artifact["sha256"]
    dl = tools_dir / "downloads"
    bin_dir = tools_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    name = url.rsplit("/", 1)[-1]
    stamp = tools_dir / "installed" / tool
    installed = sha + ("+patched" if tool in SOURCE_PATCHES else "")
    if stamp.exists() and stamp.read_text() == installed:
        print(f"{tool} {image.get('tool_version', '')} already installed")
        return

    if tool == "aws-cli":
        archive = fetch(url, sha, dl / name)
        work = tools_dir / "aws-cli-src"
        shutil.rmtree(work, ignore_errors=True)
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(work)
        for rel in ("aws/install", "aws/dist/aws", "aws/dist/aws_completer"):
            executable(work / rel)
        subprocess.run(
            [
                str(work / "aws" / "install"),
                "--install-dir",
                str(tools_dir / "aws-cli"),
                "--bin-dir",
                str(tools_dir / "aws-cli-bin"),
                "--update",
            ],
            check=True,
        )
        shutil.rmtree(work)
    elif tool == "minio-mc":
        # dl.min.io answers 410 Gone for every archived mc release, so build the
        # same release tag from source instead.
        if image["tool_version"] != "RELEASE.2025-08-13T08-35-41Z":
            raise SystemExit(f"minio-mc: no source commit pinned for {image['tool_version']}")
        subprocess.run(
            ["go", "install", f"github.com/minio/mc@{MC_COMMIT}"],
            env={**os.environ, "GOBIN": str(bin_dir)},
            check=True,
        )
    elif tool == "ps3":
        shutil.copyfile(fetch(url, sha, dl / name), bin_dir / "pS3")
        executable(bin_dir / "pS3")
    elif tool == "rclone":
        archive = fetch(url, sha, dl / name)
        with zipfile.ZipFile(archive) as zf:
            member = f"{name.removesuffix('.zip')}/rclone"
            (bin_dir / "rclone").write_bytes(zf.read(member))
        executable(bin_dir / "rclone")
    elif tool == "s3kor":
        tar_member(fetch(url, sha, dl / name), "s3kor", bin_dir / "s3kor")
    elif tool == "s5cmd":
        tar_member(fetch(url, sha, dl / name), "s5cmd", bin_dir / "s5cmd")
    elif tool == "s7cmd":
        tar_member(fetch(url, sha, dl / name), "s7cmd", bin_dir / "s7cmd")
    elif tool == "s3p":
        work = tools_dir / "s3p"
        work.mkdir(parents=True, exist_ok=True)
        for f in ("package.json", "package-lock.json"):
            shutil.copyfile(study / "tools/s3p/build" / f, work / f)
        subprocess.run(["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=work, check=True)
        wrapper(bin_dir / "s3p", f'exec node "{work}/node_modules/s3p/bin/s3p" "$@"')
    elif tool == "s4cmd":
        archive = fetch(url, sha, dl / name)
        venv = tools_dir / "s4cmd-venv"
        subprocess.run([sys.executable, "-m", "venv", "--clear", str(venv)], check=True)
        subprocess.run(
            [
                str(venv / "bin" / "python"),
                "-m",
                "pip",
                "install",
                "--quiet",
                "--require-hashes",
                "--no-deps",
                "-r",
                str(study / "tools/s4cmd/build/requirements.txt"),
            ],
            check=True,
        )
        script = tools_dir / "s4cmd" / "s4cmd.py"
        with tarfile.open(archive) as tar:
            source = tar.extractfile("s4cmd-2.1.0/s4cmd.py")
            assert source is not None
            script.parent.mkdir(parents=True, exist_ok=True)
            script.write_bytes(source.read())
        wrapper(bin_dir / "s4cmd", f'exec "{venv}/bin/python" "{script}" "$@"')
    elif tool == "s3-fast-list":
        archive = fetch(url, sha, dl / name)
        work = tools_dir / "s3-fast-list-src"
        shutil.rmtree(work, ignore_errors=True)
        work.mkdir(parents=True)
        subprocess.run(["tar", "xzf", str(archive), "--strip-components=1", "-C", str(work)], check=True)
        shutil.copyfile(study / "tools/s3-fast-list/build/Cargo.lock", work / "Cargo.lock")
        patch_source(tool, work)
        subprocess.run(["cargo", "build", "--release", "--locked", "--quiet"], cwd=work, check=True)
        for exe in ("s3-fast-list", "ks-tool"):
            shutil.copyfile(work / "target/release" / exe, bin_dir / exe)
            executable(bin_dir / exe)
    elif tool == "swath":
        ref = f"{url}@sha256:{sha}"
        subprocess.run(["docker", "pull", "--quiet", ref], check=True, stdout=subprocess.DEVNULL)
        cid = subprocess.run(
            ["docker", "create", ref], check=True, capture_output=True, text=True
        ).stdout.strip()
        try:
            dest = tools_dir / "swath"
            shutil.rmtree(dest, ignore_errors=True)
            dest.mkdir(parents=True)
            subprocess.run(["docker", "cp", f"{cid}:/opt/java/openjdk", str(dest / "openjdk")], check=True)
            subprocess.run(["docker", "cp", f"{cid}:/opt/swath/swath.jar", str(dest / "swath.jar")], check=True)
        finally:
            subprocess.run(["docker", "rm", cid], check=True, stdout=subprocess.DEVNULL)
    else:
        raise SystemExit(f"unknown tool {tool}")
    stamp.parent.mkdir(parents=True, exist_ok=True)
    stamp.write_text(installed)
    print(f"installed {tool} {image.get('tool_version', '')}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--study", required=True, type=Path)
    parser.add_argument("--tools-dir", required=True, type=Path)
    parser.add_argument("--tools", default=",".join(ALL_TOOLS))
    args = parser.parse_args()
    for tool in filter(None, args.tools.split(",")):
        install(tool, args.study.resolve(), args.tools_dir.resolve())
    return 0


if __name__ == "__main__":
    os.umask(0o022)
    raise SystemExit(main())
