#!/usr/bin/env python3
"""Check JWT extraction and, with --context, real Helm upgrade persistence."""

import argparse
import base64
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import uuid

try:
    import tomllib
except ModuleNotFoundError:  # CI also exercises Python 3.10.
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "k8s/charts/seaweedfs"
SECTIONS = ("jwt.signing", "jwt.signing.read", "jwt.filer_signing", "jwt.filer_signing.read")
KEYS = {section: f"active-{index}" for index, section in enumerate(SECTIONS)}


def run(*args):
    return subprocess.run(args, check=True, text=True, capture_output=True).stdout


def keys(raw):
    document = tomllib.loads(raw)
    result = {}
    for section in SECTIONS:
        table = document
        for part in section.split("."):
            table = table.get(part, {})
        if "key" in table:
            result[section] = table["key"]
    return result


def fixtures():
    canonical = "\n".join(f'[{section}]\nkey = "{value}"' for section, value in KEYS.items())
    yield "canonical / final line without newline", canonical
    yield "commented stale keys", canonical.replace("key =", '# key = "stale"\nkey =')
    yield "commented headers / mixed line endings", "\n".join(
        f'# [{section}]\r\n# key = "stale"\r\n[{section}]\nkey = "{value}"'
        for section, value in KEYS.items()
    )
    yield "indented headers and keys / trailing comments", "\n".join(
        f' \t[ {section} ] \t# header [note]\n \tkey \t= \t"{value}" # key comment'
        for section, value in KEYS.items()
    )
    yield "CRLF", canonical.replace("\n", "\r\n")
    yield "brackets in comments", canonical.replace("key =", "# consult [notes]\nkey =")
    yield "quoted values and quoted key names", "\n".join((
        '[jwt.signing]\n"key" = "brackets[inside]#value"',
        "[jwt.signing.read]\n'key' = 'literal\\path[#value]'",
        r'[jwt.filer_signing]' + '\n' + r'key = "escaped\"quote\\slash\u0041"',
        '[jwt.filer_signing.read]\nkey = ""',
    ))
    yield "absent keys and sections / unrelated key", '\n'.join((
        '[jwt.signing]\nexpires_after_seconds = 10',
        '[jwt.signing.read]\nkey = "read-only"',
        '[unrelated]\nkey = "not-a-jwt-key"',
        '# [jwt.filer_signing]\n# key = "not-active"',
    ))
    yield "no existing security.toml", ""


def check_generated(value):
    decoded = base64.b64decode(value, validate=True).decode("ascii")
    assert len(decoded) == 10 and decoded.isascii() and decoded.isalnum(), "invalid generated JWT key"


def check_helpers(helm, reference_helm):
    # Exercise the real helper, not a second implementation of its matching rules.
    with tempfile.TemporaryDirectory(prefix="helm-jwt-helper-") as directory:
        chart = Path(directory)
        (chart / "templates").mkdir()
        (chart / "Chart.yaml").write_text("apiVersion: v2\nname: jwt-regression\nversion: 0.0.0\n")
        shutil.copyfile(CHART / "templates/shared/_helpers.tpl", chart / "templates/_helpers.tpl")
        entries = [
            json.dumps(section) + ': {{ include "seaweedfs.existingTomlKey" (list '
            + json.dumps(section) + ' .Values.raw) | toJson }}'
            for section in SECTIONS
        ]
        template = chart / "templates/keys.yaml"
        prefix = '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"keys"},"data":{'
        helper_template = prefix + ",".join(entries) + "}}"
        reference_entries = [
            json.dumps(section) + ': {{ dig '
            + " ".join(json.dumps(part) for part in section.split("."))
            + ' "key" "__ABSENT__" (fromToml .Values.raw) | toJson }}'
            for section in SECTIONS
        ]
        reference_template = prefix + ",".join(reference_entries) + "}}"

        def render(binary, source, raw):
            template.write_text(source)
            values = chart / "input.json"
            values.write_text(json.dumps({"raw": raw}))
            output = run(binary, "template", "keys", str(chart), "-f", str(values))
            return json.loads(output[output.index("{"):])["data"]

        for name, raw in fixtures():
            expected = keys(raw)
            tokens = render(helm, helper_template, raw)
            actual = {section: tomllib.loads("key = " + token)["key"]
                      for section, token in tokens.items() if token != ""}
            assert actual == expected, f"{name}: extracted keys differ from stored TOML"
            if reference_helm:
                reference = render(reference_helm, reference_template, raw)
                reference = {section: value for section, value in reference.items() if value != "__ABSENT__"}
                assert actual == reference, f"{name}: keys differ from fromToml/dig"
            print(f"PASS helper: {name}")

        # A present but unsupported value must not silently become a fresh key.
        try:
            render(helm, helper_template, '[jwt.signing]\nkey = """multi\nline"""')
        except subprocess.CalledProcessError as error:
            assert "refusing to replace an existing key" in error.stderr, error.stderr
        else:
            raise AssertionError("multiline existing key was silently accepted or replaced")
        print("PASS helper: unsupported existing value fails without rotation")


def check_upgrades(helm, context):
    namespace = "jwt-key-persist-" + uuid.uuid4().hex[:8]
    current = "jk-seaweedfs-security-config"
    legacy = "seaweedfs-security-config"
    kubectl = ["kubectl", "--context", context, "-n", namespace]
    release_args = ["jk", str(CHART), "--kube-context", context, "-n", namespace]
    # No workload is needed to exercise Helm's real ConfigMap lookup and update.
    for setting in (
        "master.enabled=false", "volume.enabled=false", "filer.enabled=false",
        "global.seaweedfs.createClusterRole=false",
        "global.seaweedfs.securityConfig.jwtSigning.volumeWrite=true",
        "global.seaweedfs.securityConfig.jwtSigning.volumeRead=true",
        "global.seaweedfs.securityConfig.jwtSigning.filerWrite=true",
        "global.seaweedfs.securityConfig.jwtSigning.filerRead=true",
    ):
        release_args += ["--set", setting]

    def stored():
        cm = json.loads(run(*kubectl, "get", "configmap", current, "-o", "json"))
        return keys(cm["data"]["security.toml"])

    def upgrade():
        run(helm, "upgrade", *release_args)
        return stored()

    def patch(raw):
        # Seed previous-release content without taking Helm 4's SSA ownership.
        run(*kubectl, "patch", "configmap", current, "--type=merge", "--field-manager=helm", "-p",
            json.dumps({"data": {"security.toml": raw}}))

    run(*kubectl, "create", "namespace", namespace)
    try:
        run(helm, "install", *release_args)
        initial = stored()
        assert set(initial) == set(SECTIONS), "install omitted a JWT section"
        for value in initial.values():
            check_generated(value)
        assert upgrade() == initial, "no-op upgrade changed an existing key"
        print("PASS upgrade: all four generated keys persist")

        for name, raw in fixtures():
            patch(raw)
            actual = upgrade()
            expected = keys(raw)
            assert set(actual) == set(SECTIONS), f"{name}: upgrade omitted a JWT section"
            for section in SECTIONS:
                if section in expected:
                    assert actual[section] == expected[section], f"{name}: changed {section}"
                else:
                    check_generated(actual[section])
            assert upgrade() == actual, f"{name}: subsequent upgrade changed a key"
            print(f"PASS upgrade: {name}")

        # Migration from the old chart name, followed by precedence of the current name.
        legacy_raw = "\n".join(f'[{section}]\nkey = "legacy-{index}"'
                               for index, section in enumerate(SECTIONS))
        run(*kubectl, "create", "configmap", legacy, "--from-literal=security.toml=" + legacy_raw)
        run(*kubectl, "delete", "configmap", current)
        assert upgrade() == keys(legacy_raw), "legacy ConfigMap keys were not preserved"
        print("PASS upgrade: legacy ConfigMap migration")
        current_raw = next(fixtures())[1]
        patch(current_raw)
        assert upgrade() == keys(current_raw), "legacy ConfigMap overrode current ConfigMap"
        print("PASS upgrade: current ConfigMap takes precedence")
    finally:
        run(*kubectl, "delete", "namespace", namespace, "--wait=false")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--helm", default="helm")
    parser.add_argument("--reference-helm", help="Helm >=3.17 binary for differential checks")
    parser.add_argument("--context", help="explicit disposable Kubernetes context for live upgrade checks")
    args = parser.parse_args()
    print(run(args.helm, "version", "--short").strip())
    check_helpers(args.helm, args.reference_helm)
    if args.context:
        check_upgrades(args.helm, args.context)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        print(error.stderr, file=sys.stderr)
        sys.exit(error.returncode)
