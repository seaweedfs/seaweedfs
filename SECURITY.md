# Security Policy

## Supported versions

Security fixes land in the latest release. Please reproduce against a recent
release or `master` before reporting; issues that only reproduce on old,
unsupported versions are not eligible for a fix or an advisory.

## Reporting a vulnerability

Report privately through GitHub private vulnerability reporting (the "Report a
vulnerability" button under the repository's Security tab). This keeps the
report, the fix, and any CVE in one place. Do not open a public issue.

### What a report must include

We can only act on reports that show real impact. Please include:

- Affected version (a release tag or `master` commit you reproduced on)
- The exact deployment and configuration: which components are running
  (master, volume, filer, S3, admin), which ports are reachable by the
  attacker, and what authentication is enabled
- The attacker's starting position: unauthenticated, a valid S3 user, an admin,
  or someone with access to the internal cluster network
- The trust boundary that is crossed (e.g. an unauthenticated client reading
  another tenant's data, an S3 user escalating to admin)
- A minimal, working reproduction or proof of concept
- Expected vs. actual behavior

A report without a working reproduction and a clear trust boundary is a
hardening suggestion, not a vulnerability. We are glad to receive those, but
they are handled on the normal issue tracker, not as security advisories.

### Automated and AI-assisted reports

Output from static analysis, dependency scanners, fuzzers, or LLMs is welcome
only when you have manually validated it and can supply a working reproduction
against a supported version, per the requirements above. Raw tool output,
speculative findings, or generated reports without a demonstrated exploit will
be closed as hardening suggestions.

## Trust model

SeaweedFS is built to run with its cluster components (master, volume servers,
and the raw filer API) on a trusted network. Those internal APIs are not an
authentication boundary unless you explicitly enable a control (for example
volume JWT or filer authentication) and that control is bypassed. Exposing an
internal port directly to untrusted clients is a deployment mistake, not a
vulnerability in SeaweedFS.

Reports are in scope when they cross a boundary SeaweedFS is meant to enforce,
for example:

- Unauthenticated access to data or operations that require authentication
- One S3 identity reading, writing, or deleting another identity's data
- Privilege escalation from a normal S3 user to administrative capability
- Bypass of Object Lock / retention where it is configured
- Remotely triggered data corruption or loss

Reports are generally out of scope when they require:

- Direct access to an internal cluster port that is meant to be private
- Full master, filer, or volume server access (already a full compromise)
- An insecure example configuration rather than a documented secure setup
- Local-only impact on a host the attacker already controls

## CVE assignment

When a report is confirmed, we publish an advisory and request the CVE through
GitHub. CVEs assigned by third parties without coordinating with us, or for
issues that do not cross a boundary described above, may be disputed.

## Response and disclosure

- We aim to acknowledge a valid report within a few business days.
- We will investigate, work on a fix, and coordinate a disclosure timeline
  with you.
- Please allow time for a fix before any public disclosure.
