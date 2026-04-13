# V3 Development Process

Date: 2026-04-13
Status: design draft
Purpose: define branch strategy, code review, CI/CD, release, and
engineering management for sw-block development

## 1. Current SeaweedFS Upstream Process

### 1.1 What upstream does

| Aspect | Implementation |
|--------|---------------|
| Branch model | Master-only, feature branches merged via PR |
| Merge strategy | Squash-merge with PR number in commit message |
| CI | 46 GitHub Actions workflows (build, test, Docker, CodeQL) |
| Test suite | 663 `_test.go` files + 25 integration test workflows |
| Release | Tag-based: multi-platform binaries + Docker images |
| Code review | PR-based, CodeQL security scan, no mandatory reviewers |
| Linting | `go vet` only (no golangci-lint) |
| Documentation | Wiki-based, minimal CONTRIBUTING.md |
| Issue tracking | GitHub Issues + Discussions |
| Engineering mgmt | Informal, sponsor-driven |

### 1.2 What upstream does well

- **One-command build**: `make` builds everything
- **46 CI workflows**: comprehensive S3, FUSE, EC, backend integration tests
- **Multi-platform releases**: Linux/Windows/Darwin × amd64/arm64
- **Docker multi-arch**: amd64, arm64, arm, 386 with GHCR + Docker Hub
- **Dev builds on every push**: `:dev` tagged images auto-published
- **Concurrency cancellation**: `cancel-in-progress: true` saves CI minutes

### 1.3 What upstream is missing

- No golangci-lint or external linters
- No pre-commit hooks
- No formal CONTRIBUTING.md
- No branch protection rules
- No mandatory code review
- No test coverage tracking
- No public roadmap or project boards
- No release notes automation

## 2. sw-block Branch Strategy

### 2.1 Branch model

```
master (upstream SeaweedFS)
  │
  └── feature/sw-block (long-lived, periodic rebase on master)
        │
        ├── sw-block/v1-pre-phase13    (historical checkpoint)
        ├── sw-block/v1.5-ec6-fix      (historical checkpoint)
        ├── sw-block/v1.5-post-phase13 (V1.5 perf baseline)
        │
        └── (working tree — direct commits)
```

### 2.2 Rules

**feature/sw-block** is the main development branch:
- All sw-block work commits here
- Periodic rebase on `master` to stay current with upstream
- Never force-push without team notification
- Historical checkpoints preserved as named branches for perf comparison

**master** is upstream SeaweedFS:
- sw-block does not commit directly to master
- Upstream PRs go through the normal SeaweedFS PR process
- sw-block-specific code stays on feature/sw-block

**When to create a checkpoint branch**:
- Before a major architectural change (V1→V2, V2→V3)
- Before removing/replacing a subsystem
- When a perf baseline needs to be preserved for comparison

### 2.3 Commit conventions

```
# Feature
feat: add RemoteRebuildIO — primary coordinates, replica installs

# Bug fix
fix: suppress SessionFailed after successful rebuild completion

# Documentation
doc: external failure taxonomy — 20 real bugs from Ceph/DRBD/Longhorn

# Refactor
refactor: unified primary onboarding — assignment is the trigger

# Diagnostic (temporary, remove before release)
diag: add sender registry verification after installSession

# Test
test: 16/16 SmartWAL crash tests PASS

# Performance
perf: RF=1 baseline — V1.5=47233 vs V2=46666 IOPS (within noise)
```

Format: `type: short description (imperative mood)`

For multi-line commits:
```
feat: RemoteRebuildIO — primary coordinates rebuild, replica installs

- New file: block_rebuild_remote.go
- SessionControlMsg extended with RebuildAddr trailer (v2 wire format)
- replica_barrier.go: runBaseLaneClient auto-connects to rebuild server
- Shipper state: NeedsRebuild → Rebuilding → InSync on ack sequence
```

## 3. Code Review Process

### 3.1 Review requirements

| Change type | Required review | Who reviews |
|-------------|----------------|-------------|
| Engine semantic change | Architect | Must review before merge |
| Runtime/adapter change | sw peer or architect | At least one review |
| Test-only change | Self-merge OK | Optional review |
| Design doc | Architect | Review before merge |
| Scenario YAML | Self-merge OK | Run on hardware first |
| Diagnostic/temp code | Self-merge OK | Must be removed before release |

### 3.2 Review checklist

For every code change, the reviewer checks:

```
□ Does this change match a V3 anti-pattern? (check protocol-anti-patterns.md)
□ Does it introduce a new state to the engine? (must justify per v3-clean-recovery-draft.md)
□ Does it add timer-dependent semantics? (reject — P1: timers trigger, facts decide)
□ Does it change authority for a truth domain? (must be explicit and documented)
□ Are error messages actionable? (say what happened + why + what to do)
□ Is there a test? (unit test for logic, hardware scenario for integration)
□ Does it pass go vet? (no new warnings)
□ Does it compile for linux/amd64? (GOOS=linux GOARCH=amd64 go build)
```

### 3.3 Self-review for solo work

When working alone (common with AI agents), apply the same checklist
before committing. The commit message should document the review:

```
fix: WAL retention exhaustion — remove keepup retention floor

Review: checked against A2 (transport failure → recovery type).
The retention removal means every disconnect triggers rebuild,
not catchup. This is correct for current architecture (no LBA
dirty map). Future SmartWAL + dirty map will eliminate this.

Tested: I-V3 43/43, I-R8 58/58, fast-rejoin 43/43 on hardware.
```

## 4. Testing Standards

### 4.1 Test levels

| Level | What | Where | When |
|-------|------|-------|------|
| Unit | Pure logic, no I/O | `*_test.go` next to source | Every commit |
| Unit-TCP | Localhost TCP (shipper, iSCSI, NVMe) | `*_test.go` with network | Every commit |
| Component | Real weed processes on localhost | `test/component/` | Before merge |
| Integration | Real hardware, SSH, kernel iSCSI | `testrunner/scenarios/` | Before release |
| Crash | Kill -9 + recovery verification | SmartWAL crash tests | Before release |

### 4.2 Test counts (current)

| Domain | Count |
|--------|-------|
| Engine (BlockVol core) | 18 |
| WAL | 81 |
| Replication | 73 |
| Fencing | 7 |
| Operations (snapshot, expand, scrub) | 143 |
| iSCSI | 19 |
| NVMe | 252 |
| Control plane (registry, failover, heartbeat) | 683 |
| Test runner | 179 |
| SmartWAL prototype | 16 |
| **Total** | **~1,600** |

Hardware scenarios: 147 YAML files, 6 verified on m01/m02 (196 actions).

### 4.3 Required tests for each change type

| Change type | Required test |
|-------------|--------------|
| Engine state machine | Unit test with assertion on every state transition |
| WAL/extent I/O | Crash test (write → kill → recover → verify) |
| Replication protocol | Two-node unit test (primary + replica) |
| Server layer (weed integration) | Component test or hardware scenario |
| Performance-sensitive | Perf comparison (before/after IOPS) |
| Bug fix | Regression test that reproduces the bug |

### 4.4 Hardware acceptance gate

Before any release, these 4 scenarios must PASS:

```
1. I-V3 auto-failover            43/43 actions
2. I-R8 rebuild-rejoin           58/58 actions
3. Fast rejoin                   43/43 actions
4. RF=1 perf baseline            22/22 actions (within 5% of V1.5)
```

Total: 166 actions, 0 failures allowed.

## 5. CI/CD Pipeline

### 5.1 Current state

sw-block runs on the upstream SeaweedFS CI. The `go.yml` workflow
builds and tests on every push. Block-specific tests run as part of
the full `go test ./...` suite.

### 5.2 Desired pipeline

```
On every push to feature/sw-block:
  1. go vet ./...
  2. go build ./weed                    (linux/amd64)
  3. go test ./weed/storage/blockvol/... (unit + unit-TCP)
  4. go test ./weed/server/ -run "Block" (server-layer block tests)
  5. go test ./sw-block/...             (protocol engine tests)

On PR to feature/sw-block:
  6. All of above +
  7. go build linux/arm64               (cross-compile check)
  8. golangci-lint run                  (if added)

On tag (release):
  9. All of above +
  10. Build linux/amd64 binary
  11. Build sw-test-runner binary
  12. Deploy to hardware, run 4 acceptance scenarios
  13. Build Docker image
  14. Publish release artifacts
```

### 5.3 CI configuration

```yaml
# .github/workflows/sw-block.yml
name: sw-block
on:
  push:
    branches: [feature/sw-block]
  pull_request:
    branches: [feature/sw-block]

jobs:
  build-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.23'
      - run: go vet ./...
      - run: GOOS=linux GOARCH=amd64 go build ./weed
      - run: go test ./weed/storage/blockvol/... -count=1 -timeout 5m
      - run: go test ./weed/server/ -run "Block|TestP16" -count=1 -timeout 5m
      - run: go test ./sw-block/... -count=1 -timeout 5m
```

### 5.4 Hardware CI (future)

When hardware is available for CI:

```yaml
  hardware-acceptance:
    runs-on: self-hosted  # m01/m02 runner
    needs: build-and-test
    steps:
      - run: sw-test-runner run recovery-baseline-failover.yaml
      - run: sw-test-runner run v2-rebuild-rejoin.yaml
      - run: sw-test-runner run v2-fast-rejoin-catchup.yaml
      - run: sw-test-runner run rf1-perf-compare.yaml
```

## 6. Release Process

### 6.1 Versioning

sw-block follows SeaweedFS upstream versioning with a block suffix:

```
SeaweedFS 4.19 + sw-block Phase 20 = sw-block 4.19-block.20
```

For standalone releases (before upstream merge):

```
sw-block-v2.0  (V2 stable baseline, 2026-04-11)
sw-block-v3.0  (V3 with SmartWAL, future)
```

### 6.2 Release checklist

```
□ All 4 hardware acceptance scenarios PASS (166 actions)
□ RF=1 perf within 5% of baseline
□ No known data-loss bugs
□ All diagnostic/temp code removed
□ Commit history clean (no WIP commits)
□ Design docs up to date
□ Release notes written
□ Binary built for linux/amd64
□ sw-test-runner built for linux/amd64
□ Docker image built and tagged
```

### 6.3 Release artifacts

```
sw-block-v2.0/
  weed-linux-amd64              ← main binary
  sw-test-runner-linux-amd64    ← test runner
  scenarios/                    ← YAML test scenarios
  dashboards/
    block-overview.json         ← Grafana dashboard
  alerts/
    sw-block-alerts.yml         ← Prometheus alert rules
  RELEASE-NOTES.md
```

## 7. Issue / PR Templates

### 7.1 Bug report template

```markdown
## Bug Report

**Component**: [engine / replication / iSCSI / NVMe / master / CSI]
**Version**: [git SHA or release tag]
**Hardware**: [m01/m02, or describe]

### What happened
[Clear description]

### Expected behavior
[What should have happened]

### Steps to reproduce
1. ...
2. ...

### Logs
[Relevant log lines with timestamps]

### Scenario (if applicable)
[YAML scenario file or inline]

### Anti-pattern check
Does this match a known anti-pattern from protocol-anti-patterns.md?
- [ ] A1: Timer defines semantics
- [ ] A2: Transport failure → recovery type
- [ ] A3: Ack = terminal success
- [ ] A4: Ordering determines result
- [ ] A5: Projection as control
- [ ] A6: Workaround state
- [ ] A7: Transport in engine
- [ ] None / New class
```

### 7.2 PR template

```markdown
## What this PR does

[Short description]

## Why

[Problem being solved]

## How it was tested

- [ ] Unit tests pass: `go test ./weed/storage/blockvol/...`
- [ ] Server tests pass: `go test ./weed/server/ -run "Block"`
- [ ] Hardware scenario: [which scenario, result]
- [ ] Performance check: [IOPS before/after, or N/A]

## Review checklist

- [ ] No new anti-patterns (checked against protocol-anti-patterns.md)
- [ ] No new engine state without justification
- [ ] No timer-dependent semantics
- [ ] Error messages are actionable
- [ ] go vet clean
- [ ] Compiles for linux/amd64

## Files changed

| File | Change |
|------|--------|
| ... | ... |
```

### 7.3 Design doc template

```markdown
# [Title]

Date: YYYY-MM-DD
Status: [draft / active / superseded]
Supersedes: [previous doc, if any]

## Problem
[What problem does this solve]

## Design
[The solution]

## Anti-pattern check
[Which anti-patterns does this avoid, and how]

## Test plan
[How to verify this works]

## Alternatives considered
[What else was considered and why it was rejected]
```

## 8. Engineering Management

### 8.1 Work tracking

| What | Where |
|------|-------|
| Active bugs | GitHub Issues on feature/sw-block |
| Design decisions | `sw-block/design/*.md` (append-only decision log) |
| Phase progress | `sw-block/design/v2-acceptance-evidence.md` |
| Test results | `sw-test-runner results/` (runs.db index) |
| Performance history | `sw-test-runner trend` command |

### 8.2 Decision log

Every significant decision is recorded:

```markdown
## Decision: Remove WAL retention for keepup
Date: 2026-04-08
Context: WAL fills during sustained writes, causing 30s dd_write timeout
Decision: Set RetentionFloorFn to nil. Flusher recycles freely.
Consequence: Every replica disconnect triggers rebuild, not catchup.
Future: SmartWAL + LBA dirty map will eliminate this tension.
Tested: I-V3 43/43, dd_write 132ms (was 30s)
```

### 8.3 Phase development model

Each development phase follows:

```
1. Define scope and acceptance criteria
2. Write/update design doc
3. Implement
4. Test on hardware (4 acceptance scenarios)
5. Record results in evidence doc
6. Checkpoint branch if needed
7. Move to next phase
```

### 8.4 Agent collaboration model

For AI agent (sw/tester) driven development:

| Role | Responsibility |
|------|---------------|
| Architect (human) | Direction, design review, acceptance |
| sw agent | Implementation, unit tests, component tests |
| tester agent | Hardware scenarios, evidence collection |
| manager (Claude) | Progress tracking, research, documentation |

Rules:
- Agents must not silently redefine semantic authority
- Every agent commit includes what was tested
- Agent code is reviewed against the anti-pattern checklist
- Hardware test results are the acceptance gate, not unit tests

## 9. Code Quality Tools

### 9.1 Current

| Tool | Status |
|------|--------|
| `go vet` | Active (CI) |
| `go build` cross-compile | Active (CI) |
| `-race` flag | Not on Windows (CGO required) |
| golangci-lint | Not configured |
| Pre-commit hooks | None |

### 9.2 Recommended additions

| Tool | Purpose | Priority |
|------|---------|----------|
| golangci-lint | Catch common bugs, style | P1 |
| `go test -race` on Linux CI | Race condition detection | P1 |
| Pre-commit: `go vet` + `go build` | Fast local check | P2 |
| Test coverage tracking | Identify untested code | P3 |

### 9.3 golangci-lint config (recommended)

```yaml
# .golangci.yml
linters:
  enable:
    - govet
    - errcheck
    - staticcheck
    - unused
    - gosimple
    - ineffassign
  disable:
    - exhaustive     # too noisy for proto enums
    - wrapcheck      # too strict for internal code

issues:
  exclude-rules:
    - path: _test\.go
      linters: [errcheck]  # test files don't need error checking
    - path: \.pb\.go
      linters: [all]       # generated protobuf code
```

## 10. Upstream Contribution Path

When sw-block features are ready for upstream SeaweedFS:

### 10.1 What goes upstream

| Component | Upstream candidate? | When |
|-----------|-------------------|------|
| BlockVol engine | Yes | After V3 stable |
| iSCSI target | Yes | After V3 stable |
| NVMe-oF target | Yes | After V3 stable |
| CSI driver | Yes | After K8s validation |
| SmartWAL | Maybe (if generalized) | After prototype proven |
| Protocol engine (sw-block/) | No (sw-block specific) | Never |
| Test runner | Yes (general purpose) | After P1 features |
| Design docs | No (internal) | Never |

### 10.2 Upstream PR process

1. Cherry-pick from feature/sw-block to a new branch
2. Ensure no sw-block-specific dependencies
3. Add tests that work without hardware
4. Follow upstream commit conventions
5. Submit PR to master
6. Address upstream review feedback

### 10.3 Upstream improvements to contribute back

From this session's audit (protocol-anti-patterns.md):

| Fix | File | Impact |
|-----|------|--------|
| Per-replica failure in replication | `store_replicate.go` | P0: one failure shouldn't fail all |
| Epoch-based connection identity | `data_node.go` | P1: replace Counter workaround |
| Fix Stats threshold inversion | `volume_location_list.go` | P2: reports stale data |
