# sw-test-runner Roadmap

Date: 2026-04-11
Status: active

## 1. Current State

The sw-test-runner is a YAML-driven test platform for sw-block hardware
validation. It deploys binaries, orchestrates multi-node scenarios, runs
benchmarks, injects faults, and reports results.

| Metric | Value |
|--------|-------|
| Registered actions | 37 |
| YAML scenarios (internal) | 72 |
| YAML scenarios (external) | 75 |
| Scenario categories | smoke, HA, rebuild, chaos, perf, soak, CSI |
| Hardware tested | m01/m02, 25Gbps RoCE |

### What it does well

- Deploy weed binaries to remote nodes via SSH
- Start/stop/kill weed processes (master, volume server)
- Create/delete block volumes via master gRPC API
- iSCSI and NVMe-oF connect/disconnect
- fio benchmark with JSON output + metric parsing
- dd write/read/verify with md5 checksum
- Fault injection: netem delay, iptables partition, disk fill, WAL corrupt
- Phase-based scenario sequencing with variable substitution
- Assertions: equal, greater, contains, block field checks
- Artifact collection on failure
- JUnit XML output for CI
- Parallel phase support
- Prometheus metrics scraping

### What it cannot do yet

- Compare multiple binaries (V1/V2/V3) in one run
- Compare against other systems (Ceph RBD, DRBD)
- Track results over time (no persistent history)
- Detect performance regressions automatically
- Debug failures (auto-grep logs, stop-on-fail)
- Deploy clusters from scratch (relies on pre-installed weed)
- Reuse cluster setup across scenarios (boilerplate every time)

## 2. Proven Scenarios (V2 Baseline)

These 4 scenarios define the V2 acceptance bar. All PASS as of 2026-04-11.

| Scenario | File | Actions | What it proves |
|----------|------|---------|----------------|
| I-V3 Auto-Failover | `recovery-baseline-failover.yaml` | 43/43 | Create→write→kill→promote→IO verified |
| I-R8 Rebuild-Rejoin | `v2-rebuild-rejoin.yaml` | 58/58 | Failover→write→restart→1GB rebuild→data verified |
| Fast Rejoin | `v2-fast-rejoin-catchup.yaml` | 43/43 | Kill replica→3s restart→recovery→data verified |
| RF=1 Perf Baseline | `rf1-perf-compare.yaml` | 22/22 | V1.5 vs V2 IOPS comparison (within 1.2%) |

Additional proven scenarios:

| Scenario | File | Actions | What it proves |
|----------|------|---------|----------------|
| Rebuild Retry | `v2-rebuild-failure-retry.yaml` | 52/52 | Kill during rebuild→restart→data verified |
| dm-stripe 2-server | `dm-stripe-two-server.yaml` | 42/42 | Linux striping across 2 BlockVols: 1.87x write IOPS |

## 3. Roadmap

### P0: Multi-Version Comparison Matrix

**Goal**: Run the same benchmark scenario against V1.5, V2, V3, and Ceph
in one invocation. Produce a side-by-side comparison table.

**Why P0**: CEO needs head-to-head performance numbers for investor/customer
presentations. Current workflow requires manual binary swaps and separate runs.

**Design**:

```yaml
# New top-level field in scenario YAML:
matrix:
  binary:
    - name: v1.5
      weed: /opt/work/weed-v1.5
    - name: v2
      weed: /opt/work/weed
    - name: ceph
      type: ceph
      pool: bench-pool

# Runner executes the scenario once per matrix entry,
# collects metrics, produces comparison table.
```

**Output**:

```
=== perf-compare: 4K randwrite qd=32, RF=1, 15s ===

| Binary | Write IOPS | Write P99 | Read IOPS | Read P99 |
|--------|-----------|-----------|-----------|----------|
| v1.5   | 47,233    | 1,234 us  | 62,100    | 890 us   |
| v2     | 46,666    | 1,250 us  | 61,800    | 910 us   |
| ceph   | 4,533     | 14,483 us | 65,761    | 1,319 us |
```

**New code**:

| Component | Description | Est. lines |
|-----------|-------------|-----------|
| Matrix parser | Parse `matrix:` from YAML, generate run combinations | 80 |
| Matrix executor | Run scenario N times, collect results per variant | 120 |
| Comparison reporter | Markdown/terminal table from collected metrics | 80 |
| Total | | ~280 |

**Acceptance**: `sw-test-runner compare rf1-perf-compare.yaml` produces
the table above with real numbers from hardware.

---

### P0: Ceph RBD Adapter

**Goal**: Run fio benchmarks against Ceph RBD on the same hardware, using
the same scenario YAML. Enable head-to-head comparison.

**Why P0**: Validates the 13.5x write IOPS advantage claim. Without this,
performance claims are from separate, non-reproducible runs.

**Prerequisites**: Ceph cluster deployed on m01/m02 (one monitor, two OSDs).
This is a one-time manual setup.

**New actions**:

| Action | Parameters | What it does |
|--------|-----------|--------------|
| `ceph_create_image` | pool, name, size | `rbd create --size {size} {pool}/{name}` |
| `ceph_map` | pool, name, node, save_as | `rbd map {pool}/{name}` → returns /dev/rbdN |
| `ceph_unmap` | device, node | `rbd unmap {device}` |
| `ceph_delete_image` | pool, name | `rbd rm {pool}/{name}` |

**Est. lines**: ~100 (4 actions, each ~25 lines wrapping CLI commands)

**Acceptance**: `ceph_create_image` + `ceph_map` + `fio_json` + `ceph_unmap`
works in a scenario YAML.

---

### P1: Result Index + History (text-db)

**Goal**: Index all run bundles in a lightweight text database. Support
listing, searching, comparing, and cleaning up old runs.

**Why P1**: Runs accumulate as directories on the test node. Without an
index, you can't find old results, compare trends, or clean up disk space.

**What already exists** (RunBundle system):

```
results/
  20260409-175214-d099/           ← one dir per run (timestamp-based)
    manifest.json                 ← run identity: scenario, git SHA, binary hash
    scenario.yaml                 ← frozen copy of input YAML
    result.json                   ← full structured result (phases, actions, vars)
    result.xml                    ← JUnit XML
    result.html                   ← HTML report
    artifacts/                    ← collected logs on failure
  20260410-012330-5d99/
    ...
```

Each run is self-contained. manifest.json has: run_id, scenario_name,
scenario_sha256, git_sha, host, status, command_line, started_at, finished_at.

**What's missing**: an index across runs.

**Design**: Add a `runs.db` text file (one JSON line per run) that acts
as a lightweight index. No external database dependency.

```
results/
  runs.db                         ← newline-delimited JSON (one line per run)
  20260409-175214-d099/
    manifest.json
    result.json
    ...
```

**runs.db format** (one JSON line per entry):

```json
{"run_id":"20260409-175214-d099","scenario":"v2-rebuild-rejoin","status":"pass","actions":58,"passed":58,"failed":0,"duration_ms":181795,"iops_write":46666,"iops_read":0,"git_sha":"8ecc50645","binary_md5":"4bcf08","started_at":"2026-04-09T17:52:14Z","dir":"20260409-175214-d099","disk_mb":12}
{"run_id":"20260410-012330-5d99","scenario":"recovery-baseline-failover","status":"pass","actions":43,"passed":43,"failed":0,"duration_ms":111583,"iops_write":28733,"git_sha":"8ecc50645","binary_md5":"4bcf08","started_at":"2026-04-10T01:23:30Z","dir":"20260410-012330-5d99","disk_mb":8}
```

**New CLI commands**:

```bash
# List all runs, most recent first
sw-test-runner list
  RUN_ID                  SCENARIO                    STATUS  ACTIONS  IOPS_W   DURATION
  20260411-140236-8012    dm-stripe-two-server        pass    42/42    79001    1m10s
  20260410-012330-5d99    recovery-baseline-failover   pass    43/43    28733    1m52s
  20260409-175214-d099    v2-rebuild-rejoin           pass    58/58    —        3m02s

# Filter by scenario
sw-test-runner list --scenario rebuild
sw-test-runner list --status fail
sw-test-runner list --since 2026-04-10

# Show trend for a scenario
sw-test-runner trend rf1-perf-compare
  DATE        GIT_SHA     IOPS_WRITE  IOPS_READ   STATUS
  2026-04-10  8ecc506     47233       —           pass (v1.5)
  2026-04-10  8ecc506     46666       —           pass (v2)

# Compare two runs
sw-test-runner diff 20260410-run1 20260410-run2

# Clean up old runs (keep last N per scenario)
sw-test-runner gc --keep 10
  Deleted 23 runs, freed 1.2 GB

# Rebuild index from existing run directories
sw-test-runner reindex
  Scanned 45 run directories, indexed 45 entries
```

**New code**:

| Component | Description | Est. lines |
|-----------|-------------|-----------|
| Index writer | Append one JSON line to runs.db after each run | 50 |
| Index reader | Parse runs.db, filter/sort/search | 80 |
| `list` command | Terminal table from index | 60 |
| `trend` command | Filter by scenario, show metrics over time | 60 |
| `gc` command | Delete old run dirs, update index | 50 |
| `reindex` command | Scan run dirs, rebuild runs.db from manifest+result | 60 |
| Disk size calculator | `du -s` each run dir, store in index | 20 |
| Total | | ~380 |

**Regression detection**:

```
sw-test-runner trend rf1-perf-compare --check
  Latest: 46,666 IOPS
  Baseline (last 10): 47,100 ± 800 IOPS
  Status: OK (within 1 stddev)

sw-test-runner trend rf1-perf-compare --check
  Latest: 38,000 IOPS
  Baseline (last 10): 47,100 ± 800 IOPS
  Status: REGRESSION (-19.3%, > 2 stddev)
```

**Acceptance**: After 5 runs, `sw-test-runner list` shows all 5 with
metrics. `sw-test-runner gc --keep 3` deletes the 2 oldest.

---

### P1: Cluster Templates

**Goal**: Remove the 20-line cluster boilerplate from every scenario.
Define reusable topology+cluster configs.

**Why P1**: Every new scenario copies the same `cluster-start` phase.
Changes to cluster config (ports, dirs) require editing every scenario.

**Design**:

```yaml
# templates/two-node-roce.yaml
cluster:
  master:
    node: m02
    port: 9433
    dir: /tmp/sw-master
    extra_args: "-ip=10.0.0.3"
  volumes:
    - node: m02
      port: 18480
      dir: /tmp/sw-vs1
      block_dir: /tmp/sw-vs1/blocks
      block_listen: ":3295"
      extra_args: "-ip=10.0.0.3"
    - node: m01
      port: 18480
      dir: /tmp/sw-vs2
      block_dir: /tmp/sw-vs2/blocks
      block_listen: ":3295"
      extra_args: "-ip=10.0.0.1"
```

```yaml
# scenario references template:
name: rebuild-rejoin
cluster: two-node-roce       # ← replaces 20 lines of cluster-start
phases:
  - name: create-and-write
    ...
```

**New code**:

| Component | Description | Est. lines |
|-----------|-------------|-----------|
| Template loader | Parse cluster template YAML | 60 |
| Auto cluster-start phase | Generate start actions from template | 80 |
| Auto cleanup phase | Generate stop actions from template | 40 |
| Variable injection | Expose `{{ master_pid }}`, `{{ vs1_pid }}`, etc. | 30 |
| Total | | ~210 |

**Acceptance**: `v2-rebuild-rejoin.yaml` works with `cluster: two-node-roce`
instead of inline cluster-start phase.

---

### P2: Debug Mode

**Goal**: When a scenario fails, automatically collect diagnostic
information and optionally pause for manual investigation.

**Why P2**: This session's debugging workflow was: scenario fails → SSH
to node → grep logs → find root cause → fix → redeploy → rerun. The
runner should automate the grep+collect step.

**New CLI flags**:

```bash
sw-test-runner run scenario.yaml --debug          # auto-grep + stop on fail
sw-test-runner run scenario.yaml --stop-on-fail   # pause, don't cleanup
sw-test-runner run scenario.yaml --grep "pattern"  # grep all node logs
```

**Auto-diagnostic on failure**:

```
=== FAILURE DIAGNOSTIC ===
Phase: restart-old-primary
Action: wait_volume_healthy (timeout 60s)

--- m01 volume.log (last 20 lines matching "error|fail|warn|panic") ---
W0409 15:41:18 recovery: rebuild execution failed: sender not found
...

--- m02 volume.log (last 20 lines matching "error|fail|warn|panic") ---
(clean)

--- m02 master.log (last 10 lines matching "failover|promote|assign") ---
I0409 15:41:12 failover: promoted replica for "v2-rebuild"
...
```

**New code**:

| Component | Description | Est. lines |
|-----------|-------------|-----------|
| `--stop-on-fail` handler | Skip cleanup, print SSH commands | 30 |
| Auto-grep on failure | SSH to all nodes, grep key patterns | 80 |
| Log merge by timestamp | Merge multi-node logs, sort by time | 60 |
| Diagnostic formatter | Terminal-friendly failure report | 40 |
| Total | | ~210 |

**Acceptance**: When `v2-rebuild-rejoin.yaml` fails, the runner
automatically shows the "sender not found" error from the logs
without manual SSH.

---

### P2: RF=2 Performance Benchmark Suite

**Goal**: Standardized benchmark scenario that matches the V1 bench
parameters exactly. Produces results comparable to the CEO's numbers.

**Why P2**: Our current V2 perf numbers are RF=1 only. The CEO's
benchmark was RF=2 sync_all over NVMe/TCP. We need the same config.

**Scenario**:

```yaml
name: rf2-perf-benchmark
cluster: two-node-roce

phases:
  - name: create-volume
    actions:
      - action: create_block_volume
        name: perf-vol
        size_bytes: "1073741824"
        replica_factor: "2"
        durability_mode: "sync_all"

  - name: benchmark
    actions:
      # Warmup
      - action: fio_json
        rw: randwrite
        bs: 4k
        iodepth: "32"
        runtime: "10"

      # 4K random write
      - action: fio_json
        rw: randwrite
        bs: 4k
        iodepth: "32"
        runtime: "15"
        save_as: write_result

      # 4K random read
      - action: fio_json
        rw: randread
        bs: 4k
        iodepth: "32"
        runtime: "15"
        save_as: read_result

      # 128K sequential write
      - action: fio_json
        rw: write
        bs: 128k
        iodepth: "16"
        runtime: "15"
        save_as: seq_write_result

      # Report
      - action: print
        msg: |
          RF=2 sync_all Performance:
          4K randwrite: {{ write_iops }} IOPS, P99={{ write_p99 }}us
          4K randread:  {{ read_iops }} IOPS, P99={{ read_p99 }}us
          128K seqwrite: {{ seq_bw }} MB/s
```

---

### P3: Full Cluster Deployment

**Goal**: Deploy a complete sw-block cluster (master + N volume servers +
CSI driver) from scratch on bare metal or K8s.

**Why P3**: Currently assumes pre-installed binaries and manual OS setup.
For CI/CD and customer POC, need push-button deployment.

**Scope**:

| Component | What it does |
|-----------|-------------|
| OS prereqs | Install iscsiadm, nvme-cli, fio, dmsetup |
| Binary deployment | Build + SCP weed binary to all nodes |
| Cluster bootstrap | Start master, wait ready, start volume servers |
| Volume provisioning | Create volumes, configure replication |
| Client setup | iSCSI/NVMe-oF discovery + login |
| Health check | Verify all components healthy |

**Effort**: ~500 lines. Depends on target environment (bare metal vs K8s).

---

### P3: pgbench / Application Benchmark

**Goal**: Run real database benchmarks (pgbench TPC-B, sysbench OLTP)
on sw-block volumes to produce application-level metrics.

**Why P3**: fio shows raw block performance. Customers care about
"how fast is PostgreSQL on your storage."

**New actions**:

| Action | What it does |
|--------|-------------|
| `pgbench_init` | `pgbench -i -s {scale} -h {host} {db}` |
| `pgbench_run` | `pgbench -c {clients} -T {time} -h {host} {db}` → parse TPS |
| `start_postgres` | Start PostgreSQL on a block PV |
| `stop_postgres` | Stop PostgreSQL cleanly |

---

## 4. Implementation Priority

| Priority | Feature | Lines | Impact |
|----------|---------|-------|--------|
| **P0** | Multi-version comparison matrix | 280 | CEO benchmark table |
| **P0** | Ceph RBD adapter | 100 | Head-to-head validation |
| **P1** | Structured results + trends | 300 | Regression detection |
| **P1** | Cluster templates | 210 | Scenario authoring speed |
| **P2** | Debug mode | 210 | Debugging speed |
| **P2** | RF=2 perf benchmark suite | 1 scenario | V1 number comparison |
| **P3** | Full cluster deployment | 500 | CI/CD, customer POC |
| **P3** | pgbench / app benchmarks | 200 | Customer-facing metrics |
| **Total** | | ~1,800 | |

## 5. Versioning Strategy

The testrunner should work across sw-block versions:

| Version | Binary | Scenarios | Expected behavior |
|---------|--------|-----------|-------------------|
| V1.5 | `weed-v1.5` | `rf1-perf-compare` only | RF=1 perf baseline |
| V2 | `weed` (current) | All 147 scenarios | Full coverage |
| V3 | `weed-v3` (future) | Same 4 acceptance scenarios | Must match V2 results |
| Ceph | `rbd` CLI | Perf scenarios via adapter | Comparison baseline |

The matrix runner handles this — same scenario, different binary, automatic
comparison.

## 6. Non-Goals

The testrunner should NOT become:

- A full CI/CD pipeline (use GitHub Actions / Jenkins for that)
- A monitoring system (use Prometheus/Grafana for ongoing monitoring)
- A configuration management tool (use Ansible/Terraform for cluster setup)
- A log aggregation system (use ELK/Loki for production log analysis)

It IS: a focused hardware validation and benchmark tool for sw-block
development and customer demos.
