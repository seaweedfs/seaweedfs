# V3 Observability Design

Date: 2026-04-13
Status: design draft
Purpose: define logging, metrics, tracing, debugging, and alerting for sw-block

## 1. Current State

### 1.1 What exists

| Component | Status | Location |
|-----------|--------|----------|
| Prometheus metrics | Partial | `blockvol/metrics.go`, volume server `/metrics` |
| Grafana dashboard | V1 draft | `monitoring/dashboards/block-overview.json` |
| glog logging | Active but unstructured | Throughout all Go code |
| pprof | Active | `net/http/pprof` on volume server debug port |
| Audit logging | None | — |
| Distributed tracing | None | — |
| Debug zip | None | — |
| Structured logging | None (plain text via glog) | — |

### 1.2 Problems with current logging

SeaweedFS (and sw-block) uses `glog` — Google's plain-text logger:

```
I0409 15:41:18.946772 volume_server_block.go:886 core [...]: event=replication.RebuildStarted mode=needs_rebuild ...
```

Issues:
- **Not structured**: key=value pairs embedded in free text, not machine-parseable
- **No channels**: replication, iSCSI, WAL, admin ops all go to the same log
- **No correlation**: no request ID or trace ID across nodes
- **No audit trail**: admin operations (create, delete, promote) are mixed with debug logs
- **Grep is the only query tool**: we spent hours in this session grep-ing for "sender not found"

## 2. Structured Logging

### 2.1 Channel model (from CockroachDB)

Route logs by audience, not by severity:

| Channel | Audience | What goes here |
|---------|----------|---------------|
| `OPS` | Operator | Volume lifecycle (create, delete, expand), config changes, server start/stop |
| `STORAGE` | Developer | WAL append, flush, SyncCache, extent I/O, crash recovery |
| `REPL` | Developer + Operator | Ship, barrier, catch-up, rebuild, probe, session start/complete/fail |
| `ISCSI` | Developer | Session login/logout, SCSI commands, connection events |
| `NVME` | Developer | NVMe-oF session, queue management, command processing |
| `AUDIT` | Security / Compliance | Admin mutations: who did what, when, result |
| `HEALTH` | Monitoring | Health score changes, scrub results, degraded/recovery events |

### 2.2 Log format

Use structured JSON for machine processing:

```json
{
  "ts": "2026-04-09T15:41:18.946Z",
  "level": "info",
  "channel": "REPL",
  "node": "10.0.0.3:18480",
  "volume": "mydb",
  "epoch": 2,
  "msg": "rebuild session completed",
  "replica": "10.0.0.1:18480",
  "achieved_lsn": 284756,
  "duration_ms": 2100,
  "blocks_transferred": 262144
}
```

Compare to current:
```
I0409 15:41:18 block_rebuild_remote.go:145 remote rebuild: session 2 completed (achieved=284756)
```

The structured version is greppable, queryable (jq), and indexable (Loki/ELK).

### 2.3 Implementation

Use `zap` (Uber's structured logger) as the logging backend. SeaweedFS
already uses glog everywhere, so the migration is incremental:

**Phase 1**: New block code uses zap directly
**Phase 2**: Wrapper that sends to both glog (for backward compat) and zap
**Phase 3**: Migrate existing glog calls in block code to zap channels

```go
// Block logger initialization
var (
    opsLog     = zap.L().Named("OPS")
    storageLog = zap.L().Named("STORAGE")
    replLog    = zap.L().Named("REPL")
    iscsiLog   = zap.L().Named("ISCSI")
    auditLog   = zap.L().Named("AUDIT")
    healthLog  = zap.L().Named("HEALTH")
)

// Usage:
replLog.Info("rebuild session completed",
    zap.String("volume", "mydb"),
    zap.String("replica", "10.0.0.1:18480"),
    zap.Uint64("achieved_lsn", 284756),
    zap.Duration("duration", 2100*time.Millisecond),
    zap.Int("blocks", 262144),
)
```

### 2.4 Log levels

| Level | When to use | Example |
|-------|-------------|---------|
| `DEBUG` | Internal detail, normally off | WAL entry encode/decode |
| `INFO` | Normal operation events | Volume created, rebuild completed |
| `WARN` | Abnormal but recoverable | Barrier failed, shipper degraded |
| `ERROR` | Requires attention | Crash recovery found CRC mismatch |
| `FATAL` | Cannot continue | Extent file corruption, unrecoverable |

Rule from etcd: **choose level based on whether human intervention is needed.** INFO = no action. WARN = monitor. ERROR = investigate. FATAL = fix now.

## 3. Distributed Tracing

### 3.1 Why it matters

This session's debugging workflow: scenario fails → grep primary log → grep replica log → manually correlate timestamps → find root cause. This took hours for each bug.

With tracing, the same debug session becomes:

```
Open Jaeger UI → find trace for the rebuild session → see spans:

Primary: handleReplicaProbeResult         [0ms - 5ms]
  └── applyCoreEvent(NeedsRebuildObserved) [1ms - 2ms]
  └── installSession(SessionRebuild)       [2ms - 3ms]
  └── startTask → runRebuild              [3ms - 10ms]
      └── PlanRebuild                     [3ms - 4ms]
      └── RebuildStarted → StartRebuildCommand [4ms - 5ms]
      └── ExecutePendingRebuild           [5ms - 6ms]
      └── RemoteRebuildIO.TransferFullBase [6ms - 2100ms]
          └── dial replica ctrl            [6ms - 11ms]
          └── send session control         [11ms - 12ms]
          └── read ack: accepted           [12ms - 15ms]
Replica: handleSessionControl             [12ms - 13ms]
  └── StartRebuildSession                 [13ms - 14ms]
  └── runBaseLaneClient                   [14ms - 2095ms]
      └── dial rebuild server              [14ms - 19ms]
      └── receive 262144 blocks           [19ms - 2090ms]
      └── MarkBaseComplete                [2090ms - 2091ms]
      └── TryComplete → send ack          [2091ms - 2095ms]
Primary: read ack: completed              [2095ms - 2096ms]
  └── OnRebuildCompleted                  [2096ms - 2100ms]
```

One view, both nodes, sub-millisecond timing for every step.

### 3.2 Implementation

Use OpenTelemetry with trace context propagation in the replication
wire protocol:

```go
// Add trace context to WAL shipping frame header
type ShipFrame struct {
    MsgType   byte
    TraceID   [16]byte  // OpenTelemetry trace ID (128-bit)
    SpanID    [8]byte   // OpenTelemetry span ID (64-bit)
    Payload   []byte
}

// On primary: start span, inject into frame
ctx, span := tracer.Start(ctx, "Ship")
frame.TraceID = span.SpanContext().TraceID()
frame.SpanID = span.SpanContext().SpanID()

// On replica: extract from frame, continue trace
ctx = otel.ContextWithRemoteSpanContext(ctx, ...)
_, span := tracer.Start(ctx, "ReplicaApply")
```

### 3.3 What to trace

| Path | Spans |
|------|-------|
| **Write** | SCSI Write → WriteLBA → WAL append → Ship → replica apply → ack |
| **SyncCache** | SCSI SyncCache → extent fsync → WAL fsync → barrier → replica fsync → ack |
| **Rebuild** | Probe → NeedsRebuild → StartRebuild → dial → session control → base transfer → completion |
| **Failover** | Master detects disconnect → lease expiry → promote → assignment delivery → new primary serves |

### 3.4 Export

Support multiple backends:
- **Jaeger**: for development/debugging
- **OTLP collector**: for production (routes to Grafana Tempo, Honeycomb, etc.)
- **Off**: no overhead when not needed (default)

```bash
weed server -block \
  -otel.endpoint=jaeger:4317 \
  -otel.sample_rate=0.01    # 1% sampling in production
```

## 4. Metrics

### 4.1 Must-have metrics

#### Volume I/O

```prometheus
# IOPS (counter — derive rate in Grafana)
sw_block_read_ops_total{volume="mydb"}
sw_block_write_ops_total{volume="mydb"}
sw_block_trim_ops_total{volume="mydb"}

# Throughput
sw_block_read_bytes_total{volume="mydb"}
sw_block_write_bytes_total{volume="mydb"}

# Latency (histogram — get p50/p99/p999 in Grafana)
sw_block_read_latency_seconds{volume="mydb"}
sw_block_write_latency_seconds{volume="mydb"}
# Buckets: 50us, 100us, 250us, 500us, 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms

# Queue depth
sw_block_pending_ops{volume="mydb",op="write"}
```

#### WAL

```prometheus
sw_block_wal_entries_total{volume="mydb"}
sw_block_wal_bytes_written_total{volume="mydb"}
sw_block_wal_fsync_latency_seconds{volume="mydb"}
sw_block_wal_recycle_total{volume="mydb"}
sw_block_wal_head_lsn{volume="mydb"}
sw_block_wal_tail_lsn{volume="mydb"}
```

#### Replication

```prometheus
sw_block_ship_latency_seconds{volume="mydb",replica="10.0.0.1"}
sw_block_ship_bytes_total{volume="mydb",replica="10.0.0.1"}
sw_block_replica_lag_lsn{volume="mydb",replica="10.0.0.1"}
sw_block_barrier_latency_seconds{volume="mydb"}
sw_block_barrier_success_total{volume="mydb"}
sw_block_barrier_failure_total{volume="mydb"}
```

#### Health / State

```prometheus
sw_block_volume_mode{volume="mydb"} 1
# 0=offline, 1=healthy, 2=degraded, 3=rebuilding, 4=needs_rebuild
sw_block_health_score{volume="mydb"} 1.0
sw_block_replica_state{volume="mydb",replica="10.0.0.1"} 3
# 0=disconnected, 1=connecting, 2=catching_up, 3=in_sync, 4=degraded, 5=needs_rebuild
```

#### Events

```prometheus
sw_block_failover_total{volume="mydb"}
sw_block_rebuild_total{volume="mydb"}
sw_block_rebuild_duration_seconds{volume="mydb"}
sw_block_scrub_errors_total{volume="mydb"}
```

#### Capacity

```prometheus
sw_block_volume_size_bytes{volume="mydb"}
sw_block_volume_used_bytes{volume="mydb"}
sw_block_snapshot_count{volume="mydb"}
sw_block_cluster_capacity_bytes
sw_block_cluster_used_bytes
```

#### iSCSI / NVMe

```prometheus
sw_block_iscsi_sessions_active{portal="10.0.0.1:3260"}
sw_block_iscsi_login_total{portal="10.0.0.1:3260"}
sw_block_iscsi_command_total{portal="10.0.0.1:3260",opcode="write10"}
sw_block_nvme_connections_active{subsystem="mydb"}
```

### 4.2 What already exists vs what's needed

| Metric category | Exists? | Where |
|-----------------|---------|-------|
| WAL entries, pressure | Yes | `blockvol/metrics.go` |
| Barrier latency | Yes | `wal_shipper.go` |
| Health score | Yes | `health_score.go` |
| Scrub errors | Yes | `scrub.go` |
| Role/epoch | Yes | heartbeat |
| **I/O IOPS/latency histogram** | **No** | Need to add in WriteLBA/ReadLBA |
| **Rebuild duration** | **No** | Need to add in recovery manager |
| **Replica lag LSN** | **Partial** | In heartbeat but not Prometheus |
| **iSCSI session count** | **No** | Need to add in iSCSI target |

## 5. Debugging Tools

### 5.1 Debug zip

```bash
weed shell
> block.debug.zip -output=/tmp/debug.zip

Collecting...
  ✓ Volume status (3 volumes)
  ✓ Server health (2 servers)
  ✓ WAL status (head/tail/recycled per volume)
  ✓ Replica state (shipper state, lag, last contact)
  ✓ Recent events (last 100 failover/rebuild/degraded)
  ✓ Goroutine dump (242 goroutines)
  ✓ Heap profile (resident: 128MB, alloc: 89MB)
  ✓ Logs (last 1h, all channels)
  ✓ Metrics snapshot (last 5min counters)

Written: /tmp/debug.zip (23MB)
```

Contents:
```
debug-20260413-143000/
  cluster.json           ← block.health output as JSON
  volumes/
    mydb.json            ← block.status output per volume
  logs/
    node-10.0.0.1/
      ops.log            ← last 1h, OPS channel
      repl.log           ← last 1h, REPL channel
      storage.log        ← last 1h, STORAGE channel
    node-10.0.0.3/
      ops.log
      repl.log
      storage.log
  profiles/
    goroutines.txt       ← all goroutine stacks
    heap.pb.gz           ← Go heap profile (pprof format)
    cpu-10s.pb.gz        ← 10-second CPU profile
  metrics/
    snapshot.json         ← all Prometheus metrics at collection time
  config/
    master.json          ← master config
    volume-server.json   ← volume server config per node
```

### 5.2 Log merge

```bash
# Merge logs from all nodes, sorted by timestamp
weed shell
> block.debug.merge-logs -dir=/tmp/debug-20260413/logs

2026-04-09T15:41:18.946Z [node1] [REPL] rebuild session started replica=10.0.0.1
2026-04-09T15:41:18.947Z [node2] [REPL] handleSessionControl start_rebuild session=2
2026-04-09T15:41:18.948Z [node2] [STORAGE] base lane client starting addr=10.0.0.3:5260
2026-04-09T15:41:20.950Z [node2] [STORAGE] base lane complete: 262144 blocks
2026-04-09T15:41:20.951Z [node1] [REPL] rebuild session completed achieved=284756
```

One timeline, both nodes, channel-tagged. The exact view we needed during this session's debugging but had to build manually with grep.

### 5.3 Live tail

```bash
# Real-time log streaming filtered by channel and volume
weed shell
> block.debug.tail -channel=REPL -volume=mydb

2026-04-13T14:30:01.123Z [INFO] ship entry lsn=284757 → 10.0.0.1
2026-04-13T14:30:01.124Z [INFO] ship entry lsn=284758 → 10.0.0.1
2026-04-13T14:30:02.000Z [WARN] barrier timeout target=284758 replica=10.0.0.1
```

## 6. Audit Logging

### 6.1 What to audit

Every admin mutation that changes cluster state:

| Operation | Logged fields |
|-----------|--------------|
| Volume create | actor, name, size, RF, durability mode |
| Volume delete | actor, name, size, snapshot count |
| Volume expand | actor, name, old_size, new_size |
| Manual promote | actor, volume, old_primary, new_primary, epoch |
| Snapshot create | actor, volume, snapshot_id |
| Snapshot delete | actor, volume, snapshot_id |
| Config change | actor, key, old_value, new_value |
| Server add/remove | actor, server_address |
| Rebuild trigger | actor (master/auto), volume, source, target |
| CHAP credential change | actor, portal, operation |

### 6.2 Audit log format

```json
{
  "ts": "2026-04-13T14:30:00.000Z",
  "channel": "AUDIT",
  "actor": "admin@10.0.0.100",
  "operation": "volume.delete",
  "target": "old-vol",
  "details": {
    "size_bytes": 53687091200,
    "replica_factor": 2,
    "snapshots_deleted": 3,
    "epoch": 5
  },
  "result": "success"
}
```

### 6.3 Actor identification

| Source | Actor format |
|--------|-------------|
| `weed shell` | `shell@{client_ip}` |
| REST API | `api@{client_ip}` or authenticated user |
| CSI driver | `csi@{node_name}` |
| Master auto-action | `master@{master_addr}` (e.g., auto-failover) |
| Primary auto-action | `primary@{vs_addr}` (e.g., auto-rebuild) |

## 7. Alert Design

### 7.1 Three tiers

| Tier | Condition | Action | Example |
|------|-----------|--------|---------|
| **PAGE** | Data at risk | Wake someone | Volume offline (all replicas down), data corruption (CRC mismatch), split-brain detected |
| **TICKET** | Service degraded | Next business day | Volume degraded (one replica down >5min), rebuild stalled >10min, scrub errors, disk >85% |
| **LOG** | Informational | No action | Rebuild started/completed, failover event, snapshot created, config change |

### 7.2 Anti-patterns to avoid

| Anti-pattern | How to avoid |
|-------------|--------------|
| Same severity for everything | Use the three tiers above |
| Static thresholds on dynamic workloads | Alert on P99 change, not absolute value |
| Cascading alerts (1 OSD down → 10 alerts) | Group by volume/server in Alertmanager |
| No context in alerts | Include volume name, server, epoch, current state |
| Stale thresholds | Monthly alert audit: did this alert lead to action? |

### 7.3 Recommended Prometheus rules

```yaml
groups:
  - name: sw-block-page
    rules:
      - alert: BlockVolumeOffline
        expr: sw_block_volume_mode == 0
        for: 1m
        labels: { severity: page }
        annotations:
          summary: "Volume {{ $labels.volume }} is OFFLINE"
          action: "Check server connectivity. Run 'block.status {{ $labels.volume }}'"

      - alert: BlockDataCorruption
        expr: sw_block_scrub_errors_total > 0
        labels: { severity: page }
        annotations:
          summary: "Volume {{ $labels.volume }} has {{ $value }} scrub errors"
          action: "Run 'block.status {{ $labels.volume }}'. Check extent integrity."

  - name: sw-block-ticket
    rules:
      - alert: BlockVolumeDegraded
        expr: sw_block_volume_mode == 2
        for: 5m
        labels: { severity: ticket }
        annotations:
          summary: "Volume {{ $labels.volume }} degraded for >5min"
          action: "Check replica state. Run 'block.status {{ $labels.volume }}'"

      - alert: BlockRebuildStalled
        expr: sw_block_rebuild_duration_seconds > 600
        labels: { severity: ticket }
        annotations:
          summary: "Rebuild for {{ $labels.volume }} stalled ({{ $value | humanizeDuration }})"

      - alert: BlockCapacityHigh
        expr: sw_block_cluster_used_bytes / sw_block_cluster_capacity_bytes > 0.85
        labels: { severity: ticket }
        annotations:
          summary: "Block cluster capacity at {{ $value | humanizePercentage }}"

      - alert: BlockWriteLatencyHigh
        expr: histogram_quantile(0.99, rate(sw_block_write_latency_seconds_bucket[5m])) > 0.01
        for: 10m
        labels: { severity: ticket }
        annotations:
          summary: "Volume {{ $labels.volume }} P99 write latency >10ms"

  - name: sw-block-log
    rules:
      - alert: BlockRebuildCompleted
        expr: increase(sw_block_rebuild_total[5m]) > 0
        labels: { severity: log }
        annotations:
          summary: "Rebuild completed for {{ $labels.volume }}"
```

## 8. Implementation Priority

| Priority | Feature | Effort | Impact |
|----------|---------|--------|--------|
| **P0** | I/O latency histogram in WriteLBA/ReadLBA | 50 lines | Perf visibility |
| **P0** | Rebuild duration metric | 20 lines | Recovery visibility |
| **P1** | Audit logging channel | 100 lines | Compliance, admin visibility |
| **P1** | Debug zip command | 200 lines | Debugging speed |
| **P1** | Alert rules template | 1 YAML file | Production monitoring |
| **P1** | Grafana dashboard update | Update JSON | Visual monitoring |
| **P2** | Structured logging (zap channels) | 300 lines migration | Log queryability |
| **P2** | Log merge tool | 80 lines | Multi-node debugging |
| **P3** | OpenTelemetry tracing | 500 lines | End-to-end latency analysis |
| **P3** | Trace context in replication protocol | Wire format change | Cross-node trace correlation |
