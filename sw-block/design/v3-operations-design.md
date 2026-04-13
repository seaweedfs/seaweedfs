# V3 Operations Design

Date: 2026-04-13
Status: design draft
Purpose: define the user-facing operations experience for sw-block

## 1. Design Principles

### 1.1 One command to start

A developer should go from zero to a working block volume in under 10
seconds with one command. No config files, no YAML, no separate services.

### 1.2 Dry-run by default

Every destructive operation previews what it will do. The user must
explicitly confirm with `-apply`. This is already the SeaweedFS shell
convention — extend it to all block operations.

### 1.3 Shell is the operations interface

`weed shell` is the single entry point for admin operations. No separate
CLI tool. The shell already provides tab completion, command history,
distributed locking, and 90+ commands. Block operations plug into the
same framework.

### 1.4 Health at a glance

An admin should never need to grep logs to understand cluster state.
`block.health` shows everything in one screen. Green/yellow/red, not
log lines.

### 1.5 Developer sees only PVC

K8s developers interact with storage through `PersistentVolumeClaim`.
They never see iSCSI, NVMe-oF, replication, failover, or rebuild.
The CSI driver handles everything.

### 1.6 Three personas, three interfaces

| Persona | Interface | What they see |
|---------|-----------|---------------|
| Developer | K8s PVC / REST API | "Give me 100GB storage" |
| Operator | `weed shell` | Volume health, failover, rebuild, snapshots |
| Platform engineer | Config + monitoring | Cluster sizing, Prometheus, alerting |

## 2. Setup Experience

### 2.1 Single-node development (10 seconds)

```bash
# Download
curl -O https://github.com/seaweedfs/seaweedfs/releases/latest/weed

# Start everything in one process
./weed server -block -block.size=100G -dir=/data

# Output:
# Master started on :9333
# Volume server started on :8480
# Block service started (iSCSI :3260)
# Created default volume "vol-1" (100GB, RF=1)
#
# Connect from this machine:
#   iscsiadm -m discovery -t st -p localhost:3260
#   iscsiadm -m node -T iqn.2024-01.com.seaweedfs:vol.vol-1 -l
#
# Or use NVMe-oF:
#   nvme discover -t tcp -a 127.0.0.1 -s 4420
```

The output tells the user exactly what to do next. No documentation
needed for the first connection.

### 2.2 Two-node replicated (30 seconds)

```bash
# Node 1 (master + volume server):
./weed server -block -block.size=100G -ip=10.0.0.1

# Node 2 (volume server only):
./weed volume -block -block.size=100G -ip=10.0.0.3 \
  -mserver=10.0.0.1:9333

# Create replicated volume:
./weed shell -master=10.0.0.1:9333
> block.create -name=mydb -size=100G -rf=2 -apply
  Created "mydb" (100GB, RF=2, sync_all)
  Primary: 10.0.0.1:8480
  Replica: 10.0.0.3:8480
  iSCSI:   iqn.2024-01.com.seaweedfs:vol.mydb at 10.0.0.1:3260
```

### 2.3 Kubernetes (2 minutes)

```bash
# Install CSI driver
helm repo add seaweedfs https://seaweedfs.github.io/charts
helm install sw-block seaweedfs/sw-block-csi \
  --set master.address=10.0.0.1:9333

# Create StorageClass
kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: sw-block
provisioner: seaweedfs.com/block
parameters:
  replicaFactor: "2"
  durabilityMode: sync_all
allowVolumeExpansion: true
EOF

# Developer just uses PVC:
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-database
spec:
  storageClassName: sw-block
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 100Gi
EOF
```

### 2.4 Setup comparison

| System | Commands to first working volume | Time |
|--------|--------------------------------|------|
| sw-block | 1 (`weed server -block`) | 10s |
| MinIO | 1 (`minio server`) | 5s (object only, no block) |
| SeaweedFS (object) | 1 (`weed server`) | 5s |
| Longhorn | 3 (`helm install` + PVC) | 5min |
| Ceph | 5-8 (`cephadm bootstrap` + OSD + RBD) | 30min |
| TrueNAS | Web installer + wizard | 15min |

## 3. Shell Commands

### 3.1 Command naming convention

All block commands start with `block.`. Subcommands use dots.
This matches the existing shell convention (`volume.list`, `fs.ls`,
`ec.encode`).

### 3.2 Volume lifecycle

```
block.create     Create a new block volume
block.delete     Delete a block volume (dry-run by default)
block.list       List all block volumes with health status
block.status     Show detailed status of one volume
block.expand     Expand volume size (online, no downtime)
```

### 3.3 Data protection

```
block.snapshot          Create a CoW snapshot
block.snapshot.list     List snapshots for a volume
block.snapshot.delete   Delete a snapshot
block.snapshot.export   Export snapshot to S3 (SeaweedFS object store)
block.snapshot.restore  Restore volume from snapshot
```

### 3.4 HA operations

```
block.promote    Manual failover: promote replica to primary
block.rebuild    Trigger manual rebuild for a replica
block.assign     Change replica assignment (add/remove replica)
```

### 3.5 Cluster health

```
block.health     Cluster-wide health summary
block.servers    List block-capable volume servers
block.events     Recent events (failover, rebuild, degraded)
```

### 3.6 Benchmarking

```
block.bench      Run fio benchmark on a volume (creates temp volume if needed)
block.bench.compare  Compare performance of two runs
```

### 3.7 Detailed command specs

#### block.list

```
> block.list
  NAME          SIZE    RF  MODE             PRIMARY          REPLICA          HEALTH  IOPS
  pvc-data-01   100G    2   publish_healthy  10.0.0.1:8480    10.0.0.3:8480    1.0     45.2K
  pvc-db-01     50G     2   degraded         10.0.0.3:8480    (rebuilding 78%) 0.8     38.1K
  pvc-temp      200G    1   healthy          10.0.0.1:8480    —                1.0     52.0K

> block.list -json
  [{"name":"pvc-data-01","size":107374182400,"rf":2,"mode":"publish_healthy",...}]
```

Columns:
- NAME: volume name
- SIZE: allocated size
- RF: replica factor
- MODE: engine projection mode
- PRIMARY: current primary server
- REPLICA: replica server(s) with status
- HEALTH: 0.0-1.0 score
- IOPS: current write IOPS (from last heartbeat)

#### block.status

```
> block.status mydb
  Volume:     mydb
  Size:       100G (used: 34G, 34%)
  RF:         2 (sync_all)
  Mode:       publish_healthy
  Epoch:      3
  
  Primary:    10.0.0.1:8480
    WAL head: 284756
    Checkpoint: 284700
    Uptime:   3d 14h
  
  Replica:    10.0.0.3:8480
    State:    in_sync
    Flushed:  284756
    Lag:      0 entries
  
  iSCSI:      iqn.2024-01.com.seaweedfs:vol.mydb at 10.0.0.1:3260
  NVMe-oF:    nqn.2024-01.com.seaweedfs:vol.mydb at 10.0.0.1:4420
  
  Snapshots:
    mydb@2026-04-13T10:30  34G  (3 hours ago)
    mydb@2026-04-12T22:00  32G  (15 hours ago)
  
  Recent events:
    2026-04-11 02:12  rebuild completed (2.1s, 1GB transferred)
    2026-04-09 22:54  auto-failover (10.0.0.1 → 10.0.0.3, epoch 1→2)
  
  Health:     1.0 (last scrub: 2026-04-12, 0 errors)
```

#### block.health

```
> block.health
  ╔═══════════════════════════════════════╗
  ║  BLOCK CLUSTER HEALTH: OK            ║
  ╚═══════════════════════════════════════╝
  
  Volumes:    12 total
              11 healthy
               1 degraded (pvc-db-01: rebuilding)
               0 offline
  
  Servers:    3 online, 0 offline
              10.0.0.1:8480  ██████████  6 volumes, 450G used
              10.0.0.3:8480  ████████    5 volumes, 380G used
              10.0.0.5:8480  ██████      4 volumes, 280G used
  
  Capacity:   1.1T used / 3.0T total (37%)
  
  Last 24h:
    Failovers: 0
    Rebuilds:  1 (pvc-db-01, in progress, 78%)
    Scrub errors: 0
  
  Alerts:     none
```

#### block.create (dry-run)

```
> block.create -name=analytics -size=500G -rf=2
  DRY RUN — no changes made
  
  Would create volume "analytics":
    Size:       500G
    RF:         2 (sync_all)
    Primary:    10.0.0.5:8480 (most free space: 720G)
    Replica:    10.0.0.1:8480 (550G free, different rack)
    iSCSI IQN: iqn.2024-01.com.seaweedfs:vol.analytics
  
  Run with -apply to execute.

> block.create -name=analytics -size=500G -rf=2 -apply
  Created "analytics" (500GB, RF=2, sync_all)
  Primary: 10.0.0.5:8480
  Replica: 10.0.0.1:8480
  iSCSI:   iqn.2024-01.com.seaweedfs:vol.analytics at 10.0.0.5:3260
```

#### block.delete (always requires -apply -confirm)

```
> block.delete -name=old-vol
  DRY RUN — no changes made
  
  Would delete volume "old-vol":
    Size:       50G (used: 23G)
    RF:         2 (primary: 10.0.0.1, replica: 10.0.0.3)
    Snapshots:  3 (would also be deleted)
    Age:        45 days
  
  WARNING: This permanently destroys all data and snapshots.
  Run with -apply -confirm to execute.

> block.delete -name=old-vol -apply -confirm
  Deleted "old-vol" (50GB, 3 snapshots removed)
```

#### block.promote (manual failover)

```
> block.promote -name=mydb -target=10.0.0.3:8480
  DRY RUN — no changes made
  
  Would promote 10.0.0.3:8480 to primary for "mydb":
    Current primary: 10.0.0.1:8480 (epoch 2)
    New primary:     10.0.0.3:8480 (would become epoch 3)
    Replica state:   in_sync (lag: 0 entries)
    Impact:          iSCSI connections will reconnect (~2s disruption)
  
  Run with -apply to execute.

> block.promote -name=mydb -target=10.0.0.3:8480 -apply
  Promoted 10.0.0.3:8480 to primary for "mydb" (epoch 2→3)
  Old primary 10.0.0.1:8480 will rejoin as replica after next heartbeat.
```

## 4. REST API

### 4.1 Endpoints

All under `/block/` prefix on the master server.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/block/volumes` | List all block volumes |
| `POST` | `/block/volumes` | Create block volume |
| `GET` | `/block/volumes/{name}` | Get volume status |
| `DELETE` | `/block/volumes/{name}` | Delete volume |
| `PUT` | `/block/volumes/{name}/expand` | Expand volume |
| `POST` | `/block/volumes/{name}/promote` | Manual failover |
| `POST` | `/block/volumes/{name}/snapshots` | Create snapshot |
| `GET` | `/block/volumes/{name}/snapshots` | List snapshots |
| `DELETE` | `/block/volumes/{name}/snapshots/{id}` | Delete snapshot |
| `GET` | `/block/health` | Cluster health summary |
| `GET` | `/block/servers` | List block-capable servers |
| `GET` | `/block/events` | Recent events |

### 4.2 Examples

```bash
# Create volume
curl -X POST http://master:9333/block/volumes \
  -H "Content-Type: application/json" \
  -d '{"name":"mydb","sizeBytes":107374182400,"replicaFactor":2}'

# Response:
{
  "name": "mydb",
  "sizeBytes": 107374182400,
  "replicaFactor": 2,
  "durabilityMode": "sync_all",
  "primary": "10.0.0.1:8480",
  "replica": "10.0.0.3:8480",
  "iscsiIqn": "iqn.2024-01.com.seaweedfs:vol.mydb",
  "iscsiPortal": "10.0.0.1:3260",
  "epoch": 1,
  "mode": "bootstrap_pending"
}

# Get health
curl http://master:9333/block/health

# Response:
{
  "status": "ok",
  "volumes": {"total": 12, "healthy": 11, "degraded": 1},
  "servers": {"total": 3, "online": 3},
  "capacity": {"usedBytes": 1181116006400, "totalBytes": 3221225472000}
}
```

## 5. Observability

### 5.1 Prometheus metrics

Already implemented. Key metrics:

```prometheus
# Volume level
sw_block_volume_mode{name="mydb"} 1                    # 1=healthy
sw_block_volume_iops{name="mydb",op="write"} 45200
sw_block_volume_size_bytes{name="mydb"} 107374182400
sw_block_volume_used_bytes{name="mydb"} 36507222016

# Replica level
sw_block_replica_state{name="mydb",server="10.0.0.3"} 3  # 3=in_sync
sw_block_replica_lag_entries{name="mydb",server="10.0.0.3"} 0

# Cluster level
sw_block_volumes_total 12
sw_block_volumes_healthy 11
sw_block_volumes_degraded 1
sw_block_servers_online 3

# Events
sw_block_failovers_total 2
sw_block_rebuilds_total 5
sw_block_rebuild_duration_seconds{name="mydb"} 2.1
```

### 5.2 Alerting rules

Recommended Prometheus alert rules:

```yaml
groups:
  - name: sw-block
    rules:
      - alert: BlockVolumeDegraded
        expr: sw_block_volume_mode != 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Block volume {{ $labels.name }} is degraded"

      - alert: BlockVolumeOffline
        expr: sw_block_volume_mode == 0
        for: 1m
        labels:
          severity: critical

      - alert: BlockRebuildStalled
        expr: sw_block_rebuild_duration_seconds > 300
        labels:
          severity: warning
          summary: "Rebuild for {{ $labels.name }} taking > 5 minutes"

      - alert: BlockCapacityHigh
        expr: sw_block_cluster_used_bytes / sw_block_cluster_total_bytes > 0.85
        labels:
          severity: warning
          summary: "Block cluster capacity > 85%"
```

### 5.3 Grafana dashboard

Provide a pre-built Grafana dashboard JSON that shows:
- Cluster health panel (big green/yellow/red indicator)
- Volume list with mode, health, IOPS
- IOPS over time (per-volume and aggregate)
- Latency percentiles (p50, p99, p999)
- Rebuild/failover event markers
- Capacity trend
- Per-server utilization

## 6. Error Messages

Every error should tell the user what happened, why, and what to do:

```
# Bad:
Error: volume not found

# Good:
Error: block volume "mydb" not found.
Available volumes: pvc-data-01, pvc-db-01, pvc-temp
Run 'block.list' to see all volumes.
```

```
# Bad:
Error: cannot create volume

# Good:
Error: cannot create volume "analytics" (500GB, RF=2).
Reason: not enough servers. Need 2 servers for RF=2, but only 1 is online.
Run 'block.servers' to check server status.
```

```
# Bad:
Error: promote failed

# Good:
Error: cannot promote 10.0.0.3:8480 for "mydb".
Reason: replica is not in_sync (current state: rebuilding, 78% complete).
Wait for rebuild to complete, then retry.
Run 'block.status mydb' for details.
```

## 7. Comparison with Competitors

### 7.1 What we steal

| From | What | How |
|------|------|-----|
| MinIO | One-command startup | `weed server -block` |
| SeaweedFS | Shell as operations interface | `block.*` commands in `weed shell` |
| Ceph | Dry-run on all destructive ops | `-apply` flag required |
| Ceph | Declarative cluster spec | Future: `block.apply -f cluster.yaml` |
| Longhorn | Auto-rebuild on config change | Change RF in shell, system handles rebuild |
| Longhorn | One-click snapshot | `block.snapshot mydb` (one command, no params) |
| TrueNAS | iSCSI setup wizard | `block.setup` interactive guide |
| TrueNAS | Actionable error messages | Every error says what to do next |

### 7.2 What we do better

| Area | Competitors | sw-block |
|------|------------|----------|
| Setup time | Ceph: 30min, Longhorn: 5min | 10 seconds |
| Write IOPS (RF=2) | Ceph: 4.5K | 61K (13.5x) |
| Rebuild time (1GB) | Ceph: minutes | 2 seconds |
| CLI | Ceph: many tools, Longhorn: kubectl | Single `weed shell` |
| Object + Block | Separate systems | Same binary, same cluster |

### 7.3 What we do not provide (yet)

| Feature | Who has it | Priority |
|---------|-----------|----------|
| Web dashboard | Ceph, Longhorn, TrueNAS | P3 |
| Declarative cluster spec | Ceph `ceph orch` | P3 |
| Grafana dashboard JSON | Ceph, Longhorn | P2 |
| Multi-cluster management | None (all single-cluster) | Not planned |

## 8. Implementation Phases

### Phase 1: Shell commands (P0)

Implement in `weed/shell/`:
- `block.list` — volume list with health
- `block.status` — detailed volume status
- `block.create` — create with dry-run
- `block.delete` — delete with dry-run + confirm
- `block.health` — cluster health summary

These commands call existing master gRPC APIs. No new backend code
needed — just shell-side formatting.

Estimated: ~400 lines, 5 new files in `weed/shell/`.

### Phase 2: REST API (P1)

Implement in `weed/server/master_server_handlers_block.go`:
- CRUD endpoints for volumes
- Health endpoint
- Server list endpoint

These wrap existing `BlockVolumeRegistry` and `blockAssignmentQueue`
methods. JSON request/response.

Estimated: ~300 lines.

### Phase 3: Enhanced operations (P2)

- `block.snapshot` / `block.snapshot.list`
- `block.expand`
- `block.promote` with dry-run
- `block.events` (recent failover/rebuild log)
- `block.bench` (built-in fio wrapper)

### Phase 4: Developer experience (P2)

- `weed server -block` one-click mode
- Actionable error messages for all block operations
- Connection string output on volume creation
- `block.setup` interactive wizard

### Phase 5: Dashboards (P3)

- Grafana dashboard JSON
- Prometheus alert rules template
- Web UI for block volume management (if demand exists)
