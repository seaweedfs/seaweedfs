# SeaweedFS Block Storage -- Getting Started

Block storage exposes SeaweedFS volumes as `/dev/sdX` block devices via iSCSI.
You can format them with ext4/xfs, mount them, and use them like any disk.

## Prerequisites

- Linux host with `open-iscsi` installed
- Docker with compose plugin (`docker compose`)

```bash
# Install iSCSI initiator (Ubuntu/Debian)
sudo apt-get install -y open-iscsi

# Verify
sudo systemctl start iscsid
```

## Quick Start (5 minutes)

### 1. Build the image

```bash
# From the seaweedfs repo root
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o docker/compose/weed ./weed
cd docker
docker build -f Dockerfile.local -t seaweedfs-block:local .
```

### 2. Start the cluster

```bash
cd docker/compose

# Set HOST_IP to your machine's IP (for remote iSCSI clients)
# Use 127.0.0.1 for local-only testing
HOST_IP=127.0.0.1 docker compose -f local-block-compose.yml up -d
```

Wait ~5 seconds for the volume server to register with the master.

### 3. Create a block volume

```bash
curl -s -X POST http://localhost:9333/block/volume \
  -H "Content-Type: application/json" \
  -d '{"name":"myvolume","size_bytes":1073741824}'
```

This creates a 1GB block volume, auto-assigns it as primary, and starts the
iSCSI target. The response includes the IQN and iSCSI address.

### 4. Connect via iSCSI

```bash
# Discover targets
sudo iscsiadm -m discovery -t sendtargets -p 127.0.0.1:3260

# Login
sudo iscsiadm -m node -T iqn.2024-01.com.seaweedfs:vol.myvolume \
  -p 127.0.0.1:3260 --login

# Find the new device
lsblk | grep sd
```

### 5. Format and mount

```bash
# Format with ext4
sudo mkfs.ext4 /dev/sdX

# Mount
sudo mkdir -p /mnt/myvolume
sudo mount /dev/sdX /mnt/myvolume

# Use it like any filesystem
echo "hello" | sudo tee /mnt/myvolume/test.txt
```

### 6. Cleanup

```bash
sudo umount /mnt/myvolume
sudo iscsiadm -m node -T iqn.2024-01.com.seaweedfs:vol.myvolume \
  -p 127.0.0.1:3260 --logout
docker compose -f local-block-compose.yml down -v
```

## API Reference

All endpoints are on the master server (default: port 9333).

### Create volume

```
POST /block/volume
Content-Type: application/json

{
  "name": "myvolume",
  "size_bytes": 1073741824,
  "disk_type": "ssd",
  "replica_placement": "001",
  "durability_mode": "best_effort"
}
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | yes | -- | Volume name (alphanumeric + hyphens) |
| `size_bytes` | yes | -- | Volume size in bytes |
| `disk_type` | no | `""` | Disk type hint: `ssd`, `hdd` |
| `replica_placement` | no | `000` | SeaweedFS placement: `000` (no replica), `001` (1 replica same rack) |
| `durability_mode` | no | `best_effort` | `best_effort`, `sync_all`, `sync_quorum` |
| `replica_factor` | no | `2` | Number of copies: 1, 2, or 3 |

### List volumes

```
GET /block/volumes
```

Returns JSON array of all block volumes with status, role, epoch, IQN, etc.

### Lookup volume

```
GET /block/volume/{name}
```

### Delete volume

```
DELETE /block/volume/{name}
```

### Assign role

```
POST /block/assign
Content-Type: application/json

{
  "name": "myvolume",
  "epoch": 2,
  "role": "primary",
  "lease_ttl_ms": 30000
}
```

Roles: `primary`, `replica`, `stale`, `rebuilding`.

### Cluster status

```
GET /block/status
```

Returns volume count, server count, failover stats, queue depth.

## Remote Client Setup

To connect from a remote machine (not the Docker host):

1. Set `HOST_IP` to the Docker host's network-reachable IP:
   ```bash
   HOST_IP=192.168.1.100 docker compose -f local-block-compose.yml up -d
   ```

2. On the client machine:
   ```bash
   sudo iscsiadm -m discovery -t sendtargets -p 192.168.1.100:3260
   sudo iscsiadm -m node -T iqn.2024-01.com.seaweedfs:vol.myvolume \
     -p 192.168.1.100:3260 --login
   ```

## Volume Lifecycle

```
create  -->  primary (serving I/O via iSCSI)
                |
           unmount/remount OK (lease auto-renewed by master)
                |
         assign replica  -->  WAL shipping active
                |
          kill primary   -->  promote replica  -->  new primary
                |
          old primary    -->  rebuild from new primary
```

Key points:
- **Lease renewal is automatic.** The master continuously renews the primary's
  write lease via the heartbeat stream. Unmount/remount works without manual
  intervention.
- **Epoch fencing.** Each role change bumps the epoch. Old primaries cannot
  write after being demoted -- even if they still have the lease.
- **Volumes survive container restart.** Data is stored in the Docker volume
  at `/data/blocks/`. The volume server re-registers with the master on restart.

## Troubleshooting

**iSCSI login fails with "No records found"**
- Run discovery first: `sudo iscsiadm -m discovery -t sendtargets -p HOST:3260`

**Device not appearing after login**
- Check `dmesg | tail` for SCSI errors
- Verify the volume is assigned as primary: `curl http://HOST:9333/block/volumes`

**I/O errors on write**
- Check volume role is `primary` (not `none` or `stale`)
- Check master is running (lease renewal requires master heartbeat)

**Stuck iSCSI session after container restart**
- Force logout: `sudo iscsiadm -m node -T IQN -p HOST:PORT --logout`
- If stuck: `sudo ss -K dst HOST dport = 3260` to kill the TCP connection
- Then re-discover and login

## Docker Compose Reference

```yaml
# local-block-compose.yml
services:
  master:
    image: seaweedfs-block:local
    ports:
      - "9333:9333"      # HTTP API
      - "19333:19333"     # gRPC
    command: ["master", "-ip=master", "-ip.bind=0.0.0.0", "-mdir=/data"]

  volume:
    image: seaweedfs-block:local
    ports:
      - "8280:8080"       # Volume HTTP
      - "18280:18080"     # Volume gRPC
      - "3260:3260"       # iSCSI target
    command: >
      volume -ip=volume -master=master:9333 -dir=/data
      -block.dir=/data/blocks
      -block.listen=0.0.0.0:3260
      -block.portal=${HOST_IP:-127.0.0.1}:3260,1
```

Key flags:
- `-block.dir`: Directory for `.blk` volume files
- `-block.listen`: iSCSI target listen address (inside container)
- `-block.portal`: iSCSI portal address reported to clients (must be reachable)
