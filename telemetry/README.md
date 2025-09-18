# SeaweedFS Telemetry System

A privacy-respecting telemetry system for SeaweedFS that collects cluster-level usage statistics and provides visualization through Prometheus and Grafana.

## Features

- **Privacy-First Design**: Uses in-memory cluster IDs (regenerated on restart), no personal data collection
- **Prometheus Integration**: Native Prometheus metrics for monitoring and alerting
- **Grafana Dashboards**: Pre-built dashboards for data visualization
- **Protocol Buffers**: Efficient binary data transmission for optimal performance
- **Opt-in Only**: Disabled by default, requires explicit configuration
- **Docker Compose**: Complete monitoring stack deployment
- **Automatic Cleanup**: Configurable data retention policies

## Architecture

```
SeaweedFS Cluster → Telemetry Client → Telemetry Server → Prometheus → Grafana
                       (protobuf)         (metrics)      (queries)
```

## Data Transmission

The telemetry system uses **Protocol Buffers exclusively** for efficient binary data transmission:

- **Compact Format**: 30-50% smaller than JSON
- **Fast Serialization**: Better performance than text-based formats
- **Type Safety**: Strong typing with generated Go structs
- **Schema Evolution**: Built-in versioning support

### Protobuf Schema

```protobuf
message TelemetryData {
  string cluster_id = 1;           // In-memory generated UUID
  string version = 2;              // SeaweedFS version
  string os = 3;                   // Operating system
  // Field 4 reserved (was features)
  // Field 5 reserved (was deployment)
  int32 volume_server_count = 6;   // Number of volume servers
  uint64 total_disk_bytes = 7;     // Total disk usage
  int32 total_volume_count = 8;    // Total volume count
  int32 filer_count = 9;           // Number of filer servers
  int32 broker_count = 10;         // Number of broker servers
  int64 timestamp = 11;            // Collection timestamp
}
```

## Privacy Approach

- **No Personal Data**: No hostnames, IP addresses, or user information
- **In-Memory IDs**: Cluster IDs are generated in-memory and change on restart
- **Aggregated Data**: Only cluster-level statistics, no individual file/user data
- **Opt-in Only**: Telemetry is disabled by default
- **Transparent**: Open source implementation, clear data collection policy

## Collected Data

| Field | Description | Example |
|-------|-------------|---------|
| `cluster_id` | In-memory UUID (changes on restart) | `a1b2c3d4-...` |
| `version` | SeaweedFS version | `3.45` |
| `os` | Operating system and architecture | `linux/amd64` |
| `volume_server_count` | Number of volume servers | `5` |
| `total_disk_bytes` | Total disk usage across cluster | `1073741824` |
| `total_volume_count` | Total number of volumes | `120` |
| `filer_count` | Number of filer servers | `2` |
| `broker_count` | Number of broker servers | `1` |
| `timestamp` | When data was collected | `1640995200` |

## Quick Start

### 1. Deploy Telemetry Server

```bash
# Clone and start the complete monitoring stack
git clone https://github.com/seaweedfs/seaweedfs.git
cd seaweedfs
docker compose -f telemetry/docker-compose.yml up -d

# Or run the server directly
cd telemetry/server
go run . -port=8080 -dashboard=true
```

### 2. Configure SeaweedFS

```bash
# Enable telemetry in SeaweedFS master (uses default telemetry.seaweedfs.com)
weed master -telemetry=true

# Or in server mode
weed server -telemetry=true

# Or specify custom telemetry server
weed master -telemetry=true -telemetry.url=http://localhost:8080/api/collect
```

### 3. Access Dashboards

- **Telemetry Server**: http://localhost:8080
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

## Configuration

### SeaweedFS Master/Server

```bash
# Enable telemetry
-telemetry=true

# Set custom telemetry server URL (optional, defaults to telemetry.seaweedfs.com)
-telemetry.url=http://your-telemetry-server:8080/api/collect
```

### Telemetry Server

```bash
# Server configuration
-port=8080                    # Server port
-dashboard=true               # Enable built-in dashboard
-cleanup=24h                  # Cleanup interval
-max-age=720h                 # Maximum data retention (30 days)

# Example
./telemetry-server -port=8080 -dashboard=true -cleanup=24h -max-age=720h
```

## Prometheus Metrics

The telemetry server exposes these Prometheus metrics:

### Cluster Metrics
- `seaweedfs_telemetry_total_clusters`: Total unique clusters (30 days)
- `seaweedfs_telemetry_active_clusters`: Active clusters (7 days)

### Per-Cluster Metrics
- `seaweedfs_telemetry_volume_servers{cluster_id, version, os}`: Volume servers per cluster
- `seaweedfs_telemetry_disk_bytes{cluster_id, version, os}`: Disk usage per cluster  
- `seaweedfs_telemetry_volume_count{cluster_id, version, os}`: Volume count per cluster
- `seaweedfs_telemetry_filer_count{cluster_id, version, os}`: Filer servers per cluster
- `seaweedfs_telemetry_broker_count{cluster_id, version, os}`: Broker servers per cluster
- `seaweedfs_telemetry_cluster_info{cluster_id, version, os}`: Cluster metadata

### Server Metrics
- `seaweedfs_telemetry_reports_received_total`: Total telemetry reports received

## API Endpoints

### Data Collection
```bash
# Submit telemetry data (protobuf only)
POST /api/collect
Content-Type: application/x-protobuf
[TelemetryRequest protobuf data]
```

### Statistics (JSON for dashboard/debugging)
```bash
# Get aggregated statistics
GET /api/stats

# Get recent cluster instances
GET /api/instances?limit=100

# Get metrics over time
GET /api/metrics?days=30
```

### Monitoring
```bash
# Prometheus metrics
GET /metrics
```

## Docker Deployment

### Complete Stack (Recommended)

```yaml
# docker-compose.yml
version: '3.8'
services:
  telemetry-server:
    build:
      context: ../
      dockerfile: telemetry/server/Dockerfile
    ports:
      - "8080:8080"
    command: ["-port=8080", "-dashboard=true", "-cleanup=24h"]
    
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./grafana-provisioning:/etc/grafana/provisioning
      - ./grafana-dashboard.json:/var/lib/grafana/dashboards/seaweedfs.json
```

```bash
# Deploy the stack
docker compose -f telemetry/docker-compose.yml up -d

# Scale telemetry server if needed
docker compose -f telemetry/docker-compose.yml up -d --scale telemetry-server=3
```

### Server Only

```bash
# Build and run telemetry server (build from repo root to include all sources)
docker build -t seaweedfs-telemetry -f telemetry/server/Dockerfile .
docker run -p 8080:8080 seaweedfs-telemetry -port=8080 -dashboard=true
```

## Development

### Protocol Buffer Development

```bash
# Generate protobuf code
cd telemetry
protoc --go_out=. --go_opt=paths=source_relative proto/telemetry.proto

# The generated code is already included in the repository
```

### Build from Source

```bash
# Build telemetry server
cd telemetry/server
go build -o telemetry-server .

# Build SeaweedFS with telemetry support
cd ../..
go build -o weed ./weed
```

### Testing

```bash
# Test telemetry server
cd telemetry/server
go test ./...

# Test protobuf communication (requires protobuf tools)
# See telemetry client code for examples
```

## Grafana Dashboard

The included Grafana dashboard provides:

- **Overview**: Total and active clusters, version distribution
- **Resource Usage**: Volume servers and disk usage over time  
- **Infrastructure**: Operating system distribution and server counts
- **Growth Trends**: Historical growth patterns

### Custom Queries

```promql
# Total active clusters
seaweedfs_telemetry_active_clusters

# Disk usage by version
sum by (version) (seaweedfs_telemetry_disk_bytes)

# Volume servers by operating system
sum by (os) (seaweedfs_telemetry_volume_servers)

# Filer servers by version
sum by (version) (seaweedfs_telemetry_filer_count)

# Broker servers across all clusters
sum(seaweedfs_telemetry_broker_count)

# Growth rate (weekly)
increase(seaweedfs_telemetry_total_clusters[7d])
```

## Security Considerations

- **Network Security**: Use HTTPS in production environments
- **Access Control**: Implement authentication for Grafana and Prometheus
- **Data Retention**: Configure appropriate retention policies
- **Monitoring**: Monitor the telemetry infrastructure itself

## Troubleshooting

### Common Issues

**SeaweedFS not sending data:**
```bash
# Check telemetry configuration
weed master -h | grep telemetry

# Verify connectivity
curl -v http://your-telemetry-server:8080/api/collect
```

**Server not receiving data:**
```bash
# Check server logs
docker-compose logs telemetry-server

# Verify metrics endpoint
curl http://localhost:8080/metrics
```

**Prometheus not scraping:**
```bash
# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Verify configuration
docker-compose logs prometheus
```

### Debugging

```bash
# Enable verbose logging in SeaweedFS
weed master -v=2 -telemetry=true

# Check telemetry server metrics
curl http://localhost:8080/metrics | grep seaweedfs_telemetry

# Test data flow
curl http://localhost:8080/api/stats
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This telemetry system is part of SeaweedFS and follows the same Apache 2.0 license. 