# SeaweedFS Dashboard

A modern, web-based administration interface for SeaweedFS clusters built with **Gin + Templ + HTMX** architecture pattern following the reference design from `/Users/chrislu/dev/parker/ranger/cmd/dashboard/`.

![Dashboard Screenshot](docs/dashboard-preview.png)

## Features

### 🚀 **Core Capabilities**

- **Cluster Management**: Real-time topology visualization and health monitoring
- **Volume Operations**: Create, manage, and monitor volumes across the cluster
- **File Browser**: Upload, download, and manage files through web interface
- **Metrics Dashboard**: Performance monitoring with real-time charts and analytics
- **System Maintenance**: Automated cleanup, compaction, and repair operations
- **Configuration Management**: Cluster settings and operational parameters

### 🎨 **Modern UI/UX**

- **Responsive Design**: Works seamlessly on desktop, tablet, and mobile devices
- **Real-time Updates**: HTMX-powered dynamic content without page refreshes
- **Interactive Charts**: Live metrics visualization with Chart.js
- **Bootstrap 5**: Modern, accessible, and mobile-first design
- **Font Awesome Icons**: Comprehensive icon library for better visual experience

### 🔒 **Security Features**

- **Optional Authentication**: Configurable username/password protection
- **TLS/HTTPS Support**: Secure communication with certificate-based encryption
- **Session Management**: Secure session handling with configurable timeouts
- **CSRF Protection**: Cross-site request forgery prevention
- **Input Validation**: Comprehensive data sanitization and validation

## Quick Start

### Prerequisites

- SeaweedFS cluster with master and filer running
- Go 1.21+ for building from source
- Modern web browser (Chrome, Firefox, Safari, Edge)

### Installation

#### Option 1: Build from Source

```bash
# Clone SeaweedFS repository
git clone https://github.com/seaweedfs/seaweedfs.git
cd seaweedfs

# Build SeaweedFS with dashboard
make

# Run dashboard
./weed dashboard -port=9999 \
                 -masters="localhost:9333" \
                 -filer="localhost:8888" \
                 -adminPassword="your-secure-password"
```

#### Option 2: Docker

```bash
# Run dashboard container
docker run -d \
  --name seaweedfs-dashboard \
  -p 9999:9999 \
  seaweedfs/seaweedfs:latest \
  dashboard \
  -masters="master:9333" \
  -filer="filer:8888" \
  -adminPassword="your-secure-password"
```

#### Option 3: Docker Compose

```yaml
version: '3.8'
services:
  dashboard:
    image: seaweedfs/seaweedfs:latest
    command: >
      dashboard
      -port=9999
      -masters=master1:9333,master2:9333
      -filer=filer:8888
      -adminUser=admin
      -adminPassword=secretpassword
      -tlsCert=/certs/dashboard.crt
      -tlsKey=/certs/dashboard.key
    ports:
      - "9999:9999"
    volumes:
      - ./certs:/certs:ro
    environment:
      - DASHBOARD_SESSION_SECRET=your-random-secret-key
```

### First Access

1. Open your browser and navigate to `http://localhost:9999`
2. If authentication is enabled, login with your credentials
3. Explore the cluster topology and system status
4. Access different sections via the sidebar navigation

## Configuration

### Command Line Options

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `9999` | Dashboard server port |
| `-masters` | `localhost:9333` | Comma-separated master server addresses |
| `-filer` | `localhost:8888` | Filer server address |
| `-adminUser` | `admin` | Dashboard admin username |
| `-adminPassword` | `""` | Dashboard admin password (empty = no auth) |
| `-tlsCert` | `""` | Path to TLS certificate file |
| `-tlsKey` | `""` | Path to TLS private key file |
| `-sessionSecret` | `auto-generated` | Session encryption secret |
| `-metricsServer` | `""` | Metrics server for monitoring integration |

### Environment Variables

```bash
export DASHBOARD_PORT=9999
export DASHBOARD_MASTERS="master1:9333,master2:9333"
export DASHBOARD_FILER="filer:8888"
export DASHBOARD_ADMIN_USER="admin"
export DASHBOARD_ADMIN_PASSWORD="secretpassword"
export DASHBOARD_TLS_CERT="/path/to/cert.pem"
export DASHBOARD_TLS_KEY="/path/to/key.pem"
export DASHBOARD_SESSION_SECRET="your-random-secret"
```

### Security Configuration

#### Enable HTTPS

```bash
# Generate self-signed certificate (development only)
openssl req -x509 -newkey rsa:4096 -keyout dashboard.key -out dashboard.crt -days 365 -nodes

# Run dashboard with TLS
./weed dashboard -tlsCert=dashboard.crt -tlsKey=dashboard.key
```

#### Production Security Checklist

- ✅ Use strong `adminPassword` (minimum 12 characters)
- ✅ Enable TLS with valid certificates
- ✅ Set random `sessionSecret` (32+ characters)
- ✅ Configure firewall rules to restrict access
- ✅ Use reverse proxy (nginx/Apache) for additional security
- ✅ Enable audit logging
- ✅ Regular security updates

## Architecture

### Technology Stack

```
┌─────────────────────────────────────────────────────────┐
│                    Frontend Layer                       │
├─────────────────────────────────────────────────────────┤
│ Bootstrap 5 + Font Awesome + HTMX + Custom CSS/JS     │
├─────────────────────────────────────────────────────────┤
│                  Template Layer                         │
├─────────────────────────────────────────────────────────┤
│ Templ (Type-safe Go templates)                        │
├─────────────────────────────────────────────────────────┤
│                   Web Framework                         │
├─────────────────────────────────────────────────────────┤
│ Gin (Go HTTP Router & Middleware)                     │
├─────────────────────────────────────────────────────────┤
│                   Business Logic                        │
├─────────────────────────────────────────────────────────┤
│ Dashboard Server + Handlers + Services                │
├─────────────────────────────────────────────────────────┤
│                   Data Sources                          │
├─────────────────────────────────────────────────────────┤
│ SeaweedFS Master + Filer + Volume Servers (gRPC)      │
└─────────────────────────────────────────────────────────┘
```

### Component Structure

```
weed/cmd/dashboard/
├── dashboard.go                 # Main entry point & routing
├── dash/                        # Core dashboard logic
│   ├── dashboard_server.go      # Server struct & cluster ops
│   ├── handler_auth.go          # Authentication
│   ├── handler_dashboard.go     # Main dashboard
│   ├── handler_cluster.go       # Cluster management
│   ├── handler_volumes.go       # Volume operations
│   ├── handler_filer.go         # File management
│   ├── handler_metrics.go       # Monitoring & metrics
│   ├── handler_config.go        # Configuration
│   ├── handler_maintenance.go   # System maintenance
│   └── middleware.go            # Authentication middleware
├── view/                        # UI templates
│   ├── layout/
│   │   └── layout.templ         # Base layout & login
│   └── app/
│       ├── dashboard.templ      # Dashboard page
│       ├── cluster.templ        # Cluster topology
│       ├── volumes.templ        # Volume management
│       ├── filer.templ          # File browser
│       ├── metrics.templ        # Monitoring
│       └── template_helpers.go  # Utility functions
├── static/                      # Static assets
│   ├── css/
│   │   └── dashboard.css        # Custom styles
│   ├── js/
│   │   └── dashboard.js         # Interactive features
│   └── images/
│       └── favicon.ico          # Site icon
└── docs/                        # Documentation
    └── dashboard-preview.png    # Screenshot
```

## API Reference

### Authentication Endpoints

```http
GET  /login                     # Show login form
POST /login                     # Process login
GET  /logout                    # Logout user
GET  /health                    # Health check
```

### Dashboard Endpoints

```http
GET  /dashboard                 # Main dashboard
GET  /overview                  # Cluster overview (JSON)
```

### Cluster Management

```http
GET  /cluster                   # Cluster topology page
GET  /cluster/topology          # Topology data (JSON)
GET  /cluster/status            # Cluster status (JSON)
POST /cluster/grow              # Grow volumes
POST /cluster/vacuum            # Vacuum operation
POST /cluster/rebalance         # Rebalance cluster
```

### Volume Management

```http
GET    /volumes                 # Volumes list page
GET    /volumes/list            # Volumes data (JSON)
GET    /volumes/details/:id     # Volume details (JSON)
POST   /volumes/create          # Create new volume
DELETE /volumes/delete/:id      # Delete volume
POST   /volumes/replicate/:id   # Replicate volume
```

### File Management

```http
GET    /filer                   # File browser page
GET    /filer/browser           # File browser interface
GET    /filer/browser/api/*path # File operations API
POST   /filer/upload            # Upload files
DELETE /filer/delete            # Delete files
POST   /filer/mkdir             # Create directory
```

### Monitoring & Metrics

```http
GET /metrics                    # Metrics dashboard
GET /metrics/data               # Metrics data (JSON)
GET /metrics/realtime           # Real-time metrics (JSON)
GET /metrics/performance        # Performance metrics (JSON)
GET /metrics/storage            # Storage metrics (JSON)
```

### System Management

```http
GET  /config                    # Configuration page
GET  /config/current            # Current config (JSON)
POST /config/update             # Update configuration
GET  /config/backup             # Backup configuration

GET  /logs                      # Logs viewer
GET  /logs/:type                # Specific log type
GET  /logs/download/:type       # Download logs

GET  /maintenance               # Maintenance page
POST /maintenance/gc            # Garbage collection
POST /maintenance/compact       # Volume compaction
POST /maintenance/fix           # Fix volumes
GET  /maintenance/status        # Maintenance status (JSON)
```

## Customization

### Custom Themes

```css
/* static/css/custom-theme.css */
:root {
  --primary-color: #007bff;
  --secondary-color: #6c757d;
  --success-color: #28a745;
  --warning-color: #ffc107;
  --danger-color: #dc3545;
}

.navbar-brand {
  color: var(--primary-color) !important;
}
```

### Custom JavaScript

```javascript
// static/js/custom.js
document.addEventListener('DOMContentLoaded', function() {
  // Add custom dashboard functionality
  setupCustomCharts();
  enableAdvancedFeatures();
});

function setupCustomCharts() {
  // Custom chart implementations
}
```

### Environment-specific Configuration

```bash
# Development
./weed dashboard -port=9999 -adminPassword="" # No auth for dev

# Staging
./weed dashboard -port=9999 -adminPassword="staging-pass" -tlsCert=staging.crt

# Production
./weed dashboard -port=443 -adminPassword="strong-prod-pass" -tlsCert=prod.crt
```

## Troubleshooting

### Common Issues

#### Dashboard Won't Start

```bash
# Check if port is already in use
netstat -tulpn | grep :9999

# Check master connectivity
curl http://localhost:9333/dir/status

# Check filer connectivity
curl http://localhost:8888/
```

#### Authentication Issues

```bash
# Reset session secret
./weed dashboard -sessionSecret="new-random-secret"

# Disable authentication temporarily
./weed dashboard -adminPassword=""
```

#### TLS Certificate Issues

```bash
# Verify certificate
openssl x509 -in dashboard.crt -text -noout

# Test TLS connection
openssl s_client -connect localhost:9999
```

### Debug Mode

```bash
# Enable debug logging
./weed dashboard -v=4

# Enable Gin debug mode
export GIN_MODE=debug
./weed dashboard
```

### Performance Tuning

```bash
# Increase cache duration
export DASHBOARD_CACHE_TTL=60s

# Adjust concurrent connections
export DASHBOARD_MAX_CONNECTIONS=1000

# Enable compression
export DASHBOARD_ENABLE_GZIP=true
```

## Development

### Prerequisites

- Go 1.21+
- Node.js 18+ (for frontend tooling)
- Templ CLI tool (`go install github.com/a-h/templ/cmd/templ@latest`)

### Development Setup

```bash
# Clone repository
git clone https://github.com/seaweedfs/seaweedfs.git
cd seaweedfs/weed/cmd/dashboard

# Install dependencies
go mod download

# Generate templates
templ generate

# Run in development mode
go run dashboard.go -port=9999 -adminPassword="" -v=2
```

### Building

```bash
# Build for current platform
go build -o dashboard dashboard.go

# Cross-compile for Linux
GOOS=linux GOARCH=amd64 go build -o dashboard-linux dashboard.go

# Build with version info
go build -ldflags "-X main.version=v1.0.0 -X main.buildTime=$(date -u +%Y%m%d-%H%M%S)" dashboard.go
```

### Testing

```bash
# Run unit tests
go test ./dash/...

# Run integration tests
go test -tags=integration ./...

# Run with coverage
go test -cover ./dash/...

# Benchmark tests
go test -bench=. ./dash/...
```

## Contributing

### Code Style

- Follow Go conventions and gofmt formatting
- Use meaningful variable and function names
- Add comments for exported functions and types
- Write tests for new functionality
- Update documentation for user-facing changes

### Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes and add tests
4. Commit your changes (`git commit -m 'Add amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../../LICENSE) file for details.

## Support

- **Documentation**: [SeaweedFS Wiki](https://github.com/seaweedfs/seaweedfs/wiki)
- **Issues**: [GitHub Issues](https://github.com/seaweedfs/seaweedfs/issues)
- **Community**: [SeaweedFS Discussions](https://github.com/seaweedfs/seaweedfs/discussions)
- **Slack**: [SeaweedFS Slack Channel](https://join.slack.com/t/seaweedfs/shared_invite/zt-1234567890-abcdefghijk)

---

**Made with ❤️ by the SeaweedFS Community** 