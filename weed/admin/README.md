# SeaweedFS Admin Component

A modern web-based administration interface for SeaweedFS clusters built with Go, Gin, Templ, and Bootstrap.

## Features

- **Dashboard**: Real-time cluster status and metrics
- **Master Management**: Monitor master nodes and leadership status
- **Volume Server Management**: View volume servers, capacity, and health
- **Object Store Bucket Management**: Create, delete, and manage Object Store buckets with web interface
- **S3 Tables Management**: Manage table buckets, namespaces, tables, tags, and policies via the admin UI
- **System Health**: Overall cluster health monitoring
- **Responsive Design**: Bootstrap-based UI that works on all devices
- **Authentication**: Optional user authentication with sessions
- **TLS Support**: HTTPS support for production deployments

## Building

### Using the Admin Makefile

The admin component has its own Makefile for development and building:

```bash
# Navigate to admin directory
cd weed/admin

# View all available targets
make help

# Generate templates and build
make build

# Development mode with template watching
make dev

# Run the admin server
make run

# Clean build artifacts
make clean
```

### Using the Root Makefile

The root SeaweedFS Makefile automatically integrates the admin component:

```bash
# From the root directory
make install          # Builds weed with admin component
make full_install     # Full build with all tags
make test            # Runs tests including admin component

# Admin-specific targets from root
make admin-generate  # Generate admin templates
make admin-build     # Build admin component
make admin-run       # Run admin server
make admin-dev       # Development mode
make admin-clean     # Clean admin artifacts
```

### Manual Building

If you prefer to build manually:

```bash
# Install templ compiler
go install github.com/a-h/templ/cmd/templ@latest

# Generate templates
templ generate

# Build the main weed binary
cd ../../../
go build -o weed ./weed
```

## Development

### Template Development

The admin interface uses [Templ](https://templ.guide/) for type-safe HTML templates:

```bash
# Watch for template changes and auto-regenerate
make watch

# Or manually generate templates
make generate

# Format templates
make fmt
```

### File Structure

```
weed/admin/
├── Makefile              # Admin-specific build tasks
├── README.md             # This file
├── admin.go              # Main application entry point
├── dash/                 # Server and handler logic
│   ├── admin_server.go   # HTTP server setup
│   ├── handler_admin.go  # Admin dashboard handlers
│   ├── handler_auth.go   # Authentication handlers
│   └── middleware.go     # HTTP middleware
├── static/               # Static assets
│   ├── css/admin.css     # Admin-specific styles
│   └── js/admin.js       # Admin-specific JavaScript
└── view/                 # Templates
    ├── app/              # Application templates
    │   ├── admin.templ   # Main dashboard template
    │   ├── s3_buckets.templ # Object Store bucket management template
    │   ├── s3tables_*.templ # S3 Tables management templates
    │   └── *_templ.go    # Generated Go code
    └── layout/           # Layout templates
        ├── layout.templ  # Base layout template
        └── layout_templ.go # Generated Go code
```

### Object Store Management

The admin interface includes Object Store and S3 Tables management capabilities:

- Create/delete Object Store buckets and adjust quotas or ownership.
- Manage S3 Tables buckets, namespaces, and tables.
- Update S3 Tables policies and tags via the UI and API endpoints.

## Usage

### Basic Usage

```bash
# Start admin interface on default port (23646)
weed admin

# Start with custom configuration
weed admin -port=8080 -masters="master1:9333,master2:9333"

# Start with authentication
weed admin -adminUser=admin -adminPassword=secret123

# Start with HTTPS
weed admin -port=443 -tlsCert=/path/to/cert.pem -tlsKey=/path/to/key.pem
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `-port` | 23646 | Admin server port |
| `-masters` | localhost:9333 | Comma-separated master servers |
| `-adminUser` | admin | Admin username (if auth enabled) |
| `-adminPassword` | "" | Admin password (empty = no auth) |
| `-tlsCert` | "" | Path to TLS certificate |
| `-tlsKey` | "" | Path to TLS private key |

### Docker Usage

```bash
# Build Docker image with admin component
make docker-build

# Run with Docker
docker run -p 23646:23646 seaweedfs/seaweedfs:latest admin -masters=host.docker.internal:9333
```

## Development Workflow

### Quick Start

```bash
# Clone and setup
git clone <seaweedfs-repo>
cd seaweedfs/weed/admin

# Install dependencies and build
make install-deps
make build

# Start development server
make dev
```

### Making Changes

1. **Template Changes**: Edit `.templ` files in `view/`
   - Templates auto-regenerate in development mode
   - Use `make generate` to manually regenerate

2. **Go Code Changes**: Edit `.go` files
   - Restart the server to see changes
   - Use `make build` to rebuild

3. **Static Assets**: Edit files in `static/`
   - Changes are served immediately

### Testing

```bash
# Run admin component tests
make test

# Run from root directory
make admin-test

# Lint code
make lint

# Format code
make fmt
```

## Production Deployment

### Security Considerations

1. **Authentication**: Always set `adminPassword` in production
2. **HTTPS**: Use TLS certificates for encrypted connections
3. **Firewall**: Restrict admin interface access to authorized networks

### Example Production Setup

```bash
# Production deployment with security
weed admin \
  -port=443 \
  -masters="master1:9333,master2:9333,master3:9333" \
  -adminUser=admin \
  -adminPassword=your-secure-password \
  -tlsCert=/etc/ssl/certs/admin.crt \
  -tlsKey=/etc/ssl/private/admin.key
```

### Monitoring

The admin interface provides endpoints for monitoring:

- `GET /health` - Health check endpoint
- `GET /metrics` - Prometheus metrics (if enabled)
- `GET /api/status` - JSON status information

## Troubleshooting

### Common Issues

1. **Templates not found**: Run `make generate` to create template files
2. **Build errors**: Ensure `templ` is installed with `make install-templ`
3. **Static files not loading**: Check that `static/` directory exists and has proper files
4. **Connection errors**: Verify master and filer addresses are correct

### Debug Mode

```bash
# Enable debug logging
weed -v=2 admin

# Check generated templates
ls -la view/app/*_templ.go view/layout/*_templ.go
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `make test`
5. Format code: `make fmt`
6. Submit a pull request

## Architecture

The admin component follows a clean architecture:

- **Presentation Layer**: Templ templates + Bootstrap CSS
- **HTTP Layer**: Gin router with middleware
- **Business Logic**: Handler functions in `dash/` package
- **Data Layer**: Communicates with SeaweedFS masters and filers

This separation makes the code maintainable and testable. 
