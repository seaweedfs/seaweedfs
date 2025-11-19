# ARM64 Support for FoundationDB Integration

This document explains how to run FoundationDB integration tests on ARM64 systems (Apple Silicon M1/M2/M3 Macs).

## Problem

The official FoundationDB Docker images (`foundationdb/foundationdb:7.1.61`) are only available for `linux/amd64` architecture. When running on ARM64 systems, you'll encounter "Illegal instruction" errors. Apple now publishes official ARM64 Debian packages (starting with 7.4.5), which this repo downloads directly for native workflows.

## Solutions

We provide **three different approaches** to run FoundationDB on ARM64:

### 1. 🚀 ARM64 Native (Recommended for Development)

**Pros:** Native performance, no emulation overhead  
**Cons:** Requires downloading ~100MB of FoundationDB packages on first run

```bash
# Build and run ARM64-native FoundationDB from source
make setup-arm64
make test-arm64
```

This approach:
- Downloads the official FoundationDB 7.4.5 ARM64 packages
- Takes ~2-3 minutes on first run (no source compilation)
- Provides native performance
- Uses `docker-compose.arm64.yml`

### 2. 🐳 x86 Emulation (Quick Setup)

**Pros:** Fast setup, uses official images  
**Cons:** Slower runtime performance due to emulation

```bash
# Run x86 images with Docker emulation
make setup-emulated
make test-emulated
```

This approach:
- Uses Docker's x86 emulation
- Quick setup with official images  
- May have performance overhead
- Uses standard `docker-compose.yml` with platform specification

### 3. 📝 Mock Testing (Fastest)

**Pros:** No dependencies, always works, fast execution  
**Cons:** Doesn't test real FoundationDB integration

```bash
# Run mock tests (no FoundationDB cluster needed)
make test-mock
make test-reliable
```

## Files Overview

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Standard setup with platform specification |
| `docker-compose.arm64.yml` | ARM64-native setup with source builds |
| `Dockerfile.fdb-arm64` | Multi-stage build for ARM64 FoundationDB |
| `README.ARM64.md` | This documentation |

## Performance Comparison

| Approach | Setup Time | Runtime Performance | Compatibility |
|----------|------------|-------------------|---------------|
| ARM64 Native | 2-3 min | ⭐⭐⭐⭐⭐ | ARM64 only |
| x86 Emulation | 2-3 min | ⭐⭐⭐ | ARM64 + x86 |
| Mock Testing | < 1 min | ⭐⭐⭐⭐⭐ | Any platform |

## Quick Start Commands

```bash
# For ARM64 Mac users - choose your approach:

# Option 1: ARM64 native (best performance)
make clean && make setup-arm64

# Option 2: x86 emulation (faster setup) 
make clean && make setup-emulated

# Option 3: Mock testing (no FDB needed)
make test-mock

# Clean up everything
make clean
```

## Troubleshooting

### Build Timeouts
If ARM64 builds timeout, increase Docker build timeout:
```bash
export DOCKER_BUILDKIT=1
export BUILDKIT_PROGRESS=plain
make setup-arm64
```

### Memory Issues
ARM64 builds require significant memory:
- Increase Docker memory limit to 8GB+
- Close other applications during build

### Platform Detection
Verify your platform:
```bash
docker info | grep -i arch
uname -m  # Should show arm64
```

## CI/CD Recommendations

- **Development**: Use `make test-mock` for fast feedback
- **ARM64 CI**: Use `make setup-arm64` 
- **x86 CI**: Use `make setup` (standard)
- **Multi-platform CI**: Run both depending on runner architecture

## Architecture Details

The ARM64 solution now uses the official FoundationDB 7.4.5 aarch64 packages:

1. **Builder Stage**: Downloads prebuilt FoundationDB client libraries
   - Uses Debian-based Go image for compiling SeaweedFS
   - Verifies SHA256 checksums before installing the deb package

2. **Runtime Stage**: Copies the already-installed artifacts
   - SeaweedFS runtime layers reuse the validated libraries
   - FoundationDB server containers install the prebuilt server + client packages with checksum verification

This keeps the setup time short while preserving native ARM64 performance and strong supply-chain guarantees.
