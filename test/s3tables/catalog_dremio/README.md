# Dremio Iceberg Catalog Integration Tests

This directory contains integration tests for Dremio with SeaweedFS's Iceberg REST Catalog implementation.

## Prerequisites

- Docker (for running Dremio container)
- SeaweedFS built and available as `weed` in your PATH
- Go 1.18+

## Test Files

- `dremio_catalog_test.go` - Core catalog operations and connectivity tests
- `dremio_crud_operations_test.go` - Schema and table CRUD operations
- `dremio_deterministic_location_test.go` - Table location and multi-level namespace tests

## Running Tests Locally

### Quick Start

```bash
cd test/s3tables/catalog_dremio
go test -v -timeout 20m ./...
```

The Dremio tests are currently skipped by default because the harness does not
yet bootstrap a Dremio admin user, create an Iceberg REST source, or poll the
asynchronous SQL API for job results. Enable them only after that setup is
implemented:

```bash
SEAWEEDFS_RUN_DREMIO_TESTS=1 go test -v -timeout 20m ./...
```

### Running Specific Tests

```bash
# Run only catalog connectivity tests
go test -v -run TestDremioIcebergCatalog

# Run only CRUD tests
go test -v -run TestSchemaCRUD
go test -v -run TestTableCRUD

# Run only location tests
go test -v -run TestDeterministicTableLocation
```

### Skipping Integration Tests

The Dremio tests are skipped unless `SEAWEEDFS_RUN_DREMIO_TESTS=1` is set.
Short mode also skips them:

```bash
go test -short ./...
```

## How Tests Work

1. **Environment Setup**: Each test creates a temporary SeaweedFS instance with:
   - Master, Volume, and Filer services
   - S3 API endpoint
   - Iceberg REST Catalog endpoint

2. **Docker Container**: Tests start a Dremio container configured to:
   - Connect to the local SeaweedFS Iceberg REST API
   - Use S3 for data storage
   - Create and manage Iceberg tables

3. **Test Execution**: Tests verify:
   - Basic catalog connectivity
   - Schema creation and listing
   - Table creation, insertion, and querying
   - Multi-level namespace support
   - Table locations and paths

4. **Cleanup**: All resources (containers, ports, temporary files) are cleaned up automatically

## Test Scenarios

### TestDremioIcebergCatalog
- Starts SeaweedFS and Dremio
- Verifies Iceberg REST API connectivity
- Tests basic schema creation

### TestDremioTableOperations
- Creates schemas and tables
- Inserts data
- Queries data with COUNT()

### TestSchemaCRUD
- Tests Create, Read, Delete operations on schemas
- Verifies schema listing

### TestTableCRUD
- Tests Create, Read, Insert, Delete on tables
- Verifies table operations

### TestDeterministicTableLocation
- Tests explicit table location specification
- Verifies data is stored at the correct S3 path

### TestMultiLevelNamespace
- Tests multi-level namespace support (e.g., "analytics.daily")
- Verifies queries work correctly with dot-separated namespaces

## Configuration

Tests use default configuration:
- S3 Access Key: `AKIAIOSFODNN7EXAMPLE`
- S3 Secret Key: `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`
- Region: `us-west-2`
- Warehouse Bucket: `iceberg-tables`

## Troubleshooting

### Docker Connection Issues
- Ensure Docker is running: `docker version`
- Check host-gateway routing: `docker run --add-host host.docker.internal:host-gateway`

### Port Conflicts
- Tests allocate random ports to avoid conflicts
- If ports are exhausted, close other services

### Dremio Container Timeout
- First startup may take 60-120 seconds
- Check Dremio logs: `docker logs <container-id>`

### SQL Execution Failures
- Verify Dremio SQL endpoint is accessible
- Check SeaweedFS Iceberg REST API is running
- Review error messages in container logs

## CI/CD Integration

These tests are not run by the pull request workflows yet. Re-enable CI only
after the Dremio bootstrap and SQL polling pieces are implemented, otherwise CI
only reports skipped tests.

## Known Limitations

- SQL output parsing depends on Dremio's output format
- Dremio REST API requests require admin bootstrap and source configuration
- Some Dremio-specific features may not be fully tested
- Multi-container networking uses Docker's `host.docker.internal`

## Related Tests

- `test/s3tables/catalog_trino/` - Similar tests for Trino
- `test/s3tables/catalog_spark/` - Similar tests for Spark
- `test/s3tables/catalog/` - Base Iceberg catalog tests
