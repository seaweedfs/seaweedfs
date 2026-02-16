// Package catalog provides integration tests for the Iceberg REST Catalog API.
// This file adds PyIceberg-based compatibility tests using Docker.
package catalog

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestPyIcebergRestCatalog tests the Iceberg REST Catalog using PyIceberg client in Docker.
// This provides a more comprehensive test than DuckDB as PyIceberg fully exercises the REST API.
//
// Prerequisites:
//   - Docker must be available
//   - SeaweedFS must be running with Iceberg REST enabled
//
// To run manually:
//
//	cd test/s3tables/catalog
//	docker compose -f docker-compose.test.yaml up --build
func TestPyIcebergRestCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping PyIceberg integration test")
	}

	env.StartSeaweedFS(t)

	// Create the test bucket first
	bucketName := "pyiceberg-compat-test"
	createTableBucket(t, env, bucketName)

	// Build the test working directory path
	testDir := filepath.Join(env.seaweedDir, "test", "s3tables", "catalog")

	// Run PyIceberg test using Docker
	catalogURL := fmt.Sprintf("http://host.docker.internal:%d", env.icebergPort)
	s3Endpoint := fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)
	warehouse := fmt.Sprintf("s3://%s/", bucketName)

	// Build the test image first for faster repeated runs
	buildCmd := exec.Command("docker", "build", "-t", "iceberg-rest-test", "-f", "Dockerfile.pyiceberg", ".")
	buildCmd.Dir = testDir
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build test image: %v\n%s", err, string(out))
	}

	cmd := exec.Command("docker", "run", "--rm",
		"--add-host", "host.docker.internal:host-gateway",
		"-e", fmt.Sprintf("AWS_ENDPOINT_URL=%s", s3Endpoint),
		"-v", fmt.Sprintf("%s:/app:ro", testDir),
		"iceberg-rest-test",
		"python3", "/app/test_rest_catalog.py",
		"--catalog-url", catalogURL,
		"--warehouse", warehouse,
		"--prefix", bucketName,
	)
	cmd.Dir = testDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	t.Logf("Running PyIceberg REST catalog test...")
	t.Logf("  Catalog URL: %s", catalogURL)
	t.Logf("  Warehouse: %s", warehouse)

	if err := cmd.Run(); err != nil {
		t.Errorf("PyIceberg test failed: %v", err)
	}
}
