package catalog

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestLanceNamespaceRealClient drives the namespace with the Lance client rather
// than hand-built HTTP. The Go tests beside this one cover the catalog surface;
// what this adds is that the location and storage_options the namespace vends
// are actually enough to write and read a dataset, which needs the S3 layout
// guard, the endpoint and the credentials all to be right at once.
//
// To run manually:
//
//	cd test/s3tables/catalog
//	docker build -t lance-namespace-test -f Dockerfile.lance .
func TestLanceNamespaceRealClient(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := sharedEnv
	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Lance client integration test")
	}

	bucketName := "lance-client-test-" + randomSuffix()
	createTableBucket(t, env, bucketName)

	testDir := filepath.Join(env.seaweedDir, "test", "s3tables", "catalog")

	buildCmd := exec.Command("docker", "build", "-t", "lance-namespace-test", "-f", "Dockerfile.lance", ".")
	buildCmd.Dir = testDir
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build test image: %v\n%s", err, string(out))
	}

	namespaceURL := fmt.Sprintf("http://host.docker.internal:%d", env.lancePort)
	s3Endpoint := fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)

	cmd := exec.Command("docker", "run", "--rm",
		"--add-host", "host.docker.internal:host-gateway",
		"-v", fmt.Sprintf("%s:/app:ro", testDir),
		"lance-namespace-test",
		"python3", "/app/test_lance_namespace.py",
		"--namespace-url", namespaceURL,
		"--s3-endpoint", s3Endpoint,
		"--bucket", bucketName,
	)
	cmd.Dir = testDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	t.Logf("Running Lance client test against %s", namespaceURL)
	if err := cmd.Run(); err != nil {
		t.Errorf("Lance client test failed: %v", err)
	}
}
