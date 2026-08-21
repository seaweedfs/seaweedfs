package lifecycle

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

const lanceImage = "seaweedfs-lifecycle-lance"

// TestLanceTableLifecycle is the Iceberg test's counterpart. A Lance table is
// declared through the namespace, written a fragment at a time, compacted, its
// superseded versions dropped, and read again - and the read has to answer
// exactly what the write put there. Compaction rewrites fragments the way
// Iceberg compaction rewrites parquet files, and the failure that suite exists
// for is the kind that reads back without an error and answers wrongly.
func TestLanceTableLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if shared == nil {
		t.Skip("no cluster")
	}
	if !testutil.HasDocker() {
		t.Skip("Docker not available")
	}

	env := shared
	bucket := "lifecycle-lance-" + randomSuffix()
	namespace, table := "ml", "events"
	env.createTableBucket(t, bucket, "LANCE")
	buildClientImage(t, lanceImage, "Dockerfile.lance")

	before := env.lance(t, "write", bucket, namespace, table)
	assertSeeded(t, before)
	if before.Fragments != batches {
		t.Fatalf("the client wrote %d fragments, want one per append (%d)", before.Fragments, batches)
	}

	env.maintainLanceTable(t, bucket, namespace, table)

	after := env.lance(t, "verify", bucket, namespace, table)
	assertSameData(t, before, after)
	// Without this the comparison above would pass on a table nothing touched.
	if after.Fragments >= before.Fragments {
		t.Fatalf("maintenance merged nothing: %d fragments before, %d after",
			before.Fragments, after.Fragments)
	}

	env.lance(t, "drop", bucket, namespace, table)
	if env.entryExists(t, fmt.Sprintf("/buckets/%s/%s/%s/", bucket, namespace, table)) {
		t.Fatal("the dropped table's data is still on disk")
	}
}

// maintainLanceTable compacts the table and drops what compaction superseded.
//
// Lance maintenance lives in the Rust worker, so where its toolchain is around
// the handlers themselves do the work against this table. Where it is not, the
// same two lance calls those handlers wrap run in the client container instead.
// The lifecycle is checked either way; only the layer above the format changes.
// WEED_LANCE_MAINTENANCE picks one - CI sets "library", because a cold build of
// the lance crate costs more than the layer it is checking.
func (env *environment) maintainLanceTable(t *testing.T, bucket, namespace, table string) {
	t.Helper()

	if !maintainWithWorker(t) {
		env.lance(t, "maintain", bucket, namespace, table)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "cargo", "test",
		"-p", "weed-lance-worker", "--test", "lifecycle", "--", "--nocapture")
	cmd.Dir = filepath.Join(env.rootDir, "seaweed-worker")
	cmd.Env = append(cmd.Environ(),
		fmt.Sprintf("WEED_LANCE_NAMESPACE=http://127.0.0.1:%d", env.lancePort),
		fmt.Sprintf("WEED_LANCE_TABLE=%s$%s$%s", bucket, namespace, table),
		"AWS_ACCESS_KEY_ID="+accessKey,
		"AWS_SECRET_ACCESS_KEY="+secretKey,
		"AWS_REGION=us-east-1",
		fmt.Sprintf("AWS_ENDPOINT_URL=http://127.0.0.1:%d", env.s3Port),
	)
	out, err := cmd.CombinedOutput()
	t.Logf("lance worker:\n%s", out)
	if err != nil {
		t.Fatalf("the Lance maintenance worker failed: %v", err)
	}
}

// maintainWithWorker says whether to maintain through the Rust worker.
func maintainWithWorker(t *testing.T) bool {
	t.Helper()

	switch os.Getenv("WEED_LANCE_MAINTENANCE") {
	case "library":
		t.Log("maintaining through the lance library, as WEED_LANCE_MAINTENANCE asks")
		return false
	case "worker":
		return true
	}
	if _, err := exec.LookPath("cargo"); err != nil {
		t.Log("cargo is not installed, maintaining through the lance library rather than the worker")
		return false
	}
	return true
}

func (env *environment) lance(t *testing.T, phase, bucket, namespace, table string) tally {
	t.Helper()

	out := env.runClient(t, lanceImage, phase, "/app/lance_lifecycle.py",
		"--namespace-url", env.containerURL(env.lancePort),
		"--s3-endpoint", env.containerURL(env.s3Port),
		"--bucket", bucket,
		"--namespace", namespace,
		"--table", table,
	)
	if phase == "drop" {
		return tally{}
	}
	return decodeTally(t, out)
}
