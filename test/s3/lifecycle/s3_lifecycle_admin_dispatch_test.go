package lifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// Drives the real admin->worker dispatch path via the run endpoint and
// asserts the dispatch reaches the filer (no dial error) and deletes
// the backdated object. The Makefile pins filer.port.grpc off the
// FILER_PORT+10000 convention so a raw-address forwarding regression
// resurfaces here.
func TestLifecycleAdminDispatchSucceedsWithCustomFilerGrpcPort(t *testing.T) {
	adminEndpoint := envOr("ADMIN_ENDPOINT", defaultAdminEndpoint)

	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("admin-dispatch")
	mustCreateBucket(t, c, bucket)
	putExpirationLifecycle(t, c, bucket, "expire/", 1)
	const oldKey = "expire/old.txt"
	putObject(t, c, bucket, oldKey, "old")
	backdateMtime(t, fc, bucket, oldKey, 30)

	waitForLifecycleWorkerReady(t, adminEndpoint)

	// Lifecycle is a long-running batch; the run endpoint cancels it at
	// this timeout, which converts a healthy run into canceled_count=1.
	const runTimeoutSeconds = 30
	body, err := json.Marshal(map[string]any{
		"timeout_seconds": runTimeoutSeconds,
	})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost,
		adminEndpoint+"/api/plugin/job-types/s3_lifecycle/run",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	t.Logf("admin /api/plugin/job-types/s3_lifecycle/run response: %v", payload)
	require.Equal(t, http.StatusOK, resp.StatusCode, "admin run endpoint failed: %v", payload)

	require.GreaterOrEqual(t, jsonNumber(t, payload, "detected_count"), 1)
	require.Equal(t, 0, jsonNumber(t, payload, "error_count"),
		"dispatched job errored — likely filer_grpc_address was raw host:httpPort.grpcPort")

	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(oldKey),
		})
		return err != nil
	}, 30*time.Second, 500*time.Millisecond,
		"expected %s/%s to be deleted after admin-dispatched lifecycle run", bucket, oldKey)
}

func jsonNumber(t *testing.T, payload map[string]any, key string) int {
	t.Helper()
	raw, ok := payload[key]
	require.Truef(t, ok, "response missing key %q: %v", key, payload)
	switch v := raw.(type) {
	case float64:
		return int(v)
	case json.Number:
		n, err := v.Int64()
		require.NoErrorf(t, err, "key %q is not an int: %v", key, raw)
		return int(n)
	default:
		t.Fatalf("key %q has unexpected type %T (%v)", key, raw, raw)
		return 0
	}
}

func waitForLifecycleWorkerReady(t *testing.T, adminEndpoint string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, adminEndpoint+"/api/plugin/scheduler-states", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		var payload any
		_ = json.NewDecoder(resp.Body).Decode(&payload)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK && strings.Contains(fmt.Sprintf("%v", payload), "s3_lifecycle") {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("admin never reported an s3_lifecycle-capable worker")
}
