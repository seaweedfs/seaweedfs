package volume_server_rust_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func mustNewRequest(t testing.TB, method, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("create request %s %s: %v", method, url, err)
	}
	return req
}

// TestRustHealthzEndpoint verifies that the Rust volume server responds to
// GET /healthz with HTTP 200.
func TestRustHealthzEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartRustVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	resp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/healthz"))
	_ = framework.ReadAllAndClose(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected /healthz 200, got %d", resp.StatusCode)
	}
}

// TestRustStatusEndpoint verifies that GET /status returns 200 with a JSON
// body containing a "version" field. The Rust server uses lowercase field
// names in its axum JSON responses.
func TestRustStatusEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartRustVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	resp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/status"))
	body := framework.ReadAllAndClose(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected /status 200, got %d, body: %s", resp.StatusCode, string(body))
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode /status JSON: %v", err)
	}

	if _, ok := payload["Version"]; !ok {
		t.Fatalf("/status JSON missing \"Version\" field, keys: %v", keys(payload))
	}
}

// TestRustPingRPC verifies the gRPC Ping RPC returns non-zero timestamps.
func TestRustPingRPC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartRustVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Ping(ctx, &volume_server_pb.PingRequest{})
	if err != nil {
		t.Fatalf("Ping RPC failed: %v", err)
	}
	if resp.GetStartTimeNs() == 0 {
		t.Fatalf("Ping StartTimeNs should be non-zero")
	}
	if resp.GetStopTimeNs() == 0 {
		t.Fatalf("Ping StopTimeNs should be non-zero")
	}
	if resp.GetStopTimeNs() < resp.GetStartTimeNs() {
		t.Fatalf("Ping StopTimeNs (%d) should be >= StartTimeNs (%d)", resp.GetStopTimeNs(), resp.GetStartTimeNs())
	}
}

// TestRustAllocateAndWriteReadDelete exercises the full needle lifecycle:
// allocate a volume via gRPC, upload bytes via HTTP POST, read them back
// via HTTP GET, delete via HTTP DELETE, then confirm GET returns 404.
func TestRustAllocateAndWriteReadDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartRustVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(1)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 1001, 0xAABBCCDD)
	data := []byte("rust-volume-server-integration-test-payload")

	// Upload
	uploadResp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(), fid, data)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	// Read back
	getResp := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(), fid)
	getBody := framework.ReadAllAndClose(t, getResp)
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("read expected 200, got %d", getResp.StatusCode)
	}
	if string(getBody) != string(data) {
		t.Fatalf("read body mismatch: got %q, want %q", string(getBody), string(data))
	}

	// Delete
	deleteResp := framework.DoRequest(t, httpClient, mustNewRequest(t, http.MethodDelete, cluster.VolumeAdminURL()+"/"+fid))
	_ = framework.ReadAllAndClose(t, deleteResp)
	if deleteResp.StatusCode != http.StatusAccepted && deleteResp.StatusCode != http.StatusOK {
		t.Fatalf("delete expected 202 or 200, got %d", deleteResp.StatusCode)
	}

	// Verify 404 after delete
	gone := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(), fid)
	_ = framework.ReadAllAndClose(t, gone)
	if gone.StatusCode != http.StatusNotFound {
		t.Fatalf("read after delete expected 404, got %d", gone.StatusCode)
	}
}

// TestRustVolumeLifecycle tests the volume admin gRPC lifecycle:
// allocate, check status, unmount, mount, delete.
func TestRustVolumeLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartRustVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const volumeID = uint32(2)
	framework.AllocateVolume(t, client, volumeID, "")

	// VolumeStatus should succeed on a freshly allocated volume.
	statusResp, err := client.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeStatus failed: %v", err)
	}
	if statusResp.GetFileCount() != 0 {
		t.Fatalf("new volume should be empty, got file_count=%d", statusResp.GetFileCount())
	}

	// Unmount then remount.
	if _, err = client.VolumeUnmount(ctx, &volume_server_pb.VolumeUnmountRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("VolumeUnmount failed: %v", err)
	}
	if _, err = client.VolumeMount(ctx, &volume_server_pb.VolumeMountRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("VolumeMount failed: %v", err)
	}

	// Delete.
	if _, err = client.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{VolumeId: volumeID, OnlyEmpty: true}); err != nil {
		t.Fatalf("VolumeDelete failed: %v", err)
	}

	// VolumeStatus should fail after delete.
	_, err = client.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	if err == nil {
		t.Fatalf("VolumeStatus should fail after delete")
	}
}

// TestRustGetSetState verifies GetState returns a non-nil state and SetState
// echoes the state back.
func TestRustGetSetState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartRustVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// GetState should return non-nil state.
	getResp, err := client.GetState(ctx, &volume_server_pb.GetStateRequest{})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	if getResp.GetState() == nil {
		t.Fatalf("GetState returned nil state")
	}

	// SetState should echo back the state.
	setResp, err := client.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{
			Version: getResp.GetState().GetVersion(),
		},
	})
	if err != nil {
		t.Fatalf("SetState failed: %v", err)
	}
	if setResp.GetState() == nil {
		t.Fatalf("SetState returned nil state")
	}
	if setResp.GetState().GetVersion() < getResp.GetState().GetVersion() {
		t.Fatalf("SetState version should not decrease: got %d, had %d",
			setResp.GetState().GetVersion(), getResp.GetState().GetVersion())
	}
}

// TestRustVolumeServerStatus verifies VolumeServerStatus returns a version
// string and at least one disk status entry.
func TestRustVolumeServerStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartRustVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.VolumeServerStatus(ctx, &volume_server_pb.VolumeServerStatusRequest{})
	if err != nil {
		t.Fatalf("VolumeServerStatus failed: %v", err)
	}
	if resp.GetVersion() == "" {
		t.Fatalf("VolumeServerStatus returned empty version")
	}
	if len(resp.GetDiskStatuses()) == 0 {
		t.Fatalf("VolumeServerStatus returned no disk statuses")
	}
}

// TestRustMetricsEndpointIsNotOnAdminPortByDefault verifies that the default
// volume admin listener does not expose Prometheus metrics. Go serves metrics
// only on the dedicated metrics listener when -metricsPort is configured.
func TestRustMetricsEndpointIsNotOnAdminPortByDefault(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartRustVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	resp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/metrics"))
	body := framework.ReadAllAndClose(t, resp)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected admin /metrics 400 when metricsPort is unset, got %d body=%s", resp.StatusCode, string(body))
	}
}

// keys returns the keys of a map for diagnostic messages.
func keys(m map[string]interface{}) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
