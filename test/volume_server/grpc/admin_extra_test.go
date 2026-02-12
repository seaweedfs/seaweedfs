package volume_server_grpc_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestVolumeNeedleStatusForUploadedFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(21)
	const needleID = uint64(778899)
	const cookie = uint32(0xA1B2C3D4)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, needleID, cookie)
	client := framework.NewHTTPClient()
	payload := []byte("needle-status-payload")
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload status: expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	statusResp, err := grpcClient.VolumeNeedleStatus(ctx, &volume_server_pb.VolumeNeedleStatusRequest{
		VolumeId: volumeID,
		NeedleId: needleID,
	})
	if err != nil {
		t.Fatalf("VolumeNeedleStatus failed: %v", err)
	}
	if statusResp.GetNeedleId() != needleID {
		t.Fatalf("needle id mismatch: got %d want %d", statusResp.GetNeedleId(), needleID)
	}
	if statusResp.GetCookie() != cookie {
		t.Fatalf("cookie mismatch: got %d want %d", statusResp.GetCookie(), cookie)
	}
	if statusResp.GetSize() == 0 {
		t.Fatalf("expected non-zero needle size")
	}
}

func mustNewRequest(t testing.TB, method, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("create request %s %s: %v", method, url, err)
	}
	return req
}

func TestVolumeConfigureInvalidReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(22)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := grpcClient.VolumeConfigure(ctx, &volume_server_pb.VolumeConfigureRequest{
		VolumeId:    volumeID,
		Replication: "bad-replication",
	})
	if err != nil {
		t.Fatalf("VolumeConfigure returned grpc error: %v", err)
	}
	if resp.GetError() == "" {
		t.Fatalf("VolumeConfigure expected response error for invalid replication")
	}
	if !strings.Contains(strings.ToLower(resp.GetError()), "replication") {
		t.Fatalf("VolumeConfigure error should mention replication, got: %q", resp.GetError())
	}
}

func TestPingVolumeTargetAndLeaveAffectsHealthz(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pingResp, err := grpcClient.Ping(ctx, &volume_server_pb.PingRequest{
		TargetType: cluster.VolumeServerType,
		Target:     clusterHarness.VolumeServerAddress(),
	})
	if err != nil {
		t.Fatalf("Ping target volume server failed: %v", err)
	}
	if pingResp.GetRemoteTimeNs() == 0 {
		t.Fatalf("expected remote timestamp from ping target volume server")
	}

	if _, err = grpcClient.VolumeServerLeave(ctx, &volume_server_pb.VolumeServerLeaveRequest{}); err != nil {
		t.Fatalf("VolumeServerLeave failed: %v", err)
	}

	client := framework.NewHTTPClient()
	healthURL := clusterHarness.VolumeAdminURL() + "/healthz"
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, healthURL))
		_ = framework.ReadAllAndClose(t, resp)
		if resp.StatusCode == http.StatusServiceUnavailable {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected healthz to return 503 after leave, got %d", resp.StatusCode)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestPingUnknownAndUnreachableTargetPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	unknownResp, err := grpcClient.Ping(ctx, &volume_server_pb.PingRequest{
		TargetType: "unknown-type",
		Target:     "127.0.0.1:12345",
	})
	if err != nil {
		t.Fatalf("Ping unknown target type should not return grpc error, got: %v", err)
	}
	if unknownResp.GetRemoteTimeNs() != 0 {
		t.Fatalf("Ping unknown target type expected remote_time_ns=0, got %d", unknownResp.GetRemoteTimeNs())
	}
	if unknownResp.GetStopTimeNs() <= unknownResp.GetStartTimeNs() {
		t.Fatalf("Ping unknown target type expected stop_time_ns > start_time_ns")
	}

	_, err = grpcClient.Ping(ctx, &volume_server_pb.PingRequest{
		TargetType: cluster.MasterType,
		Target:     "127.0.0.1:1",
	})
	if err == nil {
		t.Fatalf("Ping master target should fail when target is unreachable")
	}
	if !strings.Contains(err.Error(), "ping master") {
		t.Fatalf("Ping master unreachable error mismatch: %v", err)
	}
}
