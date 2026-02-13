package volume_server_grpc_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestStateAndStatusRPCs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	initialState, err := client.GetState(ctx, &volume_server_pb.GetStateRequest{})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	if initialState.GetState() == nil {
		t.Fatalf("GetState returned nil state")
	}

	setResp, err := client.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{
			Maintenance: true,
			Version:     initialState.GetState().GetVersion(),
		},
	})
	if err != nil {
		t.Fatalf("SetState(maintenance=true) failed: %v", err)
	}
	if !setResp.GetState().GetMaintenance() {
		t.Fatalf("expected maintenance=true after SetState")
	}

	setResp, err = client.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{
			Maintenance: false,
			Version:     setResp.GetState().GetVersion(),
		},
	})
	if err != nil {
		t.Fatalf("SetState(maintenance=false) failed: %v", err)
	}
	if setResp.GetState().GetMaintenance() {
		t.Fatalf("expected maintenance=false after SetState")
	}

	statusResp, err := client.VolumeServerStatus(ctx, &volume_server_pb.VolumeServerStatusRequest{})
	if err != nil {
		t.Fatalf("VolumeServerStatus failed: %v", err)
	}
	if statusResp.GetVersion() == "" {
		t.Fatalf("VolumeServerStatus returned empty version")
	}
	if len(statusResp.GetDiskStatuses()) == 0 {
		t.Fatalf("VolumeServerStatus returned no disk statuses")
	}
	if statusResp.GetState() == nil {
		t.Fatalf("VolumeServerStatus returned nil state")
	}
	if statusResp.GetMemoryStatus() == nil {
		t.Fatalf("VolumeServerStatus returned nil memory status")
	}
	if statusResp.GetMemoryStatus().GetGoroutines() <= 0 {
		t.Fatalf("VolumeServerStatus memory status should report goroutines, got %d", statusResp.GetMemoryStatus().GetGoroutines())
	}

	pingResp, err := client.Ping(ctx, &volume_server_pb.PingRequest{})
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	if pingResp.GetStartTimeNs() == 0 || pingResp.GetStopTimeNs() == 0 {
		t.Fatalf("Ping timestamps should be non-zero: %+v", pingResp)
	}
	if pingResp.GetStopTimeNs() < pingResp.GetStartTimeNs() {
		t.Fatalf("Ping stop time should be >= start time: %+v", pingResp)
	}
}

func TestSetStateVersionMismatchAndNilStateNoop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	initialState, err := client.GetState(ctx, &volume_server_pb.GetStateRequest{})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	initialVersion := initialState.GetState().GetVersion()

	staleResp, err := client.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{
			Maintenance: true,
			Version:     initialVersion + 1,
		},
	})
	if err == nil {
		t.Fatalf("SetState with stale version should fail")
	}
	if !strings.Contains(err.Error(), "version mismatch") {
		t.Fatalf("SetState stale version error mismatch: %v", err)
	}
	if staleResp.GetState().GetVersion() != initialVersion {
		t.Fatalf("SetState stale version should not mutate server version: got %d want %d", staleResp.GetState().GetVersion(), initialVersion)
	}
	if staleResp.GetState().GetMaintenance() != initialState.GetState().GetMaintenance() {
		t.Fatalf("SetState stale version should not mutate maintenance flag")
	}

	nilResp, err := client.SetState(ctx, &volume_server_pb.SetStateRequest{})
	if err != nil {
		t.Fatalf("SetState nil-state request should be no-op success: %v", err)
	}
	if nilResp.GetState().GetVersion() != initialVersion {
		t.Fatalf("SetState nil-state should keep version unchanged: got %d want %d", nilResp.GetState().GetVersion(), initialVersion)
	}
	if nilResp.GetState().GetMaintenance() != initialState.GetState().GetMaintenance() {
		t.Fatalf("SetState nil-state should keep maintenance unchanged")
	}
}
