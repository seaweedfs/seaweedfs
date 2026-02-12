package volume_server_grpc_test

import (
	"context"
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
