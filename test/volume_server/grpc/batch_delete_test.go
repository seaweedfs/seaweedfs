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

func TestBatchDeleteInvalidFidAndMaintenanceMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{FileIds: []string{"bad-fid"}})
	if err != nil {
		t.Fatalf("BatchDelete invalid fid should return response, got error: %v", err)
	}
	if len(resp.GetResults()) != 1 {
		t.Fatalf("expected one batch delete result, got %d", len(resp.GetResults()))
	}
	if got := resp.GetResults()[0].GetStatus(); got != 400 {
		t.Fatalf("invalid fid expected status 400, got %d", got)
	}

	stateResp, err := client.GetState(ctx, &volume_server_pb.GetStateRequest{})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	_, err = client.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{Maintenance: true, Version: stateResp.GetState().GetVersion()},
	})
	if err != nil {
		t.Fatalf("SetState maintenance=true failed: %v", err)
	}

	_, err = client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{FileIds: []string{"1,1234567890ab"}})
	if err == nil {
		t.Fatalf("BatchDelete should fail when maintenance mode is enabled")
	}
	if !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("expected maintenance mode error, got: %v", err)
	}
}
