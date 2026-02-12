package volume_server_grpc_test

import (
	"context"
	"net/http"
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

func TestBatchDeleteCookieMismatchAndSkipCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(31)
	const needleID = uint64(900001)
	const correctCookie = uint32(0x1122AABB)
	const wrongCookie = uint32(0x1122AABC)
	framework.AllocateVolume(t, client, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, needleID, correctCookie)
	uploadResp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(), fid, []byte("batch-delete-cookie-check"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	wrongCookieFid := framework.NewFileID(volumeID, needleID, wrongCookie)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mismatchResp, err := client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{
		FileIds:         []string{wrongCookieFid},
		SkipCookieCheck: false,
	})
	if err != nil {
		t.Fatalf("BatchDelete with cookie check failed: %v", err)
	}
	if len(mismatchResp.GetResults()) != 1 {
		t.Fatalf("BatchDelete cookie mismatch expected 1 result, got %d", len(mismatchResp.GetResults()))
	}
	if mismatchResp.GetResults()[0].GetStatus() != http.StatusBadRequest {
		t.Fatalf("BatchDelete cookie mismatch expected status 400, got %d", mismatchResp.GetResults()[0].GetStatus())
	}

	skipCheckResp, err := client.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{
		FileIds:         []string{wrongCookieFid},
		SkipCookieCheck: true,
	})
	if err != nil {
		t.Fatalf("BatchDelete skip cookie check failed: %v", err)
	}
	if len(skipCheckResp.GetResults()) != 1 {
		t.Fatalf("BatchDelete skip check expected 1 result, got %d", len(skipCheckResp.GetResults()))
	}
	if skipCheckResp.GetResults()[0].GetStatus() != http.StatusAccepted {
		t.Fatalf("BatchDelete skip check expected status 202, got %d", skipCheckResp.GetResults()[0].GetStatus())
	}

	readAfterDelete := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(), fid)
	_ = framework.ReadAllAndClose(t, readAfterDelete)
	if readAfterDelete.StatusCode != http.StatusNotFound {
		t.Fatalf("read after skip-check batch delete expected 404, got %d", readAfterDelete.StatusCode)
	}
}
