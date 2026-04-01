package volume_server_http_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestReplicatedUploadSucceedsImmediatelyAfterAllocate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartDualVolumeCluster(t, matrix.P1())

	conn0, grpc0 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer conn0.Close()
	conn1, grpc1 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(1))
	defer conn1.Close()

	const volumeID = uint32(115)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &volume_server_pb.AllocateVolumeRequest{
		VolumeId:    volumeID,
		Replication: "001",
		Version:     uint32(needle.GetCurrentVersion()),
	}
	if _, err := grpc0.AllocateVolume(ctx, req); err != nil {
		t.Fatalf("allocate replicated volume on node0: %v", err)
	}
	if _, err := grpc1.AllocateVolume(ctx, req); err != nil {
		t.Fatalf("allocate replicated volume on node1: %v", err)
	}

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 881001, 0x0B0C0D0E)
	payload := []byte("replicated-upload-after-allocate")

	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(0), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("replicated upload expected 201, got %d", uploadResp.StatusCode)
	}

	replicaReadURL := clusterHarness.VolumeAdminURL(1) + "/" + fid
	var replicaBody []byte
	if !waitForHTTPStatus(t, client, replicaReadURL, http.StatusOK, 10*time.Second, func(resp *http.Response) {
		replicaBody = framework.ReadAllAndClose(t, resp)
	}) {
		t.Fatalf("replica did not become readable within deadline")
	}
	if string(replicaBody) != string(payload) {
		t.Fatalf("replica body mismatch: got %q want %q", string(replicaBody), string(payload))
	}
}
