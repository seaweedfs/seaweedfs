package volume_server_http_test

import (
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func TestReadDeletedQueryReturnsDeletedNeedleData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(94)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 551234, 0xCAFE1234)
	payload := []byte("read-deleted-needle-payload")

	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	deleteResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodDelete, clusterHarness.VolumeAdminURL()+"/"+fid))
	_ = framework.ReadAllAndClose(t, deleteResp)
	if deleteResp.StatusCode != http.StatusAccepted {
		t.Fatalf("delete expected 202, got %d", deleteResp.StatusCode)
	}

	normalRead := framework.ReadBytes(t, client, clusterHarness.VolumeAdminURL(), fid)
	_ = framework.ReadAllAndClose(t, normalRead)
	if normalRead.StatusCode != http.StatusNotFound {
		t.Fatalf("normal read after delete expected 404, got %d", normalRead.StatusCode)
	}

	readDeletedReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid+"?readDeleted=true")
	readDeletedResp := framework.DoRequest(t, client, readDeletedReq)
	readDeletedBody := framework.ReadAllAndClose(t, readDeletedResp)
	if readDeletedResp.StatusCode != http.StatusOK {
		t.Fatalf("read with readDeleted=true expected 200, got %d", readDeletedResp.StatusCode)
	}
	if string(readDeletedBody) != string(payload) {
		t.Fatalf("readDeleted body mismatch: got %q want %q", string(readDeletedBody), string(payload))
	}
}
