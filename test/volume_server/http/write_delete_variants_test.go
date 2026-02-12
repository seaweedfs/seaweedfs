package volume_server_http_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func TestWriteUnchangedAndDeleteEdgeVariants(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(87)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	const key = uint64(999001)
	const cookie = uint32(0xDEADBEEF)
	fid := framework.NewFileID(volumeID, key, cookie)
	client := framework.NewHTTPClient()
	payload := []byte("unchanged-write-content")

	firstUpload := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, payload)
	firstUploadResp := framework.DoRequest(t, client, firstUpload)
	_ = framework.ReadAllAndClose(t, firstUploadResp)
	if firstUploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("first upload expected 201, got %d", firstUploadResp.StatusCode)
	}

	secondUpload := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, payload)
	secondUploadResp := framework.DoRequest(t, client, secondUpload)
	_ = framework.ReadAllAndClose(t, secondUploadResp)
	if secondUploadResp.StatusCode != http.StatusNoContent {
		t.Fatalf("second unchanged upload expected 204, got %d", secondUploadResp.StatusCode)
	}
	if secondUploadResp.Header.Get("ETag") == "" {
		t.Fatalf("second unchanged upload expected ETag header")
	}

	wrongCookieFid := framework.NewFileID(volumeID, key, cookie+1)
	wrongCookieDelete := framework.DoRequest(t, client, mustNewRequest(t, http.MethodDelete, clusterHarness.VolumeAdminURL()+"/"+wrongCookieFid))
	_ = framework.ReadAllAndClose(t, wrongCookieDelete)
	if wrongCookieDelete.StatusCode != http.StatusBadRequest {
		t.Fatalf("delete with mismatched cookie expected 400, got %d", wrongCookieDelete.StatusCode)
	}

	missingDelete := framework.DoRequest(t, client, mustNewRequest(t, http.MethodDelete, clusterHarness.VolumeAdminURL()+"/"+framework.NewFileID(volumeID, key+1, cookie)))
	missingDeleteBody := framework.ReadAllAndClose(t, missingDelete)
	if missingDelete.StatusCode != http.StatusNotFound {
		t.Fatalf("delete missing needle expected 404, got %d", missingDelete.StatusCode)
	}

	var payloadMap map[string]int64
	if err := json.Unmarshal(missingDeleteBody, &payloadMap); err != nil {
		t.Fatalf("decode delete missing response: %v", err)
	}
	if payloadMap["size"] != 0 {
		t.Fatalf("delete missing needle expected size=0, got %d", payloadMap["size"])
	}
}
