package volume_server_http_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func TestMultiRangeReadReturnsMultipartPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(97)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 771999, 0x0A1B2C3D)
	payload := []byte("0123456789abcdef")
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	multiRangeReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	multiRangeReq.Header.Set("Range", "bytes=0-1,4-5")
	multiRangeResp := framework.DoRequest(t, client, multiRangeReq)
	multiRangeBody := framework.ReadAllAndClose(t, multiRangeResp)
	if multiRangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("multi-range expected 206, got %d", multiRangeResp.StatusCode)
	}
	if !strings.Contains(multiRangeResp.Header.Get("Content-Type"), "multipart/byteranges") {
		t.Fatalf("multi-range content-type mismatch: %q", multiRangeResp.Header.Get("Content-Type"))
	}

	bodyText := string(multiRangeBody)
	if !strings.Contains(bodyText, "01") || !strings.Contains(bodyText, "45") {
		t.Fatalf("multi-range body missing expected segments: %q", bodyText)
	}
}
