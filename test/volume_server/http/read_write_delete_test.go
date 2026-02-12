package volume_server_http_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func TestUploadReadRangeHeadDeleteRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(7)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, 123456, 0xA1B2C3D4)
	data := []byte("hello-volume-server-integration")
	client := framework.NewHTTPClient()

	uploadResp := framework.UploadBytes(t, client, cluster.VolumeAdminURL(), fid, data)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload status: expected 201, got %d", uploadResp.StatusCode)
	}

	getResp := framework.ReadBytes(t, client, cluster.VolumeAdminURL(), fid)
	getBody := framework.ReadAllAndClose(t, getResp)
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get status: expected 200, got %d", getResp.StatusCode)
	}
	if string(getBody) != string(data) {
		t.Fatalf("get body mismatch: got %q want %q", string(getBody), string(data))
	}

	rangeReq := mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/"+fid)
	rangeReq.Header.Set("Range", "bytes=0-4")
	rangeResp := framework.DoRequest(t, client, rangeReq)
	rangeBody := framework.ReadAllAndClose(t, rangeResp)
	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status: expected 206, got %d", rangeResp.StatusCode)
	}
	if got, want := string(rangeBody), "hello"; got != want {
		t.Fatalf("range body mismatch: got %q want %q", got, want)
	}

	headResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodHead, cluster.VolumeAdminURL()+"/"+fid))
	_ = framework.ReadAllAndClose(t, headResp)
	if headResp.StatusCode != http.StatusOK {
		t.Fatalf("head status: expected 200, got %d", headResp.StatusCode)
	}
	if got := headResp.Header.Get("Content-Length"); got != strconv.Itoa(len(data)) {
		t.Fatalf("head content-length mismatch: got %q want %d", got, len(data))
	}

	deleteResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodDelete, cluster.VolumeAdminURL()+"/"+fid))
	_ = framework.ReadAllAndClose(t, deleteResp)
	if deleteResp.StatusCode != http.StatusAccepted {
		t.Fatalf("delete status: expected 202, got %d", deleteResp.StatusCode)
	}

	notFoundResp := framework.ReadBytes(t, client, cluster.VolumeAdminURL(), fid)
	_ = framework.ReadAllAndClose(t, notFoundResp)
	if notFoundResp.StatusCode != http.StatusNotFound {
		t.Fatalf("read after delete: expected 404, got %d", notFoundResp.StatusCode)
	}
}

func TestInvalidReadPathReturnsBadRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	resp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/invalid,needle"))
	_ = framework.ReadAllAndClose(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid read expected 400, got %d", resp.StatusCode)
	}
}
