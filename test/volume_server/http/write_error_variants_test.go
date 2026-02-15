package volume_server_http_test

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func TestWriteInvalidVidAndFidReturnBadRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	invalidVidReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/invalid,12345678", []byte("x"))
	invalidVidResp := framework.DoRequest(t, client, invalidVidReq)
	_ = framework.ReadAllAndClose(t, invalidVidResp)
	if invalidVidResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("write with invalid vid expected 400, got %d", invalidVidResp.StatusCode)
	}

	invalidFidReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/1,bad", []byte("x"))
	invalidFidResp := framework.DoRequest(t, client, invalidFidReq)
	_ = framework.ReadAllAndClose(t, invalidFidResp)
	if invalidFidResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("write with invalid fid expected 400, got %d", invalidFidResp.StatusCode)
	}
}

func TestWriteMalformedMultipartAndMD5Mismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(98)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 772001, 0x1A2B3C4D)

	malformedMultipartReq, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+fid, strings.NewReader("not-a-valid-multipart-body"))
	if err != nil {
		t.Fatalf("create malformed multipart request: %v", err)
	}
	malformedMultipartReq.Header.Set("Content-Type", "multipart/form-data")
	malformedMultipartResp := framework.DoRequest(t, client, malformedMultipartReq)
	malformedMultipartBody := framework.ReadAllAndClose(t, malformedMultipartResp)
	if malformedMultipartResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed multipart write expected 400, got %d", malformedMultipartResp.StatusCode)
	}
	if !strings.Contains(strings.ToLower(string(malformedMultipartBody)), "boundary") {
		t.Fatalf("malformed multipart response should mention boundary parse failure, got %q", string(malformedMultipartBody))
	}

	md5MismatchReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, []byte("content-md5-mismatch-body"))
	md5MismatchReq.Header.Set("Content-MD5", "AAAAAAAAAAAAAAAAAAAAAA==")
	md5MismatchResp := framework.DoRequest(t, client, md5MismatchReq)
	md5MismatchBody := framework.ReadAllAndClose(t, md5MismatchResp)
	if md5MismatchResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("content-md5 mismatch write expected 400, got %d", md5MismatchResp.StatusCode)
	}
	if !strings.Contains(string(md5MismatchBody), "Content-MD5") {
		t.Fatalf("content-md5 mismatch response should mention Content-MD5, got %q", string(md5MismatchBody))
	}
}

func TestWriteRejectsPayloadOverFileSizeLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P1()
	profile.FileSizeLimitMB = 1
	clusterHarness := framework.StartSingleVolumeCluster(t, profile)
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(99)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 772002, 0x2A3B4C5D)
	oversizedPayload := bytes.Repeat([]byte("z"), 1024*1024+1)

	oversizedReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, oversizedPayload)
	oversizedResp := framework.DoRequest(t, client, oversizedReq)
	oversizedBody := framework.ReadAllAndClose(t, oversizedResp)
	if oversizedResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized write expected 400, got %d", oversizedResp.StatusCode)
	}
	if !strings.Contains(strings.ToLower(string(oversizedBody)), "limited") {
		t.Fatalf("oversized write response should mention limit, got %q", string(oversizedBody))
	}
}
