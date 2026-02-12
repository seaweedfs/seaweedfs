package volume_server_http_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func TestReadPathShapesAndIfModifiedSince(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(93)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fullFileID := framework.NewFileID(volumeID, 771234, 0xBEEFCACE)
	uploadPayload := []byte("read-path-shape-content")
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fullFileID, uploadPayload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	parts := strings.SplitN(fullFileID, ",", 2)
	if len(parts) != 2 {
		t.Fatalf("unexpected file id format: %q", fullFileID)
	}
	fidOnly := parts[1]

	readByVidFid := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, fmt.Sprintf("%s/%d/%s", clusterHarness.VolumeAdminURL(), volumeID, fidOnly)))
	readByVidFidBody := framework.ReadAllAndClose(t, readByVidFid)
	if readByVidFid.StatusCode != http.StatusOK {
		t.Fatalf("GET /{vid}/{fid} expected 200, got %d", readByVidFid.StatusCode)
	}
	if string(readByVidFidBody) != string(uploadPayload) {
		t.Fatalf("GET /{vid}/{fid} body mismatch: got %q want %q", string(readByVidFidBody), string(uploadPayload))
	}

	readWithFilename := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, fmt.Sprintf("%s/%d/%s/%s", clusterHarness.VolumeAdminURL(), volumeID, fidOnly, "named.bin")))
	readWithFilenameBody := framework.ReadAllAndClose(t, readWithFilename)
	if readWithFilename.StatusCode != http.StatusOK {
		t.Fatalf("GET /{vid}/{fid}/{filename} expected 200, got %d", readWithFilename.StatusCode)
	}
	if string(readWithFilenameBody) != string(uploadPayload) {
		t.Fatalf("GET /{vid}/{fid}/{filename} body mismatch: got %q want %q", string(readWithFilenameBody), string(uploadPayload))
	}

	lastModified := readWithFilename.Header.Get("Last-Modified")
	if lastModified == "" {
		t.Fatalf("expected Last-Modified header on read response")
	}

	ifModifiedSinceReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fullFileID)
	ifModifiedSinceReq.Header.Set("If-Modified-Since", lastModified)
	ifModifiedSinceResp := framework.DoRequest(t, client, ifModifiedSinceReq)
	_ = framework.ReadAllAndClose(t, ifModifiedSinceResp)
	if ifModifiedSinceResp.StatusCode != http.StatusNotModified {
		t.Fatalf("If-Modified-Since expected 304, got %d", ifModifiedSinceResp.StatusCode)
	}
}

func TestMalformedVidFidPathReturnsBadRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	resp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/not-a-vid/not-a-fid"))
	_ = framework.ReadAllAndClose(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed /{vid}/{fid} expected 400, got %d", resp.StatusCode)
	}
}
