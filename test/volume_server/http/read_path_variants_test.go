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

	headIfModifiedSinceReq := mustNewRequest(t, http.MethodHead, clusterHarness.VolumeAdminURL()+"/"+fullFileID)
	headIfModifiedSinceReq.Header.Set("If-Modified-Since", lastModified)
	headIfModifiedSinceResp := framework.DoRequest(t, client, headIfModifiedSinceReq)
	headIfModifiedSinceBody := framework.ReadAllAndClose(t, headIfModifiedSinceResp)
	if headIfModifiedSinceResp.StatusCode != http.StatusNotModified {
		t.Fatalf("HEAD If-Modified-Since expected 304, got %d", headIfModifiedSinceResp.StatusCode)
	}
	if len(headIfModifiedSinceBody) != 0 {
		t.Fatalf("HEAD If-Modified-Since expected empty body, got %d bytes", len(headIfModifiedSinceBody))
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

func TestReadWrongCookieReturnsNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(95)
	const needleID = uint64(771235)
	const cookie = uint32(0xBEEFCACF)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, needleID, cookie)
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, []byte("read-cookie-mismatch-content"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	wrongCookieFid := framework.NewFileID(volumeID, needleID, cookie+1)
	getResp := framework.ReadBytes(t, client, clusterHarness.VolumeAdminURL(), wrongCookieFid)
	_ = framework.ReadAllAndClose(t, getResp)
	if getResp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET with wrong cookie expected 404, got %d", getResp.StatusCode)
	}

	headResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodHead, clusterHarness.VolumeAdminURL()+"/"+wrongCookieFid))
	headBody := framework.ReadAllAndClose(t, headResp)
	if headResp.StatusCode != http.StatusNotFound {
		t.Fatalf("HEAD with wrong cookie expected 404, got %d", headResp.StatusCode)
	}
	if len(headBody) != 0 {
		t.Fatalf("HEAD wrong-cookie response body should be empty, got %d bytes", len(headBody))
	}
}

func TestConditionalHeaderPrecedenceAndInvalidIfModifiedSince(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(99)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 772002, 0x2B3C4D5E)
	payload := []byte("conditional-precedence-content")
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	baselineResp := framework.ReadBytes(t, client, clusterHarness.VolumeAdminURL(), fid)
	_ = framework.ReadAllAndClose(t, baselineResp)
	if baselineResp.StatusCode != http.StatusOK {
		t.Fatalf("baseline read expected 200, got %d", baselineResp.StatusCode)
	}
	lastModified := baselineResp.Header.Get("Last-Modified")
	if lastModified == "" {
		t.Fatalf("baseline read expected Last-Modified header")
	}

	precedenceReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	precedenceReq.Header.Set("If-Modified-Since", lastModified)
	precedenceReq.Header.Set("If-None-Match", "\"definitely-different-etag\"")
	precedenceResp := framework.DoRequest(t, client, precedenceReq)
	precedenceBody := framework.ReadAllAndClose(t, precedenceResp)
	if precedenceResp.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional precedence expected 304, got %d", precedenceResp.StatusCode)
	}
	if len(precedenceBody) != 0 {
		t.Fatalf("conditional precedence expected empty body, got %d bytes", len(precedenceBody))
	}

	invalidIMSReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	invalidIMSReq.Header.Set("If-Modified-Since", "not-a-valid-http-date")
	invalidIMSReq.Header.Set("If-None-Match", "\"definitely-different-etag\"")
	invalidIMSResp := framework.DoRequest(t, client, invalidIMSReq)
	invalidIMSBody := framework.ReadAllAndClose(t, invalidIMSResp)
	if invalidIMSResp.StatusCode != http.StatusOK {
		t.Fatalf("invalid If-Modified-Since with mismatched etag expected 200, got %d", invalidIMSResp.StatusCode)
	}
	if string(invalidIMSBody) != string(payload) {
		t.Fatalf("invalid If-Modified-Since fallback body mismatch: got %q want %q", string(invalidIMSBody), string(payload))
	}
}
