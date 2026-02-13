package volume_server_http_test

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func TestPublicPortReadOnlyMethodBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P2())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(81)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, 123321, 0x01020304)
	originalData := []byte("public-port-original")
	replacementData := []byte("public-port-replacement")
	client := framework.NewHTTPClient()

	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, originalData)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("admin upload expected 201, got %d", uploadResp.StatusCode)
	}

	publicReadResp := framework.ReadBytes(t, client, clusterHarness.VolumePublicURL(), fid)
	publicReadBody := framework.ReadAllAndClose(t, publicReadResp)
	if publicReadResp.StatusCode != http.StatusOK {
		t.Fatalf("public GET expected 200, got %d", publicReadResp.StatusCode)
	}
	if string(publicReadBody) != string(originalData) {
		t.Fatalf("public GET body mismatch: got %q want %q", string(publicReadBody), string(originalData))
	}

	publicPostReq := newUploadRequest(t, clusterHarness.VolumePublicURL()+"/"+fid, replacementData)
	publicPostResp := framework.DoRequest(t, client, publicPostReq)
	_ = framework.ReadAllAndClose(t, publicPostResp)
	if publicPostResp.StatusCode != http.StatusOK {
		t.Fatalf("public POST expected passthrough 200, got %d", publicPostResp.StatusCode)
	}

	publicDeleteResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodDelete, clusterHarness.VolumePublicURL()+"/"+fid))
	_ = framework.ReadAllAndClose(t, publicDeleteResp)
	if publicDeleteResp.StatusCode != http.StatusOK {
		t.Fatalf("public DELETE expected passthrough 200, got %d", publicDeleteResp.StatusCode)
	}

	adminReadResp := framework.ReadBytes(t, client, clusterHarness.VolumeAdminURL(), fid)
	adminReadBody := framework.ReadAllAndClose(t, adminReadResp)
	if adminReadResp.StatusCode != http.StatusOK {
		t.Fatalf("admin GET after public POST/DELETE expected 200, got %d", adminReadResp.StatusCode)
	}
	if string(adminReadBody) != string(originalData) {
		t.Fatalf("public port should not mutate data: got %q want %q", string(adminReadBody), string(originalData))
	}
}

func TestCorsAndUnsupportedMethodBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P2())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(82)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, 789789, 0x0A0B0C0D)
	client := framework.NewHTTPClient()
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, []byte("cors-check"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("admin upload expected 201, got %d", uploadResp.StatusCode)
	}

	adminOriginReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	adminOriginReq.Header.Set("Origin", "https://example.com")
	adminOriginResp := framework.DoRequest(t, client, adminOriginReq)
	_ = framework.ReadAllAndClose(t, adminOriginResp)
	if adminOriginResp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("admin GET origin header mismatch: %q", adminOriginResp.Header.Get("Access-Control-Allow-Origin"))
	}
	if adminOriginResp.Header.Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatalf("admin GET credentials header mismatch: %q", adminOriginResp.Header.Get("Access-Control-Allow-Credentials"))
	}

	publicOriginReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumePublicURL()+"/"+fid)
	publicOriginReq.Header.Set("Origin", "https://example.com")
	publicOriginResp := framework.DoRequest(t, client, publicOriginReq)
	_ = framework.ReadAllAndClose(t, publicOriginResp)
	if publicOriginResp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("public GET origin header mismatch: %q", publicOriginResp.Header.Get("Access-Control-Allow-Origin"))
	}
	if publicOriginResp.Header.Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatalf("public GET credentials header mismatch: %q", publicOriginResp.Header.Get("Access-Control-Allow-Credentials"))
	}

	adminPatchReq, err := http.NewRequest(http.MethodPatch, clusterHarness.VolumeAdminURL()+"/"+fid, bytes.NewReader([]byte("patch")))
	if err != nil {
		t.Fatalf("create admin PATCH request: %v", err)
	}
	adminPatchResp := framework.DoRequest(t, client, adminPatchReq)
	_ = framework.ReadAllAndClose(t, adminPatchResp)
	if adminPatchResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("admin PATCH expected 400, got %d", adminPatchResp.StatusCode)
	}

	publicPatchReq, err := http.NewRequest(http.MethodPatch, clusterHarness.VolumePublicURL()+"/"+fid, bytes.NewReader([]byte("patch")))
	if err != nil {
		t.Fatalf("create public PATCH request: %v", err)
	}
	publicPatchResp := framework.DoRequest(t, client, publicPatchReq)
	_ = framework.ReadAllAndClose(t, publicPatchResp)
	if publicPatchResp.StatusCode != http.StatusOK {
		t.Fatalf("public PATCH expected passthrough 200, got %d", publicPatchResp.StatusCode)
	}
}

func TestUnsupportedMethodTraceParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P2())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(83)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, 123999, 0x01010101)
	client := framework.NewHTTPClient()
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, []byte("trace-method-check"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	adminTraceReq := mustNewRequest(t, http.MethodTrace, clusterHarness.VolumeAdminURL()+"/"+fid)
	adminTraceResp := framework.DoRequest(t, client, adminTraceReq)
	_ = framework.ReadAllAndClose(t, adminTraceResp)
	if adminTraceResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("admin TRACE expected 400, got %d", adminTraceResp.StatusCode)
	}

	publicTraceReq := mustNewRequest(t, http.MethodTrace, clusterHarness.VolumePublicURL()+"/"+fid)
	publicTraceResp := framework.DoRequest(t, client, publicTraceReq)
	_ = framework.ReadAllAndClose(t, publicTraceResp)
	if publicTraceResp.StatusCode != http.StatusOK {
		t.Fatalf("public TRACE expected passthrough 200, got %d", publicTraceResp.StatusCode)
	}
}

func TestUnsupportedMethodPropfindParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P2())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(84)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, 124000, 0x02020202)
	client := framework.NewHTTPClient()
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, []byte("propfind-method-check"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	adminReq := mustNewRequest(t, "PROPFIND", clusterHarness.VolumeAdminURL()+"/"+fid)
	adminResp := framework.DoRequest(t, client, adminReq)
	_ = framework.ReadAllAndClose(t, adminResp)
	if adminResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("admin PROPFIND expected 400, got %d", adminResp.StatusCode)
	}

	publicReq := mustNewRequest(t, "PROPFIND", clusterHarness.VolumePublicURL()+"/"+fid)
	publicResp := framework.DoRequest(t, client, publicReq)
	_ = framework.ReadAllAndClose(t, publicResp)
	if publicResp.StatusCode != http.StatusOK {
		t.Fatalf("public PROPFIND expected passthrough 200, got %d", publicResp.StatusCode)
	}

	verifyResp := framework.ReadBytes(t, client, clusterHarness.VolumeAdminURL(), fid)
	verifyBody := framework.ReadAllAndClose(t, verifyResp)
	if verifyResp.StatusCode != http.StatusOK {
		t.Fatalf("verify GET expected 200, got %d", verifyResp.StatusCode)
	}
	if string(verifyBody) != "propfind-method-check" {
		t.Fatalf("PROPFIND should not mutate data, got %q", string(verifyBody))
	}
}

func TestUnsupportedMethodConnectParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P2())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(85)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, 124001, 0x03030303)
	client := framework.NewHTTPClient()
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, []byte("connect-method-check"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	adminReq := mustNewRequest(t, "CONNECT", clusterHarness.VolumeAdminURL()+"/"+fid)
	adminResp := framework.DoRequest(t, client, adminReq)
	_ = framework.ReadAllAndClose(t, adminResp)
	if adminResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("admin CONNECT expected 400, got %d", adminResp.StatusCode)
	}

	publicReq := mustNewRequest(t, "CONNECT", clusterHarness.VolumePublicURL()+"/"+fid)
	publicResp := framework.DoRequest(t, client, publicReq)
	_ = framework.ReadAllAndClose(t, publicResp)
	if publicResp.StatusCode != http.StatusOK {
		t.Fatalf("public CONNECT expected passthrough 200, got %d", publicResp.StatusCode)
	}

	verifyResp := framework.ReadBytes(t, client, clusterHarness.VolumeAdminURL(), fid)
	verifyBody := framework.ReadAllAndClose(t, verifyResp)
	if verifyResp.StatusCode != http.StatusOK {
		t.Fatalf("verify GET expected 200, got %d", verifyResp.StatusCode)
	}
	if string(verifyBody) != "connect-method-check" {
		t.Fatalf("CONNECT should not mutate data, got %q", string(verifyBody))
	}
}
