package volume_server_http_test

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

func TestJWTAuthForWriteAndRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P3()
	clusterHarness := framework.StartSingleVolumeCluster(t, profile)
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(51)
	const needleID = uint64(123456)
	const cookie = uint32(0xABCDEF12)

	framework.AllocateVolume(t, grpcClient, volumeID, "")
	fid := framework.NewFileID(volumeID, needleID, cookie)
	payload := []byte("jwt-protected-content")
	client := framework.NewHTTPClient()

	unauthWrite := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, payload)
	unauthWriteResp := framework.DoRequest(t, client, unauthWrite)
	_ = framework.ReadAllAndClose(t, unauthWriteResp)
	if unauthWriteResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized write expected 401, got %d", unauthWriteResp.StatusCode)
	}

	invalidWrite := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, payload)
	invalidWrite.Header.Set("Authorization", "Bearer invalid")
	invalidWriteResp := framework.DoRequest(t, client, invalidWrite)
	_ = framework.ReadAllAndClose(t, invalidWriteResp)
	if invalidWriteResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("invalid write token expected 401, got %d", invalidWriteResp.StatusCode)
	}

	writeToken := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTSigningKey)), 60, fid)
	authWrite := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, payload)
	authWrite.Header.Set("Authorization", "Bearer "+string(writeToken))
	authWriteResp := framework.DoRequest(t, client, authWrite)
	_ = framework.ReadAllAndClose(t, authWriteResp)
	if authWriteResp.StatusCode != http.StatusCreated {
		t.Fatalf("authorized write expected 201, got %d", authWriteResp.StatusCode)
	}

	unauthReadReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	unauthReadResp := framework.DoRequest(t, client, unauthReadReq)
	_ = framework.ReadAllAndClose(t, unauthReadResp)
	if unauthReadResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized read expected 401, got %d", unauthReadResp.StatusCode)
	}

	readToken := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTReadKey)), 60, fid)
	authReadReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	authReadReq.Header.Set("Authorization", "Bearer "+string(readToken))
	authReadResp := framework.DoRequest(t, client, authReadReq)
	authReadBody := framework.ReadAllAndClose(t, authReadResp)
	if authReadResp.StatusCode != http.StatusOK {
		t.Fatalf("authorized read expected 200, got %d", authReadResp.StatusCode)
	}
	if string(authReadBody) != string(payload) {
		t.Fatalf("authorized read content mismatch: got %q want %q", string(authReadBody), string(payload))
	}
}

func newUploadRequest(t testing.TB, url string, payload []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("create upload request %s: %v", url, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	return req
}
