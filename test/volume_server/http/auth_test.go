package volume_server_http_test

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
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

func TestJWTAuthRejectsFidMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P3()
	clusterHarness := framework.StartSingleVolumeCluster(t, profile)
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(52)
	const needleID = uint64(223344)
	const cookie = uint32(0x10203040)
	const otherNeedleID = uint64(223345)
	const otherCookie = uint32(0x50607080)
	const wrongCookie = uint32(0x10203041)

	framework.AllocateVolume(t, grpcClient, volumeID, "")
	fid := framework.NewFileID(volumeID, needleID, cookie)
	otherFid := framework.NewFileID(volumeID, otherNeedleID, otherCookie)
	payload := []byte("jwt-fid-mismatch-content")
	client := framework.NewHTTPClient()

	writeTokenForOtherFid := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTSigningKey)), 60, otherFid)
	mismatchedWrite := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, payload)
	mismatchedWrite.Header.Set("Authorization", "Bearer "+string(writeTokenForOtherFid))
	mismatchedWriteResp := framework.DoRequest(t, client, mismatchedWrite)
	_ = framework.ReadAllAndClose(t, mismatchedWriteResp)
	if mismatchedWriteResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("write with mismatched fid token expected 401, got %d", mismatchedWriteResp.StatusCode)
	}

	wrongCookieFid := framework.NewFileID(volumeID, needleID, wrongCookie)
	writeTokenWrongCookie := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTSigningKey)), 60, wrongCookieFid)
	wrongCookieWrite := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, payload)
	wrongCookieWrite.Header.Set("Authorization", "Bearer "+string(writeTokenWrongCookie))
	wrongCookieWriteResp := framework.DoRequest(t, client, wrongCookieWrite)
	_ = framework.ReadAllAndClose(t, wrongCookieWriteResp)
	if wrongCookieWriteResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("write with wrong-cookie fid token expected 401, got %d", wrongCookieWriteResp.StatusCode)
	}

	writeTokenForFid := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTSigningKey)), 60, fid)
	validWrite := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, payload)
	validWrite.Header.Set("Authorization", "Bearer "+string(writeTokenForFid))
	validWriteResp := framework.DoRequest(t, client, validWrite)
	_ = framework.ReadAllAndClose(t, validWriteResp)
	if validWriteResp.StatusCode != http.StatusCreated {
		t.Fatalf("authorized write expected 201, got %d", validWriteResp.StatusCode)
	}

	readTokenForOtherFid := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTReadKey)), 60, otherFid)
	mismatchedReadReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	mismatchedReadReq.Header.Set("Authorization", "Bearer "+string(readTokenForOtherFid))
	mismatchedReadResp := framework.DoRequest(t, client, mismatchedReadReq)
	_ = framework.ReadAllAndClose(t, mismatchedReadResp)
	if mismatchedReadResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("read with mismatched fid token expected 401, got %d", mismatchedReadResp.StatusCode)
	}

	readTokenWrongCookie := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTReadKey)), 60, wrongCookieFid)
	wrongCookieReadReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	wrongCookieReadReq.Header.Set("Authorization", "Bearer "+string(readTokenWrongCookie))
	wrongCookieReadResp := framework.DoRequest(t, client, wrongCookieReadReq)
	_ = framework.ReadAllAndClose(t, wrongCookieReadResp)
	if wrongCookieReadResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("read with wrong-cookie fid token expected 401, got %d", wrongCookieReadResp.StatusCode)
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

func TestJWTAuthRejectsExpiredTokens(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P3()
	clusterHarness := framework.StartSingleVolumeCluster(t, profile)
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(53)
	const needleID = uint64(334455)
	const cookie = uint32(0x22334455)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, needleID, cookie)
	payload := []byte("expired-token-content")
	client := framework.NewHTTPClient()

	expiredWriteToken := mustGenExpiredToken(t, []byte(profile.JWTSigningKey), fid)
	writeReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, payload)
	writeReq.Header.Set("Authorization", "Bearer "+expiredWriteToken)
	writeResp := framework.DoRequest(t, client, writeReq)
	_ = framework.ReadAllAndClose(t, writeResp)
	if writeResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired write token expected 401, got %d", writeResp.StatusCode)
	}

	// Seed data with a valid token so read auth path can be exercised against existing content.
	validWriteToken := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTSigningKey)), 60, fid)
	validWriteReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, payload)
	validWriteReq.Header.Set("Authorization", "Bearer "+string(validWriteToken))
	validWriteResp := framework.DoRequest(t, client, validWriteReq)
	_ = framework.ReadAllAndClose(t, validWriteResp)
	if validWriteResp.StatusCode != http.StatusCreated {
		t.Fatalf("valid write expected 201, got %d", validWriteResp.StatusCode)
	}

	expiredReadToken := mustGenExpiredToken(t, []byte(profile.JWTReadKey), fid)
	readReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	readReq.Header.Set("Authorization", "Bearer "+expiredReadToken)
	readResp := framework.DoRequest(t, client, readReq)
	_ = framework.ReadAllAndClose(t, readResp)
	if readResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired read token expected 401, got %d", readResp.StatusCode)
	}
}

func TestJWTAuthViaQueryParamAndCookie(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P3()
	clusterHarness := framework.StartSingleVolumeCluster(t, profile)
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(54)
	const needleID = uint64(445566)
	const cookie = uint32(0x31415926)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, needleID, cookie)
	payload := []byte("jwt-query-cookie-content")
	client := framework.NewHTTPClient()

	writeToken := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTSigningKey)), 60, fid)
	writeReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid+"?jwt="+string(writeToken), payload)
	writeResp := framework.DoRequest(t, client, writeReq)
	_ = framework.ReadAllAndClose(t, writeResp)
	if writeResp.StatusCode != http.StatusCreated {
		t.Fatalf("query-jwt write expected 201, got %d", writeResp.StatusCode)
	}

	readToken := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTReadKey)), 60, fid)
	readReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	readReq.AddCookie(&http.Cookie{Name: "AT", Value: string(readToken)})
	readResp := framework.DoRequest(t, client, readReq)
	readBody := framework.ReadAllAndClose(t, readResp)
	if readResp.StatusCode != http.StatusOK {
		t.Fatalf("cookie-jwt read expected 200, got %d", readResp.StatusCode)
	}
	if string(readBody) != string(payload) {
		t.Fatalf("cookie-jwt read body mismatch: got %q want %q", string(readBody), string(payload))
	}
}

func TestJWTTokenSourcePrecedenceQueryOverHeader(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P3()
	clusterHarness := framework.StartSingleVolumeCluster(t, profile)
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(55)
	const needleID = uint64(556677)
	const cookie = uint32(0x99887766)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, needleID, cookie)
	otherFID := framework.NewFileID(volumeID, needleID+1, cookie+1)
	payload := []byte("jwt-precedence-content")
	client := framework.NewHTTPClient()

	validWriteToken := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTSigningKey)), 60, fid)
	invalidWriteQueryToken := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTSigningKey)), 60, otherFID)
	writeReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid+"?jwt="+string(invalidWriteQueryToken), payload)
	writeReq.Header.Set("Authorization", "Bearer "+string(validWriteToken))
	writeResp := framework.DoRequest(t, client, writeReq)
	_ = framework.ReadAllAndClose(t, writeResp)
	if writeResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("query token should take precedence over header token for write, expected 401 got %d", writeResp.StatusCode)
	}

	// Seed data with valid write token, then exercise read precedence.
	seedWriteReq := newUploadRequest(t, clusterHarness.VolumeAdminURL()+"/"+fid, payload)
	seedWriteReq.Header.Set("Authorization", "Bearer "+string(validWriteToken))
	seedWriteResp := framework.DoRequest(t, client, seedWriteReq)
	_ = framework.ReadAllAndClose(t, seedWriteResp)
	if seedWriteResp.StatusCode != http.StatusCreated {
		t.Fatalf("seed write expected 201, got %d", seedWriteResp.StatusCode)
	}

	validReadToken := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTReadKey)), 60, fid)
	invalidReadQueryToken := security.GenJwtForVolumeServer(security.SigningKey([]byte(profile.JWTReadKey)), 60, otherFID)
	readReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid+"?jwt="+string(invalidReadQueryToken))
	readReq.Header.Set("Authorization", "Bearer "+string(validReadToken))
	readResp := framework.DoRequest(t, client, readReq)
	_ = framework.ReadAllAndClose(t, readResp)
	if readResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("query token should take precedence over header token for read, expected 401 got %d", readResp.StatusCode)
	}
}

func mustGenExpiredToken(t testing.TB, key []byte, fid string) string {
	t.Helper()
	claims := security.SeaweedFileIdClaims{
		Fid: fid,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Minute)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("sign expired token: %v", err)
	}
	return signed
}
