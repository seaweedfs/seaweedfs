package volume_server_http_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func TestReadModeProxyMissingLocalVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P1()
	profile.ReadMode = "proxy"
	clusterHarness := framework.StartDualVolumeCluster(t, profile)

	conn0, grpc0 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer conn0.Close()

	const volumeID = uint32(101)
	framework.AllocateVolume(t, grpc0, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 120001, 0x0102ABCD)
	payload := []byte("proxy-read-mode-forwarded-content")

	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(0), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	readURL := clusterHarness.VolumeAdminURL(1) + "/" + fid
	var finalBody []byte
	if !waitForHTTPStatus(t, client, readURL, http.StatusOK, 10*time.Second, func(resp *http.Response) {
		finalBody = framework.ReadAllAndClose(t, resp)
	}) {
		t.Fatalf("proxy read mode did not return 200 from non-owning volume server within deadline")
	}
	if string(finalBody) != string(payload) {
		t.Fatalf("proxy read mode body mismatch: got %q want %q", string(finalBody), string(payload))
	}
}

func TestReadModeRedirectMissingLocalVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P1()
	profile.ReadMode = "redirect"
	clusterHarness := framework.StartDualVolumeCluster(t, profile)

	conn0, grpc0 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer conn0.Close()

	const volumeID = uint32(102)
	framework.AllocateVolume(t, grpc0, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 120002, 0x0102DCBA)
	payload := []byte("redirect-read-mode-content")

	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(0), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	noRedirectClient := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	readURL := clusterHarness.VolumeAdminURL(1) + "/" + fid
	var redirectLocation string
	if !waitForHTTPStatus(t, noRedirectClient, readURL, http.StatusMovedPermanently, 10*time.Second, func(resp *http.Response) {
		redirectLocation = resp.Header.Get("Location")
		_ = framework.ReadAllAndClose(t, resp)
	}) {
		t.Fatalf("redirect read mode did not return 301 from non-owning volume server within deadline")
	}
	if redirectLocation == "" {
		t.Fatalf("redirect response missing Location header")
	}
	if !strings.Contains(redirectLocation, "proxied=true") {
		t.Fatalf("redirect Location should include proxied=true, got %q", redirectLocation)
	}

	followResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, redirectLocation))
	followBody := framework.ReadAllAndClose(t, followResp)
	if followResp.StatusCode != http.StatusOK {
		t.Fatalf("following redirect expected 200, got %d", followResp.StatusCode)
	}
	if string(followBody) != string(payload) {
		t.Fatalf("redirect-follow body mismatch: got %q want %q", string(followBody), string(payload))
	}
}

func TestReadModeLocalMissingLocalVolumeReturnsNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P1()
	profile.ReadMode = "local"
	clusterHarness := framework.StartDualVolumeCluster(t, profile)

	conn0, grpc0 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer conn0.Close()

	const volumeID = uint32(103)
	framework.AllocateVolume(t, grpc0, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 120003, 0x0102BEEF)
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(0), fid, []byte("local-read-mode-content"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	readResp := framework.ReadBytes(t, client, clusterHarness.VolumeAdminURL(1), fid)
	_ = framework.ReadAllAndClose(t, readResp)
	if readResp.StatusCode != http.StatusNotFound {
		t.Fatalf("local read mode expected 404 on non-owning server, got %d", readResp.StatusCode)
	}
}

func TestReadDeletedProxyModeOnMissingLocalVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P1()
	profile.ReadMode = "proxy"
	clusterHarness := framework.StartDualVolumeCluster(t, profile)

	conn0, grpc0 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer conn0.Close()

	const volumeID = uint32(104)
	framework.AllocateVolume(t, grpc0, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 120004, 0x0102CAFE)
	payload := []byte("proxy-readDeleted-missing-local-content")

	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(0), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	deleteResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodDelete, clusterHarness.VolumeAdminURL(0)+"/"+fid))
	_ = framework.ReadAllAndClose(t, deleteResp)
	if deleteResp.StatusCode != http.StatusAccepted {
		t.Fatalf("delete expected 202, got %d", deleteResp.StatusCode)
	}

	readURL := clusterHarness.VolumeAdminURL(1) + "/" + fid + "?readDeleted=true"
	var proxiedBody []byte
	if !waitForHTTPStatus(t, client, readURL, http.StatusOK, 10*time.Second, func(resp *http.Response) {
		proxiedBody = framework.ReadAllAndClose(t, resp)
	}) {
		t.Fatalf("proxy readDeleted path did not return 200 from non-owning volume server within deadline")
	}
	if string(proxiedBody) != string(payload) {
		t.Fatalf("proxy readDeleted body mismatch: got %q want %q", string(proxiedBody), string(payload))
	}
}

func TestReadDeletedRedirectModeDropsQueryParameterParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P1()
	profile.ReadMode = "redirect"
	clusterHarness := framework.StartDualVolumeCluster(t, profile)

	conn0, grpc0 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer conn0.Close()

	const volumeID = uint32(105)
	framework.AllocateVolume(t, grpc0, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 120005, 0x0102FACE)
	payload := []byte("redirect-readDeleted-query-drop-parity")

	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(0), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	deleteResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodDelete, clusterHarness.VolumeAdminURL(0)+"/"+fid))
	_ = framework.ReadAllAndClose(t, deleteResp)
	if deleteResp.StatusCode != http.StatusAccepted {
		t.Fatalf("delete expected 202, got %d", deleteResp.StatusCode)
	}

	noRedirectClient := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	redirectURL := clusterHarness.VolumeAdminURL(1) + "/" + fid + "?readDeleted=true"
	var location string
	if !waitForHTTPStatus(t, noRedirectClient, redirectURL, http.StatusMovedPermanently, 10*time.Second, func(resp *http.Response) {
		location = resp.Header.Get("Location")
		_ = framework.ReadAllAndClose(t, resp)
	}) {
		t.Fatalf("redirect readDeleted path did not return 301 from non-owning volume server within deadline")
	}
	if location == "" {
		t.Fatalf("redirect readDeleted response missing Location header")
	}
	if !strings.Contains(location, "proxied=true") {
		t.Fatalf("redirect readDeleted Location should include proxied=true, got %q", location)
	}
	if strings.Contains(location, "readDeleted=true") {
		t.Fatalf("redirect readDeleted Location should reflect current query-drop behavior, got %q", location)
	}

	followResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, location))
	_ = framework.ReadAllAndClose(t, followResp)
	if followResp.StatusCode != http.StatusNotFound {
		t.Fatalf("redirect-follow without readDeleted query expected 404 for deleted needle, got %d", followResp.StatusCode)
	}
}

func TestReadModeRedirectPreservesCollectionQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P1()
	profile.ReadMode = "redirect"
	clusterHarness := framework.StartDualVolumeCluster(t, profile)

	conn0, grpc0 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer conn0.Close()

	const volumeID = uint32(109)
	const collection = "redirect-collection"
	framework.AllocateVolume(t, grpc0, volumeID, collection)

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 120006, 0x0102F00D)
	payload := []byte("redirect-collection-preserve-content")

	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(0), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	noRedirectClient := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	redirectURL := clusterHarness.VolumeAdminURL(1) + "/" + fid + "?collection=" + collection
	var location string
	if !waitForHTTPStatus(t, noRedirectClient, redirectURL, http.StatusMovedPermanently, 10*time.Second, func(resp *http.Response) {
		location = resp.Header.Get("Location")
		_ = framework.ReadAllAndClose(t, resp)
	}) {
		t.Fatalf("redirect collection path did not return 301 from non-owning volume server within deadline")
	}
	if location == "" {
		t.Fatalf("redirect collection response missing Location header")
	}
	if !strings.Contains(location, "proxied=true") {
		t.Fatalf("redirect collection Location should include proxied=true, got %q", location)
	}
	if !strings.Contains(location, "collection="+collection) {
		t.Fatalf("redirect collection Location should preserve collection query, got %q", location)
	}

	followResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, location))
	followBody := framework.ReadAllAndClose(t, followResp)
	if followResp.StatusCode != http.StatusOK {
		t.Fatalf("redirect-follow expected 200, got %d", followResp.StatusCode)
	}
	if string(followBody) != string(payload) {
		t.Fatalf("redirect-follow body mismatch: got %q want %q", string(followBody), string(payload))
	}
}

func waitForHTTPStatus(t testing.TB, client *http.Client, url string, expectedStatus int, timeout time.Duration, onMatch func(resp *http.Response)) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, url))
		if resp.StatusCode == expectedStatus {
			onMatch(resp)
			return true
		}
		_ = framework.ReadAllAndClose(t, resp)
		time.Sleep(200 * time.Millisecond)
	}

	return false
}
