package volume_server_http_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
)

func TestAdminStatusAndHealthz(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	statusReq, err := http.NewRequest(http.MethodGet, cluster.VolumeAdminURL()+"/status", nil)
	if err != nil {
		t.Fatalf("create status request: %v", err)
	}
	statusReq.Header.Set(request_id.AmzRequestIDHeader, "test-request-id-1")

	statusResp := framework.DoRequest(t, client, statusReq)
	statusBody := framework.ReadAllAndClose(t, statusResp)

	if statusResp.StatusCode != http.StatusOK {
		t.Fatalf("expected /status code 200, got %d, body: %s", statusResp.StatusCode, string(statusBody))
	}
	if got := statusResp.Header.Get(request_id.AmzRequestIDHeader); got != "test-request-id-1" {
		t.Fatalf("expected echoed request id, got %q", got)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(statusBody, &payload); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	for _, field := range []string{"Version", "DiskStatuses", "Volumes"} {
		if _, found := payload[field]; !found {
			t.Fatalf("status payload missing field %q", field)
		}
	}

	healthResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/healthz"))
	_ = framework.ReadAllAndClose(t, healthResp)
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected /healthz code 200, got %d", healthResp.StatusCode)
	}

	uiResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/ui/index.html"))
	uiBody := framework.ReadAllAndClose(t, uiResp)
	if uiResp.StatusCode != http.StatusOK {
		t.Fatalf("expected /ui/index.html code 200, got %d, body: %s", uiResp.StatusCode, string(uiBody))
	}
	if !strings.Contains(strings.ToLower(string(uiBody)), "volume") {
		t.Fatalf("ui page does not look like volume status page")
	}
}

func TestOptionsMethodsByPort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P2())
	client := framework.NewHTTPClient()

	adminResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodOptions, cluster.VolumeAdminURL()+"/"))
	_ = framework.ReadAllAndClose(t, adminResp)
	if adminResp.StatusCode != http.StatusOK {
		t.Fatalf("admin OPTIONS expected 200, got %d", adminResp.StatusCode)
	}
	adminAllowed := adminResp.Header.Get("Access-Control-Allow-Methods")
	for _, expected := range []string{"PUT", "POST", "GET", "DELETE", "OPTIONS"} {
		if !strings.Contains(adminAllowed, expected) {
			t.Fatalf("admin allow methods missing %q, got %q", expected, adminAllowed)
		}
	}

	publicResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodOptions, cluster.VolumePublicURL()+"/"))
	_ = framework.ReadAllAndClose(t, publicResp)
	if publicResp.StatusCode != http.StatusOK {
		t.Fatalf("public OPTIONS expected 200, got %d", publicResp.StatusCode)
	}
	publicAllowed := publicResp.Header.Get("Access-Control-Allow-Methods")
	if !strings.Contains(publicAllowed, "GET") || !strings.Contains(publicAllowed, "OPTIONS") {
		t.Fatalf("public allow methods expected GET and OPTIONS, got %q", publicAllowed)
	}
	if strings.Contains(publicAllowed, "POST") {
		t.Fatalf("public allow methods should not include POST, got %q", publicAllowed)
	}
}

func TestOptionsWithOriginIncludesCorsHeaders(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P2())
	client := framework.NewHTTPClient()

	adminReq := mustNewRequest(t, http.MethodOptions, cluster.VolumeAdminURL()+"/")
	adminReq.Header.Set("Origin", "https://example.com")
	adminResp := framework.DoRequest(t, client, adminReq)
	_ = framework.ReadAllAndClose(t, adminResp)
	if adminResp.StatusCode != http.StatusOK {
		t.Fatalf("admin OPTIONS expected 200, got %d", adminResp.StatusCode)
	}
	if adminResp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("admin OPTIONS expected Access-Control-Allow-Origin=*, got %q", adminResp.Header.Get("Access-Control-Allow-Origin"))
	}
	if adminResp.Header.Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatalf("admin OPTIONS expected Access-Control-Allow-Credentials=true, got %q", adminResp.Header.Get("Access-Control-Allow-Credentials"))
	}

	publicReq := mustNewRequest(t, http.MethodOptions, cluster.VolumePublicURL()+"/")
	publicReq.Header.Set("Origin", "https://example.com")
	publicResp := framework.DoRequest(t, client, publicReq)
	_ = framework.ReadAllAndClose(t, publicResp)
	if publicResp.StatusCode != http.StatusOK {
		t.Fatalf("public OPTIONS expected 200, got %d", publicResp.StatusCode)
	}
	if publicResp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("public OPTIONS expected Access-Control-Allow-Origin=*, got %q", publicResp.Header.Get("Access-Control-Allow-Origin"))
	}
	if publicResp.Header.Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatalf("public OPTIONS expected Access-Control-Allow-Credentials=true, got %q", publicResp.Header.Get("Access-Control-Allow-Credentials"))
	}
}

func mustNewRequest(t testing.TB, method, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("create request %s %s: %v", method, url, err)
	}
	return req
}
