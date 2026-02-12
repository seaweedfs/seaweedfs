package volume_server_http_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func TestReadPassthroughHeadersAndDownloadDisposition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(96)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fullFileID := framework.NewFileID(volumeID, 661122, 0x55667788)
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fullFileID, []byte("passthrough-header-content"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	parts := strings.SplitN(fullFileID, ",", 2)
	if len(parts) != 2 {
		t.Fatalf("unexpected file id format: %q", fullFileID)
	}
	fidOnly := parts[1]

	url := fmt.Sprintf("%s/%d/%s/%s?response-content-type=text/plain&response-cache-control=no-store&dl=true",
		clusterHarness.VolumeAdminURL(),
		volumeID,
		fidOnly,
		"report.txt",
	)
	resp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, url))
	_ = framework.ReadAllAndClose(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("passthrough read expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "text/plain" {
		t.Fatalf("response-content-type override mismatch: %q", resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("response-cache-control override mismatch: %q", resp.Header.Get("Cache-Control"))
	}
	contentDisposition := resp.Header.Get("Content-Disposition")
	if !strings.Contains(contentDisposition, "attachment") || !strings.Contains(contentDisposition, "report.txt") {
		t.Fatalf("download disposition header mismatch: %q", contentDisposition)
	}
}

func TestStaticAssetEndpoints(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	faviconResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/favicon.ico"))
	_ = framework.ReadAllAndClose(t, faviconResp)
	if faviconResp.StatusCode != http.StatusOK {
		t.Fatalf("/favicon.ico expected 200, got %d", faviconResp.StatusCode)
	}

	staticResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/seaweedfsstatic/seaweed50x50.png"))
	_ = framework.ReadAllAndClose(t, staticResp)
	if staticResp.StatusCode != http.StatusOK {
		t.Fatalf("/seaweedfsstatic/seaweed50x50.png expected 200, got %d", staticResp.StatusCode)
	}
}
