package volume_server_http_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

func gzipData(t testing.TB, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func gunzipData(t testing.TB, data []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("gunzip new reader: %v", err)
	}
	defer zr.Close()
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gunzip read: %v", err)
	}
	return out
}

func TestCompressedReadAcceptEncodingMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(103)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 772007, 0x708192A3)
	plainPayload := []byte("compressed-read-accept-encoding-matrix-content-compressed-read-accept-encoding-matrix-content")
	compressedPayload := gzipData(t, plainPayload)

	uploadReq, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+fid, bytes.NewReader(compressedPayload))
	if err != nil {
		t.Fatalf("create compressed upload request: %v", err)
	}
	uploadReq.Header.Set("Content-Type", "text/plain")
	uploadReq.Header.Set("Content-Encoding", "gzip")
	uploadResp := framework.DoRequest(t, client, uploadReq)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("compressed upload expected 201, got %d", uploadResp.StatusCode)
	}

	gzipReadReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	gzipReadReq.Header.Set("Accept-Encoding", "gzip")
	gzipReadResp := framework.DoRequest(t, client, gzipReadReq)
	gzipReadBody := framework.ReadAllAndClose(t, gzipReadResp)
	if gzipReadResp.StatusCode != http.StatusOK {
		t.Fatalf("gzip-accepted read expected 200, got %d", gzipReadResp.StatusCode)
	}
	if gzipReadResp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("gzip-accepted read expected Content-Encoding=gzip, got %q", gzipReadResp.Header.Get("Content-Encoding"))
	}
	if string(gunzipData(t, gzipReadBody)) != string(plainPayload) {
		t.Fatalf("gzip-accepted read body mismatch after gunzip")
	}

	identityReadReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	identityReadReq.Header.Set("Accept-Encoding", "identity")
	identityReadResp := framework.DoRequest(t, client, identityReadReq)
	identityReadBody := framework.ReadAllAndClose(t, identityReadResp)
	if identityReadResp.StatusCode != http.StatusOK {
		t.Fatalf("identity read expected 200, got %d", identityReadResp.StatusCode)
	}
	if identityReadResp.Header.Get("Content-Encoding") != "" {
		t.Fatalf("identity read expected no Content-Encoding header, got %q", identityReadResp.Header.Get("Content-Encoding"))
	}
	if string(identityReadBody) != string(plainPayload) {
		t.Fatalf("identity read body mismatch: got %q want %q", string(identityReadBody), string(plainPayload))
	}
}
