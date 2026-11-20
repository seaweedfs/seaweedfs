package volume_server_http_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
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

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
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

	// A range is over the decoded bytes, so it is served decoded even when gzip is accepted.
	rangeReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid)
	rangeReq.Header.Set("Accept-Encoding", "gzip")
	rangeReq.Header.Set("Range", "bytes=40-79")
	rangeResp := framework.DoRequest(t, client, rangeReq)
	rangeBody := framework.ReadAllAndClose(t, rangeResp)
	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("gzip-accepted range read expected 206, got %d", rangeResp.StatusCode)
	}
	if rangeResp.Header.Get("Content-Encoding") != "" {
		t.Fatalf("gzip-accepted range read expected no Content-Encoding header, got %q", rangeResp.Header.Get("Content-Encoding"))
	}
	if string(rangeBody) != string(plainPayload[40:80]) {
		t.Fatalf("gzip-accepted range read body mismatch: got %q want %q", string(rangeBody), string(plainPayload[40:80]))
	}

	// HEAD reports the length the matching GET would send.
	for _, tc := range []struct {
		acceptEncoding, rangeHeader, wantEncoding string
		wantLength                                int
	}{
		{"identity", "", "", len(plainPayload)},
		{"gzip", "", "gzip", len(compressedPayload)},
		{"gzip", "bytes=40-79", "", len(plainPayload)},
	} {
		headReq := mustNewRequest(t, http.MethodHead, clusterHarness.VolumeAdminURL()+"/"+fid)
		headReq.Header.Set("Accept-Encoding", tc.acceptEncoding)
		if tc.rangeHeader != "" {
			headReq.Header.Set("Range", tc.rangeHeader)
		}
		headResp := framework.DoRequest(t, client, headReq)
		_ = framework.ReadAllAndClose(t, headResp)
		if headResp.Header.Get("Content-Encoding") != tc.wantEncoding {
			t.Fatalf("HEAD %+v: Content-Encoding %q, want %q", tc, headResp.Header.Get("Content-Encoding"), tc.wantEncoding)
		}
		if headResp.Header.Get("Content-Length") != strconv.Itoa(tc.wantLength) {
			t.Fatalf("HEAD %+v: Content-Length %q, want %d", tc, headResp.Header.Get("Content-Length"), tc.wantLength)
		}
	}
}
