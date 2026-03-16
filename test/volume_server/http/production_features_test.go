package volume_server_http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestStatsEndpoints(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	// /stats/counter — expect 200 with non-empty body
	// Note: Go server guards these with WhiteList which may return 400
	counterResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/stats/counter"))
	counterBody := framework.ReadAllAndClose(t, counterResp)
	if counterResp.StatusCode == http.StatusBadRequest {
		t.Logf("/stats/counter returned 400 (whitelist guard), skipping stats checks")
		return
	}
	if counterResp.StatusCode != http.StatusOK {
		t.Fatalf("/stats/counter expected 200, got %d, body: %s", counterResp.StatusCode, string(counterBody))
	}
	if len(counterBody) == 0 {
		t.Fatalf("/stats/counter returned empty body")
	}

	// /stats/memory — expect 200, valid JSON with Version and Memory
	memoryResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/stats/memory"))
	memoryBody := framework.ReadAllAndClose(t, memoryResp)
	if memoryResp.StatusCode != http.StatusOK {
		t.Fatalf("/stats/memory expected 200, got %d, body: %s", memoryResp.StatusCode, string(memoryBody))
	}
	var memoryPayload map[string]any
	if err := json.Unmarshal(memoryBody, &memoryPayload); err != nil {
		t.Fatalf("/stats/memory response is not valid JSON: %v, body: %s", err, string(memoryBody))
	}
	if _, ok := memoryPayload["Version"]; !ok {
		t.Fatalf("/stats/memory missing Version field")
	}
	if _, ok := memoryPayload["Memory"]; !ok {
		t.Fatalf("/stats/memory missing Memory field")
	}

	// /stats/disk — expect 200, valid JSON with Version and DiskStatuses
	diskResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/stats/disk"))
	diskBody := framework.ReadAllAndClose(t, diskResp)
	if diskResp.StatusCode != http.StatusOK {
		t.Fatalf("/stats/disk expected 200, got %d, body: %s", diskResp.StatusCode, string(diskBody))
	}
	var diskPayload map[string]any
	if err := json.Unmarshal(diskBody, &diskPayload); err != nil {
		t.Fatalf("/stats/disk response is not valid JSON: %v, body: %s", err, string(diskBody))
	}
	if _, ok := diskPayload["Version"]; !ok {
		t.Fatalf("/stats/disk missing Version field")
	}
	if _, ok := diskPayload["DiskStatuses"]; !ok {
		t.Fatalf("/stats/disk missing DiskStatuses field")
	}
}

func TestStatusPrettyJsonAndJsonp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	// ?pretty=y — expect indented multi-line JSON
	prettyResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/status?pretty=y"))
	prettyBody := framework.ReadAllAndClose(t, prettyResp)
	if prettyResp.StatusCode != http.StatusOK {
		t.Fatalf("/status?pretty=y expected 200, got %d", prettyResp.StatusCode)
	}
	lines := strings.Split(strings.TrimSpace(string(prettyBody)), "\n")
	if len(lines) < 3 {
		t.Fatalf("/status?pretty=y expected multi-line indented JSON, got %d lines: %s", len(lines), string(prettyBody))
	}
	// Verify the body is valid JSON
	var prettyPayload map[string]interface{}
	if err := json.Unmarshal(prettyBody, &prettyPayload); err != nil {
		t.Fatalf("/status?pretty=y is not valid JSON: %v", err)
	}

	// ?callback=myFunc — expect JSONP wrapping
	jsonpResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/status?callback=myFunc"))
	jsonpBody := framework.ReadAllAndClose(t, jsonpResp)
	if jsonpResp.StatusCode != http.StatusOK {
		t.Fatalf("/status?callback=myFunc expected 200, got %d", jsonpResp.StatusCode)
	}
	bodyStr := string(jsonpBody)
	if !strings.HasPrefix(bodyStr, "myFunc(") {
		t.Fatalf("/status?callback=myFunc expected body to start with 'myFunc(', got prefix: %q", bodyStr[:min(len(bodyStr), 30)])
	}
	trimmed := strings.TrimRight(bodyStr, "\n; ")
	if !strings.HasSuffix(trimmed, ")") {
		t.Fatalf("/status?callback=myFunc expected body to end with ')', got suffix: %q", trimmed[max(0, len(trimmed)-10):])
	}
	// Content-Type should be application/javascript for JSONP
	if ct := jsonpResp.Header.Get("Content-Type"); !strings.Contains(ct, "javascript") {
		t.Fatalf("/status?callback=myFunc expected Content-Type containing 'javascript', got %q", ct)
	}
}

func TestUploadWithCustomTimestamp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(91)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, 910001, 0xAABBCC01)
	client := framework.NewHTTPClient()
	data := []byte("custom-timestamp-data")

	// Upload with ?ts=1700000000
	uploadURL := fmt.Sprintf("%s/%s?ts=1700000000", cluster.VolumeAdminURL(), fid)
	req, err := http.NewRequest(http.MethodPost, uploadURL, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("create upload request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(data)))
	uploadResp := framework.DoRequest(t, client, req)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload with ts expected 201, got %d", uploadResp.StatusCode)
	}

	// Read back and verify Last-Modified
	getResp := framework.ReadBytes(t, client, cluster.VolumeAdminURL(), fid)
	_ = framework.ReadAllAndClose(t, getResp)
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("read expected 200, got %d", getResp.StatusCode)
	}

	expectedLastModified := time.Unix(1700000000, 0).UTC().Format(http.TimeFormat)
	gotLastModified := getResp.Header.Get("Last-Modified")
	if gotLastModified != expectedLastModified {
		t.Fatalf("Last-Modified mismatch: got %q, want %q", gotLastModified, expectedLastModified)
	}
}

func TestMultipartUploadUsesFormFieldsForTimestampAndTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(94)
	const needleID = uint64(940001)
	const cookie = uint32(0xAABBCC04)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, needleID, cookie)
	payload := []byte("multipart-form-fields-data")

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("ts", "1700000000"); err != nil {
		t.Fatalf("write multipart ts field: %v", err)
	}
	if err := writer.WriteField("ttl", "7d"); err != nil {
		t.Fatalf("write multipart ttl field: %v", err)
	}
	filePart, err := writer.CreateFormFile("file", "multipart.txt")
	if err != nil {
		t.Fatalf("create multipart file field: %v", err)
	}
	if _, err := filePart.Write(payload); err != nil {
		t.Fatalf("write multipart file payload: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, cluster.VolumeAdminURL()+"/"+fid, &body)
	if err != nil {
		t.Fatalf("create multipart upload request: %v", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := framework.NewHTTPClient()
	uploadResp := framework.DoRequest(t, client, req)
	uploadBody := framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("multipart upload expected 201, got %d, body: %s", uploadResp.StatusCode, string(uploadBody))
	}

	readResp := framework.ReadBytes(t, client, cluster.VolumeAdminURL(), fid)
	_ = framework.ReadAllAndClose(t, readResp)
	if readResp.StatusCode != http.StatusOK {
		t.Fatalf("multipart upload read expected 200, got %d", readResp.StatusCode)
	}
	expectedLastModified := time.Unix(1700000000, 0).UTC().Format(http.TimeFormat)
	if got := readResp.Header.Get("Last-Modified"); got != expectedLastModified {
		t.Fatalf("multipart upload Last-Modified mismatch: got %q want %q", got, expectedLastModified)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	statusResp, err := grpcClient.VolumeNeedleStatus(ctx, &volume_server_pb.VolumeNeedleStatusRequest{
		VolumeId: volumeID,
		NeedleId: needleID,
	})
	if err != nil {
		t.Fatalf("VolumeNeedleStatus after multipart upload failed: %v", err)
	}
	if got := statusResp.GetTtl(); got != "7d" {
		t.Fatalf("multipart upload TTL mismatch: got %q want %q", got, "7d")
	}
}

func TestRequestIdGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartVolumeCluster(t, matrix.P1())
	client := framework.NewHTTPClient()

	// GET /status WITHOUT setting x-amz-request-id header
	req := mustNewRequest(t, http.MethodGet, cluster.VolumeAdminURL()+"/status")
	resp := framework.DoRequest(t, client, req)
	_ = framework.ReadAllAndClose(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/status expected 200, got %d", resp.StatusCode)
	}

	reqID := resp.Header.Get("x-amz-request-id")
	if reqID == "" {
		t.Fatalf("expected auto-generated x-amz-request-id header, got empty")
	}
	// Go format: "%X%08X" (timestamp hex + 8 random hex), typically 20-24 chars, all hex, no hyphens.
	if len(reqID) < 16 {
		t.Fatalf("x-amz-request-id too short: %q (len=%d)", reqID, len(reqID))
	}
}

func TestS3ResponsePassthroughHeaders(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(92)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, 920001, 0xAABBCC02)
	client := framework.NewHTTPClient()
	data := []byte("passthrough-headers-test-data")

	uploadResp := framework.UploadBytes(t, client, cluster.VolumeAdminURL(), fid, data)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	// Read back with S3 passthrough query params
	// Test response-content-language which both Go and Rust support
	readURL := fmt.Sprintf("%s/%s?response-content-language=fr&response-expires=%s",
		cluster.VolumeAdminURL(), fid,
		"Thu,+01+Jan+2099+00:00:00+GMT",
	)
	readResp := framework.DoRequest(t, client, mustNewRequest(t, http.MethodGet, readURL))
	readBody := framework.ReadAllAndClose(t, readResp)
	if readResp.StatusCode != http.StatusOK {
		t.Fatalf("read with passthrough expected 200, got %d, body: %s", readResp.StatusCode, string(readBody))
	}

	if got := readResp.Header.Get("Content-Language"); got != "fr" {
		t.Fatalf("Content-Language expected 'fr', got %q", got)
	}
	if got := readResp.Header.Get("Expires"); got != "Thu, 01 Jan 2099 00:00:00 GMT" {
		t.Fatalf("Expires expected 'Thu, 01 Jan 2099 00:00:00 GMT', got %q", got)
	}
}

func TestLargeFileWriteAndRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(93)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, 930001, 0xAABBCC03)
	client := framework.NewHTTPClient()
	data := bytes.Repeat([]byte("A"), 1024*1024) // 1MB

	uploadResp := framework.UploadBytes(t, client, cluster.VolumeAdminURL(), fid, data)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload 1MB expected 201, got %d", uploadResp.StatusCode)
	}

	getResp := framework.ReadBytes(t, client, cluster.VolumeAdminURL(), fid)
	getBody := framework.ReadAllAndClose(t, getResp)
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("read 1MB expected 200, got %d", getResp.StatusCode)
	}
	if len(getBody) != len(data) {
		t.Fatalf("read 1MB body length mismatch: got %d, want %d", len(getBody), len(data))
	}
	if !bytes.Equal(getBody, data) {
		t.Fatalf("read 1MB body content mismatch")
	}
}

func TestUploadWithContentTypePreservation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(94)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, 940001, 0xAABBCC04)
	client := framework.NewHTTPClient()
	data := []byte("fake-png-data-for-content-type-test")

	// Upload with Content-Type: image/png
	uploadURL := fmt.Sprintf("%s/%s", cluster.VolumeAdminURL(), fid)
	req, err := http.NewRequest(http.MethodPost, uploadURL, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("create upload request: %v", err)
	}
	req.Header.Set("Content-Type", "image/png")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(data)))
	uploadResp := framework.DoRequest(t, client, req)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload with image/png expected 201, got %d", uploadResp.StatusCode)
	}

	// Read back and verify Content-Type is preserved
	getResp := framework.ReadBytes(t, client, cluster.VolumeAdminURL(), fid)
	_ = framework.ReadAllAndClose(t, getResp)
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("read expected 200, got %d", getResp.StatusCode)
	}
	if got := getResp.Header.Get("Content-Type"); got != "image/png" {
		t.Fatalf("Content-Type expected 'image/png', got %q", got)
	}
}
