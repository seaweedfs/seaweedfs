package volume_server_http_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

type pausableReader struct {
	remaining  int64
	pauseAfter int64
	paused     bool
	unblock    <-chan struct{}
}

func (r *pausableReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if !r.paused && r.pauseAfter > 0 {
		n := int64(len(p))
		if n > r.pauseAfter {
			n = r.pauseAfter
		}
		for i := int64(0); i < n; i++ {
			p[i] = 'a'
		}
		r.remaining -= n
		r.pauseAfter -= n
		if r.pauseAfter == 0 {
			r.paused = true
		}
		return int(n), nil
	}
	if r.paused {
		<-r.unblock
		r.paused = false
	}
	n := int64(len(p))
	if n > r.remaining {
		n = r.remaining
	}
	for i := int64(0); i < n; i++ {
		p[i] = 'b'
	}
	r.remaining -= n
	return int(n), nil
}

func TestUploadLimitTimeoutAndReplicateBypass(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P8())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(98)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	const blockedUploadSize = 2 * 1024 * 1024 // over 1MB P8 upload limit

	unblockFirstUpload := make(chan struct{})
	firstUploadDone := make(chan error, 1)
	firstFID := framework.NewFileID(volumeID, 880001, 0x1A2B3C4D)
	go func() {
		req, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+firstFID, &pausableReader{
			remaining:  blockedUploadSize,
			pauseAfter: 1,
			unblock:    unblockFirstUpload,
		})
		if err != nil {
			firstUploadDone <- err
			return
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.ContentLength = blockedUploadSize

		resp, err := (&http.Client{}).Do(req)
		if resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		firstUploadDone <- err
	}()

	// Give the first upload time to pass limit checks and block in body processing.
	time.Sleep(300 * time.Millisecond)

	replicateFID := framework.NewFileID(volumeID, 880002, 0x5E6F7A8B)
	replicateReq, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+replicateFID+"?type=replicate", bytes.NewReader([]byte("replicate")))
	if err != nil {
		t.Fatalf("create replicate request: %v", err)
	}
	replicateReq.Header.Set("Content-Type", "application/octet-stream")
	replicateReq.ContentLength = int64(len("replicate"))
	replicateResp, err := framework.NewHTTPClient().Do(replicateReq)
	if err != nil {
		t.Fatalf("replicate request failed: %v", err)
	}
	_ = framework.ReadAllAndClose(t, replicateResp)
	if replicateResp.StatusCode != http.StatusCreated {
		t.Fatalf("replicate request expected 201 bypassing limit, got %d", replicateResp.StatusCode)
	}

	normalFID := framework.NewFileID(volumeID, 880003, 0x9C0D1E2F)
	normalReq, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+normalFID, bytes.NewReader([]byte("normal")))
	if err != nil {
		t.Fatalf("create normal request: %v", err)
	}
	normalReq.Header.Set("Content-Type", "application/octet-stream")
	normalReq.ContentLength = int64(len("normal"))

	timeoutClient := &http.Client{Timeout: 10 * time.Second}
	normalResp, err := timeoutClient.Do(normalReq)
	if err != nil {
		t.Fatalf("normal upload request failed: %v", err)
	}
	_ = framework.ReadAllAndClose(t, normalResp)
	if normalResp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("normal upload expected 429 while limit blocked, got %d", normalResp.StatusCode)
	}

	close(unblockFirstUpload)
	select {
	case <-firstUploadDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for blocked upload to finish")
	}
}

func TestUploadLimitWaitThenProceed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P8())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(111)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	const blockedUploadSize = 2 * 1024 * 1024

	unblockFirstUpload := make(chan struct{})
	firstUploadDone := make(chan error, 1)
	firstFID := framework.NewFileID(volumeID, 880601, 0x6A2B3C4D)
	go func() {
		req, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+firstFID, &pausableReader{
			remaining:  blockedUploadSize,
			pauseAfter: 1,
			unblock:    unblockFirstUpload,
		})
		if err != nil {
			firstUploadDone <- err
			return
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.ContentLength = blockedUploadSize

		resp, err := (&http.Client{}).Do(req)
		if resp != nil {
			_ = framework.ReadAllAndClose(t, resp)
		}
		firstUploadDone <- err
	}()

	time.Sleep(300 * time.Millisecond)

	type uploadResult struct {
		resp *http.Response
		err  error
	}
	secondUploadDone := make(chan uploadResult, 1)
	secondFID := framework.NewFileID(volumeID, 880602, 0x6A2B3C4E)
	go func() {
		req, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+secondFID, bytes.NewReader([]byte("wait-then-proceed")))
		if err != nil {
			secondUploadDone <- uploadResult{err: err}
			return
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.ContentLength = int64(len("wait-then-proceed"))
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		secondUploadDone <- uploadResult{resp: resp, err: err}
	}()

	time.Sleep(500 * time.Millisecond)
	close(unblockFirstUpload)

	select {
	case firstErr := <-firstUploadDone:
		if firstErr != nil {
			t.Fatalf("first blocked upload failed: %v", firstErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for first upload completion")
	}

	select {
	case result := <-secondUploadDone:
		if result.err != nil {
			t.Fatalf("second upload failed: %v", result.err)
		}
		_ = framework.ReadAllAndClose(t, result.resp)
		if result.resp.StatusCode != http.StatusCreated {
			t.Fatalf("second upload expected 201 after waiting for slot, got %d", result.resp.StatusCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for second upload completion")
	}
}

func TestUploadLimitTimeoutThenRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P8())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(113)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	const blockedUploadSize = 2 * 1024 * 1024

	unblockFirstUpload := make(chan struct{})
	firstUploadDone := make(chan error, 1)
	firstFID := framework.NewFileID(volumeID, 880801, 0x7A2B3C4D)
	go func() {
		req, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+firstFID, &pausableReader{
			remaining:  blockedUploadSize,
			pauseAfter: 1,
			unblock:    unblockFirstUpload,
		})
		if err != nil {
			firstUploadDone <- err
			return
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.ContentLength = blockedUploadSize
		resp, err := (&http.Client{}).Do(req)
		if resp != nil {
			_ = framework.ReadAllAndClose(t, resp)
		}
		firstUploadDone <- err
	}()

	time.Sleep(300 * time.Millisecond)

	timeoutFID := framework.NewFileID(volumeID, 880802, 0x7A2B3C4E)
	timeoutResp := framework.UploadBytes(t, &http.Client{Timeout: 10 * time.Second}, clusterHarness.VolumeAdminURL(), timeoutFID, []byte("should-timeout"))
	_ = framework.ReadAllAndClose(t, timeoutResp)
	if timeoutResp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second upload under blocked pressure expected 429, got %d", timeoutResp.StatusCode)
	}

	close(unblockFirstUpload)
	select {
	case firstErr := <-firstUploadDone:
		if firstErr != nil {
			t.Fatalf("first blocked upload failed: %v", firstErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for first upload completion")
	}

	recoveryFID := framework.NewFileID(volumeID, 880803, 0x7A2B3C4F)
	recoveryResp := framework.UploadBytes(t, framework.NewHTTPClient(), clusterHarness.VolumeAdminURL(), recoveryFID, []byte("recovered-upload"))
	_ = framework.ReadAllAndClose(t, recoveryResp)
	if recoveryResp.StatusCode != http.StatusCreated {
		t.Fatalf("recovery upload expected 201, got %d", recoveryResp.StatusCode)
	}
}

func TestDownloadLimitTimeoutReturnsTooManyRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P8())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(99)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	largePayload := make([]byte, 12*1024*1024) // over 1MB P8 download limit
	for i := range largePayload {
		largePayload[i] = byte(i % 251)
	}
	downloadFID := framework.NewFileID(volumeID, 880101, 0x10203040)
	uploadResp := framework.UploadBytes(t, framework.NewHTTPClient(), clusterHarness.VolumeAdminURL(), downloadFID, largePayload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("large upload expected 201, got %d", uploadResp.StatusCode)
	}

	firstResp, err := (&http.Client{}).Do(mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+downloadFID))
	if err != nil {
		t.Fatalf("first GET failed: %v", err)
	}
	if firstResp.StatusCode != http.StatusOK {
		_ = framework.ReadAllAndClose(t, firstResp)
		t.Fatalf("first GET expected 200, got %d", firstResp.StatusCode)
	}
	defer firstResp.Body.Close()

	// Keep first response body unread so server write path stays in-flight.
	time.Sleep(300 * time.Millisecond)

	secondClient := &http.Client{Timeout: 10 * time.Second}
	secondResp, err := secondClient.Do(mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+downloadFID))
	if err != nil {
		t.Fatalf("second GET failed: %v", err)
	}
	_ = framework.ReadAllAndClose(t, secondResp)
	if secondResp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second GET expected 429 while first download holds limit, got %d", secondResp.StatusCode)
	}
}

func TestDownloadLimitWaitThenProceedWithoutReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P8())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(112)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	largePayload := make([]byte, 12*1024*1024)
	for i := range largePayload {
		largePayload[i] = byte(i % 251)
	}
	fid := framework.NewFileID(volumeID, 880701, 0x60708090)
	uploadResp := framework.UploadBytes(t, framework.NewHTTPClient(), clusterHarness.VolumeAdminURL(), fid, largePayload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("large upload expected 201, got %d", uploadResp.StatusCode)
	}

	firstResp, err := (&http.Client{}).Do(mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid))
	if err != nil {
		t.Fatalf("first GET failed: %v", err)
	}
	if firstResp.StatusCode != http.StatusOK {
		_ = framework.ReadAllAndClose(t, firstResp)
		t.Fatalf("first GET expected 200, got %d", firstResp.StatusCode)
	}

	type readResult struct {
		resp *http.Response
		err  error
	}
	secondReadDone := make(chan readResult, 1)
	go func() {
		resp, readErr := (&http.Client{Timeout: 10 * time.Second}).Do(mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid))
		secondReadDone <- readResult{resp: resp, err: readErr}
	}()

	time.Sleep(500 * time.Millisecond)
	_ = firstResp.Body.Close()

	select {
	case result := <-secondReadDone:
		if result.err != nil {
			t.Fatalf("second GET failed: %v", result.err)
		}
		secondBody := framework.ReadAllAndClose(t, result.resp)
		if result.resp.StatusCode != http.StatusOK {
			t.Fatalf("second GET expected 200 after waiting for slot, got %d", result.resp.StatusCode)
		}
		if len(secondBody) != len(largePayload) {
			t.Fatalf("second GET body size mismatch: got %d want %d", len(secondBody), len(largePayload))
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for second GET completion")
	}
}

func TestDownloadLimitTimeoutThenRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P8())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(114)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	largePayload := make([]byte, 12*1024*1024)
	for i := range largePayload {
		largePayload[i] = byte(i % 251)
	}
	fid := framework.NewFileID(volumeID, 880901, 0x708090A0)
	uploadResp := framework.UploadBytes(t, framework.NewHTTPClient(), clusterHarness.VolumeAdminURL(), fid, largePayload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("large upload expected 201, got %d", uploadResp.StatusCode)
	}

	firstResp, err := (&http.Client{}).Do(mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid))
	if err != nil {
		t.Fatalf("first GET failed: %v", err)
	}
	if firstResp.StatusCode != http.StatusOK {
		_ = framework.ReadAllAndClose(t, firstResp)
		t.Fatalf("first GET expected 200, got %d", firstResp.StatusCode)
	}

	time.Sleep(300 * time.Millisecond)

	timeoutResp := framework.ReadBytes(t, &http.Client{Timeout: 10 * time.Second}, clusterHarness.VolumeAdminURL(), fid)
	_ = framework.ReadAllAndClose(t, timeoutResp)
	if timeoutResp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second GET under blocked pressure expected 429, got %d", timeoutResp.StatusCode)
	}

	_ = firstResp.Body.Close()

	recoveryResp := framework.ReadBytes(t, framework.NewHTTPClient(), clusterHarness.VolumeAdminURL(), fid)
	recoveryBody := framework.ReadAllAndClose(t, recoveryResp)
	if recoveryResp.StatusCode != http.StatusOK {
		t.Fatalf("recovery GET expected 200, got %d", recoveryResp.StatusCode)
	}
	if len(recoveryBody) != len(largePayload) {
		t.Fatalf("recovery GET body size mismatch: got %d want %d", len(recoveryBody), len(largePayload))
	}
}

func TestDownloadLimitOverageProxiesToReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P8()
	profile.ReadMode = "proxy"
	clusterHarness := framework.StartDualVolumeCluster(t, profile)

	conn0, grpc0 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer conn0.Close()
	conn1, grpc1 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(1))
	defer conn1.Close()

	const volumeID = uint32(100)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &volume_server_pb.AllocateVolumeRequest{
		VolumeId:    volumeID,
		Replication: "001",
		Version:     uint32(needle.GetCurrentVersion()),
	}
	if _, err := grpc0.AllocateVolume(ctx, req); err != nil {
		t.Fatalf("allocate replicated volume on node0: %v", err)
	}
	if _, err := grpc1.AllocateVolume(ctx, req); err != nil {
		t.Fatalf("allocate replicated volume on node1: %v", err)
	}

	largePayload := make([]byte, 12*1024*1024)
	for i := range largePayload {
		largePayload[i] = byte(i % 251)
	}
	fid := framework.NewFileID(volumeID, 880201, 0x0A0B0C0D)
	uploadResp := framework.UploadBytes(t, framework.NewHTTPClient(), clusterHarness.VolumeAdminURL(0), fid, largePayload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("replicated large upload expected 201, got %d", uploadResp.StatusCode)
	}

	replicaReadURL := clusterHarness.VolumeAdminURL(1) + "/" + fid
	if !waitForHTTPStatus(t, framework.NewHTTPClient(), replicaReadURL, http.StatusOK, 10*time.Second, func(resp *http.Response) {
		_ = framework.ReadAllAndClose(t, resp)
	}) {
		t.Fatalf("replica did not become readable within deadline")
	}

	firstResp, err := (&http.Client{}).Do(mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL(0)+"/"+fid))
	if err != nil {
		t.Fatalf("first GET failed: %v", err)
	}
	if firstResp.StatusCode != http.StatusOK {
		_ = framework.ReadAllAndClose(t, firstResp)
		t.Fatalf("first GET expected 200, got %d", firstResp.StatusCode)
	}
	defer firstResp.Body.Close()

	time.Sleep(300 * time.Millisecond)

	secondResp, err := framework.NewHTTPClient().Do(mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL(0)+"/"+fid))
	if err != nil {
		t.Fatalf("second GET failed: %v", err)
	}
	secondBody := framework.ReadAllAndClose(t, secondResp)
	if secondResp.StatusCode != http.StatusOK {
		t.Fatalf("second GET expected 200 via replica proxy fallback, got %d", secondResp.StatusCode)
	}
	if len(secondBody) != len(largePayload) {
		t.Fatalf("second GET proxied body size mismatch: got %d want %d", len(secondBody), len(largePayload))
	}
}

func TestDownloadLimitProxiedRequestSkipsReplicaFallbackAndTimesOut(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	profile := matrix.P8()
	profile.ReadMode = "proxy"
	clusterHarness := framework.StartDualVolumeCluster(t, profile)

	conn0, grpc0 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer conn0.Close()
	conn1, grpc1 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(1))
	defer conn1.Close()

	const volumeID = uint32(106)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &volume_server_pb.AllocateVolumeRequest{
		VolumeId:    volumeID,
		Replication: "001",
		Version:     uint32(needle.GetCurrentVersion()),
	}
	if _, err := grpc0.AllocateVolume(ctx, req); err != nil {
		t.Fatalf("allocate replicated volume on node0: %v", err)
	}
	if _, err := grpc1.AllocateVolume(ctx, req); err != nil {
		t.Fatalf("allocate replicated volume on node1: %v", err)
	}

	largePayload := make([]byte, 12*1024*1024)
	for i := range largePayload {
		largePayload[i] = byte(i % 251)
	}
	fid := framework.NewFileID(volumeID, 880202, 0x0A0B0D0E)
	uploadResp := framework.UploadBytes(t, framework.NewHTTPClient(), clusterHarness.VolumeAdminURL(0), fid, largePayload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("replicated large upload expected 201, got %d", uploadResp.StatusCode)
	}

	// Ensure replica path is actually available, so a non-proxied request would proxy.
	replicaReadURL := clusterHarness.VolumeAdminURL(1) + "/" + fid
	if !waitForHTTPStatus(t, framework.NewHTTPClient(), replicaReadURL, http.StatusOK, 10*time.Second, func(resp *http.Response) {
		_ = framework.ReadAllAndClose(t, resp)
	}) {
		t.Fatalf("replica did not become readable within deadline")
	}

	firstResp, err := (&http.Client{}).Do(mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL(0)+"/"+fid))
	if err != nil {
		t.Fatalf("first GET failed: %v", err)
	}
	if firstResp.StatusCode != http.StatusOK {
		_ = framework.ReadAllAndClose(t, firstResp)
		t.Fatalf("first GET expected 200, got %d", firstResp.StatusCode)
	}
	defer firstResp.Body.Close()

	time.Sleep(300 * time.Millisecond)

	// proxied=true should bypass replica fallback and hit wait/timeout branch.
	secondResp, err := framework.NewHTTPClient().Do(mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL(0)+"/"+fid+"?proxied=true"))
	if err != nil {
		t.Fatalf("second GET failed: %v", err)
	}
	_ = framework.ReadAllAndClose(t, secondResp)
	if secondResp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second GET with proxied=true expected 429 timeout path, got %d", secondResp.StatusCode)
	}
}

func TestUploadLimitDisabledAllowsConcurrentUploads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(107)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	const blockedUploadSize = 2 * 1024 * 1024
	unblockFirstUpload := make(chan struct{})
	firstUploadDone := make(chan error, 1)
	firstFID := framework.NewFileID(volumeID, 880301, 0x1A2B3C5D)
	go func() {
		req, err := http.NewRequest(http.MethodPost, clusterHarness.VolumeAdminURL()+"/"+firstFID, &pausableReader{
			remaining:  blockedUploadSize,
			pauseAfter: 1,
			unblock:    unblockFirstUpload,
		})
		if err != nil {
			firstUploadDone <- err
			return
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.ContentLength = blockedUploadSize

		resp, err := (&http.Client{}).Do(req)
		if resp != nil {
			_ = framework.ReadAllAndClose(t, resp)
		}
		firstUploadDone <- err
	}()

	time.Sleep(300 * time.Millisecond)

	secondFID := framework.NewFileID(volumeID, 880302, 0x1A2B3C5E)
	secondResp := framework.UploadBytes(t, framework.NewHTTPClient(), clusterHarness.VolumeAdminURL(), secondFID, []byte("no-limit-second-upload"))
	_ = framework.ReadAllAndClose(t, secondResp)
	if secondResp.StatusCode != http.StatusCreated {
		t.Fatalf("second upload with disabled limit expected 201, got %d", secondResp.StatusCode)
	}

	close(unblockFirstUpload)
	select {
	case <-firstUploadDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for first upload completion")
	}
}

func TestDownloadLimitDisabledAllowsConcurrentDownloads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(108)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	largePayload := make([]byte, 12*1024*1024)
	for i := range largePayload {
		largePayload[i] = byte(i % 251)
	}
	fid := framework.NewFileID(volumeID, 880401, 0x20304050)
	uploadResp := framework.UploadBytes(t, framework.NewHTTPClient(), clusterHarness.VolumeAdminURL(), fid, largePayload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("large upload expected 201, got %d", uploadResp.StatusCode)
	}

	firstResp, err := (&http.Client{}).Do(mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid))
	if err != nil {
		t.Fatalf("first GET failed: %v", err)
	}
	if firstResp.StatusCode != http.StatusOK {
		_ = framework.ReadAllAndClose(t, firstResp)
		t.Fatalf("first GET expected 200, got %d", firstResp.StatusCode)
	}
	defer firstResp.Body.Close()

	time.Sleep(300 * time.Millisecond)

	secondResp := framework.ReadBytes(t, framework.NewHTTPClient(), clusterHarness.VolumeAdminURL(), fid)
	secondBody := framework.ReadAllAndClose(t, secondResp)
	if secondResp.StatusCode != http.StatusOK {
		t.Fatalf("second GET with disabled limit expected 200, got %d", secondResp.StatusCode)
	}
	if len(secondBody) != len(largePayload) {
		t.Fatalf("second GET body size mismatch: got %d want %d", len(secondBody), len(largePayload))
	}
}

func TestDownloadLimitInvalidVidWhileOverLimitReturnsBadRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P8())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(110)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	largePayload := make([]byte, 12*1024*1024)
	for i := range largePayload {
		largePayload[i] = byte(i % 251)
	}
	fid := framework.NewFileID(volumeID, 880501, 0x50607080)
	uploadResp := framework.UploadBytes(t, framework.NewHTTPClient(), clusterHarness.VolumeAdminURL(), fid, largePayload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("large upload expected 201, got %d", uploadResp.StatusCode)
	}

	firstResp, err := (&http.Client{}).Do(mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/"+fid))
	if err != nil {
		t.Fatalf("first GET failed: %v", err)
	}
	if firstResp.StatusCode != http.StatusOK {
		_ = framework.ReadAllAndClose(t, firstResp)
		t.Fatalf("first GET expected 200, got %d", firstResp.StatusCode)
	}
	defer firstResp.Body.Close()

	time.Sleep(300 * time.Millisecond)

	invalidReq := mustNewRequest(t, http.MethodGet, clusterHarness.VolumeAdminURL()+"/not-a-vid,1234567890ab")
	invalidResp := framework.DoRequest(t, framework.NewHTTPClient(), invalidReq)
	_ = framework.ReadAllAndClose(t, invalidResp)
	if invalidResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid vid while over limit expected 400, got %d", invalidResp.StatusCode)
	}
}
