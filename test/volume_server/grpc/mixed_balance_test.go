package volume_server_grpc_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// deleteAndWaitForTombstone issues an HTTP DELETE for the given fid on
// volumeURL, asserts a successful response, then polls with GET until
// the file returns 404 (tombstone visible) or the timeout elapses.
func deleteAndWaitForTombstone(t *testing.T, httpClient *http.Client, volumeURL, fid string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/%s", volumeURL, fid), nil)
	if err != nil {
		t.Fatalf("build delete request for %s: %v", fid, err)
	}
	resp := framework.DoRequest(t, httpClient, req)
	framework.ReadAllAndClose(t, resp)
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		t.Fatalf("delete %s: expected 200 or 202, got %d", fid, resp.StatusCode)
	}
	// Poll until GET returns 404 (tombstone flushed to disk).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		getResp := framework.ReadBytes(t, httpClient, volumeURL, fid)
		status := getResp.StatusCode
		framework.ReadAllAndClose(t, getResp)
		if status == http.StatusNotFound {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("delete %s: tombstone not visible after 5s", fid)
}

// TestMixedBalanceCopyGoToRust verifies that VolumeCopy works from a Go volume
// server to a Rust volume server. This is the core operation behind volume
// balancing in a mixed Go+Rust cluster.
func TestMixedBalanceCopyGoToRust(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartMixedVolumeCluster(t, matrix.P1(), 1, 1)

	// server 0 = Go, server 1 = Rust
	conn0, goClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer conn0.Close()
	conn1, rustClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer conn1.Close()

	httpClient := framework.NewHTTPClient()

	const volumeID = uint32(50)

	// Allocate volume on Go server and upload test data
	framework.AllocateVolume(t, goClient, volumeID, "")

	testFiles := []struct {
		key    uint64
		cookie uint32
		data   []byte
	}{
		{1, 0xAABBCCDD, []byte("hello from Go server")},
		{2, 0x11223344, []byte("second file for balance test")},
		{3, 0xDEADBEEF, make([]byte, 4096)}, // larger file
	}

	for _, f := range testFiles {
		fid := framework.NewFileID(volumeID, f.key, f.cookie)
		resp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(0), fid, f.data)
		body := framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload %s: expected 201, got %d: %s", fid, resp.StatusCode, body)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Read source volume status before copy
	sourceStatus, err := goClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("ReadVolumeFileStatus on Go server: %v", err)
	}
	t.Logf("Source: dat=%d idx=%d files=%d version=%d",
		sourceStatus.GetDatFileSize(), sourceStatus.GetIdxFileSize(),
		sourceStatus.GetFileCount(), sourceStatus.GetVersion())

	// Copy volume from Go (server 0) to Rust (server 1)
	copyStream, err := rustClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
		VolumeId:       volumeID,
		SourceDataNode: cluster.VolumeServerAddress(0),
	})
	if err != nil {
		t.Fatalf("VolumeCopy call failed: %v", err)
	}

	var lastAppendAtNs uint64
	for {
		resp, recvErr := copyStream.Recv()
		if recvErr != nil {
			if recvErr != io.EOF {
				t.Fatalf("VolumeCopy recv error: %v", recvErr)
			}
			break
		}
		if resp.GetLastAppendAtNs() != 0 {
			lastAppendAtNs = resp.GetLastAppendAtNs()
		}
	}
	t.Logf("VolumeCopy completed, lastAppendAtNs=%d", lastAppendAtNs)

	// Verify: read volume status from Rust server
	targetStatus, err := rustClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("ReadVolumeFileStatus on Rust server: %v", err)
	}
	t.Logf("Target: dat=%d idx=%d files=%d version=%d",
		targetStatus.GetDatFileSize(), targetStatus.GetIdxFileSize(),
		targetStatus.GetFileCount(), targetStatus.GetVersion())

	if sourceStatus.GetDatFileSize() != targetStatus.GetDatFileSize() {
		t.Fatalf("dat file size mismatch: source=%d target=%d",
			sourceStatus.GetDatFileSize(), targetStatus.GetDatFileSize())
	}
	if sourceStatus.GetIdxFileSize() != targetStatus.GetIdxFileSize() {
		t.Fatalf("idx file size mismatch: source=%d target=%d",
			sourceStatus.GetIdxFileSize(), targetStatus.GetIdxFileSize())
	}
	if sourceStatus.GetFileCount() != targetStatus.GetFileCount() {
		t.Fatalf("file count mismatch: source=%d target=%d",
			sourceStatus.GetFileCount(), targetStatus.GetFileCount())
	}

	// Verify data can be read from Rust server
	for _, f := range testFiles {
		fid := framework.NewFileID(volumeID, f.key, f.cookie)
		resp := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(1), fid)
		body := framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("read %s from Rust server: expected 200, got %d", fid, resp.StatusCode)
		}
		if !bytes.Equal(body, f.data) {
			t.Fatalf("read %s from Rust server: content mismatch (got %d bytes, want %d)", fid, len(body), len(f.data))
		}
	}
}

// TestMixedBalanceCopyRustToGo verifies that VolumeCopy works from a Rust
// volume server to a Go volume server.
func TestMixedBalanceCopyRustToGo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartMixedVolumeCluster(t, matrix.P1(), 1, 1)

	conn0, goClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer conn0.Close()
	conn1, rustClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer conn1.Close()

	httpClient := framework.NewHTTPClient()

	const volumeID = uint32(51)

	// Allocate volume on Rust server and upload test data
	framework.AllocateVolume(t, rustClient, volumeID, "")

	testFiles := []struct {
		key    uint64
		cookie uint32
		data   []byte
	}{
		{1, 0xAABBCCDD, []byte("hello from Rust server")},
		{2, 0x11223344, []byte("second file for reverse balance")},
	}

	for _, f := range testFiles {
		fid := framework.NewFileID(volumeID, f.key, f.cookie)
		resp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(1), fid, f.data)
		body := framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload %s: expected 201, got %d: %s", fid, resp.StatusCode, body)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sourceStatus, err := rustClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("ReadVolumeFileStatus on Rust server: %v", err)
	}

	// Copy volume from Rust (server 1) to Go (server 0)
	copyStream, err := goClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
		VolumeId:       volumeID,
		SourceDataNode: cluster.VolumeServerAddress(1),
	})
	if err != nil {
		t.Fatalf("VolumeCopy call failed: %v", err)
	}
	for {
		_, recvErr := copyStream.Recv()
		if recvErr != nil {
			if recvErr != io.EOF {
				t.Fatalf("VolumeCopy recv error: %v", recvErr)
			}
			break
		}
	}

	targetStatus, err := goClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("ReadVolumeFileStatus on Go server: %v", err)
	}

	if sourceStatus.GetDatFileSize() != targetStatus.GetDatFileSize() {
		t.Fatalf("dat file size mismatch: source=%d target=%d",
			sourceStatus.GetDatFileSize(), targetStatus.GetDatFileSize())
	}
	if sourceStatus.GetIdxFileSize() != targetStatus.GetIdxFileSize() {
		t.Fatalf("idx file size mismatch: source=%d target=%d",
			sourceStatus.GetIdxFileSize(), targetStatus.GetIdxFileSize())
	}
	if sourceStatus.GetFileCount() != targetStatus.GetFileCount() {
		t.Fatalf("file count mismatch: source=%d target=%d",
			sourceStatus.GetFileCount(), targetStatus.GetFileCount())
	}

	// Verify data can be read from Go server
	for _, f := range testFiles {
		fid := framework.NewFileID(volumeID, f.key, f.cookie)
		resp := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(0), fid)
		body := framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("read %s from Go server: expected 200, got %d", fid, resp.StatusCode)
		}
		if !bytes.Equal(body, f.data) {
			t.Fatalf("read %s from Go server: content mismatch (got %d bytes, want %d)", fid, len(body), len(f.data))
		}
	}
}

// TestMixedBalanceCopyWithDeletes verifies that VolumeCopy correctly handles
// volumes that have both active and deleted needles.
func TestMixedBalanceCopyWithDeletes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartMixedVolumeCluster(t, matrix.P1(), 1, 1)

	conn0, goClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer conn0.Close()
	conn1, rustClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer conn1.Close()

	httpClient := framework.NewHTTPClient()

	const volumeID = uint32(52)
	framework.AllocateVolume(t, goClient, volumeID, "")

	// Upload files
	for i := uint64(1); i <= 5; i++ {
		fid := framework.NewFileID(volumeID, i, 0x12340000+uint32(i))
		resp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(0), fid, []byte(fmt.Sprintf("file-%d", i)))
		framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload file %d: expected 201, got %d", i, resp.StatusCode)
		}
	}

	// Delete some files and wait for tombstones to be visible
	for _, key := range []uint64{2, 4} {
		fid := framework.NewFileID(volumeID, key, 0x12340000+uint32(key))
		deleteAndWaitForTombstone(t, httpClient, cluster.VolumeAdminURL(0), fid)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sourceStatus, err := goClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("ReadVolumeFileStatus: %v", err)
	}
	t.Logf("Source after deletes: dat=%d idx=%d files=%d",
		sourceStatus.GetDatFileSize(), sourceStatus.GetIdxFileSize(),
		sourceStatus.GetFileCount())

	// Copy volume from Go to Rust
	copyStream, err := rustClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
		VolumeId:       volumeID,
		SourceDataNode: cluster.VolumeServerAddress(0),
	})
	if err != nil {
		t.Fatalf("VolumeCopy: %v", err)
	}
	var lastAppendAtNs uint64
	for {
		resp, recvErr := copyStream.Recv()
		if recvErr != nil {
			if recvErr != io.EOF {
				t.Fatalf("VolumeCopy recv error: %v", recvErr)
			}
			break
		}
		if resp.GetLastAppendAtNs() != 0 {
			lastAppendAtNs = resp.GetLastAppendAtNs()
		}
	}
	if lastAppendAtNs == 0 {
		t.Fatalf("VolumeCopy did not return a lastAppendAtNs timestamp")
	}
	t.Logf("VolumeCopy completed, lastAppendAtNs=%d", lastAppendAtNs)

	targetStatusAfterCopy, err := rustClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("ReadVolumeFileStatus on Rust: %v", err)
	}
	t.Logf("Target after copy: dat=%d idx=%d files=%d",
		targetStatusAfterCopy.GetDatFileSize(), targetStatusAfterCopy.GetIdxFileSize(),
		targetStatusAfterCopy.GetFileCount())

	if sourceStatus.GetDatFileSize() != targetStatusAfterCopy.GetDatFileSize() {
		t.Fatalf("dat file size mismatch: source=%d target=%d",
			sourceStatus.GetDatFileSize(), targetStatusAfterCopy.GetDatFileSize())
	}
	if sourceStatus.GetIdxFileSize() != targetStatusAfterCopy.GetIdxFileSize() {
		t.Fatalf("idx file size mismatch: source=%d target=%d",
			sourceStatus.GetIdxFileSize(), targetStatusAfterCopy.GetIdxFileSize())
	}

	// Tail from the copy checkpoint — source is unchanged (deletes happened
	// before copy), so tailing should not append any data.
	_, err = rustClient.VolumeTailReceiver(ctx, &volume_server_pb.VolumeTailReceiverRequest{
		VolumeId:           volumeID,
		SinceNs:            lastAppendAtNs,
		IdleTimeoutSeconds: 3,
		SourceVolumeServer: cluster.VolumeServerAddress(0),
	})
	if err != nil {
		t.Fatalf("VolumeTailReceiver: %v", err)
	}

	targetStatusAfterTail, err := rustClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("ReadVolumeFileStatus after tail: %v", err)
	}
	if targetStatusAfterTail.GetDatFileSize() != targetStatusAfterCopy.GetDatFileSize() {
		t.Fatalf("dat grew after tail: before=%d after=%d",
			targetStatusAfterCopy.GetDatFileSize(), targetStatusAfterTail.GetDatFileSize())
	}
	if targetStatusAfterTail.GetIdxFileSize() != targetStatusAfterCopy.GetIdxFileSize() {
		t.Fatalf("idx grew after tail: before=%d after=%d",
			targetStatusAfterCopy.GetIdxFileSize(), targetStatusAfterTail.GetIdxFileSize())
	}

	// Verify surviving files are readable from Rust
	for _, key := range []uint64{1, 3, 5} {
		fid := framework.NewFileID(volumeID, key, 0x12340000+uint32(key))
		resp := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(1), fid)
		body := framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("read surviving file %d from Rust: expected 200, got %d", key, resp.StatusCode)
		}
		expected := fmt.Sprintf("file-%d", key)
		if string(body) != expected {
			t.Fatalf("data mismatch for file %d: got %q want %q", key, body, expected)
		}
	}

	// Verify deleted files return 404
	for _, key := range []uint64{2, 4} {
		fid := framework.NewFileID(volumeID, key, 0x12340000+uint32(key))
		resp := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(1), fid)
		framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("read deleted file %d from Rust: expected 404, got %d", key, resp.StatusCode)
		}
	}
}

// TestMixedBalanceFullMoveGoToRust exercises the complete volume balance move
// flow: mark readonly → copy → tail → verify sizes → delete source.
// This mirrors the steps in balance_task.go Execute().
func TestMixedBalanceFullMoveGoToRust(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartMixedVolumeCluster(t, matrix.P1(), 1, 1)

	conn0, goClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer conn0.Close()
	conn1, rustClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer conn1.Close()

	httpClient := framework.NewHTTPClient()

	const volumeID = uint32(53)
	framework.AllocateVolume(t, goClient, volumeID, "")

	// Upload test data
	for i := uint64(1); i <= 5; i++ {
		fid := framework.NewFileID(volumeID, i, 0xABCD0000+uint32(i))
		resp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(0), fid, []byte(fmt.Sprintf("balance-move-file-%d", i)))
		framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload file %d: expected 201, got %d", i, resp.StatusCode)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Step 1: Copy volume to Rust server (source stays writable so we can
	// delete after copy to exercise the tail tombstone path)
	copyStream, err := rustClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
		VolumeId:       volumeID,
		SourceDataNode: cluster.VolumeServerAddress(0),
	})
	if err != nil {
		t.Fatalf("VolumeCopy: %v", err)
	}
	var lastAppendAtNs uint64
	for {
		resp, recvErr := copyStream.Recv()
		if recvErr != nil {
			if recvErr != io.EOF {
				t.Fatalf("VolumeCopy recv: %v", recvErr)
			}
			break
		}
		if resp.GetLastAppendAtNs() != 0 {
			lastAppendAtNs = resp.GetLastAppendAtNs()
		}
	}
	t.Logf("Copy done, lastAppendAtNs=%d", lastAppendAtNs)

	// Step 2: Delete file 3 on the source AFTER copy. This creates a
	// tombstone needle that the tail step must propagate to the Rust server.
	deleteAndWaitForTombstone(t, httpClient, cluster.VolumeAdminURL(0),
		framework.NewFileID(volumeID, 3, 0xABCD0003))

	// Step 3: Mark source readonly so no further writes arrive during tail
	_, err = goClient.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("mark readonly: %v", err)
	}

	// Read source status (the reference for post-tail verification)
	sourceStatus, err := goClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("read source status: %v", err)
	}
	t.Logf("Source: dat=%d idx=%d files=%d",
		sourceStatus.GetDatFileSize(), sourceStatus.GetIdxFileSize(),
		sourceStatus.GetFileCount())

	// Step 4: Tail for updates — this must pick up the delete tombstone
	_, err = rustClient.VolumeTailReceiver(ctx, &volume_server_pb.VolumeTailReceiverRequest{
		VolumeId:           volumeID,
		SinceNs:            lastAppendAtNs,
		IdleTimeoutSeconds: 5,
		SourceVolumeServer: cluster.VolumeServerAddress(0),
	})
	if err != nil {
		t.Fatalf("VolumeTailReceiver: %v", err)
	}

	// Step 5: Verify dat/idx sizes match after tail.
	// We compare file sizes (byte-level correctness) rather than file_count
	// because the tail writes the tombstone via the write path (which
	// increments file_count) while the source used the delete path (which
	// only increments deletion_count). This is consistent with Go behavior.
	targetStatus, err := rustClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("read target status: %v", err)
	}
	t.Logf("Target: dat=%d idx=%d files=%d",
		targetStatus.GetDatFileSize(), targetStatus.GetIdxFileSize(),
		targetStatus.GetFileCount())

	if sourceStatus.GetDatFileSize() != targetStatus.GetDatFileSize() {
		t.Fatalf("dat size mismatch after tail: source=%d target=%d",
			sourceStatus.GetDatFileSize(), targetStatus.GetDatFileSize())
	}
	if sourceStatus.GetIdxFileSize() != targetStatus.GetIdxFileSize() {
		t.Fatalf("idx size mismatch after tail: source=%d target=%d",
			sourceStatus.GetIdxFileSize(), targetStatus.GetIdxFileSize())
	}

	// Step 6: Delete volume from source
	_, err = goClient.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("delete source volume: %v", err)
	}

	// Verify source volume is gone
	_, err = goClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
		VolumeId: volumeID,
	})
	if err == nil {
		t.Fatalf("expected error reading deleted source volume, got nil")
	}
	t.Logf("Source volume deleted successfully (error as expected: %v)", err)

	// Verify all surviving data is readable from Rust (the new home)
	for _, key := range []uint64{1, 2, 4, 5} {
		fid := framework.NewFileID(volumeID, key, 0xABCD0000+uint32(key))
		resp := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(1), fid)
		body := framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("read file %d from Rust after move: expected 200, got %d", key, resp.StatusCode)
		}
		expected := fmt.Sprintf("balance-move-file-%d", key)
		if string(body) != expected {
			t.Fatalf("data mismatch for file %d after move: got %q want %q", key, body, expected)
		}
	}

	// Verify deleted file is 404 on Rust (tombstone propagated via tail)
	{
		fid := framework.NewFileID(volumeID, 3, 0xABCD0003)
		resp := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(1), fid)
		framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("deleted file 3 on Rust: expected 404, got %d", resp.StatusCode)
		}
	}

	t.Logf("Full balance move completed: volume %d moved from Go to Rust, source purged", volumeID)
}
