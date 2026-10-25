package volume_server_grpc_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestScrubVolumeDetectsHealthyData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(101)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	needles := []struct {
		needleID uint64
		cookie   uint32
		body     string
	}{
		{needleID: 1010001, cookie: 0xAA000001, body: "scrub-healthy-needle-one"},
		{needleID: 1010002, cookie: 0xAA000002, body: "scrub-healthy-needle-two"},
		{needleID: 1010003, cookie: 0xAA000003, body: "scrub-healthy-needle-three"},
	}
	for _, n := range needles {
		fid := framework.NewFileID(volumeID, n.needleID, n.cookie)
		uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid, []byte(n.body))
		_ = framework.ReadAllAndClose(t, uploadResp)
		if uploadResp.StatusCode != http.StatusCreated {
			t.Fatalf("upload needle %d expected 201, got %d", n.needleID, uploadResp.StatusCode)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	scrubResp, err := grpcClient.ScrubVolume(ctx, &volume_server_pb.ScrubVolumeRequest{
		VolumeIds: []uint32{volumeID},
		Mode:      volume_server_pb.VolumeScrubMode_FULL,
	})
	if err != nil {
		t.Fatalf("ScrubVolume FULL mode failed: %v", err)
	}
	if scrubResp.GetTotalVolumes() != 1 {
		t.Fatalf("ScrubVolume expected total_volumes=1, got %d", scrubResp.GetTotalVolumes())
	}
	if scrubResp.GetTotalFiles() < 3 {
		t.Fatalf("ScrubVolume expected total_files >= 3, got %d", scrubResp.GetTotalFiles())
	}
	if len(scrubResp.GetBrokenVolumeIds()) != 0 {
		t.Fatalf("ScrubVolume expected no broken volumes for healthy data, got %v: %v", scrubResp.GetBrokenVolumeIds(), scrubResp.GetDetails())
	}
}

func TestScrubVolumeLocalModeWithMultipleVolumes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeIDA = uint32(102)
	const volumeIDB = uint32(103)
	framework.AllocateVolume(t, grpcClient, volumeIDA, "")
	framework.AllocateVolume(t, grpcClient, volumeIDB, "")

	httpClient := framework.NewHTTPClient()

	fidA := framework.NewFileID(volumeIDA, 1020001, 0xBB000001)
	uploadA := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fidA, []byte("scrub-local-vol-a"))
	_ = framework.ReadAllAndClose(t, uploadA)
	if uploadA.StatusCode != http.StatusCreated {
		t.Fatalf("upload to volume A expected 201, got %d", uploadA.StatusCode)
	}

	fidB := framework.NewFileID(volumeIDB, 1030001, 0xBB000002)
	uploadB := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fidB, []byte("scrub-local-vol-b"))
	_ = framework.ReadAllAndClose(t, uploadB)
	if uploadB.StatusCode != http.StatusCreated {
		t.Fatalf("upload to volume B expected 201, got %d", uploadB.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	scrubResp, err := grpcClient.ScrubVolume(ctx, &volume_server_pb.ScrubVolumeRequest{
		Mode: volume_server_pb.VolumeScrubMode_LOCAL,
	})
	if err != nil {
		t.Fatalf("ScrubVolume LOCAL auto-select failed: %v", err)
	}
	if scrubResp.GetTotalVolumes() < 2 {
		t.Fatalf("ScrubVolume LOCAL expected total_volumes >= 2, got %d", scrubResp.GetTotalVolumes())
	}
	if len(scrubResp.GetBrokenVolumeIds()) != 0 {
		t.Fatalf("ScrubVolume LOCAL expected no broken volumes, got %v: %v", scrubResp.GetBrokenVolumeIds(), scrubResp.GetDetails())
	}
}

func TestVolumeServerStatusReturnsRealDiskStats(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	statusResp, err := grpcClient.VolumeServerStatus(ctx, &volume_server_pb.VolumeServerStatusRequest{})
	if err != nil {
		t.Fatalf("VolumeServerStatus failed: %v", err)
	}

	diskStatuses := statusResp.GetDiskStatuses()
	if len(diskStatuses) == 0 {
		t.Fatalf("VolumeServerStatus expected non-empty disk_statuses")
	}

	foundValid := false
	for _, ds := range diskStatuses {
		if ds.GetDir() != "" && ds.GetAll() > 0 && ds.GetFree() > 0 {
			foundValid = true
			break
		}
	}
	if !foundValid {
		t.Fatalf("VolumeServerStatus expected at least one disk status with Dir, All > 0, Free > 0; got %v", diskStatuses)
	}
}

func TestReadNeedleBlobAndMetaVerifiesCookie(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(104)
	const needleID = uint64(1040001)
	const cookie = uint32(0xCC000001)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, needleID, cookie)
	payload := []byte("read-needle-blob-meta-verify")
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fileStatus, err := grpcClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("ReadVolumeFileStatus failed: %v", err)
	}
	if fileStatus.GetIdxFileSize() == 0 {
		t.Fatalf("expected non-zero idx file size after upload")
	}

	idxBytes := prodCopyFileBytes(t, grpcClient, &volume_server_pb.CopyFileRequest{
		VolumeId:           volumeID,
		Ext:                ".idx",
		CompactionRevision: fileStatus.GetCompactionRevision(),
		StopOffset:         fileStatus.GetIdxFileSize(),
	})
	offset, size := prodFindNeedleOffsetAndSize(t, idxBytes, needleID)

	blobResp, err := grpcClient.ReadNeedleBlob(ctx, &volume_server_pb.ReadNeedleBlobRequest{
		VolumeId: volumeID,
		Offset:   offset,
		Size:     size,
	})
	if err != nil {
		t.Fatalf("ReadNeedleBlob failed: %v", err)
	}
	if len(blobResp.GetNeedleBlob()) == 0 {
		t.Fatalf("ReadNeedleBlob returned empty blob")
	}

	metaResp, err := grpcClient.ReadNeedleMeta(ctx, &volume_server_pb.ReadNeedleMetaRequest{
		VolumeId: volumeID,
		NeedleId: needleID,
		Offset:   offset,
		Size:     size,
	})
	if err != nil {
		t.Fatalf("ReadNeedleMeta failed: %v", err)
	}
	if metaResp.GetCookie() != cookie {
		t.Fatalf("ReadNeedleMeta cookie mismatch: got %d want %d", metaResp.GetCookie(), cookie)
	}
	if metaResp.GetCrc() == 0 {
		t.Fatalf("ReadNeedleMeta expected non-zero CRC")
	}
}

func TestBatchDeleteMultipleNeedles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(105)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	type needle struct {
		needleID uint64
		cookie   uint32
		body     string
		fid      string
	}
	needles := []needle{
		{needleID: 1050001, cookie: 0xDD000001, body: "batch-del-needle-one"},
		{needleID: 1050002, cookie: 0xDD000002, body: "batch-del-needle-two"},
		{needleID: 1050003, cookie: 0xDD000003, body: "batch-del-needle-three"},
	}
	fids := make([]string, len(needles))
	for i := range needles {
		needles[i].fid = framework.NewFileID(volumeID, needles[i].needleID, needles[i].cookie)
		fids[i] = needles[i].fid
		uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), needles[i].fid, []byte(needles[i].body))
		_ = framework.ReadAllAndClose(t, uploadResp)
		if uploadResp.StatusCode != http.StatusCreated {
			t.Fatalf("upload needle %d expected 201, got %d", needles[i].needleID, uploadResp.StatusCode)
		}
	}

	// Verify all needles are readable before delete
	for _, n := range needles {
		readResp := framework.ReadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), n.fid)
		_ = framework.ReadAllAndClose(t, readResp)
		if readResp.StatusCode != http.StatusOK {
			t.Fatalf("pre-delete read of %s expected 200, got %d", n.fid, readResp.StatusCode)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deleteResp, err := grpcClient.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{
		FileIds: fids,
	})
	if err != nil {
		t.Fatalf("BatchDelete failed: %v", err)
	}
	if len(deleteResp.GetResults()) != 3 {
		t.Fatalf("BatchDelete expected 3 results, got %d", len(deleteResp.GetResults()))
	}
	for i, result := range deleteResp.GetResults() {
		if result.GetStatus() != http.StatusAccepted {
			t.Fatalf("BatchDelete result[%d] expected status 202, got %d (error: %s)", i, result.GetStatus(), result.GetError())
		}
		if result.GetSize() <= 0 {
			t.Fatalf("BatchDelete result[%d] expected size > 0, got %d", i, result.GetSize())
		}
	}

	// Verify all needles return 404 after delete
	for _, n := range needles {
		readResp := framework.ReadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), n.fid)
		_ = framework.ReadAllAndClose(t, readResp)
		if readResp.StatusCode != http.StatusNotFound {
			t.Fatalf("post-delete read of %s expected 404, got %d", n.fid, readResp.StatusCode)
		}
	}
}

// prodCopyFileBytes streams a CopyFile response into a byte slice.
func prodCopyFileBytes(t testing.TB, grpcClient volume_server_pb.VolumeServerClient, req *volume_server_pb.CopyFileRequest) []byte {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := grpcClient.CopyFile(ctx, req)
	if err != nil {
		t.Fatalf("CopyFile start failed: %v", err)
	}

	var out []byte
	for {
		msg, recvErr := stream.Recv()
		if recvErr == io.EOF {
			return out
		}
		if recvErr != nil {
			t.Fatalf("CopyFile recv failed: %v", recvErr)
		}
		out = append(out, msg.GetFileContent()...)
	}
}

// prodFindNeedleOffsetAndSize scans idx bytes for a needle's offset and size.
func prodFindNeedleOffsetAndSize(t testing.TB, idxBytes []byte, needleID uint64) (offset int64, size int32) {
	t.Helper()

	for i := 0; i+types.NeedleMapEntrySize <= len(idxBytes); i += types.NeedleMapEntrySize {
		key, entryOffset, entrySize := idx.IdxFileEntry(idxBytes[i : i+types.NeedleMapEntrySize])
		if uint64(key) != needleID {
			continue
		}
		if entryOffset.IsZero() || entrySize <= 0 {
			continue
		}
		return entryOffset.ToActualOffset(), int32(entrySize)
	}

	t.Fatalf("needle id %d not found in idx entries", needleID)
	return 0, 0
}
