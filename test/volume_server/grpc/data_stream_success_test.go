package volume_server_grpc_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestReadWriteNeedleBlobAndMetaRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(83)
	const sourceNeedleID = uint64(333333)
	const sourceCookie = uint32(0xABCD0102)
	const clonedNeedleID = uint64(333334)

	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	payload := []byte("blob-roundtrip-content")
	fid := framework.NewFileID(volumeID, sourceNeedleID, sourceCookie)
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != 201 {
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

	idxBytes := copyFileBytes(t, grpcClient, &volume_server_pb.CopyFileRequest{
		VolumeId:           volumeID,
		Ext:                ".idx",
		CompactionRevision: fileStatus.GetCompactionRevision(),
		StopOffset:         fileStatus.GetIdxFileSize(),
	})
	offset, size := findNeedleOffsetAndSize(t, idxBytes, sourceNeedleID)

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
		NeedleId: sourceNeedleID,
		Offset:   offset,
		Size:     size,
	})
	if err != nil {
		t.Fatalf("ReadNeedleMeta failed: %v", err)
	}
	if metaResp.GetCookie() != sourceCookie {
		t.Fatalf("ReadNeedleMeta cookie mismatch: got %d want %d", metaResp.GetCookie(), sourceCookie)
	}

	_, err = grpcClient.WriteNeedleBlob(ctx, &volume_server_pb.WriteNeedleBlobRequest{
		VolumeId:   volumeID,
		NeedleId:   clonedNeedleID,
		Size:       size,
		NeedleBlob: blobResp.GetNeedleBlob(),
	})
	if err != nil {
		t.Fatalf("WriteNeedleBlob failed: %v", err)
	}

	clonedStatus, err := grpcClient.VolumeNeedleStatus(ctx, &volume_server_pb.VolumeNeedleStatusRequest{
		VolumeId: volumeID,
		NeedleId: clonedNeedleID,
	})
	if err != nil {
		t.Fatalf("VolumeNeedleStatus for cloned needle failed: %v", err)
	}
	if clonedStatus.GetNeedleId() != sourceNeedleID {
		t.Fatalf("cloned needle status id mismatch: got %d want %d", clonedStatus.GetNeedleId(), sourceNeedleID)
	}
	if clonedStatus.GetCookie() != sourceCookie {
		t.Fatalf("cloned needle cookie mismatch: got %d want %d", clonedStatus.GetCookie(), sourceCookie)
	}

	clonedReadResp := framework.ReadBytes(t, client, clusterHarness.VolumeAdminURL(), framework.NewFileID(volumeID, clonedNeedleID, sourceCookie))
	clonedReadBody := framework.ReadAllAndClose(t, clonedReadResp)
	if clonedReadResp.StatusCode != 200 {
		t.Fatalf("cloned needle GET expected 200, got %d", clonedReadResp.StatusCode)
	}
	if string(clonedReadBody) != string(payload) {
		t.Fatalf("cloned needle body mismatch: got %q want %q", string(clonedReadBody), string(payload))
	}
}

func TestReadAllNeedlesStreamsUploadedRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(84)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	expected := map[uint64]string{
		444441: "read-all-needle-one",
		444442: "read-all-needle-two",
	}
	for key, body := range expected {
		resp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), framework.NewFileID(volumeID, key, 0xA0B0C0D0), []byte(body))
		_ = framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != 201 {
			t.Fatalf("upload for key %d expected 201, got %d", key, resp.StatusCode)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := grpcClient.ReadAllNeedles(ctx, &volume_server_pb.ReadAllNeedlesRequest{VolumeIds: []uint32{volumeID}})
	if err != nil {
		t.Fatalf("ReadAllNeedles start failed: %v", err)
	}

	seen := map[uint64]string{}
	for {
		msg, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("ReadAllNeedles recv failed: %v", recvErr)
		}
		if _, wanted := expected[msg.GetNeedleId()]; wanted {
			seen[msg.GetNeedleId()] = string(msg.GetNeedleBlob())
		}
	}

	for key, body := range expected {
		got, found := seen[key]
		if !found {
			t.Fatalf("ReadAllNeedles missing key %d in stream", key)
		}
		if got != body {
			t.Fatalf("ReadAllNeedles body mismatch for key %d: got %q want %q", key, got, body)
		}
	}
}

func copyFileBytes(t testing.TB, grpcClient volume_server_pb.VolumeServerClient, req *volume_server_pb.CopyFileRequest) []byte {
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

func findNeedleOffsetAndSize(t testing.TB, idxBytes []byte, needleID uint64) (offset int64, size int32) {
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
