package volume_server_grpc_test

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestVolumeSyncStatusAndReadVolumeFileStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	httpClient := framework.NewHTTPClient()
	const volumeID = uint32(41)
	framework.AllocateVolume(t, grpcClient, volumeID, "")
	fid := framework.NewFileID(volumeID, 1, 0x11112222)
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid, []byte("sync-status-payload"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	syncResp, err := grpcClient.VolumeSyncStatus(ctx, &volume_server_pb.VolumeSyncStatusRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeSyncStatus failed: %v", err)
	}
	if syncResp.GetVolumeId() != volumeID {
		t.Fatalf("VolumeSyncStatus volume id mismatch: got %d want %d", syncResp.GetVolumeId(), volumeID)
	}

	statusResp, err := grpcClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("ReadVolumeFileStatus failed: %v", err)
	}
	if statusResp.GetVolumeId() != volumeID {
		t.Fatalf("ReadVolumeFileStatus volume id mismatch: got %d want %d", statusResp.GetVolumeId(), volumeID)
	}
	if statusResp.GetVersion() == 0 {
		t.Fatalf("ReadVolumeFileStatus expected non-zero version")
	}
	if syncResp.GetTailOffset() == 0 {
		t.Fatalf("VolumeSyncStatus expected non-zero tail offset after upload")
	}
	if syncResp.GetTailOffset() != statusResp.GetDatFileSize() {
		t.Fatalf("VolumeSyncStatus tail offset mismatch: got %d want %d", syncResp.GetTailOffset(), statusResp.GetDatFileSize())
	}
}

func TestCopyAndStreamMethodsMissingVolumePaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeSyncStatus(ctx, &volume_server_pb.VolumeSyncStatusRequest{VolumeId: 98761})
	if err == nil {
		t.Fatalf("VolumeSyncStatus should fail for missing volume")
	}

	incrementalStream, err := grpcClient.VolumeIncrementalCopy(ctx, &volume_server_pb.VolumeIncrementalCopyRequest{VolumeId: 98762, SinceNs: 0})
	if err == nil {
		_, err = incrementalStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("VolumeIncrementalCopy missing-volume error mismatch: %v", err)
	}

	readAllStream, err := grpcClient.ReadAllNeedles(ctx, &volume_server_pb.ReadAllNeedlesRequest{VolumeIds: []uint32{98763}})
	if err == nil {
		_, err = readAllStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("ReadAllNeedles missing-volume error mismatch: %v", err)
	}

	copyFileStream, err := grpcClient.CopyFile(ctx, &volume_server_pb.CopyFileRequest{VolumeId: 98764, Ext: ".dat", StopOffset: 1})
	if err == nil {
		_, err = copyFileStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("CopyFile missing-volume error mismatch: %v", err)
	}

	_, err = grpcClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{VolumeId: 98765})
	if err == nil || !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("ReadVolumeFileStatus missing-volume error mismatch: %v", err)
	}
}

func TestVolumeCopyAndReceiveFileMaintenanceRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stateResp, err := grpcClient.GetState(ctx, &volume_server_pb.GetStateRequest{})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	_, err = grpcClient.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{Maintenance: true, Version: stateResp.GetState().GetVersion()},
	})
	if err != nil {
		t.Fatalf("SetState maintenance=true failed: %v", err)
	}

	copyStream, err := grpcClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{VolumeId: 1, SourceDataNode: "127.0.0.1:1234"})
	if err == nil {
		_, err = copyStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("VolumeCopy maintenance error mismatch: %v", err)
	}

	receiveClient, err := grpcClient.ReceiveFile(ctx)
	if err != nil {
		t.Fatalf("ReceiveFile client creation failed: %v", err)
	}
	_ = receiveClient.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_Info{
			Info: &volume_server_pb.ReceiveFileInfo{VolumeId: 1, Ext: ".dat"},
		},
	})
	_, err = receiveClient.CloseAndRecv()
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("ReceiveFile maintenance error mismatch: %v", err)
	}
}

func TestVolumeCopySuccessFromPeerAndMountsDestination(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartDualVolumeCluster(t, matrix.P1())
	sourceConn, sourceClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer sourceConn.Close()
	destConn, destClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(1))
	defer destConn.Close()

	const volumeID = uint32(42)
	framework.AllocateVolume(t, sourceClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 880001, 0x12345678)
	payload := []byte("volume-copy-success-payload")
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(0), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload to source expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	copyStream, err := destClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
		VolumeId:       volumeID,
		Collection:     "",
		SourceDataNode: clusterHarness.VolumeAdminAddress(0) + "." + strings.Split(clusterHarness.VolumeGRPCAddress(0), ":")[1],
	})
	if err != nil {
		t.Fatalf("VolumeCopy start failed: %v", err)
	}

	sawFinalAppendTimestamp := false
	for {
		msg, recvErr := copyStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("VolumeCopy recv failed: %v", recvErr)
		}
		if msg.GetLastAppendAtNs() > 0 {
			sawFinalAppendTimestamp = true
		}
	}
	if !sawFinalAppendTimestamp {
		t.Fatalf("VolumeCopy expected final response with last_append_at_ns")
	}

	destReadResp := framework.ReadBytes(t, httpClient, clusterHarness.VolumeAdminURL(1), fid)
	destReadBody := framework.ReadAllAndClose(t, destReadResp)
	if destReadResp.StatusCode != http.StatusOK {
		t.Fatalf("read from copied destination expected 200, got %d", destReadResp.StatusCode)
	}
	if string(destReadBody) != string(payload) {
		t.Fatalf("destination copied payload mismatch: got %q want %q", string(destReadBody), string(payload))
	}
}

func TestVolumeCopyOverwritesExistingDestinationVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartDualVolumeCluster(t, matrix.P1())
	sourceConn, sourceClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer sourceConn.Close()
	destConn, destClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(1))
	defer destConn.Close()

	const volumeID = uint32(43)
	framework.AllocateVolume(t, sourceClient, volumeID, "")
	framework.AllocateVolume(t, destClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 880002, 0x23456789)
	sourcePayload := []byte("volume-copy-overwrite-source")
	destPayload := []byte("volume-copy-overwrite-destination-old")

	sourceUploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(0), fid, sourcePayload)
	_ = framework.ReadAllAndClose(t, sourceUploadResp)
	if sourceUploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload to source expected 201, got %d", sourceUploadResp.StatusCode)
	}

	destUploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(1), fid, destPayload)
	_ = framework.ReadAllAndClose(t, destUploadResp)
	if destUploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload to destination expected 201, got %d", destUploadResp.StatusCode)
	}

	destReadBeforeResp := framework.ReadBytes(t, httpClient, clusterHarness.VolumeAdminURL(1), fid)
	destReadBeforeBody := framework.ReadAllAndClose(t, destReadBeforeResp)
	if destReadBeforeResp.StatusCode != http.StatusOK {
		t.Fatalf("destination pre-copy read expected 200, got %d", destReadBeforeResp.StatusCode)
	}
	if string(destReadBeforeBody) != string(destPayload) {
		t.Fatalf("destination pre-copy payload mismatch: got %q want %q", string(destReadBeforeBody), string(destPayload))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	copyStream, err := destClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
		VolumeId:       volumeID,
		Collection:     "",
		SourceDataNode: clusterHarness.VolumeAdminAddress(0) + "." + strings.Split(clusterHarness.VolumeGRPCAddress(0), ":")[1],
	})
	if err != nil {
		t.Fatalf("VolumeCopy overwrite start failed: %v", err)
	}

	sawFinalAppendTimestamp := false
	for {
		msg, recvErr := copyStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("VolumeCopy overwrite recv failed: %v", recvErr)
		}
		if msg.GetLastAppendAtNs() > 0 {
			sawFinalAppendTimestamp = true
		}
	}
	if !sawFinalAppendTimestamp {
		t.Fatalf("VolumeCopy overwrite expected final response with last_append_at_ns")
	}

	destReadAfterResp := framework.ReadBytes(t, httpClient, clusterHarness.VolumeAdminURL(1), fid)
	destReadAfterBody := framework.ReadAllAndClose(t, destReadAfterResp)
	if destReadAfterResp.StatusCode != http.StatusOK {
		t.Fatalf("destination post-copy read expected 200, got %d", destReadAfterResp.StatusCode)
	}
	if string(destReadAfterBody) != string(sourcePayload) {
		t.Fatalf("destination post-copy payload mismatch: got %q want %q", string(destReadAfterBody), string(sourcePayload))
	}
}

// A VolumeCopy whose caller goes away must not leave a mounted volume behind on
// the destination. weed-admin's batch balance routinely starts far more copies
// than it finishes, and each abandoned copy that still mounts costs the
// destination a volume it was never asked to hold: a per-volume index cache that
// is never reclaimed, and — under replication=000 — a second writable copy of a
// volume id that two writers can diverge.
//
// The copy is throttled so it is demonstrably still in flight when the caller
// cancels; the assertion before the cancel keeps the test from passing
// vacuously if the throttle ever stops biting.
func TestVolumeCopyCancelledByCallerDoesNotMountDestination(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartDualVolumeCluster(t, matrix.P1())
	sourceConn, sourceClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer sourceConn.Close()
	destConn, destClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(1))
	defer destConn.Close()

	const volumeID = uint32(44)
	const payloadMiB = 192
	framework.AllocateVolume(t, sourceClient, volumeID, "")

	// The fixture size is load-bearing twice over; do not shrink it to save CI
	// time without reading both reasons.
	//
	// First, the copy has to still be running when the caller cancels, and the
	// only lever for that is IoBytePerSecond. It is a no-op below ~100ms of wall
	// clock — the throttler never re-checks within its first window — and on a
	// tmpfs loopback cluster 64 MiB copies in ~110ms.
	//
	// Second, 192 MiB is above the 128 MiB progress-report interval, which is
	// what lets this test be green on Go: a failing stream.Send from that report
	// is Go's only abort signal. Measured below the interval (120 MiB), a Go
	// destination mounts the abandoned copy too, so a smaller fixture would fail
	// the Go leg without any Rust change.
	//
	// The bytes are pseudo-random because the volume server gzips compressible
	// uploads; a repeating payload lands on disk as a few KB and the throttle
	// never bites.
	httpClient := framework.NewHTTPClient()
	chunk := make([]byte, 1024*1024)
	if _, err := rand.New(rand.NewSource(11186)).Read(chunk); err != nil {
		t.Fatalf("generate payload: %v", err)
	}
	for i := 0; i < payloadMiB; i++ {
		fid := framework.NewFileID(volumeID, uint64(880100+i), 0x3456789a)
		uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(0), fid, chunk)
		_ = framework.ReadAllAndClose(t, uploadResp)
		if uploadResp.StatusCode != http.StatusCreated {
			t.Fatalf("upload %d to source expected 201, got %d", i, uploadResp.StatusCode)
		}
	}

	// 192 MiB at 16 MiB/s is about twelve seconds of copying.
	copyCtx, cancelCopy := context.WithCancel(context.Background())
	defer cancelCopy()
	_, err := destClient.VolumeCopy(copyCtx, &volume_server_pb.VolumeCopyRequest{
		VolumeId:        volumeID,
		Collection:      "",
		SourceDataNode:  clusterHarness.VolumeAdminAddress(0) + "." + strings.Split(clusterHarness.VolumeGRPCAddress(0), ":")[1],
		IoBytePerSecond: 16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("VolumeCopy start failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Premise: the copy must still be running when we cancel. If the
	// destination has already mounted, the throttle no longer bites and the
	// rest of this test would prove nothing.
	premiseCtx, cancelPremise := context.WithTimeout(context.Background(), 5*time.Second)
	_, premiseErr := destClient.ReadVolumeFileStatus(premiseCtx, &volume_server_pb.ReadVolumeFileStatusRequest{VolumeId: volumeID})
	cancelPremise()
	if premiseErr == nil {
		t.Fatalf("copy of volume %d already completed before the cancel; raise payloadMiB or lower IoBytePerSecond", volumeID)
	}

	cancelCopy()

	// Well past the ~10s the unabandoned copy would have needed: the volume
	// must never appear on the destination.
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		pollCtx, cancelPoll := context.WithTimeout(context.Background(), 5*time.Second)
		_, statusErr := destClient.ReadVolumeFileStatus(pollCtx, &volume_server_pb.ReadVolumeFileStatusRequest{VolumeId: volumeID})
		cancelPoll()
		if statusErr == nil {
			t.Fatalf("destination mounted volume %d after its VolumeCopy caller cancelled", volumeID)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
