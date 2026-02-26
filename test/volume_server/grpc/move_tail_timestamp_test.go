package volume_server_grpc_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestVolumeCopyReturnsPreciseLastAppendTimestamp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartDualVolumeCluster(t, matrix.P1())
	sourceConn, sourceClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer sourceConn.Close()
	destConn, destClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer destConn.Close()

	const volumeID = uint32(999)
	framework.AllocateVolume(t, sourceClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 1, 0x42)
	payload := []byte("move-tail-timestamp-payload")
	uploadResp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(0), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("source upload failed: status %d", uploadResp.StatusCode)
	}

	sourceDir := filepath.Join(cluster.BaseDir(), "volume0")
	datPath := storage.VolumeFileName(sourceDir, "", int(volumeID)) + ".dat"
	futureTime := time.Now().Add(2 * time.Hour)
	if err := os.Chtimes(datPath, futureTime, futureTime); err != nil {
		t.Fatalf("set future dat timestamp: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sourceDataNode := cluster.VolumeAdminAddress(0) + "." + strings.Split(cluster.VolumeGRPCAddress(0), ":")[1]
	copyStream, err := destClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
		VolumeId:       volumeID,
		SourceDataNode: sourceDataNode,
	})
	if err != nil {
		t.Fatalf("VolumeCopy start failed: %v", err)
	}

	var lastAppendAtNs uint64
	for {
		resp, recvErr := copyStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("VolumeCopy recv failed: %v", recvErr)
		}
		if ts := resp.GetLastAppendAtNs(); ts > 0 {
			lastAppendAtNs = ts
		}
	}
	if lastAppendAtNs == 0 {
		t.Fatalf("volume copy did not return a last append timestamp")
	}

	destDir := filepath.Join(cluster.BaseDir(), "volume1")
	actualLastAppend := readLastAppendAtNs(t, destDir, volumeID)
	if actualLastAppend == 0 {
		t.Fatalf("failed to compute last append timestamp from destination files")
	}

	if lastAppendAtNs != actualLastAppend {
		t.Fatalf("last append timestamp mismatch: got %d, actual %d", lastAppendAtNs, actualLastAppend)
	}
}

func readLastAppendAtNs(t testing.TB, volumeDir string, volumeID uint32) uint64 {
	t.Helper()

	baseName := storage.VolumeFileName(volumeDir, "", int(volumeID))
	idxPath := baseName + ".idx"
	idxFile, err := os.Open(idxPath)
	if err != nil {
		t.Fatalf("open idx file %s: %v", idxPath, err)
	}
	defer idxFile.Close()

	stat, err := idxFile.Stat()
	if err != nil {
		t.Fatalf("stat idx file %s: %v", idxPath, err)
	}
	if stat.Size() == 0 {
		return 0
	}
	if stat.Size()%int64(types.NeedleMapEntrySize) != 0 {
		t.Fatalf("unexpected idx file size %d", stat.Size())
	}

	buf := make([]byte, types.NeedleMapEntrySize)
	if _, err := idxFile.ReadAt(buf, stat.Size()-int64(types.NeedleMapEntrySize)); err != nil {
		t.Fatalf("read idx entry: %v", err)
	}
	_, offset, _ := idx.IdxFileEntry(buf)
	if offset.IsZero() {
		return 0
	}

	datPath := baseName + ".dat"
	datFile, err := os.Open(datPath)
	if err != nil {
		t.Fatalf("open dat file %s: %v", datPath, err)
	}
	defer datFile.Close()

	datBackend := backend.NewDiskFile(datFile)
	n, _, _, err := needle.ReadNeedleHeader(datBackend, needle.GetCurrentVersion(), offset.ToActualOffset())
	if err != nil {
		t.Fatalf("read needle header: %v", err)
	}

	tailOffset := offset.ToActualOffset() + int64(types.NeedleHeaderSize) + int64(n.Size)
	tail := make([]byte, needle.NeedleChecksumSize+types.TimestampSize)
	readCount, readErr := datBackend.ReadAt(tail, tailOffset)
	if readErr == io.EOF && readCount == len(tail) {
		readErr = nil
	}
	if readErr != nil {
		t.Fatalf("read needle tail: %v", readErr)
	}

	return util.BytesToUint64(tail[needle.NeedleChecksumSize : needle.NeedleChecksumSize+types.TimestampSize])
}

func TestVolumeMoveHandlesInFlightWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartDualVolumeCluster(t, matrix.P1())
	sourceConn, sourceClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer sourceConn.Close()
	destConn, destClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer destConn.Close()

	const volumeID = uint32(988)
	framework.AllocateVolume(t, sourceClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 7, 0xABCDEF01)
	payload := []byte("volume-move-live-payload")
	uploadResp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(0), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("initial upload failed: %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sourceDataNode := cluster.VolumeAdminAddress(0) + "." + strings.Split(cluster.VolumeGRPCAddress(0), ":")[1]
	copyStream, err := destClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
		VolumeId:       volumeID,
		SourceDataNode: sourceDataNode,
	})
	if err != nil {
		t.Fatalf("VolumeCopy start failed: %v", err)
	}

	var lastAppendAtNs uint64
	for {
		resp, recvErr := copyStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("VolumeCopy recv failed: %v", recvErr)
		}
		if ts := resp.GetLastAppendAtNs(); ts > 0 {
			lastAppendAtNs = ts
		}
	}
	if lastAppendAtNs == 0 {
		t.Fatalf("volume copy did not return a last append timestamp")
	}

	type written struct {
		fid  string
		data []byte
	}

	var writesMu sync.Mutex
	var writes []written
	var uploadErr error
	writeCtx, writeCancel := context.WithCancel(context.Background())
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		client := framework.NewHTTPClient()
		for i := 0; i < 12; i++ {
			select {
			case <-writeCtx.Done():
				return
			default:
			}
			livePayload := []byte("live-data-" + fmt.Sprintf("%02d", i))
			liveFid := framework.NewFileID(volumeID, uint64(2000+i), 0xAAAA1111+uint32(i))
			resp := framework.UploadBytes(t, client, cluster.VolumeAdminURL(0), liveFid, livePayload)
			_ = framework.ReadAllAndClose(t, resp)
			if resp.StatusCode != http.StatusCreated {
				uploadErr = fmt.Errorf("live upload failed: %d", resp.StatusCode)
				return
			}
			writesMu.Lock()
			writes = append(writes, written{fid: liveFid, data: livePayload})
			writesMu.Unlock()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	tailCtx, tailCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer tailCancel()
	_, err = destClient.VolumeTailReceiver(tailCtx, &volume_server_pb.VolumeTailReceiverRequest{
		VolumeId:           volumeID,
		SourceVolumeServer: sourceDataNode,
		SinceNs:            lastAppendAtNs,
		IdleTimeoutSeconds: 3,
	})
	if err != nil {
		writeCancel()
		writerWG.Wait()
		t.Fatalf("VolumeTailReceiver failed: %v", err)
	}

	writeCancel()
	writerWG.Wait()

	if uploadErr != nil {
		t.Fatalf("upload goroutine error: %v", uploadErr)
	}

	writesMu.Lock()
	sampleCount := len(writes)
	if sampleCount == 0 {
		writesMu.Unlock()
		t.Fatal("no live writes captured")
	}
	sample := writes
	if sampleCount > 3 {
		sample = writes[sampleCount-3:]
	}
	writesMu.Unlock()

	httpCheckClient := framework.NewHTTPClient()
	for _, w := range sample {
		resp := framework.ReadBytes(t, httpCheckClient, cluster.VolumeAdminURL(1), w.fid)
		body := framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("dest read %s status %d", w.fid, resp.StatusCode)
		}
		if !bytes.Equal(body, w.data) {
			t.Fatalf("dest read body mismatch for %s: %q vs %q", w.fid, string(body), string(w.data))
		}
	}
}
