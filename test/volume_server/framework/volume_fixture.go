package framework

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func AllocateVolume(t testing.TB, client volume_server_pb.VolumeServerClient, volumeID uint32, collection string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.AllocateVolume(ctx, &volume_server_pb.AllocateVolumeRequest{
		VolumeId:    volumeID,
		Collection:  collection,
		Replication: "000",
		Version:     uint32(needle.GetCurrentVersion()),
	})
	if err != nil {
		t.Fatalf("allocate volume %d: %v", volumeID, err)
	}
}

func NewFileID(volumeID uint32, key uint64, cookie uint32) string {
	return needle.NewFileId(needle.VolumeId(volumeID), key, cookie).String()
}

func UploadBytes(t testing.TB, client *http.Client, volumeURL, fid string, data []byte) *http.Response {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/%s", volumeURL, fid), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build upload request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(data)))
	return DoRequest(t, client, req)
}

// CorruptIndexFile appends garbage bytes to a volume's .idx file on disk so
// that CheckIndexFile detects a size mismatch during scrub.
func CorruptIndexFile(t testing.TB, baseDir string, volumeID uint32) {
	t.Helper()
	idxPath := filepath.Join(baseDir, "volume", fmt.Sprintf("%d.idx", volumeID))
	f, err := os.OpenFile(idxPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open idx file for corruption: %v", err)
	}
	defer f.Close()
	if _, err := f.Write([]byte{0xDE, 0xAD}); err != nil {
		t.Fatalf("corrupt idx file: %v", err)
	}
}

// EnableMaintenanceMode puts the volume server into maintenance mode.
func EnableMaintenanceMode(t testing.TB, ctx context.Context, client volume_server_pb.VolumeServerClient) {
	t.Helper()
	stateResp, err := client.GetState(ctx, &volume_server_pb.GetStateRequest{})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	_, err = client.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{
			Maintenance: true,
			Version:     stateResp.GetState().GetVersion(),
		},
	})
	if err != nil {
		t.Fatalf("SetState maintenance=true failed: %v", err)
	}
}

// CorruptDatFile overwrites a portion of a volume's .dat file with garbage
// bytes so that needle data verification fails during a full scrub.
func CorruptDatFile(t testing.TB, baseDir string, volumeID uint32) {
	t.Helper()
	datPath := filepath.Join(baseDir, "volume", fmt.Sprintf("%d.dat", volumeID))
	f, err := os.OpenFile(datPath, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open dat file for corruption: %v", err)
	}
	defer f.Close()
	// Write garbage past the superblock (8 bytes) to corrupt needle data.
	if _, err := f.WriteAt([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF}, 8); err != nil {
		t.Fatalf("corrupt dat file: %v", err)
	}
}

// CorruptEcxFile appends garbage bytes to a volume's .ecx file on disk so
// that CheckIndexFile detects a size mismatch during EC index scrub.
func CorruptEcxFile(t testing.TB, baseDir string, volumeID uint32) {
	t.Helper()
	ecxPath := filepath.Join(baseDir, "volume", fmt.Sprintf("%d.ecx", volumeID))
	f, err := os.OpenFile(ecxPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open ecx file for corruption: %v", err)
	}
	defer f.Close()
	if _, err := f.Write([]byte{0xDE, 0xAD}); err != nil {
		t.Fatalf("corrupt ecx file: %v", err)
	}
}

// CorruptEcShardFile truncates an EC shard file to 1 byte so that local shard
// reads fail during an EC scrub.
func CorruptEcShardFile(t testing.TB, baseDir string, volumeID uint32, shardID int) {
	t.Helper()
	shardPath := filepath.Join(baseDir, "volume", fmt.Sprintf("%d.ec%02d", volumeID, shardID))
	if err := os.Truncate(shardPath, 1); err != nil {
		t.Fatalf("truncate EC shard file %s: %v", shardPath, err)
	}
}

func ReadBytes(t testing.TB, client *http.Client, volumeURL, fid string) *http.Response {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s", volumeURL, fid), nil)
	if err != nil {
		t.Fatalf("build read request: %v", err)
	}
	return DoRequest(t, client, req)
}
