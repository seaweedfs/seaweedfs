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

func ReadBytes(t testing.TB, client *http.Client, volumeURL, fid string) *http.Response {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s", volumeURL, fid), nil)
	if err != nil {
		t.Fatalf("build read request: %v", err)
	}
	return DoRequest(t, client, req)
}
