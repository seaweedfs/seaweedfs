package framework

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
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

func ReadBytes(t testing.TB, client *http.Client, volumeURL, fid string) *http.Response {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s", volumeURL, fid), nil)
	if err != nil {
		t.Fatalf("build read request: %v", err)
	}
	return DoRequest(t, client, req)
}
