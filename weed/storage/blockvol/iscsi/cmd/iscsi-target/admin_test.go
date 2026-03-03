package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestAdmin_SnapshotCreateListDelete creates a snapshot via the API,
// lists it, deletes it, and verifies the list is empty.
func TestAdmin_SnapshotCreateListDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap-admin.blk")

	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1024 * 1024,
		BlockSize:  4096,
		WALSize:    64 * 1024,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer vol.Close()

	logger := log.New(os.Stderr, "[test] ", 0)
	adm := newAdminServer(vol, "", logger)
	ln, err := startAdminServer("127.0.0.1:0", adm)
	if err != nil {
		t.Fatalf("start admin: %v", err)
	}
	defer ln.Close()
	base := "http://" + ln.Addr().String()

	// Create snapshot 1.
	body, _ := json.Marshal(snapshotRequest{Action: "create", ID: 1})
	resp, err := http.Post(base+"/snapshot", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /snapshot create: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("create snapshot: expected 200, got %d", resp.StatusCode)
	}

	// List snapshots.
	body2, _ := json.Marshal(snapshotRequest{Action: "list"})
	resp2, err := http.Post(base+"/snapshot", "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatalf("POST /snapshot list: %v", err)
	}
	var listResp struct {
		Snapshots []struct {
			ID uint32 `json:"id"`
		} `json:"snapshots"`
	}
	json.NewDecoder(resp2.Body).Decode(&listResp)
	resp2.Body.Close()
	if len(listResp.Snapshots) != 1 || listResp.Snapshots[0].ID != 1 {
		t.Fatalf("expected 1 snapshot with ID=1, got %+v", listResp.Snapshots)
	}

	// Delete snapshot 1.
	body3, _ := json.Marshal(snapshotRequest{Action: "delete", ID: 1})
	resp3, err := http.Post(base+"/snapshot", "application/json", bytes.NewReader(body3))
	if err != nil {
		t.Fatalf("POST /snapshot delete: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("delete snapshot: expected 200, got %d", resp3.StatusCode)
	}

	// Verify empty.
	body4, _ := json.Marshal(snapshotRequest{Action: "list"})
	resp4, err := http.Post(base+"/snapshot", "application/json", bytes.NewReader(body4))
	if err != nil {
		t.Fatalf("POST /snapshot list after delete: %v", err)
	}
	var listResp2 struct {
		Snapshots []struct{} `json:"snapshots"`
	}
	json.NewDecoder(resp4.Body).Decode(&listResp2)
	resp4.Body.Close()
	if len(listResp2.Snapshots) != 0 {
		t.Fatalf("expected 0 snapshots after delete, got %d", len(listResp2.Snapshots))
	}
}

// TestAdmin_ResizeExpand resizes a volume via the API and verifies the
// new size in /status.
func TestAdmin_ResizeExpand(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "resize-admin.blk")

	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1024 * 1024,
		BlockSize:  4096,
		WALSize:    64 * 1024,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer vol.Close()

	logger := log.New(os.Stderr, "[test] ", 0)
	adm := newAdminServer(vol, "", logger)
	ln, err := startAdminServer("127.0.0.1:0", adm)
	if err != nil {
		t.Fatalf("start admin: %v", err)
	}
	defer ln.Close()
	base := "http://" + ln.Addr().String()

	// Resize to 2MB.
	newSize := uint64(2 * 1024 * 1024)
	body, _ := json.Marshal(resizeRequest{NewSizeBytes: newSize})
	resp, err := http.Post(base+"/resize", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /resize: %v", err)
	}
	var resizeResp struct {
		OK         bool   `json:"ok"`
		VolumeSize uint64 `json:"volume_size"`
	}
	json.NewDecoder(resp.Body).Decode(&resizeResp)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("resize: expected 200, got %d", resp.StatusCode)
	}
	if !resizeResp.OK || resizeResp.VolumeSize != newSize {
		t.Fatalf("resize response: %+v", resizeResp)
	}

	// Verify size in /status.
	resp2, err := http.Get(base + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	var status statusResponse
	json.NewDecoder(resp2.Body).Decode(&status)
	resp2.Body.Close()
	if status.VolumeSize != newSize {
		t.Fatalf("status VolumeSize: expected %d, got %d", newSize, status.VolumeSize)
	}
}
