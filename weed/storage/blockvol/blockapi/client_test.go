package blockapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientCreateVolume(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/block/volume" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var req CreateVolumeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.Name != "test-vol" {
			t.Errorf("expected name test-vol, got %s", req.Name)
		}
		if req.WALSizeBytes != 256<<20 {
			t.Errorf("expected wal_size_bytes %d, got %d", 256<<20, req.WALSizeBytes)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(VolumeInfo{
			Name:         req.Name,
			VolumeServer: "vs1:9333",
			SizeBytes:    req.SizeBytes,
			Epoch:        1,
			Role:         "primary",
			Status:       "active",
		})
	}))
	defer ts.Close()

	client := NewClient(ts.URL)
	info, err := client.CreateVolume(context.Background(), CreateVolumeRequest{
		Name:         "test-vol",
		SizeBytes:    1 << 30,
		WALSizeBytes: 256 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "test-vol" {
		t.Errorf("expected name test-vol, got %s", info.Name)
	}
	if info.VolumeServer != "vs1:9333" {
		t.Errorf("expected vs1:9333, got %s", info.VolumeServer)
	}
}

func TestClientListVolumes(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/block/volumes" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]VolumeInfo{
			{Name: "alpha", VolumeServer: "vs1:9333"},
			{Name: "beta", VolumeServer: "vs2:9333"},
		})
	}))
	defer ts.Close()

	client := NewClient(ts.URL)
	vols, err := client.ListVolumes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(vols) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(vols))
	}
	if vols[0].Name != "alpha" || vols[1].Name != "beta" {
		t.Errorf("unexpected volumes: %+v", vols)
	}
}

func TestClientMultiMasterFallback(t *testing.T) {
	// First server immediately rejects connections (closed listener).
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close() // close it so connections are refused

	// Second server responds normally.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]VolumeInfo{})
	}))
	defer ts.Close()

	client := NewClient(deadURL + "," + ts.URL)
	vols, err := client.ListVolumes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if vols == nil {
		t.Error("expected non-nil result")
	}
}
