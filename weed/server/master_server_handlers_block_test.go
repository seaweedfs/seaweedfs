package weed_server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

// blockTestServer creates a MasterServer + httptest.Server with block routes registered.
func blockTestServer(t *testing.T) (*MasterServer, *httptest.Server) {
	t.Helper()
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
	}
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:      fmt.Sprintf("/data/%s.blk", name),
			IQN:       fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr: server + ":3260",
		}, nil
	}
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		return nil
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	r := mux.NewRouter()
	r.HandleFunc("/block/volume", ms.blockVolumeCreateHandler).Methods("POST")
	r.HandleFunc("/block/volume/{name}", ms.blockVolumeDeleteHandler).Methods("DELETE")
	r.HandleFunc("/block/volume/{name}", ms.blockVolumeLookupHandler).Methods("GET")
	r.HandleFunc("/block/volumes", ms.blockVolumeListHandler).Methods("GET")
	r.HandleFunc("/block/assign", ms.blockAssignHandler).Methods("POST")
	r.HandleFunc("/block/servers", ms.blockServersHandler).Methods("GET")

	ts := httptest.NewServer(r)
	t.Cleanup(ts.Close)
	return ms, ts
}

func TestBlockVolumeCreateHandler(t *testing.T) {
	_, ts := blockTestServer(t)

	body, _ := json.Marshal(blockapi.CreateVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	resp, err := http.Post(ts.URL+"/block/volume", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var info blockapi.VolumeInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatal(err)
	}
	if info.Name != "vol1" {
		t.Errorf("expected name vol1, got %s", info.Name)
	}
	if info.VolumeServer == "" {
		t.Error("expected non-empty volume server")
	}
}

func TestBlockVolumeListHandler(t *testing.T) {
	ms, ts := blockTestServer(t)

	// Create two volumes via gRPC.
	for _, name := range []string{"alpha", "beta"} {
		ms.blockRegistry.Register(&BlockVolumeEntry{
			Name:         name,
			VolumeServer: "vs1:9333",
			SizeBytes:    1 << 20,
			Status:       StatusActive,
		})
	}

	resp, err := http.Get(ts.URL + "/block/volumes")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var infos []blockapi.VolumeInfo
	if err := json.NewDecoder(resp.Body).Decode(&infos); err != nil {
		t.Fatal(err)
	}
	if len(infos) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(infos))
	}
	if infos[0].Name != "alpha" || infos[1].Name != "beta" {
		t.Errorf("expected sorted [alpha, beta], got [%s, %s]", infos[0].Name, infos[1].Name)
	}
}

func TestBlockVolumeLookupHandler(t *testing.T) {
	ms, ts := blockTestServer(t)

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name:          "vol1",
		VolumeServer:  "vs1:9333",
		SizeBytes:     1 << 30,
		Epoch:         3,
		Role:          1, // RolePrimary
		Status:        StatusActive,
		ReplicaServer: "vs2:9333",
	})

	resp, err := http.Get(ts.URL + "/block/volume/vol1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var info blockapi.VolumeInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatal(err)
	}
	if info.Epoch != 3 {
		t.Errorf("expected epoch 3, got %d", info.Epoch)
	}
	if info.Role != "primary" {
		t.Errorf("expected role primary, got %s", info.Role)
	}
	if info.ReplicaServer != "vs2:9333" {
		t.Errorf("expected replica vs2:9333, got %s", info.ReplicaServer)
	}

	// Not found case.
	resp2, err := http.Get(ts.URL + "/block/volume/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp2.StatusCode)
	}
}

func TestBlockVolumeDeleteHandler(t *testing.T) {
	ms, ts := blockTestServer(t)

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "vs1:9333",
		SizeBytes:    1 << 30,
		Status:       StatusActive,
	})

	req, _ := http.NewRequest("DELETE", ts.URL+"/block/volume/vol1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify it's gone.
	if _, ok := ms.blockRegistry.Lookup("vol1"); ok {
		t.Error("expected vol1 to be unregistered")
	}
}

func TestBlockAssignHandler(t *testing.T) {
	ms, ts := blockTestServer(t)

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "vs1:9333",
		Path:         "/data/vol1.blk",
		SizeBytes:    1 << 30,
		Status:       StatusActive,
	})

	body, _ := json.Marshal(blockapi.AssignRequest{
		Name:       "vol1",
		Epoch:      5,
		Role:       "primary",
		LeaseTTLMs: 30000,
	})
	resp, err := http.Post(ts.URL+"/block/assign", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify assignment was queued.
	pending := ms.blockAssignmentQueue.Peek("vs1:9333")
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending assignment, got %d", len(pending))
	}
	if pending[0].Epoch != 5 {
		t.Errorf("expected epoch 5, got %d", pending[0].Epoch)
	}

	// Not-found case.
	body2, _ := json.Marshal(blockapi.AssignRequest{Name: "missing", Epoch: 1, Role: "primary"})
	resp2, err := http.Post(ts.URL+"/block/assign", "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp2.StatusCode)
	}
}

func TestBlockServersHandler(t *testing.T) {
	_, ts := blockTestServer(t)

	resp, err := http.Get(ts.URL + "/block/servers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var servers []blockapi.ServerInfo
	if err := json.NewDecoder(resp.Body).Decode(&servers); err != nil {
		t.Fatal(err)
	}
	if len(servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(servers))
	}
	if servers[0].Address != "vs1:9333" {
		t.Errorf("expected vs1:9333, got %s", servers[0].Address)
	}
}

func TestListAll(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "charlie", VolumeServer: "vs1:9333"})
	r.Register(&BlockVolumeEntry{Name: "alpha", VolumeServer: "vs1:9333"})
	r.Register(&BlockVolumeEntry{Name: "bravo", VolumeServer: "vs1:9333"})

	entries := r.ListAll()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if entries[0].Name != "alpha" || entries[1].Name != "bravo" || entries[2].Name != "charlie" {
		t.Errorf("expected sorted order, got %s %s %s", entries[0].Name, entries[1].Name, entries[2].Name)
	}
}

func TestServerSummaries(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("vs1:9333")
	r.MarkBlockCapable("vs2:9333")
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "vs1:9333"})
	r.Register(&BlockVolumeEntry{Name: "vol2", VolumeServer: "vs1:9333"})
	r.Register(&BlockVolumeEntry{Name: "vol3", VolumeServer: "vs2:9333"})

	summaries := r.ServerSummaries()
	if len(summaries) != 2 {
		t.Fatalf("expected 2 summaries, got %d", len(summaries))
	}
	// Sorted by address.
	if summaries[0].Address != "vs1:9333" || summaries[0].VolumeCount != 2 {
		t.Errorf("vs1: got %+v", summaries[0])
	}
	if summaries[1].Address != "vs2:9333" || summaries[1].VolumeCount != 1 {
		t.Errorf("vs2: got %+v", summaries[1])
	}
}
