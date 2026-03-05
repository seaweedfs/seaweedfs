package weed_server

import (
	"fmt"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestRegistry_RegisterLookup(t *testing.T) {
	r := NewBlockVolumeRegistry()
	entry := &BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "server1:9333",
		Path:         "/data/vol1.blk",
		IQN:          "iqn.2024.com.seaweedfs:vol1",
		ISCSIAddr:    "10.0.0.1:3260",
		SizeBytes:    1 << 30,
		Epoch:        1,
		Role:         1,
		Status:       StatusPending,
	}
	if err := r.Register(entry); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, ok := r.Lookup("vol1")
	if !ok {
		t.Fatal("Lookup: not found")
	}
	if got.Name != "vol1" || got.VolumeServer != "server1:9333" || got.Path != "/data/vol1.blk" {
		t.Fatalf("Lookup: unexpected entry: %+v", got)
	}
	if got.Status != StatusPending {
		t.Fatalf("Status: got %d, want %d", got.Status, StatusPending)
	}
}

func TestRegistry_Unregister(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/vol1.blk"})
	removed := r.Unregister("vol1")
	if removed == nil {
		t.Fatal("Unregister returned nil")
	}
	if _, ok := r.Lookup("vol1"); ok {
		t.Fatal("vol1 should not be found after Unregister")
	}
	// Double unregister returns nil.
	if r.Unregister("vol1") != nil {
		t.Fatal("double Unregister should return nil")
	}
}

func TestRegistry_DuplicateRegister(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/vol1.blk"})
	err := r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s2", Path: "/vol1.blk"})
	if err == nil {
		t.Fatal("duplicate Register should return error")
	}
}

func TestRegistry_ListByServer(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk"})
	r.Register(&BlockVolumeEntry{Name: "vol2", VolumeServer: "s1", Path: "/v2.blk"})
	r.Register(&BlockVolumeEntry{Name: "vol3", VolumeServer: "s2", Path: "/v3.blk"})

	s1Vols := r.ListByServer("s1")
	if len(s1Vols) != 2 {
		t.Fatalf("ListByServer(s1): got %d, want 2", len(s1Vols))
	}
	s2Vols := r.ListByServer("s2")
	if len(s2Vols) != 1 {
		t.Fatalf("ListByServer(s2): got %d, want 1", len(s2Vols))
	}
	s3Vols := r.ListByServer("s3")
	if len(s3Vols) != 0 {
		t.Fatalf("ListByServer(s3): got %d, want 0", len(s3Vols))
	}
}

func TestRegistry_UpdateFullHeartbeat(t *testing.T) {
	r := NewBlockVolumeRegistry()
	// Register two volumes on server s1.
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk", Status: StatusPending})
	r.Register(&BlockVolumeEntry{Name: "vol2", VolumeServer: "s1", Path: "/v2.blk", Status: StatusPending})

	// Full heartbeat reports only vol1 (vol2 is stale).
	r.UpdateFullHeartbeat("s1", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/v1.blk", Epoch: 5, Role: 1},
	})

	// vol1 should be Active.
	e1, ok := r.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 should exist after full heartbeat")
	}
	if e1.Status != StatusActive {
		t.Fatalf("vol1 status: got %d, want %d", e1.Status, StatusActive)
	}
	if e1.Epoch != 5 {
		t.Fatalf("vol1 epoch: got %d, want 5", e1.Epoch)
	}

	// vol2 should be removed (stale).
	if _, ok := r.Lookup("vol2"); ok {
		t.Fatal("vol2 should have been removed as stale")
	}
}

func TestRegistry_UpdateDeltaHeartbeat(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk", Status: StatusPending})
	r.Register(&BlockVolumeEntry{Name: "vol2", VolumeServer: "s1", Path: "/v2.blk", Status: StatusActive})

	// Delta: vol1 newly appeared, vol2 deleted.
	r.UpdateDeltaHeartbeat("s1",
		[]*master_pb.BlockVolumeShortInfoMessage{{Path: "/v1.blk"}},
		[]*master_pb.BlockVolumeShortInfoMessage{{Path: "/v2.blk"}},
	)

	// vol1 should be Active.
	e1, ok := r.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 should exist")
	}
	if e1.Status != StatusActive {
		t.Fatalf("vol1 status: got %d, want Active", e1.Status)
	}

	// vol2 should be removed.
	if _, ok := r.Lookup("vol2"); ok {
		t.Fatal("vol2 should have been removed by delta")
	}
}

func TestRegistry_PendingToActive(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "s1", Path: "/v1.blk",
		Status: StatusPending, Epoch: 1,
	})

	// Full heartbeat confirms the volume.
	r.UpdateFullHeartbeat("s1", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/v1.blk", Epoch: 1, Role: 1},
	})

	e, _ := r.Lookup("vol1")
	if e.Status != StatusActive {
		t.Fatalf("expected Active after heartbeat, got %d", e.Status)
	}
}

func TestRegistry_PickServer(t *testing.T) {
	r := NewBlockVolumeRegistry()
	// s1 has 2 volumes, s2 has 1, s3 has 0.
	r.Register(&BlockVolumeEntry{Name: "v1", VolumeServer: "s1", Path: "/v1.blk"})
	r.Register(&BlockVolumeEntry{Name: "v2", VolumeServer: "s1", Path: "/v2.blk"})
	r.Register(&BlockVolumeEntry{Name: "v3", VolumeServer: "s2", Path: "/v3.blk"})

	got, err := r.PickServer([]string{"s1", "s2", "s3"})
	if err != nil {
		t.Fatalf("PickServer: %v", err)
	}
	if got != "s3" {
		t.Fatalf("PickServer: got %q, want s3 (fewest volumes)", got)
	}
}

func TestRegistry_PickServerEmpty(t *testing.T) {
	r := NewBlockVolumeRegistry()
	_, err := r.PickServer(nil)
	if err == nil {
		t.Fatal("PickServer with no servers should return error")
	}
}

func TestRegistry_InflightLock(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// First acquire succeeds.
	if !r.AcquireInflight("vol1") {
		t.Fatal("first AcquireInflight should succeed")
	}

	// Second acquire for same name fails.
	if r.AcquireInflight("vol1") {
		t.Fatal("second AcquireInflight for same name should fail")
	}

	// Different name succeeds.
	if !r.AcquireInflight("vol2") {
		t.Fatal("AcquireInflight for different name should succeed")
	}

	// Release and re-acquire.
	r.ReleaseInflight("vol1")
	if !r.AcquireInflight("vol1") {
		t.Fatal("AcquireInflight after release should succeed")
	}

	r.ReleaseInflight("vol1")
	r.ReleaseInflight("vol2")
}

func TestRegistry_UnmarkDeadServer(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("s1")
	r.MarkBlockCapable("s2")

	servers := r.BlockCapableServers()
	if len(servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(servers))
	}

	// Simulate s1 disconnect.
	r.UnmarkBlockCapable("s1")

	servers = r.BlockCapableServers()
	if len(servers) != 1 {
		t.Fatalf("expected 1 server after unmark, got %d", len(servers))
	}
	if servers[0] != "s2" {
		t.Fatalf("expected s2, got %s", servers[0])
	}
}

func TestRegistry_FullHeartbeatUpdatesSizeBytes(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "s1", Path: "/v1.blk",
		SizeBytes: 1 << 30, Status: StatusPending,
	})

	// Heartbeat with updated size (online resize).
	r.UpdateFullHeartbeat("s1", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/v1.blk", VolumeSize: 2 << 30, Epoch: 1, Role: 1},
	})

	e, _ := r.Lookup("vol1")
	if e.SizeBytes != 2<<30 {
		t.Fatalf("SizeBytes: got %d, want %d", e.SizeBytes, 2<<30)
	}
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	r := NewBlockVolumeRegistry()
	var wg sync.WaitGroup
	n := 50

	// Concurrent register.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("vol%d", i)
			r.Register(&BlockVolumeEntry{
				Name: name, VolumeServer: "s1",
				Path: fmt.Sprintf("/v%d.blk", i),
			})
		}(i)
	}
	wg.Wait()

	// All should be findable.
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("vol%d", i)
		if _, ok := r.Lookup(name); !ok {
			t.Fatalf("vol%d not found after concurrent register", i)
		}
	}

	// Concurrent unregister.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r.Unregister(fmt.Sprintf("vol%d", i))
		}(i)
	}
	wg.Wait()

	// All should be gone.
	for i := 0; i < n; i++ {
		if _, ok := r.Lookup(fmt.Sprintf("vol%d", i)); ok {
			t.Fatalf("vol%d found after concurrent unregister", i)
		}
	}
}

func TestRegistry_SetReplica(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk"})

	err := r.SetReplica("vol1", "s2", "/replica/v1.blk", "10.0.0.2:3260", "iqn.2024.test:vol1-replica")
	if err != nil {
		t.Fatalf("SetReplica: %v", err)
	}

	e, _ := r.Lookup("vol1")
	if e.ReplicaServer != "s2" {
		t.Fatalf("ReplicaServer: got %q, want s2", e.ReplicaServer)
	}
	if e.ReplicaPath != "/replica/v1.blk" {
		t.Fatalf("ReplicaPath: got %q", e.ReplicaPath)
	}
	if e.ReplicaISCSIAddr != "10.0.0.2:3260" {
		t.Fatalf("ReplicaISCSIAddr: got %q", e.ReplicaISCSIAddr)
	}
	if e.ReplicaIQN != "iqn.2024.test:vol1-replica" {
		t.Fatalf("ReplicaIQN: got %q", e.ReplicaIQN)
	}

	// Replica server should appear in byServer index.
	s2Vols := r.ListByServer("s2")
	if len(s2Vols) != 1 || s2Vols[0].Name != "vol1" {
		t.Fatalf("ListByServer(s2): got %v, want [vol1]", s2Vols)
	}
}

func TestRegistry_ClearReplica(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk"})
	r.SetReplica("vol1", "s2", "/replica/v1.blk", "10.0.0.2:3260", "iqn.2024.test:vol1-replica")

	err := r.ClearReplica("vol1")
	if err != nil {
		t.Fatalf("ClearReplica: %v", err)
	}

	e, _ := r.Lookup("vol1")
	if e.ReplicaServer != "" {
		t.Fatalf("ReplicaServer should be empty, got %q", e.ReplicaServer)
	}
	if e.ReplicaPath != "" || e.ReplicaISCSIAddr != "" || e.ReplicaIQN != "" {
		t.Fatal("replica fields should be empty after ClearReplica")
	}

	// Replica server should be gone from byServer index.
	s2Vols := r.ListByServer("s2")
	if len(s2Vols) != 0 {
		t.Fatalf("ListByServer(s2) after clear: got %d, want 0", len(s2Vols))
	}
}

func TestRegistry_SetReplicaNotFound(t *testing.T) {
	r := NewBlockVolumeRegistry()
	err := r.SetReplica("nonexistent", "s2", "/r.blk", "addr", "iqn")
	if err == nil {
		t.Fatal("SetReplica on nonexistent volume should return error")
	}
}

func TestRegistry_SwapPrimaryReplica(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:             "vol1",
		VolumeServer:     "s1",
		Path:             "/v1.blk",
		IQN:              "iqn:vol1-primary",
		ISCSIAddr:        "10.0.0.1:3260",
		ReplicaServer:    "s2",
		ReplicaPath:      "/replica/v1.blk",
		ReplicaIQN:       "iqn:vol1-replica",
		ReplicaISCSIAddr: "10.0.0.2:3260",
		Epoch:            3,
		Role:             1,
	})

	newEpoch, err := r.SwapPrimaryReplica("vol1")
	if err != nil {
		t.Fatalf("SwapPrimaryReplica: %v", err)
	}
	if newEpoch != 4 {
		t.Fatalf("newEpoch: got %d, want 4", newEpoch)
	}

	e, _ := r.Lookup("vol1")
	// New primary should be the old replica.
	if e.VolumeServer != "s2" {
		t.Fatalf("VolumeServer after swap: got %q, want s2", e.VolumeServer)
	}
	if e.Path != "/replica/v1.blk" {
		t.Fatalf("Path after swap: got %q", e.Path)
	}
	if e.Epoch != 4 {
		t.Fatalf("Epoch after swap: got %d, want 4", e.Epoch)
	}
	// Old primary should become replica.
	if e.ReplicaServer != "s1" {
		t.Fatalf("ReplicaServer after swap: got %q, want s1", e.ReplicaServer)
	}
	if e.ReplicaPath != "/v1.blk" {
		t.Fatalf("ReplicaPath after swap: got %q", e.ReplicaPath)
	}
}

func TestFullHeartbeat_UpdatesReplicaAddrs(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "server1",
		Path:         "/data/vol1.blk",
		SizeBytes:    1 << 30,
		Status:       StatusPending,
	})

	// Full heartbeat includes replica addresses.
	r.UpdateFullHeartbeat("server1", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:            "/data/vol1.blk",
			VolumeSize:      1 << 30,
			Epoch:           5,
			Role:            1,
			ReplicaDataAddr: "10.0.0.2:14260",
			ReplicaCtrlAddr: "10.0.0.2:14261",
		},
	})

	entry, ok := r.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 not found after heartbeat")
	}
	if entry.Status != StatusActive {
		t.Fatalf("expected Active, got %v", entry.Status)
	}
	if entry.ReplicaDataAddr != "10.0.0.2:14260" {
		t.Fatalf("ReplicaDataAddr: got %q, want 10.0.0.2:14260", entry.ReplicaDataAddr)
	}
	if entry.ReplicaCtrlAddr != "10.0.0.2:14261" {
		t.Fatalf("ReplicaCtrlAddr: got %q, want 10.0.0.2:14261", entry.ReplicaCtrlAddr)
	}
}
