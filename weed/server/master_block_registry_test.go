package weed_server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
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

// --- CP8-2 new tests ---

func TestRegistry_AddReplica(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk"})

	err := r.AddReplica("vol1", ReplicaInfo{
		Server:    "s2",
		Path:      "/replica/v1.blk",
		ISCSIAddr: "10.0.0.2:3260",
		IQN:       "iqn:vol1-r1",
		DataAddr:  "s2:14260",
		CtrlAddr:  "s2:14261",
	})
	if err != nil {
		t.Fatalf("AddReplica: %v", err)
	}

	e, _ := r.Lookup("vol1")
	if len(e.Replicas) != 1 {
		t.Fatalf("Replicas len: got %d, want 1", len(e.Replicas))
	}
	if e.Replicas[0].Server != "s2" {
		t.Fatalf("Replicas[0].Server: got %q", e.Replicas[0].Server)
	}
	// Deprecated scalar should be synced.
	if e.ReplicaServer != "s2" {
		t.Fatalf("ReplicaServer (deprecated): got %q", e.ReplicaServer)
	}
	// byServer index should include replica.
	if len(r.ListByServer("s2")) != 1 {
		t.Fatalf("ListByServer(s2): got %d, want 1", len(r.ListByServer("s2")))
	}
}

func TestRegistry_AddReplica_TwoRF3(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk", ReplicaFactor: 3})

	r.AddReplica("vol1", ReplicaInfo{Server: "s2", Path: "/r1.blk", IQN: "iqn:r1"})
	r.AddReplica("vol1", ReplicaInfo{Server: "s3", Path: "/r2.blk", IQN: "iqn:r2"})

	e, _ := r.Lookup("vol1")
	if len(e.Replicas) != 2 {
		t.Fatalf("Replicas len: got %d, want 2", len(e.Replicas))
	}
	if e.Replicas[0].Server != "s2" || e.Replicas[1].Server != "s3" {
		t.Fatalf("Replicas: got %+v", e.Replicas)
	}
	// byServer index should include both.
	if len(r.ListByServer("s2")) != 1 || len(r.ListByServer("s3")) != 1 {
		t.Fatal("byServer should include both replica servers")
	}
}

func TestRegistry_AddReplica_Upsert(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk"})

	r.AddReplica("vol1", ReplicaInfo{Server: "s2", Path: "/r1.blk"})
	r.AddReplica("vol1", ReplicaInfo{Server: "s2", Path: "/r1-new.blk"})

	e, _ := r.Lookup("vol1")
	if len(e.Replicas) != 1 {
		t.Fatalf("Replicas len: got %d, want 1 (upsert, not duplicate)", len(e.Replicas))
	}
	if e.Replicas[0].Path != "/r1-new.blk" {
		t.Fatalf("Replicas[0].Path: got %q, want /r1-new.blk", e.Replicas[0].Path)
	}
}

func TestRegistry_RemoveReplica(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk"})
	r.AddReplica("vol1", ReplicaInfo{Server: "s2", Path: "/r1.blk"})
	r.AddReplica("vol1", ReplicaInfo{Server: "s3", Path: "/r2.blk"})

	err := r.RemoveReplica("vol1", "s2")
	if err != nil {
		t.Fatalf("RemoveReplica: %v", err)
	}

	e, _ := r.Lookup("vol1")
	if len(e.Replicas) != 1 {
		t.Fatalf("Replicas len: got %d, want 1", len(e.Replicas))
	}
	if e.Replicas[0].Server != "s3" {
		t.Fatalf("remaining replica should be s3, got %q", e.Replicas[0].Server)
	}
	// Deprecated scalar should sync to first remaining replica.
	if e.ReplicaServer != "s3" {
		t.Fatalf("ReplicaServer (deprecated): got %q, want s3", e.ReplicaServer)
	}
	// s2 should be removed from byServer.
	if len(r.ListByServer("s2")) != 0 {
		t.Fatalf("ListByServer(s2): got %d, want 0", len(r.ListByServer("s2")))
	}
}

func TestRegistry_PromoteBestReplica_PicksHighest(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "s1",
		Path:         "/v1.blk",
		Epoch:        5,
		Role:         1,
		Replicas: []ReplicaInfo{
			{Server: "s2", Path: "/r1.blk", IQN: "iqn:r1", ISCSIAddr: "s2:3260", HealthScore: 0.8, WALHeadLSN: 100},
			{Server: "s3", Path: "/r2.blk", IQN: "iqn:r2", ISCSIAddr: "s3:3260", HealthScore: 0.95, WALHeadLSN: 90},
		},
	})
	// Add to byServer for s2 and s3.
	r.mu.Lock()
	r.addToServer("s2", "vol1")
	r.addToServer("s3", "vol1")
	r.mu.Unlock()

	newEpoch, err := r.PromoteBestReplica("vol1")
	if err != nil {
		t.Fatalf("PromoteBestReplica: %v", err)
	}
	if newEpoch != 6 {
		t.Fatalf("newEpoch: got %d, want 6", newEpoch)
	}

	e, _ := r.Lookup("vol1")
	// s3 had higher health score → promoted.
	if e.VolumeServer != "s3" {
		t.Fatalf("VolumeServer: got %q, want s3 (higher health)", e.VolumeServer)
	}
	if e.Path != "/r2.blk" {
		t.Fatalf("Path: got %q", e.Path)
	}
	// s2 should remain in Replicas.
	if len(e.Replicas) != 1 {
		t.Fatalf("Replicas len: got %d, want 1 (s2 stays)", len(e.Replicas))
	}
	if e.Replicas[0].Server != "s2" {
		t.Fatalf("remaining replica: got %q, want s2", e.Replicas[0].Server)
	}
}

func TestRegistry_PromoteBestReplica_NoReplica(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk"})

	_, err := r.PromoteBestReplica("vol1")
	if err == nil {
		t.Fatal("PromoteBestReplica with no replicas should return error")
	}
}

func TestRegistry_PromoteBestReplica_TiebreakByLSN(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "s1",
		Path:         "/v1.blk",
		Epoch:        3,
		Replicas: []ReplicaInfo{
			{Server: "s2", Path: "/r1.blk", IQN: "iqn:r1", ISCSIAddr: "s2:3260", HealthScore: 0.9, WALHeadLSN: 50},
			{Server: "s3", Path: "/r2.blk", IQN: "iqn:r2", ISCSIAddr: "s3:3260", HealthScore: 0.9, WALHeadLSN: 100},
		},
	})
	r.mu.Lock()
	r.addToServer("s2", "vol1")
	r.addToServer("s3", "vol1")
	r.mu.Unlock()

	newEpoch, err := r.PromoteBestReplica("vol1")
	if err != nil {
		t.Fatalf("PromoteBestReplica: %v", err)
	}
	if newEpoch != 4 {
		t.Fatalf("newEpoch: got %d, want 4", newEpoch)
	}

	e, _ := r.Lookup("vol1")
	// Same health → tie-break by WALHeadLSN → s3 wins.
	if e.VolumeServer != "s3" {
		t.Fatalf("VolumeServer: got %q, want s3 (higher LSN)", e.VolumeServer)
	}
	if len(e.Replicas) != 1 || e.Replicas[0].Server != "s2" {
		t.Fatalf("remaining replica: got %+v, want [s2]", e.Replicas)
	}
}

func TestRegistry_PromoteBestReplica_KeepsOthers(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "s1",
		Path:         "/v1.blk",
		Epoch:        1,
		Replicas: []ReplicaInfo{
			{Server: "s2", Path: "/r1.blk", IQN: "iqn:r1", ISCSIAddr: "s2:3260", HealthScore: 1.0, WALHeadLSN: 100},
			{Server: "s3", Path: "/r2.blk", IQN: "iqn:r2", ISCSIAddr: "s3:3260", HealthScore: 0.5, WALHeadLSN: 100},
		},
	})
	r.mu.Lock()
	r.addToServer("s2", "vol1")
	r.addToServer("s3", "vol1")
	r.mu.Unlock()

	r.PromoteBestReplica("vol1")

	e, _ := r.Lookup("vol1")
	// s2 promoted, s3 stays.
	if e.VolumeServer != "s2" {
		t.Fatalf("VolumeServer: got %q, want s2", e.VolumeServer)
	}
	if len(e.Replicas) != 1 || e.Replicas[0].Server != "s3" {
		t.Fatalf("remaining replicas: got %+v, want [s3]", e.Replicas)
	}
}

func TestRegistry_BackwardCompatAccessors(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk"})

	e, _ := r.Lookup("vol1")
	if e.HasReplica() {
		t.Fatal("HasReplica should be false with no replicas")
	}
	if e.FirstReplica() != nil {
		t.Fatal("FirstReplica should be nil")
	}
	if e.BestReplicaForPromotion() != nil {
		t.Fatal("BestReplicaForPromotion should be nil")
	}

	r.AddReplica("vol1", ReplicaInfo{Server: "s2", Path: "/r.blk", HealthScore: 0.9})

	e, _ = r.Lookup("vol1")
	if !e.HasReplica() {
		t.Fatal("HasReplica should be true after AddReplica")
	}
	if e.FirstReplica() == nil || e.FirstReplica().Server != "s2" {
		t.Fatal("FirstReplica should return s2")
	}
	if e.ReplicaByServer("s2") == nil {
		t.Fatal("ReplicaByServer(s2) should not be nil")
	}
	if e.ReplicaByServer("s3") != nil {
		t.Fatal("ReplicaByServer(s3) should be nil")
	}
}

func TestRegistry_ReplicaFactorDefault(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk"})

	e, _ := r.Lookup("vol1")
	// ReplicaFactor defaults to 0 (zero value). API handler defaults to 2.
	if e.ReplicaFactor != 0 {
		t.Fatalf("default ReplicaFactor: got %d, want 0", e.ReplicaFactor)
	}

	// Explicit RF=3.
	r.Register(&BlockVolumeEntry{Name: "vol2", VolumeServer: "s1", Path: "/v2.blk", ReplicaFactor: 3})
	e2, _ := r.Lookup("vol2")
	if e2.ReplicaFactor != 3 {
		t.Fatalf("ReplicaFactor: got %d, want 3", e2.ReplicaFactor)
	}
}

func TestRegistry_FullHeartbeat_UpdatesHealthScore(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "s1",
		Path:         "/v1.blk",
		Status:       StatusPending,
	})

	r.UpdateFullHeartbeat("s1", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:        "/v1.blk",
			VolumeSize:  1 << 30,
			Epoch:       1,
			Role:        1,
			HealthScore: 0.85,
			ScrubErrors: 2,
			WalHeadLsn:  500,
		},
	})

	e, _ := r.Lookup("vol1")
	if e.HealthScore != 0.85 {
		t.Fatalf("HealthScore: got %f, want 0.85", e.HealthScore)
	}
	if e.WALHeadLSN != 500 {
		t.Fatalf("WALHeadLSN: got %d, want 500", e.WALHeadLSN)
	}
}

// Fix #1: Replica heartbeat must NOT delete the volume.
func TestRegistry_ReplicaHeartbeat_DoesNotDeleteVolume(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Status:       StatusActive,
		Replicas: []ReplicaInfo{
			{Server: "replica1", Path: "/data/vol1.blk"},
		},
	})

	// Replica sends heartbeat reporting its path.
	r.UpdateFullHeartbeat("replica1", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 1, Role: 2},
	})

	// Volume must still exist with primary intact.
	e, ok := r.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 should not be deleted when replica sends heartbeat")
	}
	if e.VolumeServer != "primary" {
		t.Fatalf("primary should remain 'primary', got %q", e.VolumeServer)
	}
}

// Fix #1: Replica path NOT reported → replica removed, volume preserved.
func TestRegistry_ReplicaHeartbeat_StaleReplicaRemoved(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Status:       StatusActive,
		Replicas: []ReplicaInfo{
			{Server: "replica1", Path: "/data/vol1.blk"},
			{Server: "replica2", Path: "/data/vol1.blk"},
		},
	})

	// replica1 heartbeat does NOT report vol1 path → stale replica.
	r.UpdateFullHeartbeat("replica1", []*master_pb.BlockVolumeInfoMessage{})

	// Volume still exists, but replica1 removed.
	e, ok := r.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 should exist (only replica removed)")
	}
	if len(e.Replicas) != 1 {
		t.Fatalf("expected 1 replica after stale removal, got %d", len(e.Replicas))
	}
	if e.Replicas[0].Server != "replica2" {
		t.Fatalf("remaining replica should be replica2, got %q", e.Replicas[0].Server)
	}
}

// Fix #3: Replica heartbeat after master restart reconstructs ReplicaInfo.
func TestRegistry_ReplicaHeartbeat_ReconstructsAfterRestart(t *testing.T) {
	r := NewBlockVolumeRegistry()
	// Simulate master restart: primary heartbeat re-created entry without replicas.
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Status:       StatusActive,
	})

	// Replica heartbeat arrives — vol1 exists but has no record of this server.
	r.UpdateFullHeartbeat("replica1", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 1, Role: 2, HealthScore: 0.95, WalHeadLsn: 42},
	})

	// vol1 should now have replica1 in Replicas[].
	e, ok := r.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 should exist")
	}
	if len(e.Replicas) != 1 {
		t.Fatalf("expected 1 replica after reconstruction, got %d", len(e.Replicas))
	}
	ri := e.Replicas[0]
	if ri.Server != "replica1" {
		t.Fatalf("replica server: got %q, want replica1", ri.Server)
	}
	if ri.HealthScore != 0.95 {
		t.Fatalf("replica health: got %f, want 0.95", ri.HealthScore)
	}
	if ri.WALHeadLSN != 42 {
		t.Fatalf("replica WALHeadLSN: got %d, want 42", ri.WALHeadLSN)
	}
	// byServer index should include replica1.
	entries := r.ListByServer("replica1")
	if len(entries) != 1 || entries[0].Name != "vol1" {
		t.Fatalf("ListByServer(replica1) should return vol1, got %+v", entries)
	}
}

// Fix #2: Stale replica (old heartbeat) not eligible for promotion.
func TestRegistry_PromoteBestReplica_StaleHeartbeatIneligible(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		LeaseTTL:     5 * time.Second,
		WALHeadLSN:   100,
		Replicas: []ReplicaInfo{
			{
				Server:        "stale-replica",
				Path:          "/data/vol1.blk",
				HealthScore:   1.0,
				WALHeadLSN:    100,
				LastHeartbeat: time.Now().Add(-30 * time.Second), // stale (>2×5s)
			},
		},
	})

	_, err := r.PromoteBestReplica("vol1")
	if err == nil {
		t.Fatal("expected error: stale replica should not be eligible")
	}
}

// Fix #2: Replica with WAL lag too large is not eligible.
func TestRegistry_PromoteBestReplica_WALLagIneligible(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		LeaseTTL:     30 * time.Second,
		WALHeadLSN:   1000,
		Replicas: []ReplicaInfo{
			{
				Server:        "lagging",
				Path:          "/data/vol1.blk",
				HealthScore:   1.0,
				WALHeadLSN:    800, // lag=200, tolerance=100
				LastHeartbeat: time.Now(),
			},
		},
	})

	_, err := r.PromoteBestReplica("vol1")
	if err == nil {
		t.Fatal("expected error: lagging replica should not be eligible")
	}
}

// Fix #2: Rebuilding replica is not eligible for promotion.
func TestRegistry_PromoteBestReplica_RebuildingIneligible(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		LeaseTTL:     30 * time.Second,
		WALHeadLSN:   100,
		Replicas: []ReplicaInfo{
			{
				Server:        "rebuilding",
				Path:          "/data/vol1.blk",
				HealthScore:   1.0,
				WALHeadLSN:    100,
				LastHeartbeat: time.Now(),
				Role:          blockvol.RoleToWire(blockvol.RoleRebuilding),
			},
		},
	})

	_, err := r.PromoteBestReplica("vol1")
	if err == nil {
		t.Fatal("expected error: rebuilding replica should not be eligible")
	}
}

// Fix #2: Among eligible replicas, best (health+LSN) wins.
func TestRegistry_PromoteBestReplica_EligibilityFiltersCorrectly(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		LeaseTTL:     30 * time.Second,
		WALHeadLSN:   100,
		Replicas: []ReplicaInfo{
			{
				Server:        "stale", // ineligible: old heartbeat
				Path:          "/data/vol1.blk",
				HealthScore:   1.0,
				WALHeadLSN:    100,
				LastHeartbeat: time.Now().Add(-2 * time.Minute),
			},
			{
				Server:        "good", // eligible
				Path:          "/data/vol1.blk",
				HealthScore:   0.8,
				WALHeadLSN:    95,
				LastHeartbeat: time.Now(),
			},
		},
	})

	_, err := r.PromoteBestReplica("vol1")
	if err != nil {
		t.Fatalf("expected promotion to succeed: %v", err)
	}
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "good" {
		t.Fatalf("expected 'good' promoted (only eligible), got %q", e.VolumeServer)
	}
}

// Configurable tolerance: widen tolerance to allow lagging replicas.
func TestRegistry_PromoteBestReplica_ConfigurableTolerance(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		LeaseTTL:     30 * time.Second,
		WALHeadLSN:   1000,
		Replicas: []ReplicaInfo{
			{
				Server:        "lagging",
				Path:          "/data/vol1.blk",
				HealthScore:   1.0,
				WALHeadLSN:    800, // lag=200
				LastHeartbeat: time.Now(),
			},
		},
	})

	// Default tolerance (100): lag 200 > tolerance → ineligible.
	_, err := r.PromoteBestReplica("vol1")
	if err == nil {
		t.Fatal("expected error with default tolerance")
	}

	// Widen tolerance to 250: lag 200 < tolerance → eligible.
	r.SetPromotionLSNTolerance(250)
	_, err = r.PromoteBestReplica("vol1")
	if err != nil {
		t.Fatalf("expected success with widened tolerance: %v", err)
	}
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "lagging" {
		t.Fatalf("expected 'lagging' promoted, got %q", e.VolumeServer)
	}
}

// --- LeaseGrants ---

func TestRegistry_LeaseGrants_PrimaryOnly(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// Register a primary volume.
	r.Register(&BlockVolumeEntry{
		Name:         "prim1",
		VolumeServer: "s1:18080",
		Path:         "/data/prim1.blk",
		SizeBytes:    1 << 30,
		Epoch:        5,
		Role:         blockvol.RoleToWire(blockvol.RolePrimary),
		Status:       StatusActive,
		LeaseTTL:     30 * time.Second,
	})

	// Register a replica volume on the same server.
	r.Register(&BlockVolumeEntry{
		Name:         "repl1",
		VolumeServer: "s2:18080",
		Path:         "/data/repl1.blk",
		SizeBytes:    1 << 30,
		Epoch:        3,
		Role:         blockvol.RoleToWire(blockvol.RoleReplica),
		Status:       StatusActive,
	})
	r.AddReplica("repl1", ReplicaInfo{Server: "s1:18080", Path: "/data/repl1-replica.blk"})

	// Register a none-role volume.
	r.Register(&BlockVolumeEntry{
		Name:         "none1",
		VolumeServer: "s1:18080",
		Path:         "/data/none1.blk",
		SizeBytes:    1 << 30,
		Epoch:        1,
		Role:         blockvol.RoleToWire(blockvol.RoleNone),
		Status:       StatusActive,
	})

	// LeaseGrants for s1 should only include prim1 (the primary).
	grants := r.LeaseGrants("s1:18080", nil)
	if len(grants) != 1 {
		t.Fatalf("expected 1 grant, got %d: %+v", len(grants), grants)
	}
	if grants[0].Path != "/data/prim1.blk" {
		t.Errorf("expected prim1 path, got %q", grants[0].Path)
	}
	if grants[0].Epoch != 5 {
		t.Errorf("expected epoch 5, got %d", grants[0].Epoch)
	}
	if grants[0].LeaseTtlMs != 30000 {
		t.Errorf("expected 30000ms TTL, got %d", grants[0].LeaseTtlMs)
	}
}

func TestRegistry_LeaseGrants_PendingExcluded(t *testing.T) {
	r := NewBlockVolumeRegistry()

	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "s1:18080",
		Path:         "/data/vol1.blk",
		SizeBytes:    1 << 30,
		Epoch:        2,
		Role:         blockvol.RoleToWire(blockvol.RolePrimary),
		Status:       StatusActive,
		LeaseTTL:     30 * time.Second,
	})
	r.Register(&BlockVolumeEntry{
		Name:         "vol2",
		VolumeServer: "s1:18080",
		Path:         "/data/vol2.blk",
		SizeBytes:    1 << 30,
		Epoch:        1,
		Role:         blockvol.RoleToWire(blockvol.RolePrimary),
		Status:       StatusActive,
		LeaseTTL:     30 * time.Second,
	})

	// vol1 has a pending assignment — should be excluded.
	pending := map[string]bool{"/data/vol1.blk": true}
	grants := r.LeaseGrants("s1:18080", pending)
	if len(grants) != 1 {
		t.Fatalf("expected 1 grant (vol2 only), got %d: %+v", len(grants), grants)
	}
	if grants[0].Path != "/data/vol2.blk" {
		t.Errorf("expected vol2 path, got %q", grants[0].Path)
	}
}

func TestRegistry_LeaseGrants_InactiveExcluded(t *testing.T) {
	r := NewBlockVolumeRegistry()

	r.Register(&BlockVolumeEntry{
		Name:         "pending-vol",
		VolumeServer: "s1:18080",
		Path:         "/data/pending.blk",
		SizeBytes:    1 << 30,
		Epoch:        1,
		Role:         blockvol.RoleToWire(blockvol.RolePrimary),
		Status:       StatusPending, // not yet confirmed by heartbeat
		LeaseTTL:     30 * time.Second,
	})

	grants := r.LeaseGrants("s1:18080", nil)
	if len(grants) != 0 {
		t.Fatalf("expected 0 grants for pending volume, got %d", len(grants))
	}
}

func TestRegistry_LeaseGrants_UnknownServer(t *testing.T) {
	r := NewBlockVolumeRegistry()
	grants := r.LeaseGrants("unknown:18080", nil)
	if grants != nil {
		t.Fatalf("expected nil for unknown server, got %+v", grants)
	}
}
