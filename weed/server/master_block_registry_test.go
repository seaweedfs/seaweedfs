package weed_server

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
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
	}, "")

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
	}, "")

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
	}, "")

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
	}, "")

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
	r.MarkBlockCapable("s2")
	r.MarkBlockCapable("s3")
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "s1",
		Path:         "/v1.blk",
		Epoch:        5,
		Role:         1,
		Replicas: []ReplicaInfo{
			{Server: "s2", Path: "/r1.blk", IQN: "iqn:r1", ISCSIAddr: "s2:3260", HealthScore: 0.8, WALHeadLSN: 100, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
			{Server: "s3", Path: "/r2.blk", IQN: "iqn:r2", ISCSIAddr: "s3:3260", HealthScore: 0.95, WALHeadLSN: 90, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
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
	r.MarkBlockCapable("s2")
	r.MarkBlockCapable("s3")
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "s1",
		Path:         "/v1.blk",
		Epoch:        3,
		Replicas: []ReplicaInfo{
			{Server: "s2", Path: "/r1.blk", IQN: "iqn:r1", ISCSIAddr: "s2:3260", HealthScore: 0.9, WALHeadLSN: 50, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
			{Server: "s3", Path: "/r2.blk", IQN: "iqn:r2", ISCSIAddr: "s3:3260", HealthScore: 0.9, WALHeadLSN: 100, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
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
	r.MarkBlockCapable("s2")
	r.MarkBlockCapable("s3")
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "s1",
		Path:         "/v1.blk",
		Epoch:        1,
		Replicas: []ReplicaInfo{
			{Server: "s2", Path: "/r1.blk", IQN: "iqn:r1", ISCSIAddr: "s2:3260", HealthScore: 1.0, WALHeadLSN: 100, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
			{Server: "s3", Path: "/r2.blk", IQN: "iqn:r2", ISCSIAddr: "s3:3260", HealthScore: 0.5, WALHeadLSN: 100, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
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
	}, "")

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
	}, "")

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
	r.UpdateFullHeartbeat("replica1", []*master_pb.BlockVolumeInfoMessage{}, "")

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
	// Simulate master restart: primary heartbeat re-created entry (epoch 1).
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		WALHeadLSN:   100,
		Status:       StatusActive,
	})

	// Replica heartbeat arrives — vol1 exists but has no record of this server.
	// Same epoch, lower LSN, Role=2 (replica) → added as replica.
	r.UpdateFullHeartbeat("replica1", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 1, Role: 2, HealthScore: 0.95, WalHeadLsn: 42},
	}, "")

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

// Fix #2: Replica with WAL lag too large is not eligible (when primary alive).
// EC-6 fix: WAL lag gate only applies when primary is alive.
func TestRegistry_PromoteBestReplica_WALLagIneligible(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary") // primary must be alive for WAL lag gate
	r.MarkBlockCapable("lagging") // replica must be alive for other gates to pass
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
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
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
	r.MarkBlockCapable("stale")
	r.MarkBlockCapable("good")
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
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
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
// EC-6 fix: WAL lag gate only applies when primary is alive.
func TestRegistry_PromoteBestReplica_ConfigurableTolerance(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary") // primary must be alive for WAL lag gate to apply
	r.MarkBlockCapable("lagging")
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
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
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

// B-12: PromoteBestReplica rejects dead replica (server not in blockServers).
func TestRegistry_PromoteBestReplica_DeadServerIneligible(t *testing.T) {
	r := NewBlockVolumeRegistry()
	// Intentionally do NOT mark "dead-replica" as block-capable.
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		LeaseTTL:     30 * time.Second,
		WALHeadLSN:   100,
		Replicas: []ReplicaInfo{
			{
				Server:        "dead-replica",
				Path:          "/data/vol1.blk",
				HealthScore:   1.0,
				WALHeadLSN:    100,
				LastHeartbeat: time.Now(),
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
			},
		},
	})

	_, err := r.PromoteBestReplica("vol1")
	if err == nil {
		t.Fatal("expected error: dead replica should be rejected")
	}
	if !strings.Contains(err.Error(), "server_dead") {
		t.Fatalf("error should mention server_dead, got: %v", err)
	}
}

// B-12: Dead replica rejected but alive replica promoted when both exist.
func TestRegistry_PromoteBestReplica_DeadSkipped_AlivePromoted(t *testing.T) {
	r := NewBlockVolumeRegistry()
	// Only mark s3 as alive.
	r.MarkBlockCapable("s3")
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		LeaseTTL:     30 * time.Second,
		WALHeadLSN:   100,
		Replicas: []ReplicaInfo{
			{Server: "s2-dead", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 100, LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "s3", Path: "/r2.blk", HealthScore: 0.8, WALHeadLSN: 95, LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	newEpoch, err := r.PromoteBestReplica("vol1")
	if err != nil {
		t.Fatalf("PromoteBestReplica: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("newEpoch: got %d, want 2", newEpoch)
	}
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "s3" {
		t.Fatalf("expected alive s3 promoted, got %q", e.VolumeServer)
	}
}

// EvaluatePromotion returns read-only preflight without mutating registry.
func TestRegistry_EvaluatePromotion_Basic(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("replica1")
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        5,
		LeaseTTL:     30 * time.Second,
		WALHeadLSN:   100,
		Replicas: []ReplicaInfo{
			{Server: "replica1", Path: "/r1.blk", HealthScore: 0.9, WALHeadLSN: 100, LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	pf, err := r.EvaluatePromotion("vol1")
	if err != nil {
		t.Fatalf("EvaluatePromotion: %v", err)
	}
	if !pf.Promotable {
		t.Fatalf("expected promotable, got reason: %s", pf.Reason)
	}
	if pf.Candidate == nil || pf.Candidate.Server != "replica1" {
		t.Fatalf("expected candidate replica1, got %+v", pf.Candidate)
	}

	// Registry must be unmutated.
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "primary" {
		t.Fatal("EvaluatePromotion should not mutate the registry")
	}
	if e.Epoch != 5 {
		t.Fatal("EvaluatePromotion should not bump epoch")
	}
}

// EvaluatePromotion with all replicas rejected.
func TestRegistry_EvaluatePromotion_AllRejected(t *testing.T) {
	r := NewBlockVolumeRegistry()
	// No servers marked as block-capable.
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		Replicas: []ReplicaInfo{
			{Server: "dead1", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 100, LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "dead2", Path: "/r2.blk", HealthScore: 0.9, WALHeadLSN: 100, LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	pf, err := r.EvaluatePromotion("vol1")
	if err != nil {
		t.Fatalf("EvaluatePromotion: %v", err)
	}
	if pf.Promotable {
		t.Fatal("expected not promotable")
	}
	if len(pf.Rejections) != 2 {
		t.Fatalf("expected 2 rejections, got %d", len(pf.Rejections))
	}
	for _, rej := range pf.Rejections {
		if rej.Reason != "server_dead" {
			t.Fatalf("expected server_dead rejection, got %q", rej.Reason)
		}
	}
}

// EvaluatePromotion for nonexistent volume.
func TestRegistry_EvaluatePromotion_NotFound(t *testing.T) {
	r := NewBlockVolumeRegistry()
	_, err := r.EvaluatePromotion("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent volume")
	}
}

// Replica created but never heartbeated is not promotable.
func TestRegistry_PromoteBestReplica_NoHeartbeatIneligible(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("replica1")
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		LeaseTTL:     30 * time.Second,
		WALHeadLSN:   100,
		Replicas: []ReplicaInfo{
			{
				Server:      "replica1",
				Path:        "/r1.blk",
				HealthScore: 1.0,
				WALHeadLSN:  100,
				Role:        blockvol.RoleToWire(blockvol.RoleReplica),
				// LastHeartbeat: zero — never heartbeated
			},
		},
	})

	_, err := r.PromoteBestReplica("vol1")
	if err == nil {
		t.Fatal("expected error: replica with no heartbeat should be rejected")
	}
	if !strings.Contains(err.Error(), "no_heartbeat") {
		t.Fatalf("error should mention no_heartbeat, got: %v", err)
	}
}

// Replica with unset (zero) role is not promotable.
func TestRegistry_PromoteBestReplica_UnsetRoleIneligible(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("replica1")
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		LeaseTTL:     30 * time.Second,
		WALHeadLSN:   100,
		Replicas: []ReplicaInfo{
			{
				Server:        "replica1",
				Path:          "/r1.blk",
				HealthScore:   1.0,
				WALHeadLSN:    100,
				LastHeartbeat: time.Now(),
				// Role: 0 — unset/RoleNone
			},
		},
	})

	_, err := r.PromoteBestReplica("vol1")
	if err == nil {
		t.Fatal("expected error: replica with unset role should be rejected")
	}
	if !strings.Contains(err.Error(), "wrong_role") {
		t.Fatalf("error should mention wrong_role, got: %v", err)
	}
}

// PromoteBestReplica clears RebuildListenAddr on promotion (B-11 partial fix).
func TestRegistry_PromoteBestReplica_ClearsRebuildAddr(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("replica1")
	r.Register(&BlockVolumeEntry{
		Name:              "vol1",
		VolumeServer:      "primary",
		Path:              "/data/vol1.blk",
		Epoch:             1,
		RebuildListenAddr: "primary:15000",
		Replicas: []ReplicaInfo{
			{Server: "replica1", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 100, LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	_, err := r.PromoteBestReplica("vol1")
	if err != nil {
		t.Fatalf("PromoteBestReplica: %v", err)
	}
	e, _ := r.Lookup("vol1")
	if e.RebuildListenAddr != "" {
		t.Fatalf("RebuildListenAddr should be cleared after promotion, got %q", e.RebuildListenAddr)
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

// ============================================================
// CP11B-3 T2: IsBlockCapable + VolumesWithDeadPrimary
// ============================================================

func TestRegistry_IsBlockCapable(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("vs1:8080")

	if !r.IsBlockCapable("vs1:8080") {
		t.Fatal("vs1 should be block-capable")
	}
	if r.IsBlockCapable("vs2:8080") {
		t.Fatal("vs2 should NOT be block-capable")
	}

	r.UnmarkBlockCapable("vs1:8080")
	if r.IsBlockCapable("vs1:8080") {
		t.Fatal("vs1 should no longer be block-capable after unmark")
	}
}

func TestRegistry_VolumesWithDeadPrimary_Basic(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("vs1")
	r.MarkBlockCapable("vs2")

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status:   StatusActive,
		Replicas: []ReplicaInfo{{Server: "vs2", Path: "/data/vol1.blk"}},
	})

	// Both alive → no orphans.
	orphaned := r.VolumesWithDeadPrimary("vs2")
	if len(orphaned) != 0 {
		t.Fatalf("expected 0 orphaned volumes, got %d", len(orphaned))
	}

	// Kill primary.
	r.UnmarkBlockCapable("vs1")
	orphaned = r.VolumesWithDeadPrimary("vs2")
	if len(orphaned) != 1 || orphaned[0] != "vol1" {
		t.Fatalf("expected [vol1], got %v", orphaned)
	}
}

func TestRegistry_VolumesWithDeadPrimary_PrimaryServer_NotIncluded(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("vs1")

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive,
	})

	// vs1 is the primary for vol1 — should NOT appear in orphaned list for vs1.
	orphaned := r.VolumesWithDeadPrimary("vs1")
	if len(orphaned) != 0 {
		t.Fatalf("primary server should not appear in its own orphan list, got %v", orphaned)
	}
}

// T6: EvaluatePromotion preflight includes primary liveness.
func TestRegistry_EvaluatePromotion_PrimaryDead_StillShowsCandidate(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("vs1")
	r.MarkBlockCapable("vs2")

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol1.blk", HealthScore: 1.0,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	})

	// Kill primary but keep vs2 alive.
	r.UnmarkBlockCapable("vs1")

	pf, err := r.EvaluatePromotion("vol1")
	if err != nil {
		t.Fatalf("EvaluatePromotion: %v", err)
	}
	if !pf.Promotable {
		t.Fatalf("should be promotable (vs2 alive), reason=%s", pf.Reason)
	}
	if pf.Candidate.Server != "vs2" {
		t.Fatalf("candidate should be vs2, got %q", pf.Candidate.Server)
	}
}

// ============================================================
// CP11B-3 T5: ManualPromote Dev Tests
// ============================================================

// T5: ManualPromote with empty target → auto-picks best candidate.
func TestRegistry_ManualPromote_AutoTarget(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("best")
	r.MarkBlockCapable("worse")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second, WALHeadLSN: 100,
		Replicas: []ReplicaInfo{
			{Server: "worse", Path: "/r1.blk", HealthScore: 0.5, WALHeadLSN: 100,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "best", Path: "/r2.blk", HealthScore: 1.0, WALHeadLSN: 100,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})
	// Primary not block-capable → non-force should still pass (primary_alive gate won't trigger).

	newEpoch, _, _, pf, err := r.ManualPromote("vol1", "", false)
	if err != nil {
		t.Fatalf("ManualPromote: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("epoch: got %d, want 2", newEpoch)
	}
	if !pf.Promotable {
		t.Fatal("should be promotable")
	}
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "best" {
		t.Fatalf("expected 'best' promoted, got %q", e.VolumeServer)
	}
}

// T5: ManualPromote targets a specific replica (not the best by health).
func TestRegistry_ManualPromote_SpecificTarget(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")
	r.MarkBlockCapable("r2")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 100,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "r2", Path: "/r2.blk", HealthScore: 0.5, WALHeadLSN: 50,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	// Target r2 specifically (worse health).
	newEpoch, _, _, _, err := r.ManualPromote("vol1", "r2", false)
	if err != nil {
		t.Fatalf("ManualPromote: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("epoch: got %d, want 2", newEpoch)
	}
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "r2" {
		t.Fatalf("expected r2 promoted (specific target), got %q", e.VolumeServer)
	}
}

// T5: ManualPromote with non-existent target → error.
func TestRegistry_ManualPromote_TargetNotFound(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	_, _, _, pf, err := r.ManualPromote("vol1", "nonexistent", false)
	if err == nil {
		t.Fatal("expected error for nonexistent target")
	}
	if pf.Reason != "target_not_found" {
		t.Fatalf("expected target_not_found, got %q", pf.Reason)
	}
}

// T5: ManualPromote non-force with alive primary → rejected.
func TestRegistry_ManualPromote_PrimaryAlive_Rejected(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary")
	r.MarkBlockCapable("r1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	_, _, _, pf, err := r.ManualPromote("vol1", "", false)
	if err == nil {
		t.Fatal("expected rejection when primary alive and !force")
	}
	if pf.Reason != "primary_alive" {
		t.Fatalf("expected primary_alive, got %q", pf.Reason)
	}
	// Verify no mutation.
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "primary" {
		t.Fatalf("primary should not change, got %q", e.VolumeServer)
	}
}

// T5: Force bypasses stale heartbeat and primary_alive gates.
func TestRegistry_ManualPromote_Force_StaleHeartbeat(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary")
	r.MarkBlockCapable("r1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
				LastHeartbeat: time.Now().Add(-10 * time.Minute), // stale
				Role:          blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	// Non-force: would fail on primary_alive.
	// Force: bypasses primary_alive AND stale_heartbeat.
	newEpoch, _, _, _, err := r.ManualPromote("vol1", "", true)
	if err != nil {
		t.Fatalf("force ManualPromote should succeed: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("epoch: got %d, want 2", newEpoch)
	}
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "r1" {
		t.Fatalf("expected r1 promoted via force, got %q", e.VolumeServer)
	}
}

// T5: Force does NOT bypass server_dead (hard gate).
func TestRegistry_ManualPromote_Force_StillRejectsDeadServer(t *testing.T) {
	r := NewBlockVolumeRegistry()
	// "dead" is NOT marked block-capable.
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{
			{Server: "dead", Path: "/r1.blk", HealthScore: 1.0,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	_, _, _, pf, err := r.ManualPromote("vol1", "dead", true)
	if err == nil {
		t.Fatal("force should NOT bypass server_dead")
	}
	if len(pf.Rejections) == 0 || pf.Rejections[0].Reason != "server_dead" {
		t.Fatalf("expected server_dead rejection, got %+v", pf.Rejections)
	}
}

// --- Master restart reconciliation tests ---

func TestMasterRestart_HigherEpochWins(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// First heartbeat from stale primary (epoch 5).
	r.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 1, WalHeadLsn: 100, VolumeSize: 1 << 30},
	}, "")

	entry, ok := r.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 not found after first heartbeat")
	}
	if entry.VolumeServer != "vs1:9333" {
		t.Fatalf("expected vs1 as initial primary, got %q", entry.VolumeServer)
	}

	// Second heartbeat from real primary (epoch 6 — post-failover).
	r.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 6, Role: 1, WalHeadLsn: 150, VolumeSize: 1 << 30},
	}, "")

	entry, _ = r.Lookup("vol1")
	if entry.VolumeServer != "vs2:9333" {
		t.Fatalf("expected vs2 (higher epoch) as primary, got %q", entry.VolumeServer)
	}
	if entry.Epoch != 6 {
		t.Fatalf("expected epoch 6, got %d", entry.Epoch)
	}
	// Old primary should be a replica.
	if len(entry.Replicas) != 1 || entry.Replicas[0].Server != "vs1:9333" {
		t.Fatalf("expected vs1 demoted to replica, got replicas=%+v", entry.Replicas)
	}
}

func TestMasterRestart_LowerEpochBecomesReplica(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// First heartbeat from real primary (epoch 6).
	r.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 6, Role: 1, WalHeadLsn: 150, VolumeSize: 1 << 30},
	}, "")

	// Second heartbeat from stale server (epoch 5).
	r.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 1, WalHeadLsn: 100, VolumeSize: 1 << 30},
	}, "")

	entry, _ := r.Lookup("vol1")
	if entry.VolumeServer != "vs2:9333" {
		t.Fatalf("expected vs2 (higher epoch) to stay primary, got %q", entry.VolumeServer)
	}
	if entry.Epoch != 6 {
		t.Fatalf("expected epoch 6, got %d", entry.Epoch)
	}
	if len(entry.Replicas) != 1 || entry.Replicas[0].Server != "vs1:9333" {
		t.Fatalf("expected vs1 added as replica, got replicas=%+v", entry.Replicas)
	}
}

func TestMasterRestart_SameEpoch_HigherLSNWins(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// First heartbeat: epoch 5, LSN 100.
	r.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 1, WalHeadLsn: 100, VolumeSize: 1 << 30},
	}, "")

	// Second heartbeat: same epoch 5, higher LSN 200 — heuristic: this server is more recent.
	r.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 1, WalHeadLsn: 200, VolumeSize: 1 << 30},
	}, "")

	entry, _ := r.Lookup("vol1")
	if entry.VolumeServer != "vs2:9333" {
		t.Fatalf("expected vs2 (higher LSN) as primary, got %q", entry.VolumeServer)
	}
	if len(entry.Replicas) != 1 || entry.Replicas[0].Server != "vs1:9333" {
		t.Fatalf("expected vs1 demoted to replica, got replicas=%+v", entry.Replicas)
	}
}

func TestMasterRestart_SameEpoch_SameLSN_ExistingWins(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// First heartbeat: epoch 5, LSN 100.
	r.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 1, WalHeadLsn: 100, VolumeSize: 1 << 30},
	}, "")

	// Second heartbeat: same epoch 5, same LSN 100 — existing wins (deterministic).
	r.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 1, WalHeadLsn: 100, VolumeSize: 1 << 30},
	}, "")

	entry, _ := r.Lookup("vol1")
	if entry.VolumeServer != "vs1:9333" {
		t.Fatalf("expected vs1 (existing, same LSN) to stay primary, got %q", entry.VolumeServer)
	}
	if len(entry.Replicas) != 1 || entry.Replicas[0].Server != "vs2:9333" {
		t.Fatalf("expected vs2 added as replica, got replicas=%+v", entry.Replicas)
	}
}

func TestMasterRestart_ReplicaHeartbeat_AddedCorrectly(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// Primary heartbeat first.
	r.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 1, WalHeadLsn: 100, VolumeSize: 1 << 30},
	}, "")

	// Replica heartbeat: same volume, lower epoch (stale replica never got promoted).
	r.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 4, Role: 2, WalHeadLsn: 90, VolumeSize: 1 << 30},
	}, "")

	entry, _ := r.Lookup("vol1")
	if entry.VolumeServer != "vs1:9333" {
		t.Fatalf("primary should be vs1, got %q", entry.VolumeServer)
	}
	if len(entry.Replicas) != 1 || entry.Replicas[0].Server != "vs2:9333" {
		t.Fatalf("expected vs2 as replica, got %+v", entry.Replicas)
	}
}

func TestMasterRestart_SameEpoch_RoleTrusted(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// First heartbeat: vs1 claims primary, epoch 5, LSN 50.
	r.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 1, WalHeadLsn: 50, VolumeSize: 1 << 30},
	}, "")

	// Second heartbeat: vs2 claims replica (Role=2), same epoch 5, HIGHER LSN.
	// Even though LSN is higher, it reports Role=replica, so it should NOT become primary.
	r.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 2, WalHeadLsn: 200, VolumeSize: 1 << 30},
	}, "")

	entry, _ := r.Lookup("vol1")
	if entry.VolumeServer != "vs1:9333" {
		t.Fatalf("expected vs1 (claims primary) to stay primary, got %q", entry.VolumeServer)
	}
	if len(entry.Replicas) != 1 || entry.Replicas[0].Server != "vs2:9333" {
		t.Fatalf("expected vs2 as replica, got %+v", entry.Replicas)
	}
}

func TestMasterRestart_DuplicateReplicaHeartbeat_NoDuplicate(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// Primary heartbeat.
	r.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 1, WalHeadLsn: 100, VolumeSize: 1 << 30},
	}, "")

	// Replica heartbeat — first time.
	r.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 2, WalHeadLsn: 90, VolumeSize: 1 << 30, HealthScore: 0.8},
	}, "")

	// Same replica heartbeat again — should update, not duplicate.
	r.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 5, Role: 2, WalHeadLsn: 95, VolumeSize: 1 << 30, HealthScore: 0.9},
	}, "")

	entry, _ := r.Lookup("vol1")
	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 replica (no duplicates), got %d", len(entry.Replicas))
	}
	// Should have the updated values from the second heartbeat.
	if entry.Replicas[0].WALHeadLSN != 95 {
		t.Fatalf("expected updated LSN 95, got %d", entry.Replicas[0].WALHeadLSN)
	}
	if entry.Replicas[0].HealthScore != 0.9 {
		t.Fatalf("expected updated health 0.9, got %f", entry.Replicas[0].HealthScore)
	}
}

// --- Copy semantics tests (pointer escape fix) ---

func TestLookup_ReturnsCopy(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "vs1:9333",
		Path:         "/data/vol1.blk",
		Epoch:        1,
		Role:         blockvol.RoleToWire(blockvol.RolePrimary),
		Status:       StatusActive,
	})

	// Get a copy via Lookup.
	entry, ok := r.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 not found")
	}

	// Mutate the copy.
	entry.Epoch = 999
	entry.VolumeServer = "mutated:9333"

	// Registry must be unaffected.
	original, _ := r.Lookup("vol1")
	if original.Epoch != 1 {
		t.Fatalf("Lookup copy mutation leaked: Epoch=%d, want 1", original.Epoch)
	}
	if original.VolumeServer != "vs1:9333" {
		t.Fatalf("Lookup copy mutation leaked: VolumeServer=%q", original.VolumeServer)
	}
}

func TestLookup_ReplicaSliceCopy(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "vs1:9333",
		Path:         "/data/vol1.blk",
		Status:       StatusActive,
		Replicas:     []ReplicaInfo{{Server: "vs2:9333", HealthScore: 1.0}},
	})

	entry, _ := r.Lookup("vol1")
	// Mutate replica slice on the copy.
	entry.Replicas[0].HealthScore = 0.0
	entry.Replicas = append(entry.Replicas, ReplicaInfo{Server: "vs3:9333"})

	// Registry must be unaffected.
	original, _ := r.Lookup("vol1")
	if len(original.Replicas) != 1 {
		t.Fatalf("Replica slice mutation leaked: len=%d, want 1", len(original.Replicas))
	}
	if original.Replicas[0].HealthScore != 1.0 {
		t.Fatalf("Replica HealthScore mutation leaked: %f", original.Replicas[0].HealthScore)
	}
}

func TestListAll_ReturnsCopies(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1:9333", Path: "/data/vol1.blk", Status: StatusActive,
	})

	entries := r.ListAll()
	entries[0].Epoch = 999

	original, _ := r.Lookup("vol1")
	if original.Epoch != 0 {
		t.Fatalf("ListAll copy mutation leaked: Epoch=%d", original.Epoch)
	}
}

func TestUpdateEntry_MutatesRegistry(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1:9333", Path: "/data/vol1.blk", Status: StatusActive,
	})

	r.UpdateEntry("vol1", func(e *BlockVolumeEntry) {
		e.Preset = "database"
	})

	entry, _ := r.Lookup("vol1")
	if entry.Preset != "database" {
		t.Fatalf("UpdateEntry did not mutate: Preset=%q", entry.Preset)
	}
}

func TestUpdateEntry_NotFound(t *testing.T) {
	r := NewBlockVolumeRegistry()
	err := r.UpdateEntry("nonexistent", func(e *BlockVolumeEntry) {
		e.Epoch = 99
	})
	if err == nil {
		t.Fatal("expected error for nonexistent volume")
	}
}

// TestRegistry_InflightBlocksAutoRegister verifies that heartbeat auto-register
// is suppressed while a create is in-flight for the same volume. This prevents
// a race where the replica VS heartbeat arrives before CreateBlockVolume.Register
// completes, creating a bare entry that lacks replica info.
func TestRegistry_InflightBlocksAutoRegister(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// Simulate CreateBlockVolume acquiring the inflight lock.
	if !r.AcquireInflight("vol1") {
		t.Fatal("AcquireInflight should succeed")
	}

	// Replica VS sends heartbeat reporting vol1 — while create is in-flight.
	// This should be silently skipped (not auto-registered).
	r.UpdateFullHeartbeat("replica-server:8080", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/blocks/vol1.blk", Epoch: 1, Role: 2, VolumeSize: 1 << 30},
	}, "")

	// vol1 should NOT be in the registry (auto-register was blocked).
	if _, ok := r.Lookup("vol1"); ok {
		t.Fatal("vol1 should not be auto-registered while inflight lock is held")
	}

	// Now simulate CreateBlockVolume completing: register with replicas.
	r.Register(&BlockVolumeEntry{
		Name:         "vol1",
		VolumeServer: "primary-server:8080",
		Path:         "/blocks/vol1.blk",
		SizeBytes:    1 << 30,
		Epoch:        1,
		Status:       StatusActive,
		Replicas: []ReplicaInfo{
			{Server: "replica-server:8080", Path: "/blocks/vol1.blk"},
		},
	})
	r.ReleaseInflight("vol1")

	// Entry should have the replica.
	entry, ok := r.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 should exist after Register")
	}
	if len(entry.Replicas) != 1 {
		t.Fatalf("replicas=%d, want 1", len(entry.Replicas))
	}
	if entry.Replicas[0].Server != "replica-server:8080" {
		t.Fatalf("replica server=%s", entry.Replicas[0].Server)
	}

	// After inflight released, subsequent heartbeats should update normally.
	r.UpdateFullHeartbeat("replica-server:8080", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/blocks/vol1.blk", Epoch: 2, Role: 2, VolumeSize: 1 << 30, HealthScore: 0.9},
	}, "")

	entry, _ = r.Lookup("vol1")
	if entry.Replicas[0].HealthScore != 0.9 {
		t.Fatalf("replica health not updated after inflight released: %f", entry.Replicas[0].HealthScore)
	}
}

func TestRegistry_ReplicaReadyRequiresReplicaHeartbeat(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name:         "vol-ready",
		VolumeServer: "primary-server:8080",
		Path:         "/blocks/vol-ready.blk",
		Status:       StatusActive,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   "/blocks/vol-ready.blk",
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-ready")
	if entry.ReplicaReady {
		t.Fatal("replica should not be ready before replica heartbeat confirms publication")
	}
	if !entry.ReplicaDegraded {
		t.Fatal("volume should remain degraded until replica readiness closes")
	}

	r.UpdateFullHeartbeat("replica-server:8080", []*master_pb.BlockVolumeInfoMessage{{
		Path:            "/blocks/vol-ready.blk",
		Epoch:           1,
		Role:            uint32(blockvol.RoleReplica),
		VolumeSize:      1 << 30,
		HealthScore:     0.9,
		ReplicaDataAddr: "10.0.0.2:14260",
		ReplicaCtrlAddr: "10.0.0.2:14261",
	}}, "")

	entry, _ = r.Lookup("vol-ready")
	if !entry.Replicas[0].Ready {
		t.Fatal("replica heartbeat with published receiver addresses should mark replica ready")
	}
	if !entry.ReplicaReady {
		t.Fatal("aggregate replica readiness should become true after replica heartbeat")
	}
}

func TestRegistry_UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaDegraded(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-master-consume")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "replica-server:8080",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	bs.applyCoreEvent(engine.BarrierRejected{ID: path, Reason: "barrier_timeout"})

	hb := findHeartbeatMsg(bs.CollectBlockVolumeHeartbeat(), path)
	if hb == nil {
		t.Fatal("heartbeat volume missing")
	}
	if !hb.ReplicaDegraded {
		t.Fatalf("expected core-influenced heartbeat degraded bit, hb=%+v", hb)
	}

	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name:          "vol-master-consume",
		VolumeServer:  "primary-server:8080",
		Path:          path,
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   "/blocks/vol-master-consume.blk",
			Ready:  true,
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	r.UpdateFullHeartbeat("primary-server:8080", blockvol.InfoMessagesToProto([]blockvol.BlockVolumeInfoMessage{*hb}), "")

	entry, _ := r.Lookup("vol-master-consume")
	if !entry.TransportDegraded {
		t.Fatalf("expected registry transport degraded from heartbeat, entry=%+v", entry)
	}
	if !entry.ReplicaDegraded {
		t.Fatalf("expected registry aggregate degraded from heartbeat, entry=%+v", entry)
	}
	if entry.VolumeMode != "degraded" {
		t.Fatalf("expected degraded volume mode after consume, got %q", entry.VolumeMode)
	}
}

func TestRegistry_UpdateFullHeartbeat_ConsumesCoreInfluencedReplicaReady(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-master-ready")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RoleReplica),
			LeaseTtlMs:      30000,
			ReplicaDataAddr: "127.0.0.1:0",
			ReplicaCtrlAddr: "127.0.0.1:0",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	bs.replMu.Lock()
	state := bs.replStates[path]
	if state == nil {
		bs.replMu.Unlock()
		t.Fatal("missing repl state")
	}
	state.publishHealthy = false
	bs.replMu.Unlock()

	hb := findHeartbeatMsg(bs.CollectBlockVolumeHeartbeat(), path)
	if hb == nil {
		t.Fatal("heartbeat volume missing")
	}
	if hb.ReplicaDataAddr == "" || hb.ReplicaCtrlAddr == "" {
		t.Fatalf("expected core-influenced replica addresses on heartbeat, hb=%+v", hb)
	}
	if !hb.ReplicaReady {
		t.Fatalf("expected explicit replica_ready truth on heartbeat, hb=%+v", hb)
	}
	if hb.ReplicaDegraded {
		t.Fatalf("did not expect degraded heartbeat on ready path, hb=%+v", hb)
	}

	// Prove master-side readiness now follows the explicit heartbeat truth even
	// when transport addresses are absent on the consume path.
	hb.ReplicaDataAddr = ""
	hb.ReplicaCtrlAddr = ""

	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name:          "vol-master-ready",
		VolumeServer:  "primary-server:8080",
		Path:          "/blocks/vol-master-ready-primary.blk",
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   path,
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	r.UpdateFullHeartbeat("replica-server:8080", blockvol.InfoMessagesToProto([]blockvol.BlockVolumeInfoMessage{*hb}), "")

	entry, _ := r.Lookup("vol-master-ready")
	if !entry.Replicas[0].Ready {
		t.Fatalf("expected replica ready from heartbeat addresses, entry=%+v", entry)
	}
	if !entry.ReplicaReady {
		t.Fatalf("expected aggregate replica ready after consume, entry=%+v", entry)
	}
	if entry.ReplicaDegraded {
		t.Fatalf("did not expect aggregate degraded after ready consume, entry=%+v", entry)
	}
	if entry.VolumeMode != "publish_healthy" {
		t.Fatalf("expected publish_healthy after ready consume, got %q", entry.VolumeMode)
	}
}

func TestRegistry_UpdateFullHeartbeat_ReplicaReadyFallsBackToAddressesWhenFieldAbsent(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name:          "vol-master-ready-fallback",
		VolumeServer:  "primary-server:8080",
		Path:          "/blocks/vol-master-ready-fallback-primary.blk",
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   "/blocks/vol-master-ready-fallback.blk",
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	r.UpdateFullHeartbeat("replica-server:8080", []*master_pb.BlockVolumeInfoMessage{{
		Path:            "/blocks/vol-master-ready-fallback.blk",
		ReplicaDataAddr: "10.0.0.2:4260",
		ReplicaCtrlAddr: "10.0.0.2:4261",
	}}, "")

	entry, _ := r.Lookup("vol-master-ready-fallback")
	if !entry.Replicas[0].Ready {
		t.Fatalf("expected backward-compatible ready fallback from addresses, entry=%+v", entry)
	}
	if !entry.ReplicaReady {
		t.Fatalf("expected aggregate replica ready from fallback consume, entry=%+v", entry)
	}
}

func TestRegistry_UpdateFullHeartbeat_ConsumesExplicitNeedsRebuildFromPrimaryHeartbeat(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name:          "vol-master-needs-rebuild",
		VolumeServer:  "primary-server:8080",
		Path:          "/blocks/vol-master-needs-rebuild-primary.blk",
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   "/blocks/vol-master-needs-rebuild-replica.blk",
			Ready:  true,
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	needsRebuild := true
	r.UpdateFullHeartbeat("primary-server:8080", []*master_pb.BlockVolumeInfoMessage{{
		Path:            "/blocks/vol-master-needs-rebuild-primary.blk",
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaDegraded: true,
		NeedsRebuild:    &needsRebuild,
	}}, "")

	entry, _ := r.Lookup("vol-master-needs-rebuild")
	if !entry.NeedsRebuild || !entry.HasNeedsRebuild {
		t.Fatalf("expected explicit needs_rebuild truth on entry, entry=%+v", entry)
	}
	if entry.VolumeMode != "needs_rebuild" {
		t.Fatalf("expected needs_rebuild from explicit primary heartbeat truth, got %q", entry.VolumeMode)
	}
}

func TestRegistry_UpdateFullHeartbeat_NeedsRebuildFallsBackWhenFieldAbsent(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name:          "vol-master-needs-rebuild-fallback",
		VolumeServer:  "primary-server:8080",
		Path:          "/blocks/vol-master-needs-rebuild-fallback-primary.blk",
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   "/blocks/vol-master-needs-rebuild-fallback-replica.blk",
			Ready:  true,
			Role:   blockvol.RoleToWire(blockvol.RoleRebuilding),
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	r.UpdateFullHeartbeat("primary-server:8080", []*master_pb.BlockVolumeInfoMessage{{
		Path:            "/blocks/vol-master-needs-rebuild-fallback-primary.blk",
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaDegraded: true,
	}}, "")

	entry, _ := r.Lookup("vol-master-needs-rebuild-fallback")
	if entry.HasNeedsRebuild {
		t.Fatalf("did not expect explicit needs_rebuild truth when field absent, entry=%+v", entry)
	}
	if entry.VolumeMode != "needs_rebuild" {
		t.Fatalf("expected fallback needs_rebuild from replica role heuristic, got %q", entry.VolumeMode)
	}
}

func TestRegistry_UpdateFullHeartbeat_ExplicitHealthySuppressesStaleNeedsRebuildHeuristic(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name:          "vol-master-needs-rebuild-explicit-false",
		VolumeServer:  "primary-server:8080",
		Path:          "/blocks/vol-master-needs-rebuild-explicit-false-primary.blk",
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   "/blocks/vol-master-needs-rebuild-explicit-false-replica.blk",
			Ready:  true,
			Role:   blockvol.RoleToWire(blockvol.RoleRebuilding),
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	needsRebuild := false
	r.UpdateFullHeartbeat("primary-server:8080", []*master_pb.BlockVolumeInfoMessage{{
		Path:            "/blocks/vol-master-needs-rebuild-explicit-false-primary.blk",
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaDegraded: true,
		NeedsRebuild:    &needsRebuild,
	}}, "")

	entry, _ := r.Lookup("vol-master-needs-rebuild-explicit-false")
	if !entry.HasNeedsRebuild || entry.NeedsRebuild {
		t.Fatalf("expected explicit false needs_rebuild truth on entry, entry=%+v", entry)
	}
	if entry.VolumeMode != "degraded" {
		t.Fatalf("expected explicit false to suppress stale needs_rebuild heuristic, got %q", entry.VolumeMode)
	}
}

func TestRegistry_UpdateFullHeartbeat_ConsumesExplicitPublishHealthyFromPrimaryHeartbeat(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name:          "vol-master-publish-healthy",
		VolumeServer:  "primary-server:8080",
		Path:          "/blocks/vol-master-publish-healthy-primary.blk",
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   "/blocks/vol-master-publish-healthy-replica.blk",
			Ready:  false,
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	publishHealthy := true
	r.UpdateFullHeartbeat("primary-server:8080", []*master_pb.BlockVolumeInfoMessage{{
		Path:           "/blocks/vol-master-publish-healthy-primary.blk",
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		PublishHealthy: &publishHealthy,
	}}, "")

	entry, _ := r.Lookup("vol-master-publish-healthy")
	if !entry.PublishHealthy || !entry.HasPublishHealthy {
		t.Fatalf("expected explicit publish_healthy truth on entry, entry=%+v", entry)
	}
	if entry.VolumeMode != "publish_healthy" {
		t.Fatalf("expected publish_healthy from explicit primary heartbeat truth, got %q", entry.VolumeMode)
	}
}

func TestRegistry_UpdateFullHeartbeat_ExplicitUnhealthySuppressesStalePublishHealthyHeuristic(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name:          "vol-master-publish-healthy-explicit-false",
		VolumeServer:  "primary-server:8080",
		Path:          "/blocks/vol-master-publish-healthy-explicit-false-primary.blk",
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   "/blocks/vol-master-publish-healthy-explicit-false-replica.blk",
			Ready:  true,
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	publishHealthy := false
	r.UpdateFullHeartbeat("primary-server:8080", []*master_pb.BlockVolumeInfoMessage{{
		Path:           "/blocks/vol-master-publish-healthy-explicit-false-primary.blk",
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		PublishHealthy: &publishHealthy,
	}}, "")

	entry, _ := r.Lookup("vol-master-publish-healthy-explicit-false")
	if !entry.HasPublishHealthy || entry.PublishHealthy {
		t.Fatalf("expected explicit false publish_healthy truth on entry, entry=%+v", entry)
	}
	if entry.VolumeMode != "bootstrap_pending" {
		t.Fatalf("expected explicit false to suppress stale publish_healthy heuristic, got %q", entry.VolumeMode)
	}
}

func TestRegistry_UpdateFullHeartbeat_ConsumesExplicitVolumeModeFromPrimaryHeartbeat(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		reason   string
		replicas []ReplicaInfo
		wantMode string
	}{
		{
			name:   "bootstrap_pending",
			mode:   "bootstrap_pending",
			reason: "awaiting_shipper_connected",
			replicas: []ReplicaInfo{{
				Server: "replica-server:8080",
				Path:   "/blocks/vol-master-mode-bootstrap-replica.blk",
				Ready:  true,
			}},
			wantMode: "bootstrap_pending",
		},
		{
			name:   "degraded",
			mode:   "degraded",
			reason: "barrier_timeout",
			replicas: []ReplicaInfo{{
				Server: "replica-server:8080",
				Path:   "/blocks/vol-master-mode-degraded-replica.blk",
				Ready:  true,
			}},
			wantMode: "degraded",
		},
		{
			name:   "needs_rebuild",
			mode:   "needs_rebuild",
			reason: "gap_too_large",
			replicas: []ReplicaInfo{{
				Server: "replica-server:8080",
				Path:   "/blocks/vol-master-mode-needs-rebuild-replica.blk",
				Ready:  true,
			}},
			wantMode: "needs_rebuild",
		},
		{
			name: "publish_healthy",
			mode: "publish_healthy",
			replicas: []ReplicaInfo{{
				Server: "replica-server:8080",
				Path:   "/blocks/vol-master-mode-publish-healthy-replica.blk",
				Ready:  false,
			}},
			wantMode: "publish_healthy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBlockVolumeRegistry()
			if err := r.Register(&BlockVolumeEntry{
				Name:          "vol-master-mode-" + tt.name,
				VolumeServer:  "primary-server:8080",
				Path:          "/blocks/vol-master-mode-" + tt.name + "-primary.blk",
				Status:        StatusActive,
				Role:          blockvol.RoleToWire(blockvol.RolePrimary),
				ReplicaFactor: 2,
				Replicas:      tt.replicas,
			}); err != nil {
				t.Fatalf("register: %v", err)
			}

			mode := tt.mode
			reason := tt.reason
			r.UpdateFullHeartbeat("primary-server:8080", []*master_pb.BlockVolumeInfoMessage{{
				Path:             "/blocks/vol-master-mode-" + tt.name + "-primary.blk",
				Role:             blockvol.RoleToWire(blockvol.RolePrimary),
				VolumeMode:       &mode,
				VolumeModeReason: &reason,
			}}, "")

			entry, _ := r.Lookup("vol-master-mode-" + tt.name)
			if !entry.HasHeartbeatVolumeMode || entry.HeartbeatVolumeMode != tt.mode {
				t.Fatalf("expected explicit volume_mode truth on entry, entry=%+v", entry)
			}
			if tt.reason != "" && (!entry.HasHeartbeatVolumeReason || entry.HeartbeatVolumeReason != tt.reason) {
				t.Fatalf("expected explicit volume_mode_reason truth on entry, entry=%+v", entry)
			}
			if entry.VolumeMode != tt.wantMode {
				t.Fatalf("expected explicit volume_mode %q, got %q", tt.wantMode, entry.VolumeMode)
			}
		})
	}
}

func TestRegistry_UpdateFullHeartbeat_VolumeModeFallsBackWhenFieldAbsent(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name:          "vol-master-mode-fallback",
		VolumeServer:  "primary-server:8080",
		Path:          "/blocks/vol-master-mode-fallback-primary.blk",
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   "/blocks/vol-master-mode-fallback-replica.blk",
			Ready:  true,
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	r.UpdateFullHeartbeat("primary-server:8080", []*master_pb.BlockVolumeInfoMessage{{
		Path:            "/blocks/vol-master-mode-fallback-primary.blk",
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaDegraded: true,
	}}, "")

	entry, _ := r.Lookup("vol-master-mode-fallback")
	if entry.HasHeartbeatVolumeMode {
		t.Fatalf("did not expect explicit volume_mode when field absent, entry=%+v", entry)
	}
	if entry.HasHeartbeatVolumeReason {
		t.Fatalf("did not expect explicit volume_mode_reason when field absent, entry=%+v", entry)
	}
	if entry.VolumeMode != "degraded" {
		t.Fatalf("expected fallback reconstructed degraded mode, got %q", entry.VolumeMode)
	}
}
