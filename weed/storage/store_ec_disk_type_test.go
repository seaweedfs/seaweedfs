package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// setupECStoreWithMixedDisks builds an empty two-location Store
// (HDD + SSD) and starts a goroutine that captures every
// NewEcShardsChan message published after setup. The caller is expected
// to drop an EC shard file plus matching .ecx/.ecj/.vif onto one of the
// locations and then invoke MountEcShards — this mirrors the real
// VolumeEcShardsCopy → VolumeEcShardsMount sequence and avoids racing
// with the startup-scan publish (which uses the location's disk type
// and would otherwise pollute the captured slice). The returned slice
// is appended to only by the goroutine; tests should not race against
// it directly — use waitForShardMsg instead.
func setupECStoreWithMixedDisks(t *testing.T) (store *Store, drainedShardMsgs *[]master_pb.VolumeEcShardInformationMessage, vid needle.VolumeId, collection string, plant func()) {
	t.Helper()
	tempDir := t.TempDir()
	hddDir := filepath.Join(tempDir, "hdd")
	ssdDir := filepath.Join(tempDir, "ssd")
	for _, d := range []string{hddDir, ssdDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	collection = "logs"
	vid = needle.VolumeId(4242)
	const dataShards, parityShards = 10, 4
	const datSize int64 = 10 * 1024 * 1024
	expectedShardSize := calculateExpectedShardSize(datSize, dataShards)
	diskIOProbeConfig := stats.DefaultDiskIOProbeConfig()

	store = NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{hddDir, ssdDir},
		[]int32{100, 100},
		[]util.MinFreeSpace{{}, {}},
		"",
		NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType, types.SsdType},
		nil,
		3,
		diskIOProbeConfig,
	)

	captured := []master_pb.VolumeEcShardInformationMessage{}
	drainedShardMsgs = &captured
	done := make(chan struct{})
	go func() {
		for {
			select {
			case msg := <-store.NewEcShardsChan:
				captured = append(captured, msg)
			case <-store.NewVolumesChan:
			case <-store.DeletedVolumesChan:
			case <-store.DeletedEcShardsChan:
			case <-store.StateUpdateChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})

	// Caller invokes plant() after the store exists; this mirrors a
	// VolumeEcShardsCopy delivering shard + index files onto the HDD
	// location before a subsequent VolumeEcShardsMount.
	plant = func() {
		t.Helper()
		base := erasure_coding.EcShardFileName(collection, hddDir, int(vid))
		f, err := os.Create(base + erasure_coding.ToExt(0))
		if err != nil {
			t.Fatalf("create shard: %v", err)
		}
		if err := f.Truncate(expectedShardSize); err != nil {
			f.Close()
			t.Fatalf("truncate shard: %v", err)
		}
		f.Close()
		if err := os.WriteFile(base+".ecx", make([]byte, 20), 0o644); err != nil {
			t.Fatalf("write .ecx: %v", err)
		}
		if err := os.WriteFile(base+".ecj", nil, 0o644); err != nil {
			t.Fatalf("write .ecj: %v", err)
		}
		if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
			Version:     uint32(needle.Version3),
			DatFileSize: datSize,
			EcShardConfig: &volume_server_pb.EcShardConfig{
				DataShards:   dataShards,
				ParityShards: parityShards,
			},
		}); err != nil {
			t.Fatalf("save .vif: %v", err)
		}
	}

	return store, drainedShardMsgs, vid, collection, plant
}

// waitForShardMsg returns the first captured mount message for vid, polling
// briefly because the goroutine reads asynchronously from MountEcShards.
func waitForShardMsg(t *testing.T, captured *[]master_pb.VolumeEcShardInformationMessage, vid needle.VolumeId) master_pb.VolumeEcShardInformationMessage {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, msg := range *captured {
			if msg.Id == uint32(vid) {
				return msg
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no NewEcShardsChan message captured for volume %d within 2s", vid)
	return master_pb.VolumeEcShardInformationMessage{}
}

// findHeartbeatShard returns the EC-shard heartbeat entry for vid.
func findHeartbeatShard(t *testing.T, hb *master_pb.Heartbeat, vid needle.VolumeId) *master_pb.VolumeEcShardInformationMessage {
	t.Helper()
	for _, msg := range hb.EcShards {
		if msg.Id == uint32(vid) {
			return msg
		}
	}
	t.Fatalf("no heartbeat EcShards entry for volume %d", vid)
	return nil
}

// TestMountEcShards_AppliesSourceDiskType covers the #9423 mount path: a
// shard physically lives on an HDD location, but MountEcShards is told the
// source volume's disk type was "ssd". The volume server must surface that
// "ssd" value on (a) the one-shot NewEcShardsChan notification, (b) the
// in-memory EcVolume, (c) the mounted shard, and (d) the steady-state
// CollectErasureCodingHeartbeat output.
func TestMountEcShards_AppliesSourceDiskType(t *testing.T) {
	store, captured, vid, collection, plant := setupECStoreWithMixedDisks(t)
	plant()

	if err := store.MountEcShards(collection, vid, 0, "ssd"); err != nil {
		t.Fatalf("MountEcShards: %v", err)
	}

	mountMsg := waitForShardMsg(t, captured, vid)
	if mountMsg.DiskType != "ssd" {
		t.Errorf("NewEcShardsChan disk_type = %q, want %q", mountMsg.DiskType, "ssd")
	}

	hb := store.CollectErasureCodingHeartbeat()
	heartbeatMsg := findHeartbeatShard(t, hb, vid)
	if heartbeatMsg.DiskType != "ssd" {
		t.Errorf("heartbeat EcShards disk_type = %q, want %q", heartbeatMsg.DiskType, "ssd")
	}

	// Inspect the in-memory state directly: both the EcVolume and the shard
	// it holds must report the overridden disk type so subsequent
	// unmount/delete messages stay consistent with mount.
	loc := store.Locations[0] // HDD location holds the shard
	ecVol, found := loc.FindEcVolume(vid)
	if !found {
		t.Fatalf("EC volume %d not found on HDD location after mount", vid)
	}
	if got := string(ecVol.DiskType()); got != "ssd" {
		t.Errorf("EcVolume.DiskType() = %q, want %q", got, "ssd")
	}
	shard, ok := ecVol.FindEcVolumeShard(0)
	if !ok {
		t.Fatalf("EcVolume %d has no shard 0 mounted", vid)
	}
	if got := string(shard.DiskType); got != "ssd" {
		t.Errorf("EcVolumeShard.DiskType = %q, want %q", got, "ssd")
	}
}

// TestMountEcShards_FallsBackToLocationDiskTypeWhenEmpty pins the
// pre-#9423 behavior for the empty-source path: disk-scan reload, the
// orphan-shard reconciler, and any other call site that has no
// orchestrator context passes "" and must still get the location's disk
// type on the heartbeat — i.e. the override path must not regress them.
func TestMountEcShards_FallsBackToLocationDiskTypeWhenEmpty(t *testing.T) {
	store, captured, vid, collection, plant := setupECStoreWithMixedDisks(t)
	plant()

	if err := store.MountEcShards(collection, vid, 0, ""); err != nil {
		t.Fatalf("MountEcShards: %v", err)
	}

	mountMsg := waitForShardMsg(t, captured, vid)
	// HDD location -> HardDriveType -> empty disk_type string on the wire.
	if mountMsg.DiskType != string(types.HardDriveType) {
		t.Errorf("NewEcShardsChan disk_type = %q, want %q (location's HDD)", mountMsg.DiskType, string(types.HardDriveType))
	}

	hb := store.CollectErasureCodingHeartbeat()
	heartbeatMsg := findHeartbeatShard(t, hb, vid)
	if heartbeatMsg.DiskType != string(types.HardDriveType) {
		t.Errorf("heartbeat disk_type = %q, want %q", heartbeatMsg.DiskType, string(types.HardDriveType))
	}
}
