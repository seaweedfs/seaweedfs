package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// A volume's replication lives in two places that can disagree, and the .vif is
// the one that counts.
//
// Store.ConfigureVolume rewrites the .vif and never the replica-placement byte
// in the .dat superblock, so that byte keeps whatever the volume was created
// with for good. readSuperBlock reads the byte and then overrides it from the
// .vif, which is what makes the change take effect and survive a remount.
//
// Preferring the superblock instead would silently revert every replication
// change on the next mount, while the .vif on disk still recorded what the
// operator asked for. That is worth a test rather than a reading of the code,
// because it is one line to invert and the field beside it -- version -- really
// does resolve the other way, with the superblock outranking the .vif.
func TestVifReplicationOutranksSuperBlock(t *testing.T) {
	dir := t.TempDir()

	// Created carrying 000: the byte written into the .dat, and left there.
	created, err := super_block.NewReplicaPlacementFromString("000")
	if err != nil {
		t.Fatal(err)
	}
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, created, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	if got := v.SuperBlock.ReplicaPlacement.String(); got != "000" {
		t.Fatalf("volume created with replication %q, want 000", got)
	}
	v.Close()

	// What ConfigureVolume does: rewrite the .vif alone.
	vifFile := filepath.Join(dir, "1.vif")
	info, _, _, err := volume_info.MaybeLoadVolumeInfo(vifFile)
	if err != nil {
		t.Fatalf("load vif: %v", err)
	}
	if info == nil {
		info = &volume_server_pb.VolumeInfo{}
	}
	info.Replication = "010"
	if err := volume_info.SaveVolumeInfo(vifFile, info); err != nil {
		t.Fatalf("save vif: %v", err)
	}

	// The .dat still says 000 — nothing rewrote it.
	reopened, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, nil, nil, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("reload volume: %v", err)
	}
	defer reopened.Close()

	if got := reopened.SuperBlock.ReplicaPlacement.String(); got != "010" {
		t.Errorf("replication after reload = %q, want 010 from the .vif; "+
			"a reconfigured volume must not revert to the byte in its .dat", got)
	}
}

// An empty replication in the .vif declares nothing, so the superblock stands.
// Without this a volume that never had its replication configured would be
// forced to whatever the zero value parses as.
func TestEmptyVifReplicationLeavesSuperBlockAlone(t *testing.T) {
	dir := t.TempDir()

	created, err := super_block.NewReplicaPlacementFromString("010")
	if err != nil {
		t.Fatal(err)
	}
	v, err := NewVolume(dir, dir, "", 2, NeedleMapInMemory, created, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	v.Close()

	vifFile := filepath.Join(dir, "2.vif")
	if err := volume_info.SaveVolumeInfo(vifFile, &volume_server_pb.VolumeInfo{Version: uint32(needle.GetCurrentVersion())}); err != nil {
		t.Fatalf("save vif: %v", err)
	}
	if _, err := os.Stat(vifFile); err != nil {
		t.Fatalf("vif not written: %v", err)
	}

	reopened, err := NewVolume(dir, dir, "", 2, NeedleMapInMemory, nil, nil, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("reload volume: %v", err)
	}
	defer reopened.Close()

	if got := reopened.SuperBlock.ReplicaPlacement.String(); got != "010" {
		t.Errorf("replication after reload = %q, want the superblock's 010 to stand", got)
	}
}
