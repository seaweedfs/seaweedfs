package erasure_coding

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// A present-but-malformed .vif must FAIL the mount: every new encode
// records a positive uniform block size there, and silently defaulting
// to the legacy layout would serve those shards with the wrong offset
// math. Absence stays legal — legacy volumes predate the sidecar.
func TestNewEcVolumeFailsOnMalformedVif(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "1")
	if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	if err := os.WriteFile(base+".vif", []byte("not json"), 0644); err != nil {
		t.Fatalf("seed .vif: %v", err)
	}
	_, err := NewEcVolume(types.HardDriveType, dir, dir, "", needle.VolumeId(1))
	if err == nil {
		t.Fatal("mount over a malformed .vif must fail")
	}
	if !strings.Contains(err.Error(), ".vif") {
		t.Errorf("error should name the vif: %v", err)
	}
}

func TestNewEcVolumeAbsentVifMountsWithDefaults(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "1")
	if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	ev, err := NewEcVolume(types.HardDriveType, dir, dir, "", needle.VolumeId(1))
	if err != nil {
		t.Fatalf("legacy mount without .vif must succeed: %v", err)
	}
	defer ev.Close()
	if ev.ECContext.BlockSize != 0 {
		t.Errorf("legacy mount must use the legacy layout: BlockSize=%d", ev.ECContext.BlockSize)
	}
}

// A block size no encoder could have produced (negative, or not a whole
// number of small blocks) maps every read to the wrong shard offset, so the
// mount refuses it rather than serving those bytes.
func TestNewEcVolumeRejectsImplausibleBlockSize(t *testing.T) {
	for _, blockSize := range []int64{1, -1, 3*1024*1024 + 1} {
		dir := t.TempDir()
		base := filepath.Join(dir, "1")
		if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
			t.Fatalf("seed .ecx: %v", err)
		}
		if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
			Version: uint32(needle.Version3),
			EcShardConfig: &volume_server_pb.EcShardConfig{
				DataShards: 10, ParityShards: 4, BlockSize: blockSize,
			},
		}); err != nil {
			t.Fatalf("seed .vif: %v", err)
		}
		if _, err := NewEcVolume(types.HardDriveType, dir, dir, "", needle.VolumeId(1)); err == nil {
			t.Errorf("block size %d must fail the mount", blockSize)
		}
	}
}

func TestNewEcVolumeAcceptsAlignedBlockSize(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "1")
	if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version: uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards: 10, ParityShards: 4, BlockSize: 3 * 1024 * 1024,
		},
	}); err != nil {
		t.Fatalf("seed .vif: %v", err)
	}
	ev, err := NewEcVolume(types.HardDriveType, dir, dir, "", needle.VolumeId(1))
	if err != nil {
		t.Fatalf("aligned block size must mount: %v", err)
	}
	defer ev.Close()
	if ev.ECContext.BlockSize != 3*1024*1024 {
		t.Errorf("BlockSize = %d, want 3MiB", ev.ECContext.BlockSize)
	}
}

// The .vif and the .ecsum both record the layout their generation was encoded
// with. When a generation-matching sidecar disagrees, one of them is wrong and
// reads through the other land at the wrong shard offsets — so the mount must
// refuse rather than quietly drop to unprotected reads.
func TestNewEcVolumeRejectsSidecarGeometryDisagreement(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "1")
	if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version: uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards: 10, ParityShards: 4, BlockSize: 0, // says legacy
		},
	}); err != nil {
		t.Fatalf("seed .vif: %v", err)
	}
	// The sidecar for the same generation says uniform.
	prot := &volume_server_pb.EcBitrotProtection{
		Algorithm:  volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
		BlockSize:  uint32(BitrotBlockSize),
		Generation: 0,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards: 10, ParityShards: 4, BlockSize: 3 * 1024 * 1024,
		},
	}
	if err := SaveBitrotSidecar(BitrotSidecarPath(base, 0), prot); err != nil {
		t.Fatalf("seed .ecsum: %v", err)
	}

	_, err := NewEcVolume(types.HardDriveType, dir, dir, "", needle.VolumeId(1))
	if err == nil {
		t.Fatal("a generation-matching layout disagreement must fail the mount")
	}
	if !strings.Contains(err.Error(), "block") {
		t.Errorf("the error should name the disagreement: %v", err)
	}
}
