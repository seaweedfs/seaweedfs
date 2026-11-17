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

// With no .vif the bitrot sidecar is the ONLY record of the volume's layout.
// Present but unusable — unreadable, stamped for another generation, or
// carrying an incomplete ratio — must fail the mount: answering reads from a
// guessed layout returns wrong bytes rather than none. Genuine absence stays
// legal, and is covered by TestNewEcVolumeAbsentVifMountsWithDefaults.
func TestNewEcVolumeRejectsUnusableSoleSidecar(t *testing.T) {
	seed := map[string]*volume_server_pb.EcBitrotProtection{
		"another generation": {
			Algorithm:     volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
			BlockSize:     uint32(BitrotBlockSize),
			Generation:    5,
			EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4},
		},
		"no ec config": {
			Algorithm:  volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
			BlockSize:  uint32(BitrotBlockSize),
			Generation: 0,
		},
		"incomplete ratio": {
			Algorithm:     volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
			BlockSize:     uint32(BitrotBlockSize),
			Generation:    0,
			EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10},
		},
		"invalid block size": {
			Algorithm:  volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
			BlockSize:  uint32(BitrotBlockSize),
			Generation: 0,
			EcShardConfig: &volume_server_pb.EcShardConfig{
				DataShards: 10, ParityShards: 4, BlockSize: 3*1024*1024 + 1,
			},
		},
	}
	for name, prot := range seed {
		dir := t.TempDir()
		base := filepath.Join(dir, "1")
		if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
			t.Fatalf("%s: seed .ecx: %v", name, err)
		}
		if err := SaveBitrotSidecar(BitrotSidecarPath(base, 0), prot); err != nil {
			t.Fatalf("%s: seed .ecsum: %v", name, err)
		}
		if _, err := NewEcVolume(types.HardDriveType, dir, dir, "", needle.VolumeId(1)); err == nil {
			t.Errorf("%s: an unusable sole layout record must fail the mount", name)
		}
	}
}

// Shards placed on the disk that still holds the source volume's .vif mount
// against an index on a sibling disk; the encode identity and layout come from
// the sibling's .vif, not from the source's config-free one.
func TestNewEcVolumePrefersIndexVifOverConfigFreeLocalVif(t *testing.T) {
	dataDir, idxDir := t.TempDir(), t.TempDir()
	const encodeTsNs = int64(1_700_000_000_123_456_789)
	if err := volume_info.SaveVolumeInfo(filepath.Join(dataDir, "c_1.vif"), &volume_server_pb.VolumeInfo{
		Version:  uint32(needle.Version3),
		ReadOnly: true,
	}); err != nil {
		t.Fatalf("seed source .vif: %v", err)
	}
	if err := os.WriteFile(filepath.Join(idxDir, "c_1.ecx"), []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(filepath.Join(idxDir, "c_1.vif"), &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.Version3),
		DatFileSize: 4096,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards: 12, ParityShards: 4, BlockSize: 3 * 1024 * 1024, EncodeTsNs: encodeTsNs,
		},
	}); err != nil {
		t.Fatalf("seed index .vif: %v", err)
	}

	ev, err := NewEcVolume(types.HardDriveType, dataDir, idxDir, "c", needle.VolumeId(1))
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	defer ev.Close()
	if ev.EncodeTsNs != encodeTsNs {
		t.Errorf("EncodeTsNs = %d, want %d", ev.EncodeTsNs, encodeTsNs)
	}
	if got := ev.ECContext; got.DataShards != 12 || got.ParityShards != 4 || got.BlockSize != 3*1024*1024 {
		t.Errorf("layout = %d+%d block %d, want 12+4 block 3MiB", got.DataShards, got.ParityShards, got.BlockSize)
	}
	if ev.datFileSize != 4096 {
		t.Errorf("datFileSize = %d, want 4096", ev.datFileSize)
	}
}

// A local .vif that records an EC config stays authoritative for its disk.
func TestNewEcVolumeKeepsLocalVifWithEcConfig(t *testing.T) {
	dataDir, idxDir := t.TempDir(), t.TempDir()
	if err := volume_info.SaveVolumeInfo(filepath.Join(dataDir, "1.vif"), &volume_server_pb.VolumeInfo{
		Version:       uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4, EncodeTsNs: 11},
	}); err != nil {
		t.Fatalf("seed local .vif: %v", err)
	}
	if err := os.WriteFile(filepath.Join(idxDir, "1.ecx"), []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(filepath.Join(idxDir, "1.vif"), &volume_server_pb.VolumeInfo{
		Version:       uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4, EncodeTsNs: 22},
	}); err != nil {
		t.Fatalf("seed index .vif: %v", err)
	}

	ev, err := NewEcVolume(types.HardDriveType, dataDir, idxDir, "", needle.VolumeId(1))
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	defer ev.Close()
	if ev.EncodeTsNs != 11 {
		t.Errorf("EncodeTsNs = %d, want the local 11", ev.EncodeTsNs)
	}
}

// With a shared -dir.idx the encode's .vif sits in the data dir of the disk
// that received the index, so a disk holding a source volume's config-free
// .vif takes it from that sibling, but only when the siblings agree on one run.
func TestNewEcVolumeTakesSiblingVifWithSharedIndex(t *testing.T) {
	const runA, runB = int64(1_700_000_000_000_000_011), int64(1_700_000_000_000_000_022)
	cases := []struct {
		name       string
		siblingTs  []int64
		localIndex bool
		want       int64
	}{
		{name: "one sibling", siblingTs: []int64{runA}, want: runA},
		{name: "siblings agree", siblingTs: []int64{runA, runA}, want: runA},
		{name: "siblings disagree", siblingTs: []int64{runA, runB}, want: 0},
		{name: "local index", siblingTs: []int64{runA}, localIndex: true, want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir, idxDir := t.TempDir(), t.TempDir()
			dirs := []string{dataDir}
			if err := volume_info.SaveVolumeInfo(filepath.Join(dataDir, "c_1.vif"), &volume_server_pb.VolumeInfo{
				Version:  uint32(needle.Version3),
				ReadOnly: true,
			}); err != nil {
				t.Fatalf("seed source .vif: %v", err)
			}
			if err := os.WriteFile(filepath.Join(idxDir, "c_1.ecx"), make([]byte, 20), 0644); err != nil {
				t.Fatalf("seed .ecx: %v", err)
			}
			if tc.localIndex {
				if err := os.WriteFile(filepath.Join(dataDir, "c_1.ecx"), make([]byte, 20), 0644); err != nil {
					t.Fatalf("seed local .ecx: %v", err)
				}
			}
			for _, ts := range tc.siblingTs {
				sibling := t.TempDir()
				dirs = append(dirs, sibling)
				if err := volume_info.SaveVolumeInfo(filepath.Join(sibling, "c_1.vif"), &volume_server_pb.VolumeInfo{
					Version:       uint32(needle.Version3),
					DatFileSize:   4096,
					EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4, EncodeTsNs: ts},
				}); err != nil {
					t.Fatalf("seed sibling .vif: %v", err)
				}
			}

			ev, err := NewEcVolume(types.HardDriveType, dataDir, idxDir, "c", needle.VolumeId(1), dirs...)
			if err != nil {
				t.Fatalf("mount: %v", err)
			}
			defer ev.Close()
			if ev.EncodeTsNs != tc.want {
				t.Errorf("EncodeTsNs = %d, want %d", ev.EncodeTsNs, tc.want)
			}
		})
	}
}
