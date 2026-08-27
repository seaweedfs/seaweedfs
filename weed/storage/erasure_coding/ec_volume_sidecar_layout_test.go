package erasure_coding

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

func uniformSidecar(t *testing.T, base string, ds, ps uint32, blockSize int64) {
	t.Helper()
	prot := &volume_server_pb.EcBitrotProtection{
		Algorithm:  volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
		BlockSize:  uint32(BitrotBlockSize),
		Generation: 0,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards: ds, ParityShards: ps, BlockSize: blockSize,
		},
	}
	if err := SaveBitrotSidecar(BitrotSidecarPath(base, 0), prot); err != nil {
		t.Fatalf("seed .ecsum: %v", err)
	}
}

// The sidecar records the layout at encode time, so it answers what a .vif
// cannot. A .vif that merely omits ecShardConfig is no more informative than an
// absent one — going straight to the defaults reads a uniform volume with the
// legacy offset math and returns the wrong bytes.
func TestNewEcVolumeTakesLayoutFromSidecarWhenVifHasNoConfig(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "1")
	if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	// A config-free .vif: version and dat size only, as a legacy encode left it.
	if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version: uint32(needle.Version3),
	}); err != nil {
		t.Fatalf("seed .vif: %v", err)
	}
	uniformSidecar(t, base, 12, 4, 3*1024*1024)

	ev, err := NewEcVolume(types.HardDriveType, dir, dir, "", needle.VolumeId(1))
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	defer ev.Close()
	if got := ev.ECContext; got.DataShards != 12 || got.ParityShards != 4 || got.BlockSize != 3*1024*1024 {
		t.Errorf("layout = %d+%d block %d, want 12+4 block 3MiB", got.DataShards, got.ParityShards, got.BlockSize)
	}
}

// A split -dir/-dir.idx layout keeps the sidecar with the INDEX. Probing only
// the data base reports "absent", and absent is the one answer that selects the
// legacy layout.
func TestNewEcVolumeFindsSidecarInTheIndexDirectory(t *testing.T) {
	for _, tc := range []struct {
		name    string
		writeVif bool
	}{
		{name: "no vif at all"},
		{name: "vif without ec config", writeVif: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir, idxDir := t.TempDir(), t.TempDir()
			idxBase := filepath.Join(idxDir, "2")
			if err := os.WriteFile(idxBase+".ecx", []byte{}, 0644); err != nil {
				t.Fatalf("seed .ecx: %v", err)
			}
			if tc.writeVif {
				if err := volume_info.SaveVolumeInfo(idxBase+".vif", &volume_server_pb.VolumeInfo{
					Version: uint32(needle.Version3),
				}); err != nil {
					t.Fatalf("seed .vif: %v", err)
				}
			}
			uniformSidecar(t, idxBase, 12, 4, 3*1024*1024)

			ev, err := NewEcVolume(types.HardDriveType, dataDir, idxDir, "", needle.VolumeId(2))
			if err != nil {
				t.Fatalf("mount: %v", err)
			}
			defer ev.Close()
			if got := ev.ECContext; got.DataShards != 12 || got.ParityShards != 4 || got.BlockSize != 3*1024*1024 {
				t.Errorf("layout = %d+%d block %d, want 12+4 block 3MiB", got.DataShards, got.ParityShards, got.BlockSize)
			}
		})
	}
}

// Absence stays legal — a volume encoded before either record exists is a
// genuine legacy volume, and only that case may select the legacy layout.
func TestNewEcVolumeConfigFreeVifWithNoSidecarUsesDefaults(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "3")
	if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version: uint32(needle.Version3),
	}); err != nil {
		t.Fatalf("seed .vif: %v", err)
	}
	ev, err := NewEcVolume(types.HardDriveType, dir, dir, "", needle.VolumeId(3))
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	defer ev.Close()
	if got := ev.ECContext; got.DataShards != DataShardsCount || got.ParityShards != ParityShardsCount || got.BlockSize != 0 {
		t.Errorf("layout = %d+%d block %d, want the legacy defaults", got.DataShards, got.ParityShards, got.BlockSize)
	}
}

// Present but unusable must fail the mount rather than default, in the
// config-free-vif branch exactly as in the absent-vif one.
func TestNewEcVolumeConfigFreeVifWithUnusableSidecarFails(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "4")
	if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version: uint32(needle.Version3),
	}); err != nil {
		t.Fatalf("seed .vif: %v", err)
	}
	// Stamped for another generation: not a record of these shards.
	if err := SaveBitrotSidecar(BitrotSidecarPath(base, 0), &volume_server_pb.EcBitrotProtection{
		Algorithm:     volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
		BlockSize:     uint32(BitrotBlockSize),
		Generation:    5,
		EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 12, ParityShards: 4},
	}); err != nil {
		t.Fatalf("seed .ecsum: %v", err)
	}
	if _, err := NewEcVolume(types.HardDriveType, dir, dir, "", needle.VolumeId(4)); err == nil {
		t.Fatal("an unusable sole layout record must fail the mount")
	}
}

// Startup mirroring gives each shard-bearing disk its own .ecx/.ecj/.vif but
// deliberately not the .ecsum, and a repair delivers exactly one copy. A
// runtime restricted to its own two directories therefore reports no
// protection no matter how often it reloads — so the resolution has to reach
// the sibling disk that actually received the manifest.
func TestReloadBitrotSidecarFindsItOnASiblingDisk(t *testing.T) {
	mine, sibling := t.TempDir(), t.TempDir()
	myBase := filepath.Join(mine, "7")
	if err := os.WriteFile(myBase+".ecx", []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(myBase+".vif", &volume_server_pb.VolumeInfo{
		Version: uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards: 10, ParityShards: 4, BlockSize: 0,
		},
	}); err != nil {
		t.Fatalf("seed .vif: %v", err)
	}

	ev, err := NewEcVolume(types.HardDriveType, mine, mine, "", needle.VolumeId(7))
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	defer ev.Close()
	if got := ev.bitrotStatus; got != BitrotOff {
		t.Fatalf("precondition: status = %v, want BitrotOff before the delivery", got)
	}

	// The delivery lands the manifest on the sibling disk only.
	prot := &volume_server_pb.EcBitrotProtection{
		Algorithm:  volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
		BlockSize:  uint32(BitrotBlockSize),
		Generation: 0,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards: 10, ParityShards: 4, BlockSize: 0,
		},
	}
	for i := 0; i < 14; i++ {
		prot.Shards = append(prot.Shards, &volume_server_pb.EcShardChecksums{
			ShardId: uint32(i), CoveredSize: 1, BlockCrc32C: make([]byte, 4),
		})
	}
	if err := SaveBitrotSidecar(BitrotSidecarPath(filepath.Join(sibling, "7"), 0), prot); err != nil {
		t.Fatalf("seed sibling .ecsum: %v", err)
	}

	// Reloading against only its own directories still finds nothing.
	ev.ReloadBitrotSidecar()
	if got := ev.bitrotStatus; got != BitrotOff {
		t.Errorf("own-directories reload: status = %v, want BitrotOff", got)
	}

	ev.ReloadBitrotSidecar(mine, sibling)
	if got := ev.bitrotStatus; got != BitrotOn {
		t.Errorf("cross-disk reload: status = %v, want BitrotOn", got)
	}
}

// "Does this volume already have a manifest" cannot be answered from one base
// name. The rebuild's opportunistic backfill asks it before writing a TOFU
// baseline, and a false "no" there writes a second sidecar at the data base
// that then shadows the real one — blessing whatever the shards currently say.
func TestFindBitrotSidecarSearchesEveryCandidate(t *testing.T) {
	dataDir, idxDir, sibling := t.TempDir(), t.TempDir(), t.TempDir()
	dataBase := filepath.Join(dataDir, "8")
	idxBase := filepath.Join(idxDir, "8")
	siblingBase := filepath.Join(sibling, "8")

	if got := FindBitrotSidecar(0, dataBase, idxBase, sibling); got != "" {
		t.Errorf("no sidecar anywhere should answer empty, got %q", got)
	}

	// Each location on its own: any one of them is enough to answer "yes".
	for _, tc := range []struct{ name, base string }{
		{"data directory", dataBase},
		{"index directory", idxBase},
		{"sibling disk", siblingBase},
	} {
		want := BitrotSidecarPath(tc.base, 0)
		if err := os.WriteFile(want, []byte("x"), 0644); err != nil {
			t.Fatalf("%s: seed: %v", tc.name, err)
		}
		if got := FindBitrotSidecar(0, dataBase, idxBase, sibling); got != want {
			t.Errorf("%s: got %q, want %q", tc.name, got, want)
		}
		if err := os.Remove(want); err != nil {
			t.Fatalf("%s: cleanup: %v", tc.name, err)
		}
	}

	// With copies everywhere the data base wins, which is the order every
	// resolver uses — so a stray copy there shadows the others.
	for _, base := range []string{siblingBase, idxBase, dataBase} {
		if err := os.WriteFile(BitrotSidecarPath(base, 0), []byte("x"), 0644); err != nil {
			t.Fatalf("seed %s: %v", base, err)
		}
	}
	if got, want := FindBitrotSidecar(0, dataBase, idxBase, sibling), BitrotSidecarPath(dataBase, 0); got != want {
		t.Errorf("precedence: got %q, want the data base %q", got, want)
	}
}
