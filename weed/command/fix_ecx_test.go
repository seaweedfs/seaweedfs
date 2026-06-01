package command

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// buildAndEncodeTestEcVolume writes a small volume (.dat + .idx), EC-encodes it
// into .ec00..ec13, and produces the canonical sorted .ecx. It returns the base
// path, the canonical .ecx bytes, and the original .dat size. A couple of
// needles are deleted so the .ecx must exclude them (live entries only).
func buildAndEncodeTestEcVolume(t *testing.T, dir, baseName string) (base string, canonicalEcx []byte, origDatSize int64) {
	t.Helper()
	base = filepath.Join(dir, baseName)
	version := needle.GetCurrentVersion()

	df, err := os.OpenFile(base+".dat", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	datBackend := backend.NewDiskFile(df)
	sb := super_block.SuperBlock{
		Version:          version,
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              &needle.TTL{},
	}
	if _, err := datBackend.WriteAt(sb.Bytes(), 0); err != nil {
		t.Fatal(err)
	}

	nm := needle_map.NewMemDb()
	for i := uint64(1); i <= 12; i++ {
		n := new(needle.Needle)
		n.Id = types.Uint64ToNeedleId(i)
		n.Data = make([]byte, 200+int(i))
		rand.Read(n.Data)
		n.Checksum = needle.NewCRC(n.Data)
		offset, _, _, err := n.Append(datBackend, version)
		if err != nil {
			t.Fatalf("append needle %d: %v", i, err)
		}
		// Store n.Size (the on-disk header size), exactly what the volume
		// server records in its .idx (volume_write.go: nm.Put(..., n.Size)).
		if err := nm.Set(n.Id, types.ToOffset(int64(offset)), n.Size); err != nil {
			t.Fatal(err)
		}
	}
	// Delete ids 3 and 8: append an empty needle (delete record) and drop them
	// from the index, exactly as the encode-time .ecx would reflect.
	for _, id := range []uint64{3, 8} {
		n := new(needle.Needle)
		n.Id = types.Uint64ToNeedleId(id)
		if _, _, _, err := n.Append(datBackend, version); err != nil {
			t.Fatalf("append delete record %d: %v", id, err)
		}
		if err := nm.Delete(types.Uint64ToNeedleId(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := datBackend.Sync(); err != nil {
		t.Fatal(err)
	}
	datInfo, err := df.Stat()
	if err != nil {
		t.Fatal(err)
	}
	origDatSize = datInfo.Size()
	datBackend.Close()

	idxFile, err := os.OpenFile(base+".idx", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if err := nm.AscendingVisit(func(v needle_map.NeedleValue) error {
		_, e := idxFile.Write(v.ToBytes())
		return e
	}); err != nil {
		t.Fatal(err)
	}
	idxFile.Close()
	nm.Close()

	if _, err := erasure_coding.WriteEcFiles(base, erasure_coding.BackgroundECContext()); err != nil {
		t.Fatalf("WriteEcFiles: %v", err)
	}
	if err := erasure_coding.WriteSortedFileFromIdx(base, ".ecx"); err != nil {
		t.Fatalf("WriteSortedFileFromIdx: %v", err)
	}
	canonicalEcx, err = os.ReadFile(base + ".ecx")
	if err != nil {
		t.Fatal(err)
	}
	if len(canonicalEcx) == 0 {
		t.Fatal("canonical .ecx is empty")
	}
	return base, canonicalEcx, origDatSize
}

// TestFixEcxFromShards verifies the .ecx and .vif are rebuilt purely from the
// data shards when every index/metadata file has been lost.
func TestFixEcxFromShards(t *testing.T) {
	oldData, oldParity := *fixEcDataShards, *fixEcParityShards
	*fixEcDataShards, *fixEcParityShards = 0, 0
	*fixIgnoreError = true
	t.Cleanup(func() {
		*fixIgnoreError = false
		*fixEcDataShards, *fixEcParityShards = oldData, oldParity
	})

	dir := t.TempDir()
	const volumeId = 7
	base, canonical, origDatSize := buildAndEncodeTestEcVolume(t, dir, "7")

	// Disaster: keep only the shards.
	for _, ext := range []string{".ecx", ".ecj", ".idx", ".dat", ".vif"} {
		if err := os.Remove(base + ext); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove %s: %v", base+ext, err)
		}
	}

	doFixEcxFromShards(dir, "7", "", volumeId)

	recovered, err := os.ReadFile(base + ".ecx")
	if err != nil {
		t.Fatalf("recovered .ecx not written: %v", err)
	}
	if !bytes.Equal(canonical, recovered) {
		t.Fatalf(".ecx mismatch: canonical %d bytes, recovered %d bytes", len(canonical), len(recovered))
	}

	// The reconstructed temporary .dat must not be left behind.
	if _, err := os.Stat(base + ".ecxrecover.dat"); !os.IsNotExist(err) {
		t.Fatalf("temporary reconstructed .dat was not cleaned up")
	}

	// .vif must be regenerated with the default ratio and the original .dat size.
	vi, _, found, err := volume_info.MaybeLoadVolumeInfo(base + ".vif")
	if err != nil || !found {
		t.Fatalf(".vif not regenerated: found=%v err=%v", found, err)
	}
	if got := int(vi.GetEcShardConfig().GetDataShards()); got != erasure_coding.DataShardsCount {
		t.Fatalf("data shards = %d, want %d", got, erasure_coding.DataShardsCount)
	}
	if got := int(vi.GetEcShardConfig().GetParityShards()); got != erasure_coding.ParityShardsCount {
		t.Fatalf("parity shards = %d, want %d", got, erasure_coding.ParityShardsCount)
	}
	if vi.GetDatFileSize() != origDatSize {
		t.Fatalf("dat size = %d, want %d", vi.GetDatFileSize(), origDatSize)
	}
}

// TestFixEcxFromShardsWithVif verifies that when the .vif survives (recording
// the exact .dat size and EC ratio) the .ecx is rebuilt from it and the .vif is
// left untouched.
func TestFixEcxFromShardsWithVif(t *testing.T) {
	oldData, oldParity := *fixEcDataShards, *fixEcParityShards
	*fixEcDataShards, *fixEcParityShards = 0, 0
	*fixIgnoreError = true
	t.Cleanup(func() {
		*fixIgnoreError = false
		*fixEcDataShards, *fixEcParityShards = oldData, oldParity
	})

	dir := t.TempDir()
	const volumeId = 9
	base, canonical, origDatSize := buildAndEncodeTestEcVolume(t, dir, "9")

	// Write a .vif as the volume server would after encoding.
	if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.GetCurrentVersion()),
		DatFileSize: origDatSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   uint32(erasure_coding.DataShardsCount),
			ParityShards: uint32(erasure_coding.ParityShardsCount),
		},
	}); err != nil {
		t.Fatal(err)
	}
	vifBefore, err := os.ReadFile(base + ".vif")
	if err != nil {
		t.Fatal(err)
	}

	// Lose the index but keep the surviving .vif and shards.
	for _, ext := range []string{".ecx", ".ecj", ".idx", ".dat"} {
		if err := os.Remove(base + ext); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove %s: %v", base+ext, err)
		}
	}

	doFixEcxFromShards(dir, "9", "", volumeId)

	recovered, err := os.ReadFile(base + ".ecx")
	if err != nil {
		t.Fatalf("recovered .ecx not written: %v", err)
	}
	if !bytes.Equal(canonical, recovered) {
		t.Fatalf(".ecx mismatch: canonical %d bytes, recovered %d bytes", len(canonical), len(recovered))
	}

	// An existing .vif must be left untouched.
	vifAfter, err := os.ReadFile(base + ".vif")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(vifBefore, vifAfter) {
		t.Fatalf("existing .vif was modified")
	}
}

// TestFixEcxFromShardsMissingShards verifies that when some shards (including a
// couple of data shards) are lost but at least dataShards survive, the missing
// shards are reconstructed from parity and the .ecx is still rebuilt correctly.
func TestFixEcxFromShardsMissingShards(t *testing.T) {
	oldData, oldParity := *fixEcDataShards, *fixEcParityShards
	*fixEcDataShards, *fixEcParityShards = 0, 0
	*fixIgnoreError = true
	t.Cleanup(func() {
		*fixIgnoreError = false
		*fixEcDataShards, *fixEcParityShards = oldData, oldParity
	})

	dir := t.TempDir()
	const volumeId = 11
	base, canonical, origDatSize := buildAndEncodeTestEcVolume(t, dir, "11")

	// Lose every index/metadata file plus three shards (two data: .ec02, .ec05;
	// one parity: .ec11), keeping 11 of 14 — enough to reconstruct. The highest
	// shard (.ec13) is kept so the default 10+4 ratio is inferred without a .vif.
	for _, ext := range []string{".ecx", ".ecj", ".idx", ".dat", ".vif",
		erasure_coding.ToExt(2), erasure_coding.ToExt(5), erasure_coding.ToExt(11)} {
		if err := os.Remove(base + ext); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove %s: %v", base+ext, err)
		}
	}

	doFixEcxFromShards(dir, "11", "", volumeId)

	recovered, err := os.ReadFile(base + ".ecx")
	if err != nil {
		t.Fatalf("recovered .ecx not written: %v", err)
	}
	if !bytes.Equal(canonical, recovered) {
		t.Fatalf(".ecx mismatch: canonical %d bytes, recovered %d bytes", len(canonical), len(recovered))
	}

	// The missing shards must have been reconstructed on disk.
	for _, idx := range []int{2, 5, 11} {
		if info, err := os.Stat(base + erasure_coding.ToExt(idx)); err != nil || info.Size() == 0 {
			t.Fatalf("missing shard %d was not reconstructed: err=%v", idx, err)
		}
	}

	vi, _, found, err := volume_info.MaybeLoadVolumeInfo(base + ".vif")
	if err != nil || !found {
		t.Fatalf(".vif not regenerated: found=%v err=%v", found, err)
	}
	if vi.GetDatFileSize() != origDatSize {
		t.Fatalf("dat size = %d, want %d", vi.GetDatFileSize(), origDatSize)
	}
}
