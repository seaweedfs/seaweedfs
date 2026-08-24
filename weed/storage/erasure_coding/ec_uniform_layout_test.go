package erasure_coding

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// legacyShardSize is the padded shard length the two-tier layout produces:
// whole 1GiB rows, then the remainder in 1MiB rows.
func legacyShardSize(datFileSize int64, dataShards int) int64 {
	largeRow := int64(ErasureCodingLargeBlockSize) * int64(dataShards)
	nLarge := datFileSize / largeRow
	size := nLarge * ErasureCodingLargeBlockSize
	if rem := datFileSize - nLarge*largeRow; rem > 0 {
		smallRow := int64(ErasureCodingSmallBlockSize) * int64(dataShards)
		size += (rem + smallRow - 1) / smallRow * ErasureCodingSmallBlockSize
	}
	return size
}

// The uniform layout must never change a shard's length, only the byte
// placement — capacity math and shard-size credibility checks depend on it.
func TestUniformBlockSizeMatchesLegacyShardSize(t *testing.T) {
	sizes := []int64{
		8, 1000, ErasureCodingSmallBlockSize,
		10 * ErasureCodingSmallBlockSize,
		10*ErasureCodingSmallBlockSize + 1,
		25 * 1024 * 1024,
		10 * ErasureCodingLargeBlockSize,
		10*ErasureCodingLargeBlockSize + 1,
		10*ErasureCodingLargeBlockSize - ErasureCodingSmallBlockSize,
		30 * 1024 * 1024 * 1024,
		30*1024*1024*1024 + 12345,
	}
	for _, datSize := range sizes {
		for _, ds := range []int{10, 9, 5, 1} {
			if got, want := UniformBlockSize(datSize, ds), legacyShardSize(datSize, ds); got != want {
				t.Errorf("UniformBlockSize(%d, %d) = %d, legacy shard size %d", datSize, ds, got, want)
			}
		}
	}
	if got := UniformBlockSize(0, 10); got != ErasureCodingSmallBlockSize {
		t.Errorf("UniformBlockSize(0, 10) = %d, want one small block", got)
	}
}

// encodeLayoutFixture writes a random .dat and encodes it with the given block
// sizes, returning the .dat content.
func encodeLayoutFixture(t *testing.T, baseFileName string, datSize int64, large, small int64, ctx *ECContext) []byte {
	t.Helper()
	data := make([]byte, datSize)
	rand.Read(data)
	if err := os.WriteFile(baseFileName+".dat", data, 0o644); err != nil {
		t.Fatalf("write .dat: %v", err)
	}
	if _, err := generateEcFiles(baseFileName, 256*1024, large, small, ctx); err != nil {
		t.Fatalf("generateEcFiles: %v", err)
	}
	return data
}

// readIntervals assembles a byte range from the shard files the way the read
// path does.
func readIntervals(t *testing.T, baseFileName string, ctx *ECContext, intervals []Interval) []byte {
	t.Helper()
	var assembled []byte
	for _, iv := range intervals {
		shardId, shardOffset := iv.ToShardIdAndOffset(ctx.LargeBlockSize(), ctx.SmallBlockSize())
		f, err := os.Open(baseFileName + ctx.ToExt(int(shardId)))
		if err != nil {
			t.Fatalf("open shard %d: %v", shardId, err)
		}
		buf := make([]byte, iv.Size)
		if _, err := f.ReadAt(buf, shardOffset); err != nil {
			t.Fatalf("read shard %d at %d: %v", shardId, shardOffset, err)
		}
		f.Close()
		assembled = append(assembled, buf...)
	}
	return assembled
}

// Encode 25MB — large enough that the uniform and legacy layouts place bytes
// differently — under both layouts and verify LocateData maps every probed
// range back to the original bytes.
func TestLocateDataMatchesEncoderForBothLayouts(t *testing.T) {
	const datSize = 25*1024*1024 + 12345
	dir := t.TempDir()

	uniformCtx := NewDefaultECContext("", 1)
	uniformCtx.BlockSize = UniformBlockSize(datSize, uniformCtx.DataShards)
	legacyCtx := NewDefaultECContext("", 2)

	for name, ctx := range map[string]*ECContext{"uniform": uniformCtx, "legacy": legacyCtx} {
		t.Run(name, func(t *testing.T) {
			base := fmt.Sprintf("%s/%s", dir, name)
			data := encodeLayoutFixture(t, base, datSize, ctx.LargeBlockSize(), ctx.SmallBlockSize(), ctx)

			shardSize := datSize / int64(ctx.DataShards)
			for _, probe := range []struct{ offset, size int64 }{
				{0, 1000},
				{ctx.SmallBlockSize() - 100, 200}, // straddles a block boundary
				{5 * 1024 * 1024, 4 * 1024 * 1024},
				{datSize - 2000, 2000},
			} {
				intervals := LocateData(ctx.LargeBlockSize(), ctx.SmallBlockSize(), shardSize, probe.offset, types.Size(probe.size))
				got := readIntervals(t, base, ctx, intervals)
				if !bytes.Equal(got, data[probe.offset:probe.offset+probe.size]) {
					t.Fatalf("%s layout: bytes at [%d,+%d) do not round-trip", name, probe.offset, probe.size)
				}
			}

			// The point of the uniform layout: a 2MB range inside one 3MB
			// block is a single interval instead of three 1MB stripes.
			intervals := LocateData(ctx.LargeBlockSize(), ctx.SmallBlockSize(), shardSize, 6*1024*1024+512*1024, types.Size(2*1024*1024))
			if name == "uniform" && len(intervals) != 1 {
				t.Errorf("uniform layout reads 2MB in %d intervals, want 1", len(intervals))
			}
			if name == "legacy" && len(intervals) != 3 {
				t.Errorf("legacy layout reads 2MB in %d intervals, want 3", len(intervals))
			}
		})
	}
}

// Encode under both layouts and verify WriteDatFile reconstructs the original
// .dat byte for byte — and that decoding a uniform volume with the legacy
// geometry does NOT, i.e. the recorded block size is load-bearing.
func TestUniformDecodeRoundTrip(t *testing.T) {
	const datSize = 25 * 1024 * 1024
	dir := t.TempDir()
	base := dir + "/1"

	ctx := NewDefaultECContext("", 1)
	ctx.BlockSize = UniformBlockSize(datSize, ctx.DataShards)
	data := encodeLayoutFixture(t, base, datSize, ctx.BlockSize, ctx.BlockSize, ctx)

	shardFileNames := make([]string, ctx.DataShards)
	for i := range shardFileNames {
		shardFileNames[i] = base + ctx.ToExt(i)
	}

	if err := WriteDatFile(dir+"/decoded", datSize, datSize, shardFileNames, ctx.LargeBlockSize(), ctx.SmallBlockSize()); err != nil {
		t.Fatalf("WriteDatFile: %v", err)
	}
	decoded, err := os.ReadFile(dir + "/decoded.dat")
	if err != nil {
		t.Fatalf("read decoded: %v", err)
	}
	if !bytes.Equal(decoded, data) {
		t.Fatal("uniform decode with recorded geometry does not round-trip")
	}

	if err := WriteDatFile(dir+"/wrong", datSize, datSize, shardFileNames, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize); err != nil {
		t.Fatalf("WriteDatFile legacy geometry: %v", err)
	}
	wrong, err := os.ReadFile(dir + "/wrong.dat")
	if err != nil {
		t.Fatalf("read wrong: %v", err)
	}
	if bytes.Equal(wrong, data) {
		t.Fatal("decoding a uniform volume with legacy geometry should scramble it; the layouts did not diverge")
	}
}

// Mount fixtures through NewEcVolume and read a large extent back through
// LocateEcShardNeedleInterval, proving the .vif block size steers the read
// path: a legacy .vif (no block size) keeps the legacy interpretation, a
// uniform .vif reads uniform shards.
func TestEcVolumeGeometryFromVif(t *testing.T) {
	const datSize = 25 * 1024 * 1024

	for _, layout := range []string{"legacy", "uniform"} {
		t.Run(layout, func(t *testing.T) {
			dir := t.TempDir()
			vid := needle.VolumeId(7)
			base := EcShardFileName("", dir, int(vid))

			ctx := NewDefaultECContext("", vid)
			if layout == "uniform" {
				ctx.BlockSize = UniformBlockSize(datSize, ctx.DataShards)
			}
			data := encodeLayoutFixture(t, base, datSize, ctx.LargeBlockSize(), ctx.SmallBlockSize(), ctx)

			if err := os.WriteFile(base+".ecx", nil, 0o644); err != nil {
				t.Fatalf("write .ecx: %v", err)
			}
			if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
				Version:     uint32(needle.GetCurrentVersion()),
				DatFileSize: datSize,
				EcShardConfig: &volume_server_pb.EcShardConfig{
					DataShards:   uint32(ctx.DataShards),
					ParityShards: uint32(ctx.ParityShards),
					BlockSize:    ctx.BlockSize,
				},
			}); err != nil {
				t.Fatalf("save .vif: %v", err)
			}

			ev, err := NewEcVolume(types.HardDriveType, dir, dir, "", vid)
			if err != nil {
				t.Fatalf("NewEcVolume: %v", err)
			}
			defer ev.Close()
			if ev.ECContext.BlockSize != ctx.BlockSize {
				t.Fatalf("loaded BlockSize = %d, want %d", ev.ECContext.BlockSize, ctx.BlockSize)
			}
			for i := 0; i < ctx.DataShards; i++ {
				shard, err := NewEcVolumeShard(types.HardDriveType, dir, "", vid, ShardId(i))
				if err != nil {
					t.Fatalf("NewEcVolumeShard %d: %v", i, err)
				}
				ev.AddEcVolumeShard(shard)
			}

			// Sweep a large extent through the volume's own interval mapping.
			// LocateEcShardNeedleInterval expands the size by the needle
			// overhead, so compare only the probed prefix.
			const probeSize = datSize - 64*1024
			intervals := ev.LocateEcShardNeedleInterval(needle.GetCurrentVersion(), 0, types.Size(probeSize))
			var assembled []byte
			for _, iv := range intervals {
				shardId, shardOffset := ev.IntervalToShardIdAndOffset(iv)
				shard, found := ev.FindEcVolumeShard(shardId)
				if !found {
					t.Fatalf("shard %d not mounted", shardId)
				}
				buf := make([]byte, iv.Size)
				if _, err := shard.ReadAt(buf, shardOffset); err != nil {
					t.Fatalf("read shard %d at %d: %v", shardId, shardOffset, err)
				}
				assembled = append(assembled, buf...)
			}
			if len(assembled) < probeSize {
				t.Fatalf("assembled %d bytes, want at least %d", len(assembled), probeSize)
			}
			if !bytes.Equal(assembled[:probeSize], data[:probeSize]) {
				t.Fatalf("%s layout: full-extent read through the %s .vif does not match the .dat", layout, layout)
			}
		})
	}
}
