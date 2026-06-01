package erasure_coding

import (
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestBitrotSidecarRoundTripAndSelfIntegrity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vol.ecsum")
	prot := &volume_server_pb.EcBitrotProtection{
		Algorithm:     volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
		BlockSize:     DefaultBitrotBlockSize,
		Generation:    0,
		EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4},
		Shards: []*volume_server_pb.EcShardChecksums{
			{ShardId: 0, CoveredSize: 5, BlockCrc32C: packUint32LE([]uint32{123})},
		},
		EncodeUuid: NewEncodeUUID(),
	}
	if err := SaveBitrotSidecar(path, prot); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := LoadBitrotSidecar(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.BlockSize != prot.BlockSize || len(got.Shards) != 1 || got.Shards[0].CoveredSize != 5 {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}

	// Corrupt a payload byte and confirm the self-integrity check trips.
	data, _ := os.ReadFile(path)
	data[bitrotHeaderSize+2] ^= 0xff
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadBitrotSidecar(path); err == nil {
		t.Fatalf("expected self-integrity failure after payload corruption")
	}
}

func TestShardChecksumBuilderCrossesBoundaries(t *testing.T) {
	blockSize := int64(1 << 20)
	data := make([]byte, 0, 3*blockSize+12345)
	r := rand.New(rand.NewSource(1))
	data = data[:cap(data)]
	r.Read(data)

	// Feed in odd-sized chunks that cross block boundaries.
	b := newShardChecksumBuilder(blockSize)
	for off := 0; off < len(data); off += 7777 {
		end := off + 7777
		if end > len(data) {
			end = len(data)
		}
		b.write(data[off:end])
	}
	covered, packed := b.finalize()
	if covered != int64(len(data)) {
		t.Fatalf("covered=%d want=%d", covered, len(data))
	}
	got := unpackUint32LE(packed)

	// Compare to a direct per-block CRC.
	want := []uint32{}
	for off := int64(0); off < int64(len(data)); off += blockSize {
		end := off + blockSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		want = append(want, uint32(needle.NewCRC(data[off:end])))
	}
	if len(got) != len(want) {
		t.Fatalf("block count got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("block %d crc mismatch got=%x want=%x", i, got[i], want[i])
		}
	}
}

func TestValidateBitrotManifest(t *testing.T) {
	mk := func(mut func(*volume_server_pb.EcBitrotProtection)) *volume_server_pb.EcBitrotProtection {
		p := &volume_server_pb.EcBitrotProtection{
			Algorithm:     volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
			BlockSize:     DefaultBitrotBlockSize,
			EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 2, ParityShards: 1},
		}
		for id := 0; id < 3; id++ {
			p.Shards = append(p.Shards, &volume_server_pb.EcShardChecksums{
				ShardId: uint32(id), CoveredSize: 5, BlockCrc32C: packUint32LE([]uint32{1}),
			})
		}
		if mut != nil {
			mut(p)
		}
		return p
	}
	if err := ValidateBitrotManifest(mk(nil), 2, 1); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
	bad := map[string]func(*volume_server_pb.EcBitrotProtection){
		"missing shard":   func(p *volume_server_pb.EcBitrotProtection) { p.Shards = p.Shards[:2] },
		"out of range id": func(p *volume_server_pb.EcBitrotProtection) { p.Shards[2].ShardId = 9 },
		"duplicate id":    func(p *volume_server_pb.EcBitrotProtection) { p.Shards[2].ShardId = 0 },
		"zero covered":    func(p *volume_server_pb.EcBitrotProtection) { p.Shards[0].CoveredSize = 0 },
		"bad crc count":   func(p *volume_server_pb.EcBitrotProtection) { p.Shards[0].BlockCrc32C = []byte{1, 2, 3} },
		"bad block size":  func(p *volume_server_pb.EcBitrotProtection) { p.BlockSize = 3 << 20 }, // not power of two
		"bad algorithm": func(p *volume_server_pb.EcBitrotProtection) {
			p.Algorithm = volume_server_pb.ChecksumAlgorithm_CHECKSUM_NONE
		},
	}
	for name, mut := range bad {
		if err := ValidateBitrotManifest(mk(mut), 2, 1); err == nil {
			t.Errorf("%s: expected validation error, got nil", name)
		}
	}
}

// writeRandomDat creates a <base>.dat of n bytes for encoding.
func writeRandomDat(t *testing.T, base string, n int) {
	t.Helper()
	data := make([]byte, n)
	rand.New(rand.NewSource(42)).Read(data)
	if err := os.WriteFile(base+".dat", data, 0644); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeProducesVerifiableSidecar(t *testing.T) {
	old := BitrotBlockSize
	BitrotBlockSize = 1 << 20
	t.Cleanup(func() { BitrotBlockSize = old })

	dir := t.TempDir()
	base := filepath.Join(dir, "7")
	writeRandomDat(t, base, 6*1024*1024) // ~6 MiB

	ctx := &ECContext{DataShards: 10, ParityShards: 4}
	prot, err := WriteEcFiles(base, ctx)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := SaveBitrotSidecar(BitrotSidecarPath(base, 0), prot); err != nil {
		t.Fatalf("save sidecar: %v", err)
	}
	if err := ValidateBitrotManifest(prot, 10, 4); err != nil {
		t.Fatalf("encode produced invalid manifest: %v", err)
	}

	// Every shard file verifies clean against the sidecar right after encode.
	for id := 0; id < ctx.Total(); id++ {
		entry := shardChecksums(prot, uint32(id))
		mism, verr := verifyShardFileBlocks(base+ctx.ToExt(id), entry, int64(prot.BlockSize))
		if verr != nil {
			t.Fatalf("verify shard %d: %v", id, verr)
		}
		if len(mism) != 0 {
			t.Fatalf("shard %d unexpectedly mismatched at encode time: %v", id, mism)
		}
	}

	// Corrupt one byte in shard 12 (a parity shard) and confirm detection.
	corruptOneByte(t, base+ctx.ToExt(12))
	entry := shardChecksums(prot, 12)
	mism, verr := verifyShardFileBlocks(base+ctx.ToExt(12), entry, int64(prot.BlockSize))
	if verr != nil {
		t.Fatalf("verify corrupt shard: %v", verr)
	}
	if len(mism) == 0 {
		t.Fatalf("expected to detect corruption in parity shard 12")
	}
}

func TestRebuildExcludesCorruptPresentShard(t *testing.T) {
	old := BitrotBlockSize
	BitrotBlockSize = 1 << 20
	t.Cleanup(func() { BitrotBlockSize = old })

	dir := t.TempDir()
	base := filepath.Join(dir, "9")
	writeRandomDat(t, base, 6*1024*1024)

	ctx := &ECContext{DataShards: 10, ParityShards: 4}
	prot, err := WriteEcFiles(base, ctx)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := SaveBitrotSidecar(BitrotSidecarPath(base, 0), prot); err != nil {
		t.Fatalf("save sidecar: %v", err)
	}

	// Snapshot a good copy of shard 3, then delete shard 0 (missing) and corrupt
	// shard 3 (present-but-corrupt). Rebuild must regenerate both correctly.
	good3, _ := os.ReadFile(base + ctx.ToExt(3))
	if err := os.Remove(base + ctx.ToExt(0)); err != nil {
		t.Fatal(err)
	}
	corruptOneByte(t, base+ctx.ToExt(3))

	generated, err := RebuildEcFiles(base, ctx, false)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !slices.Contains(generated, 0) || !slices.Contains(generated, 3) {
		t.Fatalf("expected shards 0 and 3 regenerated, got %v", generated)
	}

	// Shard 3 should now byte-match its original good content, and every shard
	// verifies clean against the sidecar.
	now3, _ := os.ReadFile(base + ctx.ToExt(3))
	if string(now3) != string(good3) {
		t.Fatalf("rebuilt shard 3 does not match original good content")
	}
	for id := 0; id < ctx.Total(); id++ {
		entry := shardChecksums(prot, uint32(id))
		mism, verr := verifyShardFileBlocks(base+ctx.ToExt(id), entry, int64(prot.BlockSize))
		if verr != nil || len(mism) != 0 {
			t.Fatalf("post-rebuild shard %d not clean: mism=%v err=%v", id, mism, verr)
		}
	}
}

func TestComputeProtectionFromShardsBackfill(t *testing.T) {
	old := BitrotBlockSize
	BitrotBlockSize = 1 << 20
	t.Cleanup(func() { BitrotBlockSize = old })

	dir := t.TempDir()
	base := filepath.Join(dir, "11")
	writeRandomDat(t, base, 6*1024*1024)
	ctx := &ECContext{DataShards: 10, ParityShards: 4}
	if _, err := WriteEcFiles(base, ctx); err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Backfill from the on-disk shards (no encode pass) yields a complete,
	// valid manifest that every shard verifies clean against.
	prot, err := ComputeProtectionFromShards(base, ctx, 0, nil)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if err := ValidateBitrotManifest(prot, 10, 4); err != nil {
		t.Fatalf("backfill manifest invalid: %v", err)
	}
	for id := 0; id < ctx.Total(); id++ {
		entry := shardChecksums(prot, uint32(id))
		mism, verr := verifyShardFileBlocks(base+ctx.ToExt(id), entry, int64(prot.BlockSize))
		if verr != nil || len(mism) != 0 {
			t.Fatalf("backfilled shard %d not clean: mism=%v err=%v", id, mism, verr)
		}
	}

	// A missing shard must abort backfill (never write a partial manifest).
	if err := os.Remove(base + ctx.ToExt(5)); err != nil {
		t.Fatal(err)
	}
	if _, err := ComputeProtectionFromShards(base, ctx, 0, nil); err == nil {
		t.Fatalf("expected backfill error with a missing shard")
	}
}

func TestRebuildFailClosedCleansUpGeneratedShard(t *testing.T) {
	old := BitrotBlockSize
	BitrotBlockSize = 1 << 20
	t.Cleanup(func() { BitrotBlockSize = old })

	dir := t.TempDir()
	base := filepath.Join(dir, "13")
	writeRandomDat(t, base, 6*1024*1024)
	ctx := &ECContext{DataShards: 10, ParityShards: 4}
	prot, err := WriteEcFiles(base, ctx)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Tamper one block CRC of shard 0 but re-save so the header self-integrity and
	// manifest stay valid: a well-formed but wrong ("stale") sidecar that loads as ON.
	for _, s := range prot.Shards {
		if s.ShardId == 0 && len(s.BlockCrc32C) >= 1 {
			s.BlockCrc32C[0] ^= 0xff
		}
	}
	if err := SaveBitrotSidecar(BitrotSidecarPath(base, 0), prot); err != nil {
		t.Fatalf("save sidecar: %v", err)
	}

	// Delete shard 0 so the rebuild regenerates it; the regenerated (correct)
	// bytes will not match the tampered sidecar entry -> post-verify fails closed.
	shard0 := base + ctx.ToExt(0)
	if err := os.Remove(shard0); err != nil {
		t.Fatal(err)
	}

	if _, rerr := RebuildEcFiles(base, ctx, false); rerr == nil {
		t.Fatalf("expected rebuild to fail closed on the stale sidecar")
	}
	// The genuinely-missing shard 0 must NOT be published: fail-closed cleanup
	// returns it to missing.
	if _, err := os.Stat(shard0); !os.IsNotExist(err) {
		t.Fatalf("shard 0 should have been cleaned up after a failed rebuild, err=%v", err)
	}

	// With the unsafe override, the rebuild proceeds and regenerates shard 0.
	if _, rerr := RebuildEcFiles(base, ctx, true); rerr != nil {
		t.Fatalf("rebuild with unsafeIgnoreSidecar should succeed: %v", rerr)
	}
	if _, err := os.Stat(shard0); err != nil {
		t.Fatalf("shard 0 should exist after unsafe rebuild: %v", err)
	}
}

func TestRemoveBitrotSidecars(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "5")
	// Legacy + two versioned sidecars, plus an unrelated file that must survive.
	for _, p := range []string{base + ".ecsum", base + ".ecsum.v1", base + ".ecsum.v7"} {
		if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	keep := base + ".ec00"
	if err := os.WriteFile(keep, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	RemoveBitrotSidecars(base)

	for _, p := range []string{base + ".ecsum", base + ".ecsum.v1", base + ".ecsum.v7"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("expected %s removed, err=%v", p, err)
		}
	}
	if _, err := os.Stat(keep); err != nil {
		t.Errorf("unrelated file %s should survive: %v", keep, err)
	}
}

func corruptOneByte(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], 0); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], 0); err != nil {
		t.Fatal(err)
	}
}
