package erasure_coding

// EC bitrot detection — checksum sidecar.
//
// A per-volume sidecar file stores a CRC32C (Castagnoli) checksum for every
// fixed-size block of every EC shard, so a scrub (and the reconstruction path)
// can detect silent disk corruption in any shard — including cold parity shards
// that are never read during normal serving.
//
// The sidecar is OPTIONAL: an absent or generation-mismatched sidecar simply
// means "feature off" for that generation, so old binaries, JSON-only Rust
// nodes, and rollback deployments ignore it and degrade gracefully. See
// EC_BITROT_DETECTION_DESIGN.md.
//
// On-disk layout of <base>.ecsum (legacy/generation 0) and <base>.ecsum.v<N>:
//
//	[ magic(4) | format_version(2) | payload_len(4) | payload_crc32c(4) ] [ proto payload ]
//
// The header's payload_crc32c lets a loader detect corruption of the sidecar
// itself BEFORE trusting any contents, so a rotted sidecar can never be
// mistaken for shard corruption.

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/protobuf/proto"
)

const (
	// BitrotSidecarExt is the canonical extension for the checksum sidecar.
	// Generation 0 (legacy/fresh encode) uses "<base>.ecsum"; vacuum generation
	// N uses "<base>.ecsum.v<N>", mirroring the .vif/.ecx versioned convention.
	BitrotSidecarExt = ".ecsum"

	// DefaultBitrotBlockSize is the default checksum granularity (16 MiB). It is
	// a power-of-two multiple of ErasureCodingSmallBlockSize (1 MiB) and keeps
	// the sidecar tiny (~11 KB for a 30 GB volume) while localizing corruption
	// to a 16 MiB region.
	DefaultBitrotBlockSize = 16 * 1024 * 1024

	bitrotMagic         uint32 = 0x45435355 // "ECSU"
	bitrotFormatVersion uint16 = 1
	bitrotHeaderSize           = 14 // magic(4)+version(2)+payload_len(4)+payload_crc32c(4)
)

// Config knobs (wired to volume-server flags). Defaults: protection on for new
// encodes, 16 MiB block granularity.
var (
	// BitrotProtectionEnabled controls whether encode/vacuum write a sidecar.
	// When false, generations are produced unprotected (no sidecar), which is a
	// legitimate "off" state. Detection on already-protected generations is
	// unaffected.
	BitrotProtectionEnabled = true
	// BitrotBlockSize is the checksum block granularity used for new sidecars.
	// Must remain a power-of-two multiple of 1 MiB.
	BitrotBlockSize int64 = DefaultBitrotBlockSize
)

// BitrotStatus is the resolved protection state of an EC volume's active
// generation after loading and validating its sidecar.
type BitrotStatus int

const (
	// BitrotOff: no sidecar, or a sidecar that does not describe the active
	// generation. The generation is unprotected; this is NOT corruption.
	BitrotOff BitrotStatus = iota
	// BitrotOn: a complete, well-formed, generation-matching sidecar is loaded.
	BitrotOn
	// BitrotInvalid: a generation-matching sidecar that is malformed, incomplete,
	// or self-integrity-failed. The generation is unprotected pending repair of
	// the sidecar, and an integrity alarm should fire. The rebuild path treats
	// this as fail-closed (see ec_encoder rebuild verify).
	BitrotInvalid
)

func (s BitrotStatus) String() string {
	switch s {
	case BitrotOn:
		return "on"
	case BitrotInvalid:
		return "invalid"
	default:
		return "off"
	}
}

// BitrotSidecarPath returns the sidecar path for a base file name and EC
// generation. Generation 0 is the un-suffixed legacy path; generation N>0 is
// the versioned path, consistent with how .vif/.ecx are versioned by the 2PC
// switch.
func BitrotSidecarPath(baseFileName string, generation uint32) string {
	if generation == 0 {
		return baseFileName + BitrotSidecarExt
	}
	return fmt.Sprintf("%s%s.v%d", baseFileName, BitrotSidecarExt, generation)
}

// NewEncodeUUID returns a fresh random per-encode identity used to detect a
// stale sidecar left behind by an in-place re-encode.
func NewEncodeUUID() []byte {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// rand.Read essentially never fails; fall back to a zero UUID rather
		// than aborting an encode over entropy. Stale detection still has the
		// other defenses (wholesale-mismatch cap + RS arbiter).
		glog.Warningf("ec bitrot: failed to read encode uuid entropy: %v", err)
	}
	return b
}

// isPow2MultipleOf1MiB reports whether block_size is a power-of-two multiple of
// 1 MiB (i.e. a power of two that is at least 1 MiB).
func isPow2MultipleOf1MiB(blockSize uint32) bool {
	return blockSize >= (1<<20) && bits.OnesCount32(blockSize) == 1
}

// shardChecksumBuilder accumulates the per-block CRC32C of a single shard's byte
// stream as it is written. It tolerates arbitrary chunk sizes that cross block
// boundaries, so it works for both the 256 KiB encode buffers and the 1 MiB
// rebuild buffers.
type shardChecksumBuilder struct {
	blockSize int64
	cur       needle.CRC
	curLen    int64
	total     int64
	blocks    []uint32
}

func newShardChecksumBuilder(blockSize int64) *shardChecksumBuilder {
	return &shardChecksumBuilder{blockSize: blockSize}
}

func (b *shardChecksumBuilder) write(p []byte) {
	for len(p) > 0 {
		room := b.blockSize - b.curLen
		n := int64(len(p))
		if n > room {
			n = room
		}
		b.cur = b.cur.Update(p[:n])
		b.curLen += n
		b.total += n
		p = p[n:]
		if b.curLen == b.blockSize {
			b.blocks = append(b.blocks, uint32(b.cur))
			b.cur = 0
			b.curLen = 0
		}
	}
}

// finalize flushes any partial last block and returns the covered size and the
// packed little-endian uint32 CRC array.
func (b *shardChecksumBuilder) finalize() (coveredSize int64, packed []byte) {
	if b.curLen > 0 {
		b.blocks = append(b.blocks, uint32(b.cur))
		b.cur = 0
		b.curLen = 0
	}
	return b.total, packUint32LE(b.blocks)
}

// buildProtectionFromBuilders assembles a generation-0 EcBitrotProtection from
// the per-shard checksum builders produced during an encode pass. Callers that
// produce a versioned generation (vacuum) set prot.Generation afterwards. A
// fresh encode_uuid is minted on every call so an in-place re-encode is
// distinguishable from a stale sidecar.
func buildProtectionFromBuilders(ctx *ECContext, builders []*shardChecksumBuilder, blockSize int64) *volume_server_pb.EcBitrotProtection {
	shards := make([]*volume_server_pb.EcShardChecksums, 0, len(builders))
	for id, b := range builders {
		covered, packed := b.finalize()
		shards = append(shards, &volume_server_pb.EcShardChecksums{
			ShardId:     uint32(id),
			CoveredSize: covered,
			BlockCrc32C: packed,
		})
	}
	return &volume_server_pb.EcBitrotProtection{
		Algorithm:  volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
		BlockSize:  uint32(blockSize),
		Generation: 0,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   uint32(ctx.DataShards),
			ParityShards: uint32(ctx.ParityShards),
		},
		Shards:     shards,
		EncodeUuid: NewEncodeUUID(),
	}
}

func packUint32LE(vals []uint32) []byte {
	out := make([]byte, len(vals)*4)
	for i, v := range vals {
		binary.LittleEndian.PutUint32(out[i*4:], v)
	}
	return out
}

func unpackUint32LE(b []byte) []uint32 {
	out := make([]uint32, len(b)/4)
	for i := range out {
		out[i] = binary.LittleEndian.Uint32(b[i*4:])
	}
	return out
}

// expectedBlockCount returns ceil(coveredSize/blockSize).
func expectedBlockCount(coveredSize, blockSize int64) int {
	if blockSize <= 0 {
		return 0
	}
	return int((coveredSize + blockSize - 1) / blockSize)
}

// SaveBitrotSidecar atomically writes prot to path, wrapped in the on-disk
// header with a CRC32C over the serialized payload.
func SaveBitrotSidecar(path string, prot *volume_server_pb.EcBitrotProtection) error {
	payload, err := proto.Marshal(prot)
	if err != nil {
		return fmt.Errorf("marshal bitrot sidecar: %w", err)
	}
	buf := make([]byte, bitrotHeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], bitrotMagic)
	binary.BigEndian.PutUint16(buf[4:6], bitrotFormatVersion)
	binary.BigEndian.PutUint32(buf[6:10], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[10:14], uint32(needle.NewCRC(payload)))
	copy(buf[bitrotHeaderSize:], payload)

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0644); err != nil {
		return fmt.Errorf("write bitrot sidecar tmp %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename bitrot sidecar %s: %w", path, err)
	}
	return nil
}

// LoadBitrotSidecar reads and self-integrity-checks a sidecar file. It returns
// the parsed message, or an error if the file is missing, truncated, has a bad
// magic/version, or fails the payload CRC. A self-integrity failure is a
// sidecar-integrity problem (caller maps it to BitrotInvalid), never a shard
// corruption signal.
func LoadBitrotSidecar(path string) (*volume_server_pb.EcBitrotProtection, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < bitrotHeaderSize {
		return nil, fmt.Errorf("bitrot sidecar %s too short (%d bytes)", path, len(data))
	}
	if magic := binary.BigEndian.Uint32(data[0:4]); magic != bitrotMagic {
		return nil, fmt.Errorf("bitrot sidecar %s bad magic %#x", path, magic)
	}
	if ver := binary.BigEndian.Uint16(data[4:6]); ver != bitrotFormatVersion {
		return nil, fmt.Errorf("bitrot sidecar %s unsupported format version %d", path, ver)
	}
	payloadLen := binary.BigEndian.Uint32(data[6:10])
	wantCRC := binary.BigEndian.Uint32(data[10:14])
	payload := data[bitrotHeaderSize:]
	if int(payloadLen) != len(payload) {
		return nil, fmt.Errorf("bitrot sidecar %s length mismatch: header %d, actual %d", path, payloadLen, len(payload))
	}
	if got := uint32(needle.NewCRC(payload)); got != wantCRC {
		return nil, fmt.Errorf("bitrot sidecar %s self-integrity CRC mismatch: header %#x, computed %#x", path, wantCRC, got)
	}
	prot := &volume_server_pb.EcBitrotProtection{}
	if err := proto.Unmarshal(payload, prot); err != nil {
		return nil, fmt.Errorf("unmarshal bitrot sidecar %s: %w", path, err)
	}
	return prot, nil
}

// ValidateBitrotManifest performs the disk-free manifest/syntax checks that
// every loader runs: supported algorithm, valid block size, exactly one entry
// per shard id in the active layout (no duplicates, no out-of-range ids),
// positive covered_size, and a packed-CRC count consistent with covered_size.
// It does NOT compare covered_size against on-disk shard lengths — that is a
// per-node physical check done only for locally-held shards.
func ValidateBitrotManifest(prot *volume_server_pb.EcBitrotProtection, dataShards, parityShards int) error {
	if prot.Algorithm != volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C {
		return fmt.Errorf("unsupported checksum algorithm %v", prot.Algorithm)
	}
	bs := int64(prot.BlockSize)
	if !isPow2MultipleOf1MiB(prot.BlockSize) {
		return fmt.Errorf("invalid block_size %d (must be a power-of-two multiple of 1 MiB)", prot.BlockSize)
	}
	total := dataShards + parityShards
	if total <= 0 || total > MaxShardCount {
		return fmt.Errorf("invalid active layout: data=%d parity=%d", dataShards, parityShards)
	}
	if len(prot.Shards) != total {
		return fmt.Errorf("incomplete manifest: %d shard entries, expected %d", len(prot.Shards), total)
	}
	seen := make([]bool, MaxShardCount)
	for _, s := range prot.Shards {
		if s.ShardId >= uint32(total) {
			return fmt.Errorf("shard id %d out of range [0,%d)", s.ShardId, total)
		}
		if seen[s.ShardId] {
			return fmt.Errorf("duplicate shard id %d", s.ShardId)
		}
		seen[s.ShardId] = true
		if s.CoveredSize <= 0 {
			return fmt.Errorf("shard %d has non-positive covered_size %d", s.ShardId, s.CoveredSize)
		}
		wantCount := expectedBlockCount(s.CoveredSize, bs)
		if len(s.BlockCrc32C) != wantCount*4 {
			return fmt.Errorf("shard %d crc count mismatch: %d bytes, expected %d (covered_size=%d block_size=%d)",
				s.ShardId, len(s.BlockCrc32C), wantCount*4, s.CoveredSize, prot.BlockSize)
		}
	}
	return nil
}

// shardChecksums returns the EcShardChecksums entry for a shard id, or nil.
func shardChecksums(prot *volume_server_pb.EcBitrotProtection, shardId uint32) *volume_server_pb.EcShardChecksums {
	for _, s := range prot.Shards {
		if s.ShardId == shardId {
			return s
		}
	}
	return nil
}

// verifyShardFileBlocks reads a shard file at path in block_size chunks and
// compares each block's CRC32C against the manifest entry. It returns the list
// of mismatching block indices (empty == clean) and a fatal error only for I/O
// problems or a covered_size/length mismatch (which is itself corruption of the
// shard). It does not interpret the result — the caller (scrub / rebuild)
// arbitrates shard-vs-sidecar via Reed-Solomon before acting.
func verifyShardFileBlocks(path string, entry *volume_server_pb.EcShardChecksums, blockSize int64) (mismatched []int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() != entry.CoveredSize {
		// Length drift (truncation or unexpected trailing bytes) is shard
		// corruption: report every block as mismatched so the caller treats the
		// shard as bad.
		want := unpackUint32LE(entry.BlockCrc32C)
		all := make([]int, len(want))
		for i := range all {
			all[i] = i
		}
		return all, nil
	}
	want := unpackUint32LE(entry.BlockCrc32C)
	buf := make([]byte, blockSize)
	var offset int64
	for i := 0; i < len(want); i++ {
		toRead := blockSize
		if rem := entry.CoveredSize - offset; rem < toRead {
			toRead = rem
		}
		n, rerr := readFull(f, buf[:toRead], offset)
		if rerr != nil {
			return nil, rerr
		}
		if uint32(needle.NewCRC(buf[:n])) != want[i] {
			mismatched = append(mismatched, i)
		}
		offset += int64(n)
	}
	return mismatched, nil
}

func readFull(f *os.File, buf []byte, offset int64) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := f.ReadAt(buf[total:], offset+int64(total))
		total += n
		if err != nil {
			if total == len(buf) {
				return total, nil
			}
			return total, err
		}
	}
	return total, nil
}

// ComputeProtectionFromShards builds a complete EcBitrotProtection by reading
// the on-disk bytes of every shard for a generation — the trust-on-first-use
// backfill primitive for EC volumes that were encoded before this feature. It
// requires EVERY shard to be reachable (locally or in additionalDirs) and
// returns an error otherwise, so a partial sidecar is never produced. This is
// the per-holder building block a coordinator assembles across servers; on a
// server that holds (or can reach) all shards it yields a complete manifest
// directly. Caveat: it blesses whatever bytes exist now and cannot detect
// pre-baseline corruption.
func ComputeProtectionFromShards(baseFileName string, ctx *ECContext, generation uint32, additionalDirs []string) (*volume_server_pb.EcBitrotProtection, error) {
	shards := make([]*volume_server_pb.EcShardChecksums, 0, ctx.Total())
	for id := 0; id < ctx.Total(); id++ {
		path := findShardFile(baseFileName, ctx.ToExt(id), additionalDirs)
		if path == "" {
			return nil, fmt.Errorf("bitrot backfill: shard %d missing for %s; refusing to write a partial sidecar", id, baseFileName)
		}
		covered, packed, err := computeShardFileCRCs(path, BitrotBlockSize)
		if err != nil {
			return nil, fmt.Errorf("bitrot backfill: read shard %d (%s): %w", id, path, err)
		}
		shards = append(shards, &volume_server_pb.EcShardChecksums{
			ShardId:     uint32(id),
			CoveredSize: covered,
			BlockCrc32C: packed,
		})
	}
	return &volume_server_pb.EcBitrotProtection{
		Algorithm:  volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
		BlockSize:  uint32(BitrotBlockSize),
		Generation: generation,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   uint32(ctx.DataShards),
			ParityShards: uint32(ctx.ParityShards),
		},
		Shards:     shards,
		EncodeUuid: NewEncodeUUID(),
	}, nil
}

// computeShardFileCRCs reads a shard file sequentially and returns its size and
// packed per-block CRC32C array.
func computeShardFileCRCs(path string, blockSize int64) (coveredSize int64, packed []byte, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()
	b := newShardChecksumBuilder(blockSize)
	buf := make([]byte, blockSize)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			b.write(buf[:n])
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return 0, nil, rerr
		}
		if n == 0 {
			break
		}
	}
	coveredSize, packed = b.finalize()
	return coveredSize, packed, nil
}

// BitrotProtection returns the active-generation bitrot protection and its
// status. The returned message is read-only; callers must not mutate it.
func (ev *EcVolume) BitrotProtection() (*volume_server_pb.EcBitrotProtection, BitrotStatus) {
	ev.bitrotLock.RLock()
	defer ev.bitrotLock.RUnlock()
	return ev.bitrot, ev.bitrotStatus
}

// loadActiveBitrotSidecar loads the generation-0 checksum sidecar into the
// volume. Best-effort: any failure leaves protection off/invalid without
// failing the mount. OSS only produces generation-0 (fresh-encode) sidecars.
func (ev *EcVolume) loadActiveBitrotSidecar() {
	ev.loadBitrotForGeneration(0)
}

// loadBitrotForGeneration loads and validates the sidecar describing generation
// `generation`, setting ev.bitrot/ev.bitrotStatus. Called at mount. Absent or
// generation/config-mismatched => BitrotOff; self-integrity/manifest failure =>
// BitrotInvalid; usable => BitrotOn.
func (ev *EcVolume) loadBitrotForGeneration(generation uint32) {
	ev.bitrotLock.Lock()
	defer ev.bitrotLock.Unlock()
	ev.bitrot = nil
	ev.bitrotStatus = BitrotOff

	if ev.ECContext == nil {
		return
	}
	path := findBitrotSidecar(generation, ev.DataBaseFileName(), ev.IndexBaseFileName())
	if path == "" {
		return
	}
	prot, err := LoadBitrotSidecar(path)
	if err != nil {
		glog.Warningf("ec volume %d: bitrot sidecar %s self-integrity failed: %v", ev.VolumeId, path, err)
		ev.bitrotStatus = BitrotInvalid
		return
	}
	if prot.Generation != generation {
		return // not for this generation -> off, not corruption
	}
	if prot.EcShardConfig == nil ||
		int(prot.EcShardConfig.DataShards) != ev.ECContext.DataShards ||
		int(prot.EcShardConfig.ParityShards) != ev.ECContext.ParityShards {
		return
	}
	if err := ValidateBitrotManifest(prot, ev.ECContext.DataShards, ev.ECContext.ParityShards); err != nil {
		glog.Warningf("ec volume %d: bitrot sidecar %s manifest invalid: %v", ev.VolumeId, path, err)
		ev.bitrotStatus = BitrotInvalid
		return
	}
	ev.bitrot = prot
	ev.bitrotStatus = BitrotOn
	glog.V(1).Infof("ec volume %d: loaded bitrot protection generation %d (%d shards, block_size %d)",
		ev.VolumeId, generation, len(prot.Shards), prot.BlockSize)
}

// RemoveBitrotSidecars removes the legacy <base>.ecsum and any versioned
// <base>.ecsum.v<N> sidecars for a base file name. Best-effort.
func RemoveBitrotSidecars(base string) {
	os.Remove(base + BitrotSidecarExt)
	if matches, _ := filepath.Glob(base + BitrotSidecarExt + ".v*"); matches != nil {
		for _, m := range matches {
			os.Remove(m)
		}
	}
}

// findBitrotSidecar resolves the sidecar path for a generation, searching the
// data base, the index base, and any additional directories — mirroring how
// shard/.vif lookups handle split data/idx layouts and per-disk mirrors.
func findBitrotSidecar(generation uint32, dataBase, indexBase string, additionalDirs ...string) string {
	candidates := []string{
		BitrotSidecarPath(dataBase, generation),
		BitrotSidecarPath(indexBase, generation),
	}
	base := filepath.Base(dataBase)
	for _, dir := range additionalDirs {
		candidates = append(candidates, BitrotSidecarPath(filepath.Join(dir, base), generation))
	}
	for _, c := range candidates {
		if c == "" {
			continue
		}
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}
