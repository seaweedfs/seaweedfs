package erasure_coding

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// interopSample is the fully-deterministic sidecar shared with the Rust port's
// ec_bitrot.rs `sample()`. Both binaries must serialize it to identical bytes.
func interopSample() *volume_server_pb.EcBitrotProtection {
	return &volume_server_pb.EcBitrotProtection{
		Algorithm: volume_server_pb.ChecksumAlgorithm_CHECKSUM_CRC32C,
		BlockSize: uint32(DefaultBitrotBlockSize),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   10,
			ParityShards: 4,
		},
		Shards: []*volume_server_pb.EcShardChecksums{
			{ShardId: 0, CoveredSize: 1024 * 1024, BlockCrc32C: packUint32LE([]uint32{0x01020304})},
			{ShardId: 1, CoveredSize: 1024 * 1024, BlockCrc32C: packUint32LE([]uint32{0x05060708})},
		},
		EncodeUuid: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	}
}

// canonicalInteropHex is the exact on-disk bytes of interopSample(). The Rust
// port's ec_bitrot.rs test_byte_exact_go_interop pins the SAME constant, so a
// format change in EITHER binary (new always-serialized field, renumber,
// EcShardConfig default flip, protobuf canonicalization change) fails that
// binary's test instead of silently desyncing a Go-written sidecar from a
// Rust-written one. If you intentionally change the format, regenerate this
// (run with -v to print the hex) and update the Rust constant in lock-step.
const canonicalInteropHex = "45435355000100000039cc1b826a080110808080082204080a10042a0a108080401a04040302012a0c0801108080401a04080706053210000102030405060708090a0b0c0d0e0f"

// TestBitrotSidecarBytes_RustInterop pins the on-disk bytes so a Go-side format
// drift fails here, and the Rust port asserts the same constant (cross-binary
// sidecar interop).
func TestBitrotSidecarBytes_RustInterop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "v1.ecsum")
	if err := SaveBitrotSidecar(path, interopSample()); err != nil {
		t.Fatalf("save: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := hex.EncodeToString(data); got != canonicalInteropHex {
		t.Fatalf("bitrot sidecar bytes drifted from the cross-binary canonical form;\n got=%s\nwant=%s\n(regenerate and update the Rust constant in lock-step)", got, canonicalInteropHex)
	}

	// Round-trips through the loader.
	if _, err := LoadBitrotSidecar(path); err != nil {
		t.Fatalf("load: %v", err)
	}
}
