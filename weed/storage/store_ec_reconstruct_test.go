package storage

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"

	"github.com/klauspost/reedsolomon"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// encodedInterval returns one interval's worth of every shard, Reed-Solomon encoded
// from random data, along with the context describing the ratio.
func encodedInterval(t *testing.T, intervalSize int) ([][]byte, *erasure_coding.ECContext) {
	t.Helper()

	ecCtx := erasure_coding.NewDefaultECContext("", 0)
	shardIntervals := make([][]byte, ecCtx.Total())
	r := rand.New(rand.NewSource(1))
	for i := range shardIntervals {
		shardIntervals[i] = make([]byte, intervalSize)
	}
	for i := 0; i < ecCtx.DataShards; i++ {
		r.Read(shardIntervals[i])
	}

	enc, err := reedsolomon.New(ecCtx.DataShards, ecCtx.ParityShards)
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	if err := enc.Encode(shardIntervals); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return shardIntervals, ecCtx
}

func TestReconstructEcShardIntervalRebuildsDataShard(t *testing.T) {
	shardIntervals, ecCtx := encodedInterval(t, 1024)
	ecVolume := &erasure_coding.EcVolume{VolumeId: needle.VolumeId(1), ECContext: ecCtx}

	const lost = erasure_coding.ShardId(3)
	want := bytes.Clone(shardIntervals[lost])
	shardIntervals[lost] = nil
	// drop as many others as parity allows, so the rebuild really goes through parity
	for i := ecCtx.Total() - ecCtx.ParityShards + 1; i < ecCtx.Total(); i++ {
		shardIntervals[i] = nil
	}

	if err := reconstructEcShardInterval(ecVolume, ecCtx, shardIntervals, lost); err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if !bytes.Equal(shardIntervals[lost], want) {
		t.Fatalf("rebuilt shard %d does not match the encoded bytes", lost)
	}
}

// ReconstructData rebuilds data shards only, leaving a parity slot nil. Reporting that
// as a success would hand the caller a zero-filled buffer as if it had been read.
func TestReconstructEcShardIntervalRejectsParityShard(t *testing.T) {
	shardIntervals, ecCtx := encodedInterval(t, 1024)
	ecVolume := &erasure_coding.EcVolume{VolumeId: needle.VolumeId(1), ECContext: ecCtx}

	parity := erasure_coding.ShardId(ecCtx.DataShards)
	shardIntervals[parity] = nil

	err := reconstructEcShardInterval(ecVolume, ecCtx, shardIntervals, parity)
	if err == nil {
		t.Fatalf("rebuilding parity shard %d reported success, buffer is %v", parity, shardIntervals[parity])
	}
	if !strings.Contains(err.Error(), "only data shards can be rebuilt") {
		t.Fatalf("error %q, want it to say parity cannot be rebuilt", err)
	}
}

func TestReconstructEcShardIntervalNeedsDataShardCount(t *testing.T) {
	shardIntervals, ecCtx := encodedInterval(t, 1024)
	ecVolume := &erasure_coding.EcVolume{VolumeId: needle.VolumeId(1), ECContext: ecCtx}

	// one shard short of what the ratio needs
	for i := 0; i <= ecCtx.ParityShards; i++ {
		shardIntervals[i] = nil
	}

	err := reconstructEcShardInterval(ecVolume, ecCtx, shardIntervals, 0)
	if err == nil || !strings.Contains(err.Error(), "need at least") {
		t.Fatalf("error %v, want it to report too few shards", err)
	}
}
