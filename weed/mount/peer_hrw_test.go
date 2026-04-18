package mount

import (
	"fmt"
	"testing"
)

func mkSeeds(addrs ...string) []SeedPeer {
	out := make([]SeedPeer, len(addrs))
	for i, a := range addrs {
		out[i] = SeedPeer{PeerAddr: a}
	}
	return out
}

func TestOwnerFor_EmptySeeds(t *testing.T) {
	if got := OwnerFor("3,01637037d6", nil); got != "" {
		t.Errorf("expected empty owner for empty seed list, got %q", got)
	}
}

func TestOwnerFor_Deterministic(t *testing.T) {
	seeds := mkSeeds("a:1", "b:1", "c:1", "d:1")
	fid := "3,01637037d6"
	first := OwnerFor(fid, seeds)
	for i := 0; i < 100; i++ {
		if got := OwnerFor(fid, seeds); got != first {
			t.Fatalf("non-deterministic: iter %d got %q first %q", i, got, first)
		}
	}
}

func TestOwnerFor_DistributesEvenly(t *testing.T) {
	const N = 4
	seeds := mkSeeds("a:1", "b:1", "c:1", "d:1")
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		fid := fmt.Sprintf("%d,%x", i, i*17)
		counts[OwnerFor(fid, seeds)]++
	}
	// Each seed should get roughly 25% (±10% slack for a 10k sample).
	for addr, c := range counts {
		ratio := float64(c) / 10000.0
		if ratio < 0.225 || ratio > 0.275 {
			t.Errorf("HRW distribution skewed: %s got %.3f", addr, ratio)
		}
	}
	if len(counts) != N {
		t.Errorf("expected %d distinct owners, got %d", N, len(counts))
	}
}

func TestOwnerFor_MinimalShuffleOnSeedChange(t *testing.T) {
	// Adding one seed should move ~1/(N+1) fids to the new seed and leave
	// the rest on their prior owners. Tolerance generous for a 10k sample.
	const trials = 10000
	before := mkSeeds("a:1", "b:1", "c:1")
	after := mkSeeds("a:1", "b:1", "c:1", "d:1")

	moved := 0
	toNewSeed := 0
	for i := 0; i < trials; i++ {
		fid := fmt.Sprintf("%d,%x", i, i*31)
		pre := OwnerFor(fid, before)
		post := OwnerFor(fid, after)
		if pre != post {
			moved++
			if post == "d:1" {
				toNewSeed++
			}
		}
	}
	// Expected: ~1/4 = 25% of fids move, all to the new seed.
	ratio := float64(moved) / float64(trials)
	if ratio < 0.20 || ratio > 0.30 {
		t.Errorf("expected ~25%% fids to move on seed-add, got %.3f", ratio)
	}
	if moved != toNewSeed {
		t.Errorf("expected every moved fid to land on the new seed; moved=%d toNewSeed=%d", moved, toNewSeed)
	}
}

func TestOwnerFor_TieBreakerDeterministic(t *testing.T) {
	// Two seeds with equal hash score (engineered collision is unlikely in
	// practice, but we guarantee determinism by lex-comparing addresses).
	// This test just confirms OwnerFor runs without panicking on duplicates.
	seeds := mkSeeds("a:1", "a:1", "b:1")
	fid := "3,01637037d6"
	_ = OwnerFor(fid, seeds)
}
