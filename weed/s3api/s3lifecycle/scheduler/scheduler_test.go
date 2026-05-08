package scheduler

import (
	"reflect"
	"testing"
)

func TestAssignShardsSingleWorker(t *testing.T) {
	got := AssignShards(0, 1)
	want := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestAssignShardsEvenSplit(t *testing.T) {
	a := AssignShards(0, 2)
	b := AssignShards(1, 2)
	wantA := []int{0, 1, 2, 3, 4, 5, 6, 7}
	wantB := []int{8, 9, 10, 11, 12, 13, 14, 15}
	if !reflect.DeepEqual(a, wantA) {
		t.Fatalf("worker 0: got %v, want %v", a, wantA)
	}
	if !reflect.DeepEqual(b, wantB) {
		t.Fatalf("worker 1: got %v, want %v", b, wantB)
	}
}

func TestAssignShardsUnevenSplit(t *testing.T) {
	// 16 shards / 3 workers = 5,5,5 + 1 remainder. Earlier workers absorb
	// the remainder so the slices stay contiguous: 6,5,5.
	w0 := AssignShards(0, 3)
	w1 := AssignShards(1, 3)
	w2 := AssignShards(2, 3)
	if len(w0) != 6 || w0[0] != 0 || w0[len(w0)-1] != 5 {
		t.Fatalf("w0 expected [0..5], got %v", w0)
	}
	if len(w1) != 5 || w1[0] != 6 || w1[len(w1)-1] != 10 {
		t.Fatalf("w1 expected [6..10], got %v", w1)
	}
	if len(w2) != 5 || w2[0] != 11 || w2[len(w2)-1] != 15 {
		t.Fatalf("w2 expected [11..15], got %v", w2)
	}
	// All shards covered, exactly once.
	covered := map[int]int{}
	for _, w := range [][]int{w0, w1, w2} {
		for _, s := range w {
			covered[s]++
		}
	}
	if len(covered) != 16 {
		t.Fatalf("expected 16 unique shards, got %d", len(covered))
	}
	for s, c := range covered {
		if c != 1 {
			t.Fatalf("shard %d covered %d times, expected 1", s, c)
		}
	}
}

func TestAssignShardsMoreWorkersThanShards(t *testing.T) {
	// 20 workers, 16 shards: first 16 get one shard each, last 4 get nothing.
	for i := 0; i < 16; i++ {
		got := AssignShards(i, 20)
		if len(got) != 1 || got[0] != i {
			t.Fatalf("worker %d: got %v, want [%d]", i, got, i)
		}
	}
	for i := 16; i < 20; i++ {
		if got := AssignShards(i, 20); got != nil {
			t.Fatalf("worker %d should get nil, got %v", i, got)
		}
	}
}

func TestAssignShardsBounds(t *testing.T) {
	if got := AssignShards(-1, 4); got != nil {
		t.Fatalf("idx -1: got %v, want nil", got)
	}
	if got := AssignShards(4, 4); got != nil {
		t.Fatalf("idx==total: got %v, want nil", got)
	}
	if got := AssignShards(0, 0); got != nil {
		t.Fatalf("total 0: got %v, want nil", got)
	}
}
