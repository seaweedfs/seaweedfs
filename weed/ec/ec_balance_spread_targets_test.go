package ec

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
)

// lockstepMoves is what the planner emits for volumes freshly encoded on
// different nodes of a cluster with one node per rack: every volume sends its
// shard k to the same node k.
func lockstepMoves(volumes, shards int) []ecbalancer.Move {
	var moves []ecbalancer.Move
	for v := 0; v < volumes; v++ {
		for s := 0; s < shards; s++ {
			moves = append(moves, ecbalancer.Move{
				VolumeID:   uint32(100 + v),
				ShardID:    s,
				SourceNode: fmt.Sprintf("src-%d", v),
				TargetNode: fmt.Sprintf("node-%d", s),
				Phase:      "cross_rack",
			})
		}
	}
	return moves
}

type moveRecorder struct {
	mu        sync.Mutex
	receiving map[string]int
	moving    map[uint32]int
	running   int
	peak      int
	crowded   int
	ran       map[string]int
	order     map[uint32][]int
}

func newMoveRecorder() *moveRecorder {
	return &moveRecorder{
		receiving: map[string]int{},
		moving:    map[uint32]int{},
		ran:       map[string]int{},
		order:     map[uint32][]int{},
	}
}

func (r *moveRecorder) run(m ecbalancer.Move) error {
	r.mu.Lock()
	if r.receiving[m.TargetNode] > 0 {
		r.crowded++
	}
	r.receiving[m.TargetNode]++
	r.moving[m.VolumeID]++
	r.running++
	r.peak = max(r.peak, r.running)
	r.ran[fmt.Sprintf("%d.%d->%s", m.VolumeID, m.ShardID, m.TargetNode)]++
	r.order[m.VolumeID] = append(r.order[m.VolumeID], m.ShardID)
	sameVolume := r.moving[m.VolumeID]
	r.mu.Unlock()

	time.Sleep(10 * time.Millisecond)

	r.mu.Lock()
	r.receiving[m.TargetNode]--
	r.moving[m.VolumeID]--
	r.running--
	r.mu.Unlock()
	if sameVolume > 1 {
		return fmt.Errorf("volume %d had %d moves at once", m.VolumeID, sameVolume)
	}
	return nil
}

func TestRunMovesSpreadsConcurrentCopiesOverTargets(t *testing.T) {
	moves := lockstepMoves(4, 13)
	r := newMoveRecorder()

	if err := runMovesSpreadingTargets(4, moves, r.run); err != nil {
		t.Fatal(err)
	}

	if len(r.ran) != len(moves) {
		t.Fatalf("ran %d distinct moves, want %d", len(r.ran), len(moves))
	}
	for k, n := range r.ran {
		if n != 1 {
			t.Fatalf("move %s ran %d times", k, n)
		}
	}
	if r.peak != 4 {
		t.Fatalf("peak concurrency %d, want 4", r.peak)
	}
	// Plan order sends the four first copies to node-0 together, and so on:
	// 39 of 52 moves start while another copy lands on the same node. Spreading
	// leaves only the tail, when a volume's last pending targets are all busy
	// (3 to 6 in practice).
	if r.crowded > 12 {
		t.Fatalf("%d of %d moves started while another copy was landing on the same node", r.crowded, len(moves))
	}
}

func TestRunMovesKeepsPlanOrderWithoutParallelism(t *testing.T) {
	moves := lockstepMoves(3, 4)
	r := newMoveRecorder()

	if err := runMovesSpreadingTargets(1, moves, r.run); err != nil {
		t.Fatal(err)
	}

	for v := uint32(100); v < 103; v++ {
		if got := fmt.Sprint(r.order[v]); got != "[0 1 2 3]" {
			t.Fatalf("volume %d ran shards %s, want [0 1 2 3]", v, got)
		}
	}
}

func TestRunMovesKeepsOrderOfMovesOfOneShard(t *testing.T) {
	moves := []ecbalancer.Move{
		{VolumeID: 8, ShardID: 0, TargetNode: "busy"},
		{VolumeID: 7, ShardID: 3, TargetNode: "busy"},
		{VolumeID: 7, ShardID: 3, TargetNode: "idle"},
	}
	var mu sync.Mutex
	var seen []string
	run := func(m ecbalancer.Move) error {
		mu.Lock()
		seen = append(seen, fmt.Sprintf("%d.%d->%s", m.VolumeID, m.ShardID, m.TargetNode))
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		return nil
	}

	if err := runMovesSpreadingTargets(2, moves, run); err != nil {
		t.Fatal(err)
	}

	first, second := -1, -1
	for i, s := range seen {
		switch s {
		case "7.3->busy":
			first = i
		case "7.3->idle":
			second = i
		}
	}
	if first < 0 || second < 0 || first > second {
		t.Fatalf("moves of shard 7.3 ran as %v, want ->busy before ->idle", seen)
	}
}

func TestRunMovesStopsOnlyTheFailedVolume(t *testing.T) {
	moves := lockstepMoves(2, 3)
	boom := errors.New("copy failed")
	var mu sync.Mutex
	ran := map[uint32][]int{}
	run := func(m ecbalancer.Move) error {
		mu.Lock()
		ran[m.VolumeID] = append(ran[m.VolumeID], m.ShardID)
		mu.Unlock()
		if m.VolumeID == 100 && m.ShardID == 0 {
			return boom
		}
		return nil
	}

	err := runMovesSpreadingTargets(2, moves, run)

	if !errors.Is(err, boom) {
		t.Fatalf("error %v, want %v", err, boom)
	}
	if len(ran[100]) != 1 {
		t.Fatalf("failed volume 100 ran shards %v, want only the failed one", ran[100])
	}
	if len(ran[101]) != 3 {
		t.Fatalf("volume 101 ran shards %v, want all three", ran[101])
	}
}
