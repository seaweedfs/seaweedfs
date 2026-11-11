package ec

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
)

// evacuationMoves is what volumeServer.evacuate plans: every shard of each
// volume leaves the same source node.
func evacuationMoves(volumes, shards int) []ecbalancer.Move {
	var moves []ecbalancer.Move
	for v := 0; v < volumes; v++ {
		for s := 0; s < shards; s++ {
			moves = append(moves, ecbalancer.Move{
				VolumeID:   uint32(100 + v),
				ShardID:    s,
				SourceNode: "source",
				TargetNode: []string{"node-a", "node-b", "node-c"}[(v+s)%3],
			})
		}
	}
	return moves
}

func TestRunEcShardMovesStopsOnFirstError(t *testing.T) {
	moves := evacuationMoves(3, 2)
	var ran []ecbalancer.Move
	result, err := runEcShardMoves(moves, 1, true, func(m ecbalancer.Move) error {
		ran = append(ran, m)
		if len(ran) == 2 {
			return errors.New("input/output error")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected the failed move's error")
	}
	if len(ran) != 2 {
		t.Fatalf("expected no move to start after the failure, %d ran", len(ran))
	}
	if result.Moved != 1 || result.Failed != 1 || result.NotStarted != len(moves)-2 {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestRunEcShardMovesGoesOnAfterFailure(t *testing.T) {
	moves := evacuationMoves(3, 3)
	broken := moves[0]
	ran := map[uint32]int{}
	result, err := runEcShardMoves(moves, 1, false, func(m ecbalancer.Move) error {
		ran[m.VolumeID]++
		if m.VolumeID == broken.VolumeID && m.ShardID == broken.ShardID {
			return errors.New("input/output error")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected the failed move's error")
	}
	if ran[broken.VolumeID] != 3 {
		t.Fatalf("expected the other shards of volume %d to move after its failed shard, %d moves ran", broken.VolumeID, ran[broken.VolumeID])
	}
	if result.Moved != len(moves)-1 || result.Failed != 1 || result.NotStarted != 0 {
		t.Fatalf("unexpected result %+v", result)
	}
	if len(result.FailedMoves) != 1 || result.FailedMoves[0] != broken {
		t.Fatalf("expected the broken move to be reported, got %+v", result.FailedMoves)
	}
}

func TestRunEcShardMovesInParallel(t *testing.T) {
	const parallelization = 4
	moves := evacuationMoves(20, 4)
	started := make(chan struct{}, len(moves))
	release := make(chan struct{})
	var mu sync.Mutex
	running, most := 0, 0
	var result EcShardMovesResult
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		result, err = runEcShardMoves(moves, parallelization, true, func(m ecbalancer.Move) error {
			mu.Lock()
			running++
			most = max(most, running)
			mu.Unlock()
			started <- struct{}{}
			<-release
			mu.Lock()
			running--
			mu.Unlock()
			return nil
		})
	}()
	// The first moves are held until parallelization of them have started.
	for i := 0; i < parallelization; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d moves started at once, expected %d", i, parallelization)
		}
	}
	close(release)
	<-done
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Moved != len(moves) || result.Failed != 0 || result.NotStarted != 0 {
		t.Fatalf("unexpected result %+v", result)
	}
	if most != parallelization {
		t.Fatalf("%d moves ran at once, expected %d", most, parallelization)
	}
}

func TestRunEcShardMovesStopsWhenLockIsLost(t *testing.T) {
	moves := evacuationMoves(3, 2)
	ran := 0
	result, err := runEcShardMoves(moves, 1, false, func(m ecbalancer.Move) error {
		ran++
		if ran == 2 {
			return ErrLockLost
		}
		return nil
	})
	if !errors.Is(err, ErrLockLost) {
		t.Fatalf("expected the lost lock to be reported, got %v", err)
	}
	if ran != 2 || result.Moved != 1 || result.Failed != 1 || result.NotStarted != len(moves)-2 {
		t.Fatalf("expected no move after the lock was lost even with skipping on: ran %d, %+v", ran, result)
	}
}

// A move that failed after its target registered the shard counts as failed but
// is not reported for planning again: the target's slot is taken.
func TestRunEcShardMovesKeepsShardsOnTheTargetOutOfFailedMoves(t *testing.T) {
	moves := evacuationMoves(2, 1)
	result, err := runEcShardMoves(moves, 1, false, func(m ecbalancer.Move) error {
		if m.VolumeID == moves[0].VolumeID {
			return fmt.Errorf("move ec shard: %w: delete: input/output error", errShardOnTarget)
		}
		return errors.New("copy: input/output error")
	})
	if err == nil {
		t.Fatal("expected the failed moves' errors")
	}
	if result.Failed != 2 || len(result.FailedMoves) != 1 || result.FailedMoves[0] != moves[1] {
		t.Fatalf("expected only the move without a target copy in FailedMoves, got %+v", result)
	}
	if len(result.LandedMoves) != 1 || result.LandedMoves[0] != moves[0] {
		t.Fatalf("expected the move with a target copy in LandedMoves, got %+v", result)
	}
}
