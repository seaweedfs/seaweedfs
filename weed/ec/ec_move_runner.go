package ec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/operation/volume_move"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// ErrLockLost stops every move that has not started, whatever stopOnError says,
// and fails the whole run.
var ErrLockLost = errors.New("lock is lost")

// errShardOnTarget marks a failed move whose target registered the shard: only
// the source cleanup failed, so the shard takes a slot on the target.
var errShardOnTarget = errors.New("the target has the shard")

// EcShardMovesResult counts what RunEcShardMoves did with the planned moves.
type EcShardMovesResult struct {
	Moved      int
	Failed     int
	NotStarted int
	// FailedMoves are the failed moves whose target did not register the shard;
	// their shards are still on the source.
	FailedMoves []ecbalancer.Move
	// LandedMoves are the failed moves whose target has the shard.
	LandedMoves []ecbalancer.Move
}

// RunEcShardMoves applies planned shard moves, up to maxParallelization at once,
// scheduled like ec.balance moves (runMovesSpreadingTargets): never two moves of
// one volume at a time, and the free workers go to the least busy targets. Each
// move copies the shard, verifies the target registered it and only then deletes
// it on the source (volume_move.Mover.MoveEcShards). The in-memory model is not
// touched, so the caller updates it while planning.
//
// With stopOnError no new move starts after the first failure (moves already
// running finish). Otherwise every other move still runs, including the other
// shards of the failed move's volume: unlike ec.balance moves, evacuation moves
// all leave one server for other ones, so no move waits for a slot another frees.
// Losing the admin lock stops the moves that have not started in either case.
// The errors of the failed moves are joined.
func RunEcShardMoves(env *Env, nodes []*EcNode, moves []ecbalancer.Move, maxParallelization int, ioBytePerSecond int64, stopOnError bool, writer io.Writer) (EcShardMovesResult, error) {
	byID := make(map[string]*EcNode, len(nodes))
	for _, en := range nodes {
		byID[en.Info.Id] = en
	}
	out := &lockedWriter{w: writer}
	mover := volume_move.NewMover(env.GrpcDialOption)
	return runEcShardMoves(moves, maxParallelization, stopOnError, func(m ecbalancer.Move) error {
		if !env.isLocked() {
			return ErrLockLost
		}
		src, dst := byID[m.SourceNode], byID[m.TargetNode]
		if src == nil || dst == nil {
			return fmt.Errorf("move ec shard %d.%d %s => %s: unknown volume server", m.VolumeID, m.ShardID, m.SourceNode, m.TargetNode)
		}
		move := volume_move.EcShardMove{
			VolumeId:   needle.VolumeId(m.VolumeID),
			Collection: m.Collection,
			ShardIds:   []erasure_coding.ShardId{erasure_coding.ShardId(m.ShardID)},
			Source:     pb.NewServerAddressFromDataNode(src.Info),
			Target:     pb.NewServerAddressFromDataNode(dst.Info),
			TargetDisk: m.TargetDisk,
		}
		err := mover.MoveEcShards(context.Background(), move, volume_move.EcMoveOptions{IoBytePerSecond: ioBytePerSecond, Writer: out})
		if err != nil {
			if mover.VerifyEcShards(context.Background(), move.VolumeId, move.Target, move.ShardIds) == nil {
				err = fmt.Errorf("%w: %w", errShardOnTarget, err)
			}
			return fmt.Errorf("move ec shard %d.%d %s => %s: %w", m.VolumeID, m.ShardID, m.SourceNode, m.TargetNode, err)
		}
		fmt.Fprintf(out, "moved ec shard %d.%d %s => %s\n", m.VolumeID, m.ShardID, m.SourceNode, m.TargetNode)
		return nil
	})
}

// runEcShardMoves is RunEcShardMoves with the move itself passed in.
func runEcShardMoves(moves []ecbalancer.Move, maxParallelization int, stopOnError bool, move func(ecbalancer.Move) error) (EcShardMovesResult, error) {
	var stopped atomic.Bool
	var moved, failed atomic.Int64
	var mu sync.Mutex
	var errs []error
	var failedMoves, landedMoves []ecbalancer.Move
	err := runMovesSpreadingTargets(maxParallelization, moves, func(m ecbalancer.Move) error {
		if stopped.Load() {
			return nil
		}
		if err := move(m); err != nil {
			failed.Add(1)
			mu.Lock()
			if errors.Is(err, errShardOnTarget) {
				landedMoves = append(landedMoves, m)
			} else {
				failedMoves = append(failedMoves, m)
			}
			mu.Unlock()
			if stopOnError || errors.Is(err, ErrLockLost) {
				stopped.Store(true)
				return err
			}
			// Not returned, so the runner keeps the rest of this volume's moves.
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
			return nil
		}
		moved.Add(1)
		return nil
	})
	result := EcShardMovesResult{
		Moved:       int(moved.Load()),
		Failed:      int(failed.Load()),
		FailedMoves: failedMoves,
		LandedMoves: landedMoves,
	}
	// Moves skipped after a stop, and moves dropped together with a failed move
	// of their volume, did not run.
	result.NotStarted = len(moves) - result.Moved - result.Failed
	return result, errors.Join(append(errs, err)...)
}

// lockedWriter serializes the progress lines of concurrent moves.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (lw *lockedWriter) Write(p []byte) (int, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return lw.w.Write(p)
}
