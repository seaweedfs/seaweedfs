package dailyrun

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

// cursorLoad is the outcome of one shard's Persister.Load.
type cursorLoad struct {
	cursor Cursor
	found  bool
	err    error
}

func loadShardCursors(ctx context.Context, cfg Config) map[int]cursorLoad {
	out := make(map[int]cursorLoad, len(cfg.Shards))
	for _, sh := range cfg.Shards {
		c, found, err := cfg.Persister.Load(ctx, sh)
		out[sh] = cursorLoad{cursor: c, found: found, err: err}
	}
	return out
}

// pendingWalk is one shared bucket walk covering a group of shards.
// runShard goroutines wait on done instead of running their own listing.
type pendingWalk struct {
	done     chan struct{}
	err      error
	recovery bool
}

// planWalks groups the shards runShard will send through the recovery
// and steady-state walk branches — same cursor data, same hash and
// walkerDue checks — and starts one shared walk per group, so a pass
// lists each bucket at most twice instead of once per shard.
//
// The plan is only a prediction: runShard re-loads its own cursor and
// keeps the authoritative decision. A shard whose fresh read disagrees
// (a cursor rewritten by a concurrent pass between the two loads)
// falls back to a per-shard walk in walkForShard.
func planWalks(ctx context.Context, cfg Config, snap *engine.Snapshot, runNow time.Time, cursors map[int]cursorLoad, rsh, promoted [32]byte, retentionWindow time.Duration) map[int]*pendingWalk {
	if cfg.Walker == nil || len(cfg.Shards) == 0 {
		return nil
	}
	// RulesForShard ignores shardID today, so one view serves every
	// shard. Revisit if it ever partitions rules per shard.
	_, walkView := snap.RulesForShard(cfg.Shards[0], retentionWindow)
	walkViewEmpty := walkView == nil || len(walkView.AllActions()) == 0

	var recovery, steady []int
	for _, sh := range cfg.Shards {
		cl := cursors[sh]
		if cl.err != nil {
			continue
		}
		if rsh != [32]byte{} && (!cl.found || cl.cursor.RuleSetHash != rsh || cl.cursor.PromotedHash != promoted) {
			recovery = append(recovery, sh)
			continue
		}
		var lastWalkedNs int64
		if cl.found {
			lastWalkedNs = cl.cursor.LastWalkedNs
		}
		if !walkViewEmpty && walkerDue(lastWalkedNs, runNow, cfg.WalkerInterval) {
			steady = append(steady, sh)
		}
	}

	walks := make(map[int]*pendingWalk, len(recovery)+len(steady))
	start := func(shards []int, recovery bool, view *engine.Snapshot) {
		pw := &pendingWalk{done: make(chan struct{}), recovery: recovery}
		for _, sh := range shards {
			walks[sh] = pw
		}
		go func() {
			defer close(pw.done)
			pw.err = cfg.Walker(ctx, view, shards)
		}()
	}
	if len(recovery) > 0 {
		start(recovery, true, engine.RecoveryView(snap))
	}
	if len(steady) > 0 {
		start(steady, false, walkView)
	}
	return walks
}

// walkForShard waits on the shard's planned shared walk when one covers
// this branch (same recovery/steady kind), or runs a per-shard walk when
// the plan missed — a cursor drift between the plan load and runShard's
// own load — or no plan exists (direct runShard calls, tests).
func walkForShard(ctx context.Context, walker WalkerFunc, walks map[int]*pendingWalk, shardID int, recovery bool, view *engine.Snapshot) error {
	if pw := walks[shardID]; pw != nil && pw.recovery == recovery {
		select {
		case <-pw.done:
			return pw.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return walker(ctx, view, []int{shardID})
}
