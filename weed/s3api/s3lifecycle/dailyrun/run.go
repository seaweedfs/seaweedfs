package dailyrun

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/time/rate"
)

// LifecycleClient is the same RPC contract the streaming dispatcher
// uses. Replicated here to avoid an import cycle with dispatcher; both
// shapes target s3_lifecycle_pb.SeaweedS3LifecycleInternalClient and
// neither owns the protobuf interface.
type LifecycleClient interface {
	LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error)
}

// Config bundles everything daily_run needs to make one shard-fan-out
// pass over the meta-log. Fields with zero values use documented
// defaults; required fields are checked at the top of Run.
type Config struct {
	Shards      []int
	BucketsPath string
	Engine      *engine.Engine
	FilerClient filer_pb.SeaweedFilerClient
	Client      LifecycleClient
	Persister   CursorPersister
	Lister      router.SiblingLister

	// Limiter is the optional per-worker rate.Limiter shared across all
	// shard goroutines on this worker. nil = no rate limit (the legacy
	// "drain as fast as the filer can" behavior). Phase 3 wires the
	// per-worker share from ClusterContext.Metadata into this field.
	Limiter *rate.Limiter

	// ClientName / ClientID identify this worker on the meta-log
	// subscription. ClientID 0 randomizes per-run.
	ClientName string
	ClientID   int32

	// Now overrides time.Now for tests. nil = production wall clock.
	Now func() time.Time

	// EventBudget caps how many meta-log events each shard's reader
	// consumes before returning. Zero = unbounded (the run-until-now
	// behavior — see "stop condition" below). Tests set a small value
	// to bound deterministic runs.
	EventBudget int
}

// Run executes the daily replay for every shard in cfg.Shards
// concurrently. Returns the first non-nil error from any shard; other
// shards still run to completion so a transient filer error on one
// shard doesn't lose progress on the others.
//
// Phase 2 scope: replay only. A bucket whose compiled rules require
// walker-bound dispatch (ExpirationDate, ExpiredDeleteMarker,
// NewerNoncurrent) or any rule promoted to scan_only fails the whole
// run with an UnsupportedRuleError — the handler reports this so admin
// can either revert algorithm=streaming or wait for Phase 4.
func Run(ctx context.Context, cfg Config) error {
	if err := validate(cfg); err != nil {
		return err
	}
	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}

	// Refuse runs whose snapshot includes a walker-bound action kind.
	// Done once, against the engine's current snapshot, before any
	// shard work begins. Every shard sees the same engine state so a
	// single check is sufficient.
	snap := cfg.Engine.Snapshot()
	if unsupported := checkSnapshotForUnsupported(snap); unsupported != nil {
		return unsupported
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(cfg.Shards))
	for _, sh := range cfg.Shards {
		sh := sh
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runShard(ctx, cfg, now, sh); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	// Return the first error; the rest are logged at glog level so
	// they're recoverable in the operational stream.
	var first error
	for err := range errCh {
		if first == nil {
			first = err
		} else {
			glog.V(1).Infof("daily_run: additional shard error: %v", err)
		}
	}
	return first
}

func validate(cfg Config) error {
	if cfg.Engine == nil {
		return errors.New("daily_run: nil Engine")
	}
	if cfg.FilerClient == nil {
		return errors.New("daily_run: nil FilerClient")
	}
	if cfg.Client == nil {
		return errors.New("daily_run: nil Client (LifecycleDelete RPC)")
	}
	if cfg.Persister == nil {
		return errors.New("daily_run: nil Persister")
	}
	if cfg.Lister == nil {
		return errors.New("daily_run: nil Lister")
	}
	if cfg.BucketsPath == "" {
		return errors.New("daily_run: empty BucketsPath")
	}
	for _, sh := range cfg.Shards {
		if sh < 0 || sh >= s3lifecycle.ShardCount {
			return fmt.Errorf("daily_run: shard %d out of [0, %d)", sh, s3lifecycle.ShardCount)
		}
	}
	return nil
}

// runShard executes the daily replay loop for a single shard. The
// algorithm is documented in DESIGN.md ("Algorithm" section).
//
// Phase 2 omissions vs. the full design:
//   - No walker invocation on rule-change / cold-start / retention
//     loss. The cursor is rewritten and the worker exits; Phase 4
//     wires the walker over engine.RecoveryView(snap) on these
//     branches.
//   - PromotedHash is always the empty hash. The partition-flip
//     trigger is dormant until Phase 4.
//   - All walker-bound action kinds and scan_only-promoted rules are
//     refused at validate-time (see checkSnapshotForUnsupported), so
//     replay only ever sees ExpirationDays / NoncurrentDays / AbortMPU.
func runShard(ctx context.Context, cfg Config, now func() time.Time, shardID int) error {
	persisted, found, err := cfg.Persister.Load(ctx, shardID)
	if err != nil {
		return fmt.Errorf("shard=%d: load cursor: %w", shardID, err)
	}

	snap := cfg.Engine.Snapshot()
	rsh := localReplayContentHash(snap)
	maxTTL := localMaxEffectiveTTL(snap)

	// Empty-replay sentinel: bucket has no replay-eligible rules.
	// Persist the hash so a future rule addition is detected as a
	// content change. Phase 2 only ever reaches this branch when the
	// engine has zero compiled actions for any bucket on this shard;
	// the unsupported-rule check above already rejected walker rules.
	if maxTTL == 0 {
		return cfg.Persister.Save(ctx, shardID, Cursor{
			TsNs:         0,
			RuleSetHash:  rsh,
			PromotedHash: [32]byte{}, // Phase 2: always empty
		})
	}

	// Rule-change branch: hash mismatch (and a persisted cursor exists)
	// rewinds to now - max_ttl. Phase 4 adds the walker call here.
	if found && persisted.RuleSetHash != rsh {
		next := Cursor{
			TsNs:        now().Add(-maxTTL).UnixNano(),
			RuleSetHash: rsh,
			// PromotedHash stays empty in Phase 2.
		}
		return cfg.Persister.Save(ctx, shardID, next)
	}

	// Cold start with no prior cursor: start scanning from now - max_ttl
	// so a freshly-installed worker still expires already-due objects
	// whose PUT events sit within meta-log retention. Phase 4 would
	// invoke the walker here for the longer-than-retention case; Phase 2
	// trusts that retention >= max_ttl in the deployments this code is
	// enabled on.
	startTsNs := persisted.TsNs
	floor := now().Add(-maxTTL).UnixNano()
	if startTsNs < floor {
		startTsNs = floor
	}

	lastOK, halted, err := drainShardEvents(ctx, cfg, now, shardID, snap, startTsNs)
	if err != nil {
		return fmt.Errorf("shard=%d: drain: %w", shardID, err)
	}

	// On halt the cursor stays at the last fully-successful event so
	// tomorrow's run resumes from the same point. Steady-state advances
	// to the latest scanned event.
	advance := lastOK
	if !halted && lastOK < startTsNs {
		// No events seen at all — keep the persisted cursor.
		advance = persisted.TsNs
	}
	return cfg.Persister.Save(ctx, shardID, Cursor{
		TsNs:        advance,
		RuleSetHash: rsh,
		// PromotedHash stays empty in Phase 2.
	})
}

// drainShardEvents subscribes to the meta-log starting at startTsNs,
// routes every event through router.Route, and dispatches each Match
// whose due_time is past now. Returns (last_ok_TsNs, halted, error):
//   - last_ok_TsNs: TsNs of the most recent event whose matches all
//     dispatched successfully (or NOOP_RESOLVED / SKIPPED_OBJECT_LOCK).
//     The cursor advances to this value on a normal return.
//   - halted: true when the loop stopped because of an unresolved
//     dispatch outcome (RETRY_LATER, BLOCKED, or transport error after
//     in-run retries). The cursor stays put so tomorrow re-attempts.
//   - error: stream/setup failure; cursor stays put.
func drainShardEvents(ctx context.Context, cfg Config, now func() time.Time, shardID int, snap *engine.Snapshot, startTsNs int64) (int64, bool, error) {
	clientName := cfg.ClientName
	if clientName == "" {
		clientName = "worker-s3-lifecycle-daily"
	}
	clientID := cfg.ClientID
	if clientID == 0 {
		clientID = int32(util.RandomInt32())
	}
	runUpTo := now().UnixNano()
	if startTsNs >= runUpTo {
		// Nothing to do — cursor already at or past the run boundary.
		return startTsNs, false, nil
	}

	events := make(chan *reader.Event, 64)
	rd := &reader.Reader{
		ShardID:     shardID,
		BucketsPath: cfg.BucketsPath,
		StartTsNs:   startTsNs,
		Events:      events,
		EventBudget: cfg.EventBudget,
	}

	readerCtx, cancelReader := context.WithCancel(ctx)
	defer cancelReader()

	readerDone := make(chan error, 1)
	go func() {
		readerDone <- rd.Run(readerCtx, cfg.FilerClient, clientName, clientID)
	}()

	lastOK := startTsNs
	halted := false

drain:
	for {
		select {
		case <-ctx.Done():
			cancelReader()
			break drain
		case ev, ok := <-events:
			if !ok {
				break drain
			}
			if ev == nil {
				continue
			}
			if ev.TsNs > runUpTo {
				// Reached the run boundary: events past now belong to
				// tomorrow's pass. Cancel the reader so it doesn't keep
				// pulling live events, then exit.
				cancelReader()
				break drain
			}
			matches := router.Route(ctx, snap, ev, now(), cfg.Lister)
			eventHalted, eventErr := processMatches(ctx, cfg, now, ev, matches)
			if eventErr != nil {
				// A non-dispatch error (e.g. limiter wait cancelled by
				// shutdown). Propagate so caller treats it as a halt.
				return lastOK, true, eventErr
			}
			if eventHalted {
				halted = true
				break drain
			}
			lastOK = ev.TsNs
		}
	}

	// Wait for reader to drain so its goroutine doesn't outlive us.
	cancelReader()
	if rerr := <-readerDone; rerr != nil && !errors.Is(rerr, context.Canceled) {
		glog.V(2).Infof("daily_run shard=%d: reader returned: %v", shardID, rerr)
	}
	return lastOK, halted, nil
}

// processMatches dispatches every match emitted from one event. Returns
// (halted, error). halted=true on any unresolved outcome
// (RETRY_LATER / BLOCKED / transport error after in-run retries). The
// caller exits the drain loop without advancing the cursor past this
// event.
//
// A match whose DueTime is in the future is simply skipped — it does
// NOT terminate the loop or get cached as "done for this rule." A
// single event can produce multiple matches for the same ActionKey
// against different objects (e.g., routePointerTransitionExpand emits
// one match per noncurrent sibling, each with its own SuccessorModTime
// derived from a different demoting event), so a not-yet-due sibling
// must never gate a sibling that's already past its DueTime. Without
// per-object state the only safe behavior is per-match independence.
//
// Phase 2's perf cost from skipping the per-rule early-stop is small:
// each daily run is bounded by the meta-log window and we already
// invoke the rate limiter per match. Future work can revisit a
// per-(rule, object) memoization if profiling shows it's worth the
// state — the current shape errs on the side of correctness.
func processMatches(ctx context.Context, cfg Config, now func() time.Time, ev *reader.Event, matches []router.Match) (bool, error) {
	for _, m := range matches {
		if m.DueTime.After(now()) {
			// Not yet due — skip this match, keep iterating. Other
			// matches in the same event (different objects under the
			// same rule) may still be due.
			continue
		}
		if cfg.Limiter != nil {
			if err := cfg.Limiter.Wait(ctx); err != nil {
				return true, err
			}
		}
		outcome, err := dispatchWithRetry(ctx, cfg.Client, m)
		if err != nil {
			// Exhausted in-run transport retries: halt; tomorrow retries
			// from the same cursor.
			glog.V(1).Infof("daily_run: transport error on %s/%s %s: %v",
				m.Bucket, m.ObjectKey, m.Key.ActionKind, err)
			return true, nil
		}
		switch outcome {
		case s3_lifecycle_pb.LifecycleDeleteOutcome_DONE,
			s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED,
			s3_lifecycle_pb.LifecycleDeleteOutcome_SKIPPED_OBJECT_LOCK:
			// Cursor advances. The dispatch's own metric (in the RPC
			// server) records the outcome; the daily-run side stays
			// metric-quiet to avoid double-counting.
		case s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER,
			s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED:
			glog.V(1).Infof("daily_run: %s on %s/%s %s",
				outcome, m.Bucket, m.ObjectKey, m.Key.ActionKind)
			return true, nil
		default:
			glog.V(1).Infof("daily_run: unknown outcome %v on %s/%s", outcome, m.Bucket, m.ObjectKey)
			return true, nil
		}
		_ = ev // ev kept available for future per-event logging
	}
	return false, nil
}
