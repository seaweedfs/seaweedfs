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

	// Workers caps how many shards run in parallel. Each shard owns its
	// own meta-log subscription and rate limiter is shared across all
	// of them, so the cap is about filer-side concurrency rather than
	// throughput. Zero or negative → 1 (serial, the legacy default).
	Workers int

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
	// Freeze "now" at the start of the run. Every shard, every match's
	// DueTime comparison, and every cursor-floor calculation uses the
	// same instant so a long-running shard doesn't see the boundary
	// drift relative to a short-running peer.
	runNow := now()

	// Refuse runs whose snapshot includes a walker-bound action kind
	// or any rule the router won't route. Done once against an
	// immutable snapshot — every shard reuses this exact value so a
	// mid-execution Compile can't make shards disagree about the rule
	// set or the hash.
	snap := cfg.Engine.Snapshot()
	if unsupported := checkSnapshotForUnsupported(snap); unsupported != nil {
		return unsupported
	}

	// Concurrency cap. cfg.Workers controls how many shards run in
	// parallel; the rate limiter (Phase 3) governs throughput. With
	// Workers=1 (the legacy default) the 16 shards process serially.
	workers := cfg.Workers
	if workers <= 0 {
		workers = 1
	}
	if workers > len(cfg.Shards) {
		workers = len(cfg.Shards)
	}
	sem := make(chan struct{}, workers)

	var wg sync.WaitGroup
	errCh := make(chan error, len(cfg.Shards))
	for _, sh := range cfg.Shards {
		sh := sh
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := runShard(ctx, cfg, snap, runNow, sh); err != nil {
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
//
// snap is the engine snapshot captured at the top of Run; reusing the
// same value across shards guarantees they observe identical rules and
// hashes. runNow is the frozen instant for due-time comparisons.
func runShard(ctx context.Context, cfg Config, snap *engine.Snapshot, runNow time.Time, shardID int) error {
	persisted, found, err := cfg.Persister.Load(ctx, shardID)
	if err != nil {
		return fmt.Errorf("shard=%d: load cursor: %w", shardID, err)
	}

	rsh := localReplayContentHash(snap)
	maxTTL := localMaxEffectiveTTL(snap)

	// Empty-replay sentinel: no replay-eligible active rules in the
	// snapshot. We persist the hash so a future rule addition is
	// detected as a content change. Use the hash rather than maxTTL
	// here as the explicit "no replay state" signal — both fire on
	// the same set in practice (action_kind.go only emits actions
	// when their Days field is > 0), but the hash captures intent.
	if rsh == [32]byte{} {
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
			TsNs:        runNow.Add(-maxTTL).UnixNano(),
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
	floor := runNow.Add(-maxTTL).UnixNano()
	if startTsNs < floor {
		startTsNs = floor
	}

	lastOK, _, drainErr := drainShardEvents(ctx, cfg, runNow, shardID, snap, startTsNs)
	if drainErr != nil {
		// Drain failed (transport, ctx cancel, or limiter shutdown):
		// persist whatever progress we have so subsequent runs don't
		// re-process the events we already dispatched, then propagate
		// the error. Cursor save uses ctx — if ctx is already cancelled
		// the save will fail too; that's fine, we still surface the
		// underlying drain error.
		_ = cfg.Persister.Save(ctx, shardID, Cursor{TsNs: lastOK, RuleSetHash: rsh})
		return fmt.Errorf("shard=%d: drain: %w", shardID, drainErr)
	}

	// drainShardEvents stops advancing lastOK at the first event with a
	// skipped (not-yet-due) match. Persisting that value means the next
	// run resumes from before the skipped event and re-scans it —
	// without that, future-DueTime matches would be lost. halted=true
	// adds nothing here because lastOK already reflects the
	// stuck-at-failure boundary.
	return cfg.Persister.Save(ctx, shardID, Cursor{
		TsNs:        lastOK,
		RuleSetHash: rsh,
		// PromotedHash stays empty in Phase 2.
	})
}

// drainShardEvents subscribes to the meta-log starting at startTsNs,
// routes every event through router.Route, and dispatches each Match
// whose due_time is past runNow. Returns (cursorAdvanceTo, halted, error):
//   - cursorAdvanceTo: the highest TsNs the persisted cursor may safely
//     advance to. Equals the TsNs of the last event whose matches were
//     ALL dispatched (DONE/NOOP_RESOLVED/SKIPPED_OBJECT_LOCK) AND every
//     prior event was likewise fully processed. Once an event with a
//     not-yet-due match is encountered, cursorAdvanceTo stops growing —
//     so subsequent runs re-scan that event and any after it.
//   - halted: true when an unresolved dispatch outcome (RETRY_LATER /
//     BLOCKED / transport error after in-run retries) stopped the loop.
//   - error: stream/setup failure or context cancellation; caller
//     persists cursorAdvanceTo and propagates the error so the run is
//     marked as interrupted rather than successful.
func drainShardEvents(ctx context.Context, cfg Config, runNow time.Time, shardID int, snap *engine.Snapshot, startTsNs int64) (int64, bool, error) {
	clientName := cfg.ClientName
	if clientName == "" {
		clientName = "worker-s3-lifecycle-daily"
	}
	clientID := cfg.ClientID
	if clientID == 0 {
		clientID = int32(util.RandomInt32())
	}
	runUpTo := runNow.UnixNano()
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

	cursorAdvanceTo := startTsNs
	// stuck flips to true at the first event with a skipped (not-yet-due)
	// match. From that event onward, cursorAdvanceTo no longer rises —
	// future runs must re-scan everything from cursorAdvanceTo + 1 onward
	// so the future-due matches get re-evaluated when they age in.
	stuck := false
	halted := false

drain:
	for {
		select {
		case <-ctx.Done():
			// Parent context cancellation (worker shutdown, MaxRuntime).
			// Return whatever progress was made so far and propagate the
			// error so the caller marks the run as interrupted.
			cancelReader()
			<-readerDone
			return cursorAdvanceTo, true, ctx.Err()
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
			matches := router.Route(ctx, snap, ev, runNow, cfg.Lister)
			eventSkipped, eventHalted, eventErr := processMatches(ctx, cfg, runNow, ev, matches)
			if eventErr != nil {
				// A non-dispatch error (e.g. limiter wait cancelled by
				// shutdown). Cursor stays at the last fully-processed
				// event; surface the error so the caller treats it as
				// an interrupt.
				cancelReader()
				<-readerDone
				return cursorAdvanceTo, true, eventErr
			}
			if eventHalted {
				halted = true
				break drain
			}
			if eventSkipped {
				// First event with a not-yet-due match. Don't advance
				// the cursor past it; keep processing later events to
				// dispatch any due ones, but the persisted cursor stays
				// at the previous cursorAdvanceTo so this event is
				// re-scanned tomorrow.
				stuck = true
				continue
			}
			if !stuck {
				cursorAdvanceTo = ev.TsNs
			}
		}
	}

	// Wait for reader to drain so its goroutine doesn't outlive us.
	cancelReader()
	if rerr := <-readerDone; rerr != nil && !errors.Is(rerr, context.Canceled) {
		glog.V(2).Infof("daily_run shard=%d: reader returned: %v", shardID, rerr)
	}
	return cursorAdvanceTo, halted, nil
}

// processMatches dispatches every match emitted from one event. Returns
// (skippedAny, halted, error):
//   - skippedAny=true if at least one match had a DueTime past runNow.
//     The caller must NOT advance the persisted cursor past this event
//     so the skipped match gets re-scanned in a later run.
//   - halted=true on an unresolved outcome (RETRY_LATER / BLOCKED /
//     transport error after in-run retries). Caller exits the drain
//     loop.
//   - error: propagated from limiter.Wait when ctx is cancelled.
//
// A future-DueTime match is silently skipped — it does NOT terminate
// the loop or get cached as "done for this rule." A single event can
// produce multiple matches for the same ActionKey against different
// objects (routePointerTransitionExpand emits one match per noncurrent
// sibling, each with its own SuccessorModTime derived from a different
// demoting event), so a not-yet-due sibling must never gate a sibling
// that's already past its DueTime. Without per-object state the only
// safe behavior is per-match independence; the cursor-advance gate
// upstream handles re-scanning the skipped events tomorrow.
//
// runNow is the frozen run-start instant. Using it (rather than a
// fresh now() per call) keeps the boundary stable across all matches
// in this run.
func processMatches(ctx context.Context, cfg Config, runNow time.Time, ev *reader.Event, matches []router.Match) (skippedAny, halted bool, err error) {
	for _, m := range matches {
		if m.DueTime.After(runNow) {
			skippedAny = true
			continue
		}
		if cfg.Limiter != nil {
			if waitErr := cfg.Limiter.Wait(ctx); waitErr != nil {
				return skippedAny, true, waitErr
			}
		}
		outcome, dispatchErr := dispatchWithRetry(ctx, cfg.Client, m)
		if dispatchErr != nil {
			// Exhausted in-run transport retries: halt; tomorrow retries
			// from the same cursor.
			glog.V(1).Infof("daily_run: transport error on %s/%s %s: %v",
				m.Bucket, m.ObjectKey, m.Key.ActionKind, dispatchErr)
			return skippedAny, true, nil
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
			return skippedAny, true, nil
		default:
			glog.V(1).Infof("daily_run: unknown outcome %v on %s/%s", outcome, m.Bucket, m.ObjectKey)
			return skippedAny, true, nil
		}
		_ = ev // ev kept available for future per-event logging
	}
	return skippedAny, false, nil
}
