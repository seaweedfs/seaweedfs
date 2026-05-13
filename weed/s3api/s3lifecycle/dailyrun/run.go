package dailyrun

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/time/rate"
)

// LifecycleClient mirrors dispatcher's contract; duplicated to avoid an
// import cycle.
type LifecycleClient interface {
	LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error)
}

// WalkerFunc handles the per-shard bucket walk for a given engine view.
// Phase 4b uses it on the recovery branch (rule-content edit / partition
// flip) so already-due objects across the rewritten rule set get caught
// before the cursor rewinds.
type WalkerFunc func(ctx context.Context, view *engine.Snapshot, shardID int) error

type Config struct {
	Shards      []int
	BucketsPath string
	Engine      *engine.Engine
	FilerClient filer_pb.SeaweedFilerClient
	Client      LifecycleClient
	Persister   CursorPersister
	Lister      router.SiblingLister

	// Workers <= 0 -> 1 (serial).
	Workers int

	// nil -> no rate limit. Shared across all shard goroutines.
	Limiter *rate.Limiter

	// Meta-log retention boundary. Rules whose effective TTL exceeds
	// this can't be serviced by replay alone and get partitioned into
	// the walk view (engine.PromotedHash). 0 falls back to maxTTL,
	// which keeps PromotedHash empty and the partition-flip recovery
	// trigger dormant.
	RetentionWindow time.Duration

	// Walker is invoked on the recovery branch (rule-content edit or
	// partition flip) before the cursor rewind. It receives the
	// engine.RecoveryView so only the rules that need bulk re-evaluation
	// are walked, and the per-shard ID so the implementation can filter
	// entries. nil disables walker invocation entirely — the cursor
	// still rewinds, matching Phase 4a behavior.
	Walker WalkerFunc

	// WalkerInterval throttles the steady-state and empty-replay walker
	// invocations: the walker fires only when the time since the
	// per-shard cursor's LastWalkedNs exceeds this value. Cold-start
	// and recovery walker fires (RecoveryView) are unconditional —
	// those are bounded events that must run once when the trigger
	// condition is met. 0 means "fire on every pass" (the prior
	// behavior, suitable for s3tests-style rapid-cadence drivers and
	// for in-repo integration tests).
	//
	// Production deployments should set this to roughly the walk cost
	// budget per shard per cluster: e.g., 1h for a small cluster, 6h
	// for a large one. The walker reads the whole bucket subtree it
	// covers, so a too-short interval crushes the filer; a too-long
	// interval delays ExpirationDate and ExpiredObjectDeleteMarker
	// dispatches.
	WalkerInterval time.Duration

	ClientName string
	// 0 -> randomized per-run.
	ClientID int32

	// nil -> time.Now.
	Now func() time.Time

	// 0 -> unbounded.
	EventBudget int
}

// Run executes the daily replay for every shard in cfg.Shards
// concurrently. Returns the first shard error; the rest log and run to
// completion so one shard's transient failure doesn't lose other shards'
// progress.
//
// All cfg.Shards share one meta-log subscription. The Reader's
// ShardPredicate accepts any shard in cfg.Shards, and a fan-out
// goroutine routes events to per-shard channels by ev.ShardID. This
// replaces the earlier per-shard Reader that opened 16 SubscribeMetadata
// streams to filer per pass.
func Run(ctx context.Context, cfg Config) error {
	if err := validate(cfg); err != nil {
		return err
	}
	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	// Freeze "now" so shards agree on the boundary.
	runNow := now()
	startedAt := time.Now()

	// Capture once so a mid-run Compile can't make shards disagree.
	snap := cfg.Engine.Snapshot()
	rsh := engine.ReplayContentHash(snap)
	maxTTL := engine.MaxEffectiveTTL(snap)

	// Since all shards share one subscription, every shard goroutine
	// must be live to drain its channel — capping concurrency would
	// stall the fan-out. cfg.Workers no longer gates shard concurrency
	// (dispatch is throttled via cfg.Limiter); accepted for backwards
	// compatibility but otherwise inert.
	_ = cfg.Workers

	// rsh==[32]byte{} means no replay-eligible rules — runShard's
	// walker-only branch fires and saves; no subscription needed.
	var (
		shardEvents     map[int]chan *reader.Event
		readerDone      chan error
		fanoutDone      chan struct{}
		cancelRead      context.CancelFunc
		globalStartTsNs int64
	)
	if rsh != [32]byte{} {
		// Pre-load all per-shard cursors so the shared subscription
		// can start from min(per-shard startTsNs). Using the cold-start
		// floor (runNow - maxTTL) globally would skip past pending
		// events on shards whose cursor is older than the floor —
		// exactly the case where a rule's TTL equals maxTTL and an
		// older event still has DueTime <= runNow.
		globalStartTsNs = computeGlobalStartTsNs(ctx, cfg, runNow, maxTTL)
		shardEvents, readerDone, fanoutDone, cancelRead = startSharedSubscription(ctx, cfg, runNow, globalStartTsNs)
		defer cancelRead()
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(cfg.Shards))
	for _, sh := range cfg.Shards {
		sh := sh
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ch <-chan *reader.Event
			if shardEvents != nil {
				ch = shardEvents[sh]
			}
			if err := runShard(ctx, cfg, snap, runNow, sh, ch); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)

	// Tear down the shared subscription. cancelRead unblocks both the
	// reader's gRPC stream and the fan-out's send loop; we wait on both
	// so their goroutines don't outlive Run.
	if cancelRead != nil {
		cancelRead()
		<-fanoutDone
		if rerr := <-readerDone; rerr != nil && !errors.Is(rerr, context.Canceled) && !errors.Is(rerr, context.DeadlineExceeded) {
			glog.V(2).Infof("daily_run: shared reader returned: %v", rerr)
		}
	}

	var first error
	errCount := 0
	for err := range errCh {
		errCount++
		if first == nil {
			first = err
		} else {
			glog.V(1).Infof("daily_run: additional shard error: %v", err)
		}
	}
	status := "ok"
	if first != nil {
		status = "error"
	}
	glog.V(0).Infof("daily_run: status=%s shards=%d errors=%d duration=%s",
		status, len(cfg.Shards), errCount, time.Since(startedAt).Round(time.Millisecond))
	return first
}

// computeGlobalStartTsNs scans every shard's persisted cursor and
// returns the minimum startTsNs the shared subscription must cover.
// For each shard the start point is the persisted cursor (steady state)
// or runNow - maxTTL (cold start, when no cursor exists). Load errors
// downgrade to the cold-start floor for that shard — failing closed
// would be worse than re-scanning maxTTL of events.
func computeGlobalStartTsNs(ctx context.Context, cfg Config, runNow time.Time, maxTTL time.Duration) int64 {
	floor := runNow.Add(-maxTTL).UnixNano()
	min := int64(0)
	first := true
	for _, sh := range cfg.Shards {
		var start int64
		if persisted, found, err := cfg.Persister.Load(ctx, sh); err == nil && found {
			start = persisted.TsNs
		} else {
			start = floor
		}
		if first || start < min {
			min = start
			first = false
		}
	}
	if first {
		// No shards (validate() should have caught this, but defensive).
		return floor
	}
	return min
}

// startSharedSubscription opens one SubscribeMetadata stream covering
// every shard in cfg.Shards and fans events out to per-shard channels.
// Subscription floor is the caller-supplied globalStartTsNs (typically
// min over per-shard cursors). Shards whose own startTsNs is fresher
// filter out already-past events themselves inside drainShardEvents.
// Events arriving with TsNs > runUpTo (the pass boundary) cause the
// fan-out to cancel the reader and close all per-shard channels,
// ending the pass.
func startSharedSubscription(ctx context.Context, cfg Config, runNow time.Time, globalStartTsNs int64) (map[int]chan *reader.Event, chan error, chan struct{}, context.CancelFunc) {
	shardSet := make(map[int]bool, len(cfg.Shards))
	shardEvents := make(map[int]chan *reader.Event, len(cfg.Shards))
	for _, sh := range cfg.Shards {
		shardSet[sh] = true
		// Per-shard channel size has to absorb event bursts between
		// drains' iterations without backpressuring the fan-out.
		// 256 covers a busy test bucket; production tuning can lift
		// this further.
		shardEvents[sh] = make(chan *reader.Event, 256)
	}

	clientName := cfg.ClientName
	if clientName == "" {
		clientName = "worker-s3-lifecycle-daily"
	}
	clientID := cfg.ClientID
	if clientID == 0 {
		clientID = int32(util.RandomInt32())
	}

	events := make(chan *reader.Event, 4*len(cfg.Shards))
	rd := &reader.Reader{
		ShardPredicate: func(id int) bool { return shardSet[id] },
		BucketsPath:    cfg.BucketsPath,
		StartTsNs:      globalStartTsNs,
		Events:         events,
		EventBudget:    cfg.EventBudget,
	}

	readerCtx, cancelReader := context.WithCancel(ctx)
	readerDone := make(chan error, 1)
	go func() {
		readerDone <- rd.Run(readerCtx, cfg.FilerClient, clientName, clientID)
	}()

	runUpTo := runNow.UnixNano()
	fanoutDone := make(chan struct{})
	go func() {
		defer close(fanoutDone)
		defer func() {
			for _, ch := range shardEvents {
				close(ch)
			}
		}()
		for {
			select {
			case <-readerCtx.Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				if ev == nil {
					continue
				}
				// Meta-log events arrive in TsNs order; the first
				// event past runUpTo means everything after is past
				// too. Cancel the reader so subsequent passes don't
				// pay for stream tail we'd drop anyway.
				if ev.TsNs > runUpTo {
					cancelReader()
					return
				}
				ch := shardEvents[ev.ShardID]
				if ch == nil {
					continue
				}
				select {
				case <-readerCtx.Done():
					return
				case ch <- ev:
				}
			}
		}
	}()

	return shardEvents, readerDone, fanoutDone, cancelReader
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
	// A negative WalkerInterval would silently fall through walkerDue's
	// `interval <= 0` branch and re-enable "walk every pass", defeating
	// the throttle whose whole point is bounding filer load. The
	// admin-config parser already clamps negative input to zero
	// (worker/tasks/s3_lifecycle/config.go), but callers using
	// dailyrun.Config directly (tests, embedders, future drivers) get
	// a loud failure instead.
	if cfg.WalkerInterval < 0 {
		return fmt.Errorf("daily_run: negative WalkerInterval %v (0 = unthrottled, positive values throttle)", cfg.WalkerInterval)
	}
	for _, sh := range cfg.Shards {
		if sh < 0 || sh >= s3lifecycle.ShardCount {
			return fmt.Errorf("daily_run: shard %d out of [0, %d)", sh, s3lifecycle.ShardCount)
		}
	}
	return nil
}

// runShard executes one daily-replay pass; see DESIGN.md for algorithm.
// Three walker invocation paths under cfg.Walker (when set):
//   - cold start / recovery: RecoveryView. Unconditional — these are
//     bounded events (first run for a shard, or rule-content edit / partition
//     flip) and the walker must run once when the trigger condition is met.
//   - steady state: RulesForShard's walk view, throttled by cfg.WalkerInterval
//     against persisted.LastWalkedNs so the walker fires on its own cadence
//     independently of how often Run() is invoked.
//   - empty replay: same throttling — buckets with only walker-bound rules
//     use the walk view, and a too-fast Run() driver would otherwise burn
//     the filer with a full subtree scan per tick.
func runShard(ctx context.Context, cfg Config, snap *engine.Snapshot, runNow time.Time, shardID int, events <-chan *reader.Event) error {
	shardLabel := strconv.Itoa(shardID)
	shardStart := time.Now()
	defer func() {
		stats.S3LifecycleDailyRunShardDurationSeconds.WithLabelValues(shardLabel).Observe(time.Since(shardStart).Seconds())
	}()
	persisted, found, err := cfg.Persister.Load(ctx, shardID)
	if err != nil {
		return fmt.Errorf("shard=%d: load cursor: %w", shardID, err)
	}

	// engine.ReplayContentHash has a different byte layout than the
	// Phase-2 local helper. First post-upgrade run mismatches every
	// cursor and drops into the rule-change branch below — bounded
	// one-time rewind to runNow - maxTTL, self-healing on save.
	rsh := engine.ReplayContentHash(snap)
	maxTTL := engine.MaxEffectiveTTL(snap)
	// Operator-supplied retention falls back to maxTTL. In steady
	// state every active replay rule has TTL <= maxTTL by construction,
	// so promoted is empty and the partition-flip trigger is dormant.
	// During bootstrap (rules compiled but not yet active) maxTTL is
	// 0, retentionWindow is 0, and every rule with TTL > 0 lands in
	// the walk partition; the resulting non-empty promoted forces a
	// recovery walk on the first run after rules activate, which is
	// the intended bootstrap behavior. Once the handler plumbs the
	// real meta-log retention here, PromotedHash starts catching
	// retention-driven partition flips in addition.
	retentionWindow := cfg.RetentionWindow
	if retentionWindow <= 0 {
		retentionWindow = maxTTL
	}
	promoted := engine.PromotedHash(snap, retentionWindow)

	// lastWalkedNs threads the steady-state walker's clock through the
	// cursor saves below. Start with the persisted value; only update
	// it when a steady-state / empty-replay walker fire actually
	// happens. Cold-start and recovery walker fires also bump it so
	// the next pass's throttle has a fresh anchor.
	//
	// walkedThisPass is the in-pass guard: walkerDue answers the
	// persisted-state question ("has enough time elapsed?"), but a
	// cold-start or recovery branch above the steady-state check
	// already fired the walker with RecoveryView, which is a superset
	// of every per-shard partition. Keeping the within-pass guard out
	// of walkerDue lets that function stay pure (interval/never-walked
	// logic only) and side-steps the test-injectable-runNow ambiguity
	// where two distinct passes happen to share a UnixNano.
	lastWalkedNs := persisted.LastWalkedNs
	walkedThisPass := false

	if rsh == [32]byte{} {
		// No replay-eligible rules. Walker-only rules
		// (ExpirationDate / ExpiredDeleteMarker / NewerNoncurrent)
		// or scan_only-promoted rules might still need a walk; run
		// the walker (throttled by WalkerInterval) before persisting
		// the empty cursor and returning. Without this, a bucket whose
		// only rule is walker-bound would never have it dispatched —
		// the bug TestLifecycleExpirationDateInThePast caught.
		if cfg.Walker != nil && walkerDue(lastWalkedNs, runNow, cfg.WalkerInterval) {
			if _, walkView := snap.RulesForShard(shardID, retentionWindow); walkView != nil && len(walkView.AllActions()) > 0 {
				if werr := cfg.Walker(ctx, walkView, shardID); werr != nil {
					return fmt.Errorf("shard=%d: steady walk (empty replay): %w", shardID, werr)
				}
				lastWalkedNs = runNow.UnixNano()
				walkedThisPass = true
			}
		}
		_ = walkedThisPass // empty-replay branch returns; no downstream walker.
		return cfg.Persister.Save(ctx, shardID, Cursor{
			TsNs:         0,
			RuleSetHash:  rsh,
			PromotedHash: promoted,
			LastWalkedNs: lastWalkedNs,
		})
	}

	// Recovery / cold-start walker:
	//   - found && hashes mismatch: rule edit or partition flip — walk
	//     the rewritten rule set so already-due objects fire before the
	//     cursor rewinds, then rewind for meta-log replay.
	//   - !found: first run for this shard. Pre-existing objects PUT
	//     before the rule was added live OUTSIDE the meta-log scan
	//     window (TsNs > runNow - maxTTL) and would never replay; the
	//     walker has to discover them. The streaming worker did this
	//     via BucketBootstrapper; daily-replay needs the same.
	mustWalkRecovery := found && (persisted.RuleSetHash != rsh || persisted.PromotedHash != promoted)
	mustWalkColdStart := !found
	if mustWalkRecovery || mustWalkColdStart {
		if cfg.Walker != nil {
			if werr := cfg.Walker(ctx, engine.RecoveryView(snap), shardID); werr != nil {
				return fmt.Errorf("shard=%d: recovery walk: %w", shardID, werr)
			}
			lastWalkedNs = runNow.UnixNano()
			walkedThisPass = true
		}
		if mustWalkRecovery {
			// Rule changed: rewind cursor so the sliding replay re-scans
			// the new max-TTL window and the persisted hashes match the
			// new rule set.
			next := Cursor{
				TsNs:         runNow.Add(-maxTTL).UnixNano(),
				RuleSetHash:  rsh,
				PromotedHash: promoted,
				LastWalkedNs: lastWalkedNs,
			}
			return cfg.Persister.Save(ctx, shardID, next)
		}
		// Cold start: keep TsNs=0 so the drain below floors to
		// runNow - maxTTL and the cursor is saved fresh after the run.
	}

	// Steady-state walker for walker-bound and scan_only-promoted rules.
	// RulesForShard splits the snapshot using the same retentionWindow
	// PromotedHash used, so the walk view is exactly the partition the
	// hash already accounted for. Empty walk view (no rules need walking
	// today) skips the call so non-versioned, replay-only deployments
	// don't pay an O(N) bucket-walk per run.
	//
	// Two-stage gate: walkedThisPass suppresses a double walk when the
	// cold-start branch above already fired (RecoveryView covered it).
	// walkerDue applies the persisted-state throttle for the case
	// where this is a normal pass that didn't trigger cold-start.
	if cfg.Walker != nil && !walkedThisPass && walkerDue(lastWalkedNs, runNow, cfg.WalkerInterval) {
		if _, walkView := snap.RulesForShard(shardID, retentionWindow); walkView != nil && len(walkView.AllActions()) > 0 {
			if werr := cfg.Walker(ctx, walkView, shardID); werr != nil {
				return fmt.Errorf("shard=%d: steady walk: %w", shardID, werr)
			}
			lastWalkedNs = runNow.UnixNano()
		}
	}

	// Cold start: scan from now-maxTTL so already-due objects within
	// meta-log retention still expire. In steady state honor the
	// cursor as-is: the drain freezes the cursor at the last pre-skip
	// event so pending matches with DueTime == TsNs+maxTTL stay in
	// scope across passes. Bumping forward to runNow-maxTTL would
	// orphan exactly those events (the test_lifecyclev2_expiration
	// regression: cursor saved at the no-match event right before
	// the not-yet-due expire3 PUT, then floor at runNow=PUT+maxTTL
	// equals PUT — bumping past the expire3 event itself).
	startTsNs := persisted.TsNs
	if !found {
		startTsNs = runNow.Add(-maxTTL).UnixNano()
	}

	lastOK, _, drainErr := drainShardEvents(ctx, cfg, runNow, shardID, snap, startTsNs, events)
	// Cursor save uses a fresh ctx because the steady-state drain exits
	// via passCtx cancellation (the only signal the filer subscription
	// gets when no new events arrive). Saving with the canceled passCtx
	// would silently drop the cursor and the next pass would re-replay
	// from the same floor — defeating advancement entirely.
	saveCtx, saveCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer saveCancel()
	if drainErr != nil {
		_ = cfg.Persister.Save(saveCtx, shardID, Cursor{TsNs: lastOK, RuleSetHash: rsh, PromotedHash: promoted, LastWalkedNs: lastWalkedNs})
		// passCtx timeout is the expected end-of-pass for an idle
		// subscription; not a real error. Other drain errors still
		// propagate.
		if errors.Is(drainErr, context.DeadlineExceeded) || errors.Is(drainErr, context.Canceled) {
			return nil
		}
		return fmt.Errorf("shard=%d: drain: %w", shardID, drainErr)
	}

	return cfg.Persister.Save(saveCtx, shardID, Cursor{
		TsNs:         lastOK,
		RuleSetHash:  rsh,
		PromotedHash: promoted,
		LastWalkedNs: lastWalkedNs,
	})
}

// walkerDue answers the persisted-state throttle: has enough time
// elapsed since the last walker fire on this shard? interval == 0
// means "fire every pass" (the prior, unconditional behavior).
// lastWalkedNs == 0 means "never walked steady-state" — fire to
// establish the throttle anchor. Otherwise gate on
// (runNow - lastWalkedNs) >= interval.
//
// Within-pass double-fire suppression (cold-start / recovery walker
// already ran this pass; steady-state branch must not re-walk a
// superset partition) lives in runShard's walkedThisPass local flag
// rather than here. Keeping walkerDue pure means the function answers
// one question and a test that injects two distinct passes with the
// same runNow doesn't get falsely throttled.
func walkerDue(lastWalkedNs int64, runNow time.Time, interval time.Duration) bool {
	if interval <= 0 || lastWalkedNs == 0 {
		return true
	}
	return runNow.UnixNano()-lastWalkedNs >= int64(interval)
}

// drainShardEvents reads pre-fanned-out events for this shard from the
// shared meta-log subscription and dispatches matches whose due_time is
// past runNow. cursorAdvanceTo stops growing at the first event with a
// not-yet-due match so that event is re-scanned in a later run.
// halted=true marks an unresolved dispatch outcome.
//
// The subscription itself lives at the Run() level — one filer stream
// fans out to per-shard channels via shardEventDispatcher. Each shard
// drains its own channel and advances its own cursor; future-TsNs
// events (relative to this pass's runNow) are filtered out by the
// dispatcher before they reach this channel.
func drainShardEvents(ctx context.Context, cfg Config, runNow time.Time, shardID int, snap *engine.Snapshot, startTsNs int64, events <-chan *reader.Event) (int64, bool, error) {
	cursorAdvanceTo := startTsNs
	stuck := false
	halted := false

drain:
	for {
		select {
		case <-ctx.Done():
			return cursorAdvanceTo, true, ctx.Err()
		case ev, ok := <-events:
			if !ok {
				break drain
			}
			if ev == nil {
				continue
			}
			// The global subscription starts at min(per-shard startTsNs),
			// so this shard may receive events that are already past its
			// own cursor. Skip them rather than re-dispatching.
			if ev.TsNs <= startTsNs {
				continue
			}
			stats.S3LifecycleDailyRunEventsScanned.WithLabelValues(strconv.Itoa(shardID)).Inc()
			matches := router.Route(ctx, snap, ev, runNow, cfg.Lister)
			eventSkipped, eventHalted, eventErr := processMatches(ctx, cfg, runNow, ev, matches)
			if eventErr != nil {
				return cursorAdvanceTo, true, eventErr
			}
			if eventHalted {
				halted = true
				break drain
			}
			if eventSkipped {
				stuck = true
				continue
			}
			if !stuck {
				cursorAdvanceTo = ev.TsNs
			}
		}
	}

	return cursorAdvanceTo, halted, nil
}

// processMatches dispatches matches from one event. A future-DueTime
// match is silently skipped (per-match, not per-rule): one event can
// emit multiple matches for the same ActionKey across noncurrent
// siblings, so a not-yet-due sibling must not gate a due one. The
// cursor-advance gate upstream re-scans skipped events tomorrow.
func processMatches(ctx context.Context, cfg Config, runNow time.Time, ev *reader.Event, matches []router.Match) (skippedAny, halted bool, err error) {
	for _, m := range matches {
		if m.DueTime.After(runNow) {
			skippedAny = true
			continue
		}
		if cfg.Limiter != nil {
			waitStart := time.Now()
			if waitErr := cfg.Limiter.Wait(ctx); waitErr != nil {
				return skippedAny, true, waitErr
			}
			stats.S3LifecycleDispatchLimiterWaitSeconds.Observe(time.Since(waitStart).Seconds())
		}
		outcome, dispatchErr := dispatchWithRetry(ctx, cfg.Client, m)
		if dispatchErr != nil {
			glog.V(1).Infof("daily_run: transport error on %s/%s %s: %v",
				m.Bucket, m.ObjectKey, m.Key.ActionKind, dispatchErr)
			// "RPC_ERROR" matches the streaming dispatcher's label
			// (dispatcher/dispatcher.go) so transport failures
			// aggregate under one outcome key across paths.
			stats.S3LifecycleDispatchCounter.WithLabelValues(m.Bucket, m.Key.ActionKind.String(), "RPC_ERROR").Inc()
			return skippedAny, true, nil
		}
		stats.S3LifecycleDispatchCounter.WithLabelValues(m.Bucket, m.Key.ActionKind.String(), outcome.String()).Inc()
		switch outcome {
		case s3_lifecycle_pb.LifecycleDeleteOutcome_DONE,
			s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED,
			s3_lifecycle_pb.LifecycleDeleteOutcome_SKIPPED_OBJECT_LOCK:
		case s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER,
			s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED:
			glog.V(1).Infof("daily_run: %s on %s/%s %s",
				outcome, m.Bucket, m.ObjectKey, m.Key.ActionKind)
			return skippedAny, true, nil
		default:
			glog.V(1).Infof("daily_run: unknown outcome %v on %s/%s", outcome, m.Bucket, m.ObjectKey)
			return skippedAny, true, nil
		}
		_ = ev
	}
	return skippedAny, false, nil
}
