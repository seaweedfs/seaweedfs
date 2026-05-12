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

// runShard executes one daily-replay pass; see DESIGN.md for algorithm.
// Two walker invocations under cfg.Walker (when set):
//   - recovery branch: RecoveryView, so already-due objects across the
//     rewritten rule set fire before the cursor rewinds.
//   - steady state: RulesForShard's walk view, so walker-bound and
//     scan_only-promoted rules fire every day even when replay rules
//     are unchanged.
func runShard(ctx context.Context, cfg Config, snap *engine.Snapshot, runNow time.Time, shardID int) error {
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

	if rsh == [32]byte{} {
		return cfg.Persister.Save(ctx, shardID, Cursor{
			TsNs:         0,
			RuleSetHash:  rsh,
			PromotedHash: promoted,
		})
	}

	// Recovery: rule-content edit (RuleSetHash mismatch) or partition
	// flip (PromotedHash mismatch). Walk the rewritten rule set so
	// already-due objects fire before the cursor rewinds; then rewind
	// and let the sliding meta-log replay catch up steady state.
	if found && (persisted.RuleSetHash != rsh || persisted.PromotedHash != promoted) {
		if cfg.Walker != nil {
			if werr := cfg.Walker(ctx, engine.RecoveryView(snap), shardID); werr != nil {
				return fmt.Errorf("shard=%d: recovery walk: %w", shardID, werr)
			}
		}
		next := Cursor{
			TsNs:         runNow.Add(-maxTTL).UnixNano(),
			RuleSetHash:  rsh,
			PromotedHash: promoted,
		}
		return cfg.Persister.Save(ctx, shardID, next)
	}

	// Steady-state walker for walker-bound and scan_only-promoted rules.
	// RulesForShard splits the snapshot using the same retentionWindow
	// PromotedHash used, so the walk view is exactly the partition the
	// hash already accounted for. Empty walk view (no rules need walking
	// today) skips the call so non-versioned, replay-only deployments
	// don't pay an O(N) bucket-walk per run.
	if cfg.Walker != nil {
		if _, walkView := snap.RulesForShard(shardID, retentionWindow); walkView != nil && len(walkView.AllActions()) > 0 {
			if werr := cfg.Walker(ctx, walkView, shardID); werr != nil {
				return fmt.Errorf("shard=%d: steady walk: %w", shardID, werr)
			}
		}
	}

	// Cold start: scan from now-maxTTL so already-due objects within
	// meta-log retention still expire.
	startTsNs := persisted.TsNs
	floor := runNow.Add(-maxTTL).UnixNano()
	if startTsNs < floor {
		startTsNs = floor
	}

	lastOK, _, drainErr := drainShardEvents(ctx, cfg, runNow, shardID, snap, startTsNs)
	if drainErr != nil {
		_ = cfg.Persister.Save(ctx, shardID, Cursor{TsNs: lastOK, RuleSetHash: rsh, PromotedHash: promoted})
		return fmt.Errorf("shard=%d: drain: %w", shardID, drainErr)
	}

	return cfg.Persister.Save(ctx, shardID, Cursor{
		TsNs:         lastOK,
		RuleSetHash:  rsh,
		PromotedHash: promoted,
	})
}

// drainShardEvents subscribes to the meta-log from startTsNs and
// dispatches matches whose due_time is past runNow. cursorAdvanceTo
// stops growing at the first event with a not-yet-due match so that
// event is re-scanned in a later run. halted=true marks an unresolved
// dispatch outcome.
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
	stuck := false
	halted := false

drain:
	for {
		select {
		case <-ctx.Done():
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
				cancelReader()
				break drain
			}
			stats.S3LifecycleDailyRunEventsScanned.WithLabelValues(strconv.Itoa(shardID)).Inc()
			matches := router.Route(ctx, snap, ev, runNow, cfg.Lister)
			eventSkipped, eventHalted, eventErr := processMatches(ctx, cfg, runNow, ev, matches)
			if eventErr != nil {
				cancelReader()
				<-readerDone
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

	cancelReader()
	if rerr := <-readerDone; rerr != nil && !errors.Is(rerr, context.Canceled) {
		glog.V(2).Infof("daily_run shard=%d: reader returned: %v", shardID, rerr)
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
