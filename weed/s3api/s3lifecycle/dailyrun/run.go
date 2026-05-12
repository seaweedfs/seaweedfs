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
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/time/rate"
)

// LifecycleClient mirrors dispatcher's contract; duplicated to avoid an
// import cycle.
type LifecycleClient interface {
	LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error)
}

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

	// Capture once so a mid-run Compile can't make shards disagree.
	snap := cfg.Engine.Snapshot()
	if unsupported := checkSnapshotForUnsupported(snap); unsupported != nil {
		return unsupported
	}

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

// runShard executes one daily-replay pass; see DESIGN.md for algorithm.
// Phase 2: no walker on rule-change / cold-start; PromotedHash trigger
// is dormant until Phase 4b wires real retention.
// checkSnapshotForUnsupported already rejected walker-bound and
// scan_only rules.
func runShard(ctx context.Context, cfg Config, snap *engine.Snapshot, runNow time.Time, shardID int) error {
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
	// Operator-supplied retention falls back to maxTTL, which forces
	// promoted-empty (no rule's TTL can exceed the max). Once the
	// walker handles walk-bound rules, the handler will pass the real
	// meta-log retention here and PromotedHash starts catching
	// partition flips.
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
	// flip (PromotedHash mismatch — dormant until real retention).
	// Phase 4b adds the walker here; until then we rewind and let the
	// sliding meta-log replay catch up.
	if found && (persisted.RuleSetHash != rsh || persisted.PromotedHash != promoted) {
		next := Cursor{
			TsNs:         runNow.Add(-maxTTL).UnixNano(),
			RuleSetHash:  rsh,
			PromotedHash: promoted,
		}
		return cfg.Persister.Save(ctx, shardID, next)
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
			return skippedAny, true, nil
		}
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
