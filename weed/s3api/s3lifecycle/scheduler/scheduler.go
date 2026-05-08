package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dispatcher"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
)

// defaultRefreshInterval lives in refresh_*.go so the s3tests build can
// shrink the engine-rebuild cadence to seconds.
const (
	defaultRetryBackoff = 5 * time.Second
)

// Scheduler runs N pipeline goroutines, one per worker, each owning a
// contiguous shard slice of [0, ShardCount). It periodically rebuilds the
// engine snapshot from the filer's bucket configs.
type Scheduler struct {
	BucketsPath string
	Engine      *engine.Engine
	Persister   reader.Persister
	Client      dispatcher.LifecycleClient
	FilerClient filer_pb.SeaweedFilerClient
	ClientID    int32
	ClientName  string

	Workers         int
	DispatchTick    time.Duration
	CheckpointTick  time.Duration
	RefreshInterval time.Duration
	RetryBackoff    time.Duration
}

// Run blocks until ctx is canceled. Spawns Workers + 1 goroutines: one
// engine-refresh ticker plus one Pipeline runner per worker.
func (s *Scheduler) Run(ctx context.Context) error {
	if s.Engine == nil || s.Persister == nil || s.Client == nil || s.FilerClient == nil {
		return errors.New("scheduler: missing required dependency")
	}
	if s.BucketsPath == "" {
		return errors.New("scheduler: BucketsPath required")
	}

	workers := s.Workers
	if workers <= 0 {
		workers = 1
	}
	refresh := s.RefreshInterval
	if refresh <= 0 {
		refresh = defaultRefreshInterval
	}
	retry := s.RetryBackoff
	if retry <= 0 {
		retry = defaultRetryBackoff
	}

	// Build all pipelines up front and remember which shard each owns.
	// Doing this before Run lets the bootstrap injector route an event
	// to the right pipeline by shard, and lets InjectEvent's lazy events
	// channel be primed before the per-bucket walker starts pushing.
	type pipelineSlot struct {
		pipeline *dispatcher.Pipeline
		shards   []int
	}
	var slots []*pipelineSlot
	pipelinesByShard := make(map[int]*dispatcher.Pipeline)
	for i := 0; i < workers; i++ {
		shardSet := AssignShards(i, workers)
		if len(shardSet) == 0 {
			continue
		}
		slot := &pipelineSlot{
			pipeline: &dispatcher.Pipeline{
				Shards:         shardSet,
				BucketsPath:    s.BucketsPath,
				Engine:         s.Engine,
				Persister:      s.Persister,
				Client:         s.Client,
				FilerClient:    s.FilerClient,
				ClientID:       s.ClientID,
				ClientName:     fmt.Sprintf("%s-w%02d", s.ClientName, i),
				DispatchTick:   s.DispatchTick,
				CheckpointTick: s.CheckpointTick,
			},
			shards: shardSet,
		}
		slots = append(slots, slot)
		for _, sh := range shardSet {
			pipelinesByShard[sh] = slot.pipeline
		}
	}

	bs := &BucketBootstrapper{
		FilerClient: s.FilerClient,
		BucketsPath: s.BucketsPath,
		Injector:    pipelineFanout(pipelinesByShard),
	}

	s.refreshEngine(ctx, bs)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(refresh)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.refreshEngine(ctx, bs)
			}
		}
	}()

	for _, slot := range slots {
		slot := slot
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := slot.pipeline.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					glog.Warningf("lifecycle scheduler: shards=%v: %v", slot.shards, err)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(retry):
				}
			}
		}()
	}

	wg.Wait()
	return nil
}

func (s *Scheduler) refreshEngine(ctx context.Context, bs *BucketBootstrapper) {
	inputs, parseErrors, err := LoadCompileInputs(ctx, s.FilerClient, s.BucketsPath)
	if err != nil {
		glog.Warningf("lifecycle scheduler: refresh engine: %v", err)
		return
	}
	for _, pe := range parseErrors {
		glog.Warningf("lifecycle scheduler: malformed config in bucket %s: %v", pe.Bucket, pe.Err)
	}
	s.Engine.Compile(inputs, engine.CompileOptions{PriorStates: AllActivePriorStates(inputs)})
	if bs != nil {
		buckets := make([]string, 0, len(inputs))
		for _, in := range inputs {
			buckets = append(buckets, in.Bucket)
		}
		bs.KickOffNew(ctx, buckets)
	}
}

// pipelineFanout routes a synthesized event to the pipeline that owns
// its shard. Returned as an EventInjector so the bootstrapper doesn't
// know about pipelines.
type pipelineFanout map[int]*dispatcher.Pipeline

func (f pipelineFanout) InjectEvent(ctx context.Context, ev *reader.Event) error {
	if ev == nil {
		return nil
	}
	p, ok := f[ev.ShardID]
	if !ok {
		// Shard isn't covered by any pipeline in this scheduler — nothing
		// to do. This shouldn't happen with AssignShards covering [0, ShardCount),
		// but stay tolerant if a future change introduces gaps.
		return nil
	}
	return p.InjectEvent(ctx, ev)
}

// AssignShards returns the contiguous shard slice for worker idx of total.
// Shards distribute as evenly as possible: with ShardCount=16 and total=3,
// workers receive [0..5], [6..10], [11..15].
func AssignShards(idx, total int) []int {
	if total <= 0 || idx < 0 || idx >= total {
		return nil
	}
	shardCount := s3lifecycle.ShardCount
	base := shardCount / total
	extra := shardCount % total
	var lo, hi int
	if idx < extra {
		lo = idx * (base + 1)
		hi = lo + base + 1
	} else {
		lo = idx*base + extra
		hi = lo + base
	}
	if hi > shardCount {
		hi = shardCount
	}
	if lo >= hi {
		return nil
	}
	out := make([]int, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, i)
	}
	return out
}
