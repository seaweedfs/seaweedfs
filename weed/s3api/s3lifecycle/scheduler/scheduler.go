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

const (
	defaultRefreshInterval = 5 * time.Minute
	defaultRetryBackoff    = 5 * time.Second
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

	s.refreshEngine(ctx)

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
				s.refreshEngine(ctx)
			}
		}
	}()

	for i := 0; i < workers; i++ {
		shards := AssignShards(i, workers)
		if len(shards) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, shardSet []int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				p := &dispatcher.Pipeline{
					Shards:         shardSet,
					BucketsPath:    s.BucketsPath,
					Engine:         s.Engine,
					Persister:      s.Persister,
					Client:         s.Client,
					FilerClient:    s.FilerClient,
					ClientID:       s.ClientID,
					ClientName:     fmt.Sprintf("%s-w%02d", s.ClientName, idx),
					DispatchTick:   s.DispatchTick,
					CheckpointTick: s.CheckpointTick,
				}
				if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					glog.Warningf("lifecycle scheduler: worker=%d shards=%v: %v", idx, shardSet, err)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(retry):
				}
			}
		}(i, shards)
	}

	wg.Wait()
	return nil
}

func (s *Scheduler) refreshEngine(ctx context.Context) {
	inputs, parseErrors, err := LoadCompileInputs(ctx, s.FilerClient, s.BucketsPath)
	if err != nil {
		glog.Warningf("lifecycle scheduler: refresh engine: %v", err)
		return
	}
	for _, pe := range parseErrors {
		glog.Warningf("lifecycle scheduler: malformed config in bucket %s: %v", pe.Bucket, pe.Err)
	}
	s.Engine.Compile(inputs, engine.CompileOptions{PriorStates: AllActivePriorStates(inputs)})
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
