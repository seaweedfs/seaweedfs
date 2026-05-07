package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
)

// Pipeline composes the per-shard reader, router, dispatcher, and cursor
// checkpoint into a single Run loop. One Pipeline per (worker, shard).
type Pipeline struct {
	ShardID     int
	BucketsPath string

	Engine    *engine.Engine
	Cursor    *reader.Cursor
	Persister reader.Persister
	Blockers  BlockerStore
	Client    LifecycleClient

	FilerClient filer_pb.SeaweedFilerClient
	ClientID    int32
	ClientName  string

	// Tick cadence for the dispatcher. Zero = defaultDispatchTick.
	DispatchTick time.Duration
	// Cadence for cursor checkpoint writes. Zero = defaultCheckpointTick.
	CheckpointTick time.Duration
	// EventBudget caps reader events per Run; zero = unbounded (run until
	// ctx cancellation). Used by the worker scheduler to bound a single
	// READ task.
	EventBudget int

	// EventBuffer sets the channel capacity between reader and router
	// goroutines. Zero = defaultEventBuffer.
	EventBuffer int
}

const (
	defaultDispatchTick   = 5 * time.Second
	defaultCheckpointTick = 30 * time.Second
	defaultEventBuffer    = 1024
)

// Run blocks until ctx is canceled or a fatal error occurs. On exit, the
// cursor is persisted; in-flight schedule entries are dropped (the meta-log
// is the durable buffer, so a restart re-derives them).
func (p *Pipeline) Run(ctx context.Context) error {
	if p.Engine == nil || p.Cursor == nil || p.Persister == nil ||
		p.Blockers == nil || p.Client == nil || p.FilerClient == nil {
		return errors.New("pipeline: missing required dependency")
	}
	if p.BucketsPath == "" {
		return errors.New("pipeline: BucketsPath required")
	}

	// 1. Restore cursor + replay blocker freezes.
	state, err := p.Persister.Load(ctx, p.ShardID)
	if err != nil {
		return fmt.Errorf("cursor load: %w", err)
	}
	p.Cursor.Restore(state)

	dispatch := &Dispatcher{
		ShardID:  p.ShardID,
		Client:   p.Client,
		Cursor:   p.Cursor,
		Blockers: p.Blockers,
		Schedule: router.NewSchedule(),
	}
	if err := dispatch.ReplayBlockers(ctx); err != nil {
		return fmt.Errorf("blocker replay: %w", err)
	}

	// 2. Wire reader -> router -> schedule via a buffered channel.
	bufSize := p.EventBuffer
	if bufSize <= 0 {
		bufSize = defaultEventBuffer
	}
	events := make(chan *reader.Event, bufSize)
	rd := &reader.Reader{
		ShardID:     p.ShardID,
		BucketsPath: p.BucketsPath,
		Cursor:      p.Cursor,
		Events:      events,
		EventBudget: p.EventBudget,
	}
	rd.LogStartup()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var readerErr error

	// Reader goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(events)
		readerErr = rd.Run(runCtx, p.FilerClient, p.ClientName, p.ClientID)
		if readerErr != nil && !errors.Is(readerErr, context.Canceled) {
			glog.Errorf("lifecycle reader: shard=%d: %v", p.ShardID, readerErr)
		}
		cancel() // wake the dispatcher goroutine to drain & exit
	}()

	// Router/dispatcher goroutine: pulls events, routes them, ticks schedule.
	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatchTick := p.DispatchTick
		if dispatchTick <= 0 {
			dispatchTick = defaultDispatchTick
		}
		checkpointTick := p.CheckpointTick
		if checkpointTick <= 0 {
			checkpointTick = defaultCheckpointTick
		}
		dt := time.NewTicker(dispatchTick)
		defer dt.Stop()
		ct := time.NewTicker(checkpointTick)
		defer ct.Stop()
		snap := p.Engine.Snapshot()

		for {
			select {
			case <-runCtx.Done():
				dispatch.Tick(context.Background(), time.Now())
				return
			case ev, ok := <-events:
				if !ok {
					dispatch.Tick(context.Background(), time.Now())
					return
				}
				if snap == nil {
					snap = p.Engine.Snapshot()
				}
				for _, m := range router.Route(snap, ev, time.Now()) {
					dispatch.Schedule.Add(m)
				}
			case <-dt.C:
				snap = p.Engine.Snapshot() // refresh between tick boundaries
				dispatch.Tick(runCtx, time.Now())
			case <-ct.C:
				if err := p.Persister.Save(runCtx, p.ShardID, p.Cursor.Snapshot()); err != nil {
					glog.Warningf("lifecycle cursor checkpoint: shard=%d: %v", p.ShardID, err)
				}
			}
		}
	}()

	wg.Wait()

	// Final cursor checkpoint on graceful shutdown.
	if err := p.Persister.Save(context.Background(), p.ShardID, p.Cursor.Snapshot()); err != nil {
		glog.Warningf("lifecycle cursor final save: shard=%d: %v", p.ShardID, err)
	}

	if readerErr != nil && !errors.Is(readerErr, context.Canceled) {
		return readerErr
	}
	return nil
}
