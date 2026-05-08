package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Pipeline composes the reader, router, dispatcher, and cursor checkpoint
// into a single Run loop. One Pipeline can handle a contiguous shard span
// or any explicit set of shards via Shards; ShardID still works for the
// single-shard case (and is preferred for short-form configuration).
//
// Internally there is exactly one filer subscription regardless of how
// many shards Shards contains; events are filtered by the reader's
// ShardPredicate and routed to the matching shard's Cursor + Schedule
// inside the existing dispatch goroutine — no per-shard goroutines.
type Pipeline struct {
	ShardID     int   // used when Shards is empty
	Shards      []int // overrides ShardID when non-empty
	BucketsPath string

	Engine *engine.Engine
	// Cursor is consulted only when len(Shards) <= 1. Range mode allocates
	// a fresh Cursor per shard internally.
	Cursor    *reader.Cursor
	Persister reader.Persister
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
	shutdownDrainTimeout  = 30 * time.Second
	shutdownSaveTimeout   = 5 * time.Second
)

// shardState bundles per-shard mutable state so the single dispatch
// goroutine can route an event to the right cursor + schedule by lookup.
type shardState struct {
	cursor   *reader.Cursor
	dispatch *Dispatcher
}

// Run blocks until ctx is canceled or a fatal error occurs. On exit, every
// shard's cursor is persisted; in-flight schedule entries are dropped
// (the meta-log is the durable buffer, so a restart re-derives them).
func (p *Pipeline) Run(ctx context.Context) error {
	if p.Engine == nil || p.Persister == nil || p.Client == nil || p.FilerClient == nil {
		return errors.New("pipeline: missing required dependency")
	}
	if p.BucketsPath == "" {
		return errors.New("pipeline: BucketsPath required")
	}

	// Resolve the active shard set. Single-shard configurations populate
	// either Shards=[N] or Shards=nil with ShardID=N (latter is the legacy
	// path that also supplies a Cursor); both feed the same range model.
	shardIDs := p.Shards
	if len(shardIDs) == 0 {
		shardIDs = []int{p.ShardID}
	}
	shardSet := make(map[int]struct{}, len(shardIDs))
	for _, s := range shardIDs {
		if s < 0 || s >= s3lifecycle.ShardCount {
			return fmt.Errorf("pipeline: shard %d out of [0,%d)", s, s3lifecycle.ShardCount)
		}
		shardSet[s] = struct{}{}
	}

	// Per-shard cursor + dispatcher. Cursors restore from the durable
	// store; freezes re-arm naturally when the reader re-encounters the
	// poison event at MinTsNs and the dispatch state machine drives it
	// back to BLOCKED.
	states := make(map[int]*shardState, len(shardIDs))
	var minStartTsNs int64 = -1
	for _, shardID := range shardIDs {
		c := p.Cursor
		if len(shardIDs) != 1 || c == nil {
			c = reader.NewCursor()
		}
		state, err := p.Persister.Load(ctx, shardID)
		if err != nil {
			return fmt.Errorf("cursor load shard=%d: %w", shardID, err)
		}
		c.Restore(state)
		states[shardID] = &shardState{
			cursor: c,
			dispatch: &Dispatcher{
				ShardID:  shardID,
				Client:   p.Client,
				Cursor:   c,
				Schedule: router.NewSchedule(),
			},
		}
		if mt := c.MinTsNs(); mt > 0 && (minStartTsNs < 0 || mt < minStartTsNs) {
			minStartTsNs = mt
		}
	}
	if minStartTsNs < 0 {
		minStartTsNs = 0
	}

	bufSize := p.EventBuffer
	if bufSize <= 0 {
		bufSize = defaultEventBuffer
	}
	events := make(chan *reader.Event, bufSize)
	rd := &reader.Reader{
		BucketsPath: p.BucketsPath,
		ShardPredicate: func(s int) bool {
			_, ok := shardSet[s]
			return ok
		},
		StartTsNs:   minStartTsNs,
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
		if readerErr != nil && !isCtxShutdown(readerErr) {
			glog.Errorf("lifecycle reader: shards=%v: %v", shardIDs, readerErr)
		}
		cancel() // wake the dispatcher goroutine to drain & exit
	}()

	// Router/dispatcher goroutine: pulls events, routes them to per-shard
	// schedules, ticks every shard's dispatcher on the same cadence, and
	// checkpoints every shard's cursor on the checkpoint cadence. One
	// goroutine handles all shards — there is no fan-out per shard.
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

		drainAll := func() {
			drainCtx, drainCancel := context.WithTimeout(context.Background(), shutdownDrainTimeout)
			defer drainCancel()
			now := time.Now()
			for _, st := range states {
				st.dispatch.Tick(drainCtx, now)
			}
		}

		for {
			select {
			case <-runCtx.Done():
				drainAll()
				return
			case ev, ok := <-events:
				if !ok {
					drainAll()
					return
				}
				st := states[ev.ShardID]
				if st == nil {
					continue
				}
				if snap == nil {
					snap = p.Engine.Snapshot()
				}
				for _, m := range router.Route(snap, ev, time.Now()) {
					st.dispatch.Schedule.Add(m)
				}
			case <-dt.C:
				snap = p.Engine.Snapshot()
				now := time.Now()
				for _, st := range states {
					st.dispatch.Tick(runCtx, now)
				}
			case <-ct.C:
				for shardID, st := range states {
					if err := p.Persister.Save(runCtx, shardID, st.cursor.Snapshot()); err != nil {
						glog.Warningf("lifecycle cursor checkpoint: shard=%d: %v", shardID, err)
					}
				}
			}
		}
	}()

	wg.Wait()

	// Final cursor checkpoint on graceful shutdown.
	for shardID, st := range states {
		saveCtx, saveCancel := context.WithTimeout(context.Background(), shutdownSaveTimeout)
		err := p.Persister.Save(saveCtx, shardID, st.cursor.Snapshot())
		saveCancel()
		if err != nil {
			glog.Warningf("lifecycle cursor final save: shard=%d: %v", shardID, err)
		}
	}

	if readerErr != nil && !isCtxShutdown(readerErr) {
		return readerErr
	}
	return nil
}

// isCtxShutdown reports whether err is a graceful ctx-driven shutdown
// (Canceled or DeadlineExceeded), including the gRPC status forms that
// don't unwrap to the std-lib ctx errors.
func isCtxShutdown(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if code := status.Code(err); code == codes.Canceled || code == codes.DeadlineExceeded {
		return true
	}
	return false
}
