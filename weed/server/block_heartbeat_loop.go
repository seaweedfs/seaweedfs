package weed_server

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// minHeartbeatInterval is the minimum allowed heartbeat interval.
// Prevents time.NewTicker panics from zero/negative values.
const minHeartbeatInterval = time.Millisecond

// BlockVolumeHeartbeatCollector periodically collects block volume status
// and delivers it via StatusCallback. Standalone — does not touch the
// existing gRPC heartbeat stream. When proto is updated, the callback
// will be wired to stream.Send().
type BlockVolumeHeartbeatCollector struct {
	blockService *BlockService
	interval     time.Duration
	stopCh       chan struct{}
	done         chan struct{}
	stopOnce     sync.Once
	started      atomic.Bool // BUG-CP4B2-1: tracks whether Run() was called

	// StatusCallback is called each tick with collected status.
	StatusCallback func([]blockvol.BlockVolumeInfoMessage)
}

// NewBlockVolumeHeartbeatCollector creates a collector that calls
// StatusCallback every interval with the current block volume status.
// Intervals ≤ 0 are clamped to minHeartbeatInterval (BUG-CP4B2-2).
func NewBlockVolumeHeartbeatCollector(
	bs *BlockService, interval time.Duration,
) *BlockVolumeHeartbeatCollector {
	if interval <= 0 {
		interval = minHeartbeatInterval
	}
	return &BlockVolumeHeartbeatCollector{
		blockService: bs,
		interval:     interval,
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
	}
}

// Run blocks until Stop() is called. Collects status on each tick.
func (c *BlockVolumeHeartbeatCollector) Run() {
	c.started.Store(true)
	defer close(c.done)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			msgs := c.blockService.Store().CollectBlockVolumeHeartbeat()
			c.safeCallback(msgs)
		case <-c.stopCh:
			return
		}
	}
}

// safeCallback invokes StatusCallback with panic recovery (BUG-CP4B2-3).
func (c *BlockVolumeHeartbeatCollector) safeCallback(msgs []blockvol.BlockVolumeInfoMessage) {
	if c.StatusCallback == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("block heartbeat: callback panic: %v", r)
		}
	}()
	c.StatusCallback(msgs)
}

// Stop signals Run() to exit and waits for it to finish.
// Safe to call even if Run() was never started (BUG-CP4B2-1).
func (c *BlockVolumeHeartbeatCollector) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
	if !c.started.Load() {
		return // Run() never started — nothing to wait for.
	}
	<-c.done
}
