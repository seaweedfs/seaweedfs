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
// and delivers it via StatusCallback. Standalone -- does not touch the
// existing gRPC heartbeat stream. When proto is updated, the callback
// will be wired to stream.Send().
type BlockVolumeHeartbeatCollector struct {
	blockService *BlockService
	interval     time.Duration
	stopCh       chan struct{}
	done         chan struct{}
	stopOnce     sync.Once
	started      atomic.Bool // BUG-CP4B2-1: tracks whether Run() was called

	// cbMu protects the callback/source fields against concurrent
	// read (Run goroutine) and write (SetXxx callers). BUG-CP4B3-2.
	cbMu               sync.Mutex
	statusCallback     func([]blockvol.BlockVolumeInfoMessage)
	assignmentSource   func() []blockvol.BlockVolumeAssignment
	assignmentCallback func([]blockvol.BlockVolumeAssignment, []error)
}

// NewBlockVolumeHeartbeatCollector creates a collector that calls
// the status callback every interval with the current block volume status.
// Intervals <= 0 are clamped to minHeartbeatInterval (BUG-CP4B2-2).
func NewBlockVolumeHeartbeatCollector(
	bs *BlockService, interval time.Duration,
) *BlockVolumeHeartbeatCollector {
	if bs == nil {
		panic("block heartbeat: nil BlockService")
	}
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

// SetStatusCallback sets the function called each tick with collected status.
// Safe to call before or after Run() (BUG-CP4B3-2).
func (c *BlockVolumeHeartbeatCollector) SetStatusCallback(fn func([]blockvol.BlockVolumeInfoMessage)) {
	c.cbMu.Lock()
	c.statusCallback = fn
	c.cbMu.Unlock()
}

// SetAssignmentSource sets the function called each tick to fetch pending
// assignments. If nil, no assignments are processed.
// Safe to call before or after Run() (BUG-CP4B3-2).
func (c *BlockVolumeHeartbeatCollector) SetAssignmentSource(fn func() []blockvol.BlockVolumeAssignment) {
	c.cbMu.Lock()
	c.assignmentSource = fn
	c.cbMu.Unlock()
}

// SetAssignmentCallback sets the function called after processing with
// per-assignment errors. If nil, errors are silently dropped (already
// logged by ProcessBlockVolumeAssignments).
// Safe to call before or after Run() (BUG-CP4B3-2).
func (c *BlockVolumeHeartbeatCollector) SetAssignmentCallback(fn func([]blockvol.BlockVolumeAssignment, []error)) {
	c.cbMu.Lock()
	c.assignmentCallback = fn
	c.cbMu.Unlock()
}

// Run blocks until Stop() is called. Collects status on each tick,
// then processes any pending assignments.
func (c *BlockVolumeHeartbeatCollector) Run() {
	c.started.Store(true)
	defer close(c.done)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Outbound: collect and report status.
			msgs := c.blockService.CollectBlockVolumeHeartbeat()
			c.safeCallback(msgs)
			// Inbound: process any pending assignments.
			c.processAssignments()
		case <-c.stopCh:
			return
		}
	}
}

// processAssignments fetches and applies pending assignments from master.
func (c *BlockVolumeHeartbeatCollector) processAssignments() {
	c.cbMu.Lock()
	src := c.assignmentSource
	c.cbMu.Unlock()
	if src == nil {
		return
	}
	assignments := c.safeFuncCall(src)
	if len(assignments) == 0 {
		return
	}
	errs := c.blockService.ApplyAssignments(assignments)
	c.cbMu.Lock()
	cb := c.assignmentCallback
	c.cbMu.Unlock()
	if cb != nil {
		c.safeAssignmentCallback(cb, assignments, errs)
	}
}

// safeCallback loads and invokes the status callback with panic recovery
// (BUG-CP4B2-3). Lock is held only for the load, not during the call.
func (c *BlockVolumeHeartbeatCollector) safeCallback(msgs []blockvol.BlockVolumeInfoMessage) {
	c.cbMu.Lock()
	fn := c.statusCallback
	c.cbMu.Unlock()
	if fn == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("block heartbeat: callback panic: %v", r)
		}
	}()
	fn(msgs)
}

// safeFuncCall invokes an AssignmentSource with panic recovery.
func (c *BlockVolumeHeartbeatCollector) safeFuncCall(
	fn func() []blockvol.BlockVolumeAssignment,
) []blockvol.BlockVolumeAssignment {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("block heartbeat: assignment source panic: %v", r)
		}
	}()
	return fn()
}

// safeAssignmentCallback invokes an AssignmentCallback with panic recovery.
func (c *BlockVolumeHeartbeatCollector) safeAssignmentCallback(
	fn func([]blockvol.BlockVolumeAssignment, []error),
	assignments []blockvol.BlockVolumeAssignment, errs []error,
) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("block heartbeat: assignment callback panic: %v", r)
		}
	}()
	fn(assignments, errs)
}

// Stop signals Run() to exit and waits for it to finish.
// Safe to call even if Run() was never started (BUG-CP4B2-1).
func (c *BlockVolumeHeartbeatCollector) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
	if !c.started.Load() {
		return // Run() never started -- nothing to wait for.
	}
	<-c.done
}
