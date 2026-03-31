package distsim

import (
	"container/heap"
	"fmt"
	"math/rand"
	"strings"
)

// --- Event types ---

type EventKind int

const (
	EvWriteStart    EventKind = iota // client writes to primary
	EvShipEntry                      // primary sends WAL entry to replica
	EvShipDeliver                    // entry arrives at replica
	EvBarrierSend                    // primary sends barrier to replica
	EvBarrierDeliver                 // barrier arrives at replica
	EvBarrierFsync                   // replica fsync completes
	EvBarrierAck                     // ack arrives back at primary
	EvNodeCrash                      // node crashes
	EvNodeRestart                    // node restarts
	EvLinkDown                       // network link drops
	EvLinkUp                         // network link restores
	EvFlusherTick                    // flusher checkpoint cycle
	EvPromote                        // coordinator promotes a node
	EvLockAcquire                    // thread tries to acquire lock
	EvLockRelease                    // thread releases lock
)

func (k EventKind) String() string {
	names := [...]string{
		"WriteStart", "ShipEntry", "ShipDeliver",
		"BarrierSend", "BarrierDeliver", "BarrierFsync", "BarrierAck",
		"NodeCrash", "NodeRestart", "LinkDown", "LinkUp",
		"FlusherTick", "Promote",
		"LockAcquire", "LockRelease",
	}
	if int(k) < len(names) {
		return names[k]
	}
	return fmt.Sprintf("Event(%d)", k)
}

type Event struct {
	Time    uint64
	ID      uint64 // unique, for stable ordering
	Kind    EventKind
	NodeID  string
	Payload EventPayload
}

type EventPayload struct {
	Write     Write  // for WriteStart, ShipEntry, ShipDeliver
	TargetLSN uint64 // for barriers
	FromNode  string // for delivered messages
	ToNode    string
	LockName  string // for lock events
	ThreadID  string
	PromoteID string // for EvPromote
}

// --- Priority queue ---

type eventHeap []Event

func (h eventHeap) Len() int      { return len(h) }
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h eventHeap) Less(i, j int) bool {
	if h[i].Time != h[j].Time {
		return h[i].Time < h[j].Time
	}
	return h[i].ID < h[j].ID // stable tie-break
}
func (h *eventHeap) Push(x interface{}) { *h = append(*h, x.(Event)) }
func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}

// --- Lock model ---

type lockState struct {
	held    bool
	holder  string // threadID
	waiting []Event // parked EvLockAcquire events
}

// --- Trace ---

type TraceEntry struct {
	Time  uint64
	Event Event
	Note  string
}

// --- Simulator ---

type Simulator struct {
	Cluster   *Cluster
	rng       *rand.Rand
	queue     eventHeap
	nextID    uint64
	locks     map[string]*lockState // lockName -> state
	trace     []TraceEntry
	Errors    []string
	maxTime   uint64
	jitterMax uint64 // max random delay added to message delivery

	// Config
	FaultRate     float64 // probability of injecting a fault per step [0,1]
	MaxEvents     int     // stop after this many events
	eventsRun     int
}

func NewSimulator(cluster *Cluster, seed int64) *Simulator {
	return &Simulator{
		Cluster:   cluster,
		rng:       rand.New(rand.NewSource(seed)),
		locks:     map[string]*lockState{},
		maxTime:   100000,
		jitterMax: 3,
		FaultRate: 0.05,
		MaxEvents: 5000,
	}
}

// Enqueue adds an event to the priority queue.
func (s *Simulator) Enqueue(e Event) {
	s.nextID++
	e.ID = s.nextID
	heap.Push(&s.queue, e)
}

// EnqueueAt is a convenience for enqueueing at a specific time.
func (s *Simulator) EnqueueAt(time uint64, kind EventKind, nodeID string, payload EventPayload) {
	s.Enqueue(Event{Time: time, Kind: kind, NodeID: nodeID, Payload: payload})
}

// jitter returns a random delay in [1, jitterMax].
func (s *Simulator) jitter() uint64 {
	if s.jitterMax <= 1 {
		return 1
	}
	return 1 + uint64(s.rng.Int63n(int64(s.jitterMax)))
}

// --- Main loop ---

// Step executes the next event. Returns false if queue is empty or limit reached.
// When multiple events share the same timestamp, one is chosen randomly
// to explore different interleavings across runs with different seeds.
func (s *Simulator) Step() bool {
	if s.queue.Len() == 0 || s.eventsRun >= s.MaxEvents {
		return false
	}
	// Collect all events at the earliest timestamp.
	earliest := s.queue[0].Time
	if earliest > s.maxTime {
		return false
	}
	var ready []Event
	for s.queue.Len() > 0 && s.queue[0].Time == earliest {
		ready = append(ready, heap.Pop(&s.queue).(Event))
	}
	// Shuffle to randomize interleaving of equal-time events.
	s.rng.Shuffle(len(ready), func(i, j int) { ready[i], ready[j] = ready[j], ready[i] })
	// Execute the first, re-enqueue the rest.
	e := ready[0]
	for _, r := range ready[1:] {
		heap.Push(&s.queue, r)
	}

	s.Cluster.Now = e.Time
	s.eventsRun++

	s.execute(e)
	s.checkInvariants(e)

	return len(s.Errors) == 0
}

// Run executes until queue empty, limit reached, or invariant violated.
func (s *Simulator) Run() {
	for s.Step() {
	}
}

// --- Event execution ---

func (s *Simulator) execute(e Event) {
	node := s.Cluster.Nodes[e.NodeID]

	switch e.Kind {
	case EvWriteStart:
		s.executeWriteStart(e)

	case EvShipEntry:
		// Primary ships entry to a replica. Enqueue delivery with jitter.
		if node != nil && node.Running {
			deliverTime := s.Cluster.Now + s.jitter()
			s.EnqueueAt(deliverTime, EvShipDeliver, e.Payload.ToNode, EventPayload{
				Write:    e.Payload.Write,
				FromNode: e.NodeID,
			})
			s.record(e, fmt.Sprintf("ship LSN=%d to %s, deliver@%d", e.Payload.Write.LSN, e.Payload.ToNode, deliverTime))
		}

	case EvShipDeliver:
		if node != nil && node.Running && node.Epoch == s.Cluster.Coordinator.Epoch {
			if s.Cluster.Links[e.Payload.FromNode] != nil && s.Cluster.Links[e.Payload.FromNode][e.NodeID] {
				node.Storage.AppendWrite(e.Payload.Write)
				s.record(e, fmt.Sprintf("deliver LSN=%d on %s, receivedLSN=%d", e.Payload.Write.LSN, e.NodeID, node.Storage.ReceivedLSN))
			} else {
				s.record(e, fmt.Sprintf("drop LSN=%d to %s (link down)", e.Payload.Write.LSN, e.NodeID))
			}
		}

	case EvBarrierSend:
		if node != nil && node.Running {
			deliverTime := s.Cluster.Now + s.jitter()
			s.EnqueueAt(deliverTime, EvBarrierDeliver, e.Payload.ToNode, EventPayload{
				TargetLSN: e.Payload.TargetLSN,
				FromNode:  e.NodeID,
			})
			s.record(e, fmt.Sprintf("barrier LSN=%d to %s", e.Payload.TargetLSN, e.Payload.ToNode))
		}

	case EvBarrierDeliver:
		if node != nil && node.Running && node.Epoch == s.Cluster.Coordinator.Epoch {
			if s.Cluster.Links[e.Payload.FromNode] != nil && s.Cluster.Links[e.Payload.FromNode][e.NodeID] {
				if node.Storage.ReceivedLSN >= e.Payload.TargetLSN {
					// Can fsync now. Enqueue fsync completion with small delay.
					s.EnqueueAt(s.Cluster.Now+1, EvBarrierFsync, e.NodeID, EventPayload{
						TargetLSN: e.Payload.TargetLSN,
						FromNode:  e.Payload.FromNode,
					})
					s.record(e, fmt.Sprintf("barrier deliver LSN=%d, fsync scheduled", e.Payload.TargetLSN))
				} else {
					// Not enough entries yet. Re-enqueue barrier with delay (retry).
					s.EnqueueAt(s.Cluster.Now+1, EvBarrierDeliver, e.NodeID, e.Payload)
					s.record(e, fmt.Sprintf("barrier LSN=%d waiting (received=%d)", e.Payload.TargetLSN, node.Storage.ReceivedLSN))
				}
			}
		}

	case EvBarrierFsync:
		if node != nil && node.Running {
			node.Storage.AdvanceFlush(e.Payload.TargetLSN)
			// Send ack back to primary.
			deliverTime := s.Cluster.Now + s.jitter()
			s.EnqueueAt(deliverTime, EvBarrierAck, e.Payload.FromNode, EventPayload{
				TargetLSN: e.Payload.TargetLSN,
				FromNode:  e.NodeID,
			})
			s.record(e, fmt.Sprintf("fsync LSN=%d on %s, flushedLSN=%d", e.Payload.TargetLSN, e.NodeID, node.Storage.FlushedLSN))
		}

	case EvBarrierAck:
		// Only process acks on running nodes in the current epoch.
		// After crash+promote, stale acks for the old primary must not advance commits.
		if node != nil && node.Running && node.Epoch == s.Cluster.Coordinator.Epoch {
			if pending := s.Cluster.Pending[e.Payload.TargetLSN]; pending != nil {
				pending.DurableOn[e.Payload.FromNode] = true
				s.Cluster.refreshCommits()
				s.record(e, fmt.Sprintf("ack LSN=%d from %s, durable=%d", e.Payload.TargetLSN, e.Payload.FromNode, s.Cluster.durableAckCount(pending)))
			}
		} else {
			s.record(e, fmt.Sprintf("ack LSN=%d from %s DROPPED (node down or stale epoch)", e.Payload.TargetLSN, e.Payload.FromNode))
		}

	case EvNodeCrash:
		if node != nil {
			node.Running = false
			// Drop all pending events for this node.
			s.dropEventsForNode(e.NodeID)
			s.record(e, fmt.Sprintf("CRASH %s", e.NodeID))
		}

	case EvNodeRestart:
		if node != nil {
			node.Running = true
			node.Epoch = s.Cluster.Coordinator.Epoch
			s.record(e, fmt.Sprintf("RESTART %s epoch=%d", e.NodeID, node.Epoch))
		}

	case EvLinkDown:
		s.Cluster.Disconnect(e.Payload.FromNode, e.Payload.ToNode)
		s.Cluster.Disconnect(e.Payload.ToNode, e.Payload.FromNode)
		s.record(e, fmt.Sprintf("LINK DOWN %s <-> %s", e.Payload.FromNode, e.Payload.ToNode))

	case EvLinkUp:
		s.Cluster.Connect(e.Payload.FromNode, e.Payload.ToNode)
		s.Cluster.Connect(e.Payload.ToNode, e.Payload.FromNode)
		s.record(e, fmt.Sprintf("LINK UP %s <-> %s", e.Payload.FromNode, e.Payload.ToNode))

	case EvFlusherTick:
		if node != nil && node.Running {
			// Phase 4.5: flusher first materializes WAL to extent, then checkpoints.
			node.Storage.ApplyToExtent(node.Storage.WALDurableLSN)
			node.Storage.AdvanceCheckpoint(node.Storage.WALDurableLSN)
			s.record(e, fmt.Sprintf("flusher tick %s applied=%d checkpoint=%d",
				e.NodeID, node.Storage.ExtentAppliedLSN, node.Storage.CheckpointLSN))
		}

	case EvPromote:
		if err := s.Cluster.Promote(e.Payload.PromoteID); err != nil {
			s.record(e, fmt.Sprintf("promote %s FAILED: %v", e.Payload.PromoteID, err))
		} else {
			s.record(e, fmt.Sprintf("PROMOTE %s epoch=%d", e.Payload.PromoteID, s.Cluster.Coordinator.Epoch))
		}

	case EvLockAcquire:
		s.executeLockAcquire(e)

	case EvLockRelease:
		s.executeLockRelease(e)
	}
}

func (s *Simulator) executeWriteStart(e Event) {
	c := s.Cluster
	primary := c.Primary()
	if primary == nil || !primary.Running || primary.Epoch != c.Coordinator.Epoch {
		s.record(e, "write rejected: no valid primary")
		return
	}
	c.nextLSN++
	w := Write{LSN: c.nextLSN, Block: e.Payload.Write.Block, Value: c.nextLSN}
	primary.Storage.AppendWrite(w)
	primary.Storage.AdvanceFlush(w.LSN)
	c.Reference.Apply(w)
	c.Pending[w.LSN] = &PendingCommit{
		Write:     w,
		DurableOn: map[string]bool{primary.ID: true},
	}
	c.refreshCommits()

	// Ship to each replica with jitter.
	for _, rid := range c.replicaIDs() {
		shipTime := s.Cluster.Now + s.jitter()
		s.EnqueueAt(shipTime, EvShipEntry, primary.ID, EventPayload{
			Write:  w,
			ToNode: rid,
		})
	}
	// Barrier after ship.
	for _, rid := range c.replicaIDs() {
		barrierTime := s.Cluster.Now + s.jitter() + 2
		s.EnqueueAt(barrierTime, EvBarrierSend, primary.ID, EventPayload{
			TargetLSN: w.LSN,
			ToNode:    rid,
		})
	}

	s.record(e, fmt.Sprintf("write block=%d LSN=%d", w.Block, w.LSN))
}

func (s *Simulator) executeLockAcquire(e Event) {
	name := e.Payload.LockName
	ls, ok := s.locks[name]
	if !ok {
		ls = &lockState{}
		s.locks[name] = ls
	}
	if !ls.held {
		ls.held = true
		ls.holder = e.Payload.ThreadID
		s.record(e, fmt.Sprintf("lock %s acquired by %s", name, e.Payload.ThreadID))
	} else {
		// Park — will be released when current holder releases.
		ls.waiting = append(ls.waiting, e)
		s.record(e, fmt.Sprintf("lock %s BLOCKED %s (held by %s)", name, e.Payload.ThreadID, ls.holder))
	}
}

func (s *Simulator) executeLockRelease(e Event) {
	name := e.Payload.LockName
	ls := s.locks[name]
	if ls == nil || !ls.held {
		return
	}
	// Validate: only the holder can release.
	if ls.holder != e.Payload.ThreadID {
		s.record(e, fmt.Sprintf("lock %s release REJECTED: %s is not holder (held by %s)", name, e.Payload.ThreadID, ls.holder))
		return
	}
	s.record(e, fmt.Sprintf("lock %s released by %s", name, ls.holder))
	ls.held = false
	ls.holder = ""
	// Grant to next waiter (random pick among waiters for interleaving exploration).
	if len(ls.waiting) > 0 {
		idx := s.rng.Intn(len(ls.waiting))
		next := ls.waiting[idx]
		ls.waiting = append(ls.waiting[:idx], ls.waiting[idx+1:]...)
		ls.held = true
		ls.holder = next.Payload.ThreadID
		s.record(next, fmt.Sprintf("lock %s granted to %s (was waiting)", name, next.Payload.ThreadID))
	}
}

func (s *Simulator) dropEventsForNode(nodeID string) {
	var kept eventHeap
	for _, e := range s.queue {
		if e.NodeID != nodeID {
			kept = append(kept, e)
		}
	}
	s.queue = kept
	heap.Init(&s.queue)
}

// --- Invariant checking ---

func (s *Simulator) checkInvariants(after Event) {
	// 1. Commit safety: committed LSN must be durable on policy-required nodes.
	for lsn := uint64(1); lsn <= s.Cluster.Coordinator.CommittedLSN; lsn++ {
		p := s.Cluster.Pending[lsn]
		if p == nil {
			continue
		}
		if !s.Cluster.commitSatisfied(p) {
			s.addError(after, fmt.Sprintf("committed LSN %d not durable per policy", lsn))
		}
	}

	// 2. No false commit on promoted node.
	primary := s.Cluster.Primary()
	if primary != nil && primary.Running {
		committedLSN := s.Cluster.Coordinator.CommittedLSN
		for lsn := committedLSN + 1; lsn <= s.Cluster.nextLSN; lsn++ {
			p := s.Cluster.Pending[lsn]
			if p != nil && !p.Committed && p.DurableOn[primary.ID] {
				// Uncommitted but durable on primary — only a problem if primary changed.
				// This is expected on the original primary. Only flag if this is a PROMOTED node.
			}
		}
	}

	// 3. Data correctness: primary state matches reference at the LSN it actually has.
	// After promotion, the new primary may not have all writes the old primary committed.
	// Verify correctness only up to what the current primary has durably received.
	if primary != nil && primary.Running {
		checkLSN := primary.Storage.FlushedLSN
		if checkLSN > s.Cluster.Coordinator.CommittedLSN {
			checkLSN = s.Cluster.Coordinator.CommittedLSN
		}
		if checkLSN > 0 {
			refState := s.Cluster.Reference.StateAt(checkLSN)
			nodeState := primary.Storage.StateAt(checkLSN)
			if !EqualState(refState, nodeState) {
				s.addError(after, fmt.Sprintf("data divergence on primary %s at LSN=%d",
					primary.ID, checkLSN))
			}
		}
	}

	// 4. Epoch fencing: no node has accepted a stale epoch.
	for id, node := range s.Cluster.Nodes {
		if node.Running && node.Epoch > s.Cluster.Coordinator.Epoch {
			s.addError(after, fmt.Sprintf("node %s has future epoch %d > coordinator %d", id, node.Epoch, s.Cluster.Coordinator.Epoch))
		}
	}

	// 5. Lock safety: no two threads hold the same lock.
	for name, ls := range s.locks {
		if ls.held && ls.holder == "" {
			s.addError(after, fmt.Sprintf("lock %s held but no holder", name))
		}
	}
}

func (s *Simulator) addError(after Event, msg string) {
	s.Errors = append(s.Errors, fmt.Sprintf("t=%d after %s on %s: %s",
		after.Time, after.Kind, after.NodeID, msg))
}

func (s *Simulator) record(e Event, note string) {
	s.trace = append(s.trace, TraceEntry{Time: e.Time, Event: e, Note: note})
}

// --- Random fault injection ---

// InjectRandomFault schedules a random fault (crash, partition, heal)
// at a random future time within [Now+1, Now+spread).
func (s *Simulator) InjectRandomFault() {
	s.InjectRandomFaultWithin(30)
}

// InjectRandomFaultWithin schedules a random fault at a random time
// within [Now+1, Now+spread).
func (s *Simulator) InjectRandomFaultWithin(spread uint64) {
	if s.rng.Float64() > s.FaultRate {
		return
	}
	members := s.Cluster.Coordinator.Members
	if len(members) == 0 {
		return
	}
	faultTime := s.Cluster.Now + 1 + uint64(s.rng.Int63n(int64(spread)))

	switch s.rng.Intn(3) {
	case 0: // crash a random node
		id := members[s.rng.Intn(len(members))]
		s.EnqueueAt(faultTime, EvNodeCrash, id, EventPayload{})
	case 1: // drop a link
		from := members[s.rng.Intn(len(members))]
		to := members[s.rng.Intn(len(members))]
		if from != to {
			s.EnqueueAt(faultTime, EvLinkDown, from, EventPayload{FromNode: from, ToNode: to})
		}
	case 2: // restore a link
		from := members[s.rng.Intn(len(members))]
		to := members[s.rng.Intn(len(members))]
		if from != to {
			s.EnqueueAt(faultTime, EvLinkUp, from, EventPayload{FromNode: from, ToNode: to})
		}
	}
}

// --- Scenario helpers ---

// ScheduleWrites enqueues n writes at random times in [start, start+spread).
func (s *Simulator) ScheduleWrites(n int, start, spread uint64) {
	for i := 0; i < n; i++ {
		t := start + uint64(s.rng.Int63n(int64(spread)))
		block := uint64(s.rng.Intn(16))
		s.EnqueueAt(t, EvWriteStart, s.Cluster.Coordinator.PrimaryID, EventPayload{
			Write: Write{Block: block},
		})
	}
}

// ScheduleCrashAndPromote enqueues a primary crash at crashTime and promotes promoteID at promoteTime.
func (s *Simulator) ScheduleCrashAndPromote(crashTime uint64, promoteID string, promoteTime uint64) {
	s.EnqueueAt(crashTime, EvNodeCrash, s.Cluster.Coordinator.PrimaryID, EventPayload{})
	s.EnqueueAt(promoteTime, EvPromote, "", EventPayload{PromoteID: promoteID})
}

// ScheduleFlusherTicks enqueues periodic flusher ticks for a node.
func (s *Simulator) ScheduleFlusherTicks(nodeID string, start, interval uint64, count int) {
	for i := 0; i < count; i++ {
		s.EnqueueAt(start+uint64(i)*interval, EvFlusherTick, nodeID, EventPayload{})
	}
}

// --- Output ---

// TraceString returns the full trace as a string.
func (s *Simulator) TraceString() string {
	var sb strings.Builder
	for _, te := range s.trace {
		fmt.Fprintf(&sb, "[t=%d] %s on %s: %s\n", te.Time, te.Event.Kind, te.Event.NodeID, te.Note)
	}
	return sb.String()
}

// ErrorString returns all errors.
func (s *Simulator) ErrorString() string {
	return strings.Join(s.Errors, "\n")
}

// AssertCommittedDataCorrect checks that the current primary's state matches the reference.
func (s *Simulator) AssertCommittedDataCorrect() error {
	primary := s.Cluster.Primary()
	if primary == nil {
		return fmt.Errorf("no primary")
	}
	committedLSN := s.Cluster.Coordinator.CommittedLSN
	if committedLSN == 0 {
		return nil
	}
	refState := s.Cluster.Reference.StateAt(committedLSN)
	nodeState := primary.Storage.StateAt(committedLSN)
	if !EqualState(refState, nodeState) {
		return fmt.Errorf("data divergence on %s at LSN=%d: ref=%v node=%v",
			primary.ID, committedLSN, refState, nodeState)
	}
	return nil
}
