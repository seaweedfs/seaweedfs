package distsim

import (
	"fmt"
	"sort"
)

type Role string

const (
	RolePrimary Role = "primary"
	RoleReplica Role = "replica"
)

type CommitMode string

const (
	CommitBestEffort CommitMode = "best_effort"
	CommitSyncAll    CommitMode = "sync_all"
	CommitSyncQuorum CommitMode = "sync_quorum"
)

type MessageKind string

const (
	MsgWrite     MessageKind = "write"
	MsgBarrier   MessageKind = "barrier"
	MsgBarrierAck MessageKind = "barrier_ack"
)

type Message struct {
	Kind   MessageKind
	From   string
	To     string
	Epoch  uint64
	Write  Write
	TargetLSN uint64
}

type inFlightMessage struct {
	deliverAt uint64
	msg       Message
}

type PendingCommit struct {
	Write      Write
	DurableOn  map[string]bool
	Committed  bool
	CommittedAt uint64
}

type RecoveryClass string

const (
	RecoveryClassWALInline         RecoveryClass = "wal_inline"
	RecoveryClassExtentReferenced RecoveryClass = "extent_referenced"
)

type RecoveryRecord struct {
	Write
	Class             RecoveryClass
	PayloadResolvable bool
}

// ReplicaNodeState tracks per-node replication protocol state.
type ReplicaNodeState string

const (
	NodeStateInSync       ReplicaNodeState = "InSync"
	NodeStateLagging      ReplicaNodeState = "Lagging"
	NodeStateCatchingUp   ReplicaNodeState = "CatchingUp"
	NodeStateNeedsRebuild ReplicaNodeState = "NeedsRebuild"
	NodeStateRebuilding   ReplicaNodeState = "Rebuilding"
)

// MessageRejectReason tracks why a message was rejected.
type MessageRejectReason string

const (
	RejectEpochMismatch MessageRejectReason = "epoch_mismatch"
	RejectNodeDown      MessageRejectReason = "node_down"
	RejectLinkDown      MessageRejectReason = "link_down"
	RejectStaleEndpoint  MessageRejectReason = "stale_endpoint"
	RejectBarrierExpired MessageRejectReason = "barrier_expired"
)

// CachedEndpoint represents the primary's cached view of a replica's address.
type CachedEndpoint struct {
	DataAddr        string
	EndpointVersion uint64
}

type NodeModel struct {
	ID      string
	Role    Role
	Epoch   uint64
	Running bool

	Storage *Storage

	// Endpoint identity.
	DataAddr        string // current data-plane address
	EndpointVersion uint64 // bumped on address change (restart with new port)

	// Protocol state (V2 model extension).
	ReplicaState    ReplicaNodeState
	CatchupAttempts int // consecutive catch-up attempts without convergence
}

func NewNodeModel(id string, role Role, epoch uint64) *NodeModel {
	return &NodeModel{
		ID:              id,
		Role:            role,
		Epoch:           epoch,
		Running:         true,
		Storage:         NewStorage(),
		ReplicaState:    NodeStateInSync,
		DataAddr:        fmt.Sprintf("%s:9333", id),
		EndpointVersion: 1,
	}
}

type Coordinator struct {
	Epoch      uint64
	PrimaryID  string
	Mode       CommitMode
	Members    []string
	CommittedLSN uint64
}

// RejectedMessage records a message that was rejected with a reason.
type RejectedMessage struct {
	Msg    Message
	Reason MessageRejectReason
	Time   uint64
}

// DeliveryResult records the outcome of a message delivery attempt.
type DeliveryResult struct {
	Msg      Message
	Accepted bool
	Reason   MessageRejectReason // empty if accepted
	Time     uint64
}

type Cluster struct {
	Now         uint64
	Coordinator *Coordinator
	Reference   *Reference
	Protocol    ProtocolPolicy // version-aware protocol decisions

	Nodes       map[string]*NodeModel
	Links       map[string]map[string]bool
	Queue       []inFlightMessage
	Pending     map[uint64]*PendingCommit
	nextLSN     uint64

	// Protocol tracking.
	Rejected   []RejectedMessage  // messages rejected by deliver()
	Deliveries []DeliveryResult   // all delivery attempts with accept/reject outcome

	// Endpoint tracking: primary's cached view of replica addresses.
	CachedReplicaEndpoints map[string]CachedEndpoint

	// Timeout tracking (eventsim layer).
	Timeouts        []PendingTimeout
	FiredTimeouts   []FiredTimeout  // timeouts that fired with authority to mutate state
	IgnoredTimeouts []FiredTimeout  // timeouts that reached deadline but had no authority (stale)
	ExpiredBarriers map[barrierExpiredKey]bool // late acks for expired barriers are rejected
	TickLog         []TickEvent               // ordered event log for race debugging

	// Session tracking (ownership layer).
	Sessions       map[string]*TrackedSession // active session per replica (keyed by ReplicaID)
	SessionHistory []*TrackedSession          // all sessions for debugging
	nextSessionID  uint64

	// Timeout config: 0 = disabled.
	BarrierTimeoutTicks uint64 // auto-register barrier timeout per replica on CommitWrite

	// Catch-up config.
	MaxCatchupAttempts int // default 10; escalates to NeedsRebuild
}

func NewCluster(mode CommitMode, primaryID string, replicaIDs ...string) *Cluster {
	return NewClusterWithProtocol(mode, ProtocolV2, primaryID, replicaIDs...)
}

func NewClusterWithProtocol(mode CommitMode, proto ProtocolVersion, primaryID string, replicaIDs ...string) *Cluster {
	c := &Cluster{
		Coordinator: &Coordinator{
			Epoch:     1,
			PrimaryID: primaryID,
			Mode:      mode,
			Members:   append([]string{primaryID}, replicaIDs...),
		},
		Protocol:           ProtocolPolicy{Version: proto},
		Reference:          NewReference(),
		Nodes:              map[string]*NodeModel{},
		Links:              map[string]map[string]bool{},
		MaxCatchupAttempts: 10,
		Pending:   map[uint64]*PendingCommit{},
	}
	c.CachedReplicaEndpoints = map[string]CachedEndpoint{}
	c.ExpiredBarriers = map[barrierExpiredKey]bool{}
	c.Sessions = map[string]*TrackedSession{}
	c.AddNode(primaryID, RolePrimary)
	for _, id := range replicaIDs {
		c.AddNode(id, RoleReplica)
		n := c.Nodes[id]
		c.CachedReplicaEndpoints[id] = CachedEndpoint{
			DataAddr:        n.DataAddr,
			EndpointVersion: n.EndpointVersion,
		}
	}
	for _, from := range c.Coordinator.Members {
		for _, to := range c.Coordinator.Members {
			if from == to {
				continue
			}
			c.Connect(from, to)
		}
	}
	return c
}

func (c *Cluster) AddNode(id string, role Role) {
	c.Nodes[id] = NewNodeModel(id, role, c.Coordinator.Epoch)
	if c.Links[id] == nil {
		c.Links[id] = map[string]bool{}
	}
}

func (c *Cluster) Connect(from, to string) {
	if c.Links[from] == nil {
		c.Links[from] = map[string]bool{}
	}
	c.Links[from][to] = true
}

func (c *Cluster) Disconnect(from, to string) {
	if c.Links[from] == nil {
		return
	}
	delete(c.Links[from], to)
}

func (c *Cluster) StopNode(id string) {
	if n := c.Nodes[id]; n != nil {
		n.Running = false
	}
}

func (c *Cluster) StartNode(id string) {
	if n := c.Nodes[id]; n != nil {
		n.Running = true
		n.Epoch = c.Coordinator.Epoch
	}
}

func (c *Cluster) Primary() *NodeModel {
	return c.Nodes[c.Coordinator.PrimaryID]
}

func (c *Cluster) replicaIDs() []string {
	ids := make([]string, 0, len(c.Coordinator.Members)-1)
	for _, id := range c.Coordinator.Members {
		if id != c.Coordinator.PrimaryID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (c *Cluster) quorumSize() int {
	return len(c.Coordinator.Members)/2 + 1
}

func (c *Cluster) durableAckCount(p *PendingCommit) int {
	if p == nil {
		return 0
	}
	count := 0
	for _, id := range c.Coordinator.Members {
		if p.DurableOn[id] {
			count++
		}
	}
	return count
}

func (c *Cluster) commitSatisfied(p *PendingCommit) bool {
	switch c.Coordinator.Mode {
	case CommitBestEffort:
		return true
	case CommitSyncAll:
		return c.durableAckCount(p) == len(c.Coordinator.Members)
	case CommitSyncQuorum:
		return c.durableAckCount(p) >= c.quorumSize()
	default:
		return false
	}
}

func (c *Cluster) CommitWrite(block uint64) uint64 {
	primary := c.Primary()
	if primary == nil || !primary.Running || primary.Epoch != c.Coordinator.Epoch {
		return 0
	}
	c.nextLSN++
	w := Write{LSN: c.nextLSN, Block: block, Value: c.nextLSN}
	primary.Storage.AppendWrite(w)
	primary.Storage.AdvanceFlush(w.LSN)
	c.Reference.Apply(w)
	c.Pending[w.LSN] = &PendingCommit{
		Write:      w,
		DurableOn:  map[string]bool{primary.ID: true},
		Committed:  false,
	}
	c.refreshCommits()

	for _, id := range c.replicaIDs() {
		c.enqueue(Message{
			Kind:  MsgWrite,
			From:  primary.ID,
			To:    id,
			Epoch: c.Coordinator.Epoch,
			Write: w,
		}, c.Now+1)
		c.enqueue(Message{
			Kind:      MsgBarrier,
			From:      primary.ID,
			To:        id,
			Epoch:     c.Coordinator.Epoch,
			TargetLSN: w.LSN,
		}, c.Now+2)
		if c.BarrierTimeoutTicks > 0 {
			c.RegisterTimeout(TimeoutBarrier, id, w.LSN, c.Now+c.BarrierTimeoutTicks)
		}
	}
	return w.LSN
}

// StaleWrite simulates a partitioned old-primary attempting writes through
// the message protocol at a stale epoch. The writes go through enqueue/deliver
// and should be rejected by epoch fencing — not silently succeed.
// Returns the number of messages that were delivered (should be 0 if fencing works).
func (c *Cluster) StaleWrite(staleNodeID string, staleEpoch uint64, block uint64) int {
	node := c.Nodes[staleNodeID]
	if node == nil || !node.Running {
		return 0
	}
	// Allocate a fake LSN from the stale node's perspective.
	staleLSN := node.Storage.ReceivedLSN + 1
	w := Write{LSN: staleLSN, Block: block, Value: staleLSN}
	node.Storage.AppendWrite(w)
	node.Storage.AdvanceFlush(staleLSN)

	rejectedBefore := len(c.Rejected)

	// Try to ship to all other nodes at the stale epoch.
	for _, id := range c.Coordinator.Members {
		if id == staleNodeID {
			continue
		}
		c.enqueue(Message{
			Kind:  MsgWrite,
			From:  staleNodeID,
			To:    id,
			Epoch: staleEpoch,
			Write: w,
		}, c.Now+1)
		c.enqueue(Message{
			Kind:      MsgBarrier,
			From:      staleNodeID,
			To:        id,
			Epoch:     staleEpoch,
			TargetLSN: staleLSN,
		}, c.Now+2)
	}
	c.TickN(5)

	// Count how many of those messages were actually delivered (not rejected).
	rejectedAfter := len(c.Rejected)
	rejectedCount := rejectedAfter - rejectedBefore
	// Each target gets 2 messages (write + barrier). Total sent = (members-1) * 2.
	totalSent := (len(c.Coordinator.Members) - 1) * 2
	delivered := totalSent - rejectedCount
	return delivered
}

// CatchUpWithEscalation attempts partial catch-up and tracks attempts.
// If max attempts exceeded, transitions the replica to NeedsRebuild.
// Returns true if catch-up converged, false if escalated or in-progress.
func (c *Cluster) CatchUpWithEscalation(replicaID string, batchSize int) bool {
	replica := c.Nodes[replicaID]
	primary := c.Primary()
	if replica == nil || primary == nil {
		return false
	}
	// NeedsRebuild is sticky — don't attempt catch-up.
	if replica.ReplicaState == NodeStateNeedsRebuild {
		return false
	}

	target := c.Coordinator.CommittedLSN
	start := replica.Storage.FlushedLSN

	if start >= target {
		replica.ReplicaState = NodeStateInSync
		replica.CatchupAttempts = 0
		c.cancelRecoveryTimeouts(replicaID) // auto-cancel on convergence
		return true
	}

	recovered, err := c.RecoverReplicaFromPrimaryPartial(replicaID, start, target, batchSize)
	if err != nil {
		replica.CatchupAttempts++
		if replica.CatchupAttempts >= c.MaxCatchupAttempts {
			replica.ReplicaState = NodeStateNeedsRebuild
			c.cancelRecoveryTimeouts(replicaID) // auto-cancel on escalation
		}
		return false
	}

	if recovered >= target {
		replica.ReplicaState = NodeStateInSync
		replica.CatchupAttempts = 0
		c.cancelRecoveryTimeouts(replicaID) // auto-cancel on convergence
		return true
	}

	replica.ReplicaState = NodeStateCatchingUp
	replica.CatchupAttempts++
	if replica.CatchupAttempts >= c.MaxCatchupAttempts {
		replica.ReplicaState = NodeStateNeedsRebuild
		c.cancelRecoveryTimeouts(replicaID) // auto-cancel on escalation
	}
	return false
}

// RejectedByReason returns the count of rejected messages for a specific reason.
func (c *Cluster) RejectedByReason(reason MessageRejectReason) int {
	count := 0
	for _, r := range c.Rejected {
		if r.Reason == reason {
			count++
		}
	}
	return count
}

func (c *Cluster) recordDelivery(msg Message, accepted bool, reason MessageRejectReason) {
	c.Deliveries = append(c.Deliveries, DeliveryResult{
		Msg: msg, Accepted: accepted, Reason: reason, Time: c.Now,
	})
	// Use Write.LSN for MsgWrite, TargetLSN for barrier/ack.
	lsn := msg.TargetLSN
	if msg.Kind == MsgWrite {
		lsn = msg.Write.LSN
	}
	if accepted {
		c.logEvent(EventDeliveryAccepted, fmt.Sprintf("%s %s→%s lsn=%d",
			msg.Kind, msg.From, msg.To, lsn))
	} else {
		c.Rejected = append(c.Rejected, RejectedMessage{Msg: msg, Reason: reason, Time: c.Now})
		c.logEvent(EventDeliveryRejected, fmt.Sprintf("%s %s→%s lsn=%d reason=%s",
			msg.Kind, msg.From, msg.To, lsn, reason))
	}
}

// AcceptedCount returns the number of accepted deliveries.
func (c *Cluster) AcceptedCount() int {
	count := 0
	for _, d := range c.Deliveries {
		if d.Accepted {
			count++
		}
	}
	return count
}

// AcceptedByKind returns accepted deliveries of a specific message kind.
func (c *Cluster) AcceptedByKind(kind MessageKind) int {
	count := 0
	for _, d := range c.Deliveries {
		if d.Accepted && d.Msg.Kind == kind {
			count++
		}
	}
	return count
}

func (c *Cluster) enqueue(msg Message, deliverAt uint64) {
	c.Queue = append(c.Queue, inFlightMessage{deliverAt: deliverAt, msg: msg})
}

func (c *Cluster) Tick() {
	c.Now++
	current := append([]inFlightMessage(nil), c.Queue...)
	c.Queue = nil
	remaining := make([]inFlightMessage, 0, len(current))
	for _, item := range current {
		if item.deliverAt > c.Now {
			remaining = append(remaining, item)
			continue
		}
		if !c.deliver(item.msg) {
			remaining = append(remaining, item)
		}
	}
	c.Queue = append(c.Queue, remaining...)
	c.fireTimeouts() // eventsim: fire timeouts AFTER message delivery (data before timers)
	c.refreshCommits()
}

func (c *Cluster) TickN(n int) {
	for i := 0; i < n; i++ {
		c.Tick()
	}
}

func (c *Cluster) deliver(msg Message) bool {
	from := c.Nodes[msg.From]
	to := c.Nodes[msg.To]
	if from == nil || to == nil || !from.Running || !to.Running {
		c.recordDelivery(msg, false, RejectNodeDown)
		return true
	}
	if msg.Epoch != c.Coordinator.Epoch || to.Epoch != c.Coordinator.Epoch {
		c.recordDelivery(msg, false, RejectEpochMismatch)
		return true
	}
	if !c.Links[msg.From][msg.To] {
		c.recordDelivery(msg, false, RejectLinkDown)
		return true
	}
	// Endpoint match: primary→replica messages fail if cached endpoint is stale.
	if c.CachedReplicaEndpoints != nil && msg.From == c.Coordinator.PrimaryID {
		if cached, ok := c.CachedReplicaEndpoints[msg.To]; ok {
			if cached.EndpointVersion != to.EndpointVersion {
				c.recordDelivery(msg, false, RejectStaleEndpoint)
				return true
			}
		}
	}

	c.recordDelivery(msg, true, "")

	switch msg.Kind {
	case MsgWrite:
		to.Storage.AppendWrite(msg.Write)
	case MsgBarrier:
		if to.Storage.ReceivedLSN >= msg.TargetLSN {
			to.Storage.AdvanceFlush(msg.TargetLSN)
			c.enqueue(Message{
				Kind:      MsgBarrierAck,
				From:      to.ID,
				To:        msg.From,
				Epoch:     msg.Epoch,
				TargetLSN: msg.TargetLSN,
			}, c.Now+1)
		} else {
			c.enqueue(msg, c.Now+1)
		}
	case MsgBarrierAck:
		// Reject late ack if the barrier instance already timed out.
		if c.ExpiredBarriers[barrierExpiredKey{msg.From, msg.TargetLSN}] {
			c.recordDelivery(msg, false, RejectBarrierExpired)
			return true
		}
		if pending := c.Pending[msg.TargetLSN]; pending != nil {
			pending.DurableOn[msg.From] = true
		}
		c.CancelTimeout(TimeoutBarrier, msg.From, msg.TargetLSN)
	}
	return true
}

func (c *Cluster) refreshCommits() {
	lsns := make([]uint64, 0, len(c.Pending))
	for lsn := range c.Pending {
		lsns = append(lsns, lsn)
	}
	sort.Slice(lsns, func(i, j int) bool { return lsns[i] < lsns[j] })
	for _, lsn := range lsns {
		p := c.Pending[lsn]
		if !p.Committed && c.commitSatisfied(p) {
			p.Committed = true
			p.CommittedAt = c.Now
		}
	}

	for {
		next := c.Coordinator.CommittedLSN + 1
		p := c.Pending[next]
		if p == nil || !p.Committed {
			break
		}
		c.Coordinator.CommittedLSN = next
	}
}

func (c *Cluster) Promote(newPrimaryID string) error {
	newPrimary := c.Nodes[newPrimaryID]
	if newPrimary == nil || !newPrimary.Running {
		return fmt.Errorf("distsim: cannot promote missing/down node %s", newPrimaryID)
	}
	oldPrimary := c.Primary()
	c.Coordinator.Epoch++
	c.Coordinator.PrimaryID = newPrimaryID
	for _, n := range c.Nodes {
		if n == nil || !n.Running {
			continue
		}
		n.Epoch = c.Coordinator.Epoch
		n.Role = RoleReplica
	}
	newPrimary.Role = RolePrimary
	if oldPrimary != nil && oldPrimary.ID != newPrimaryID {
		oldPrimary.Role = RoleReplica
	}
	// Epoch bump invalidates all active recovery sessions.
	c.InvalidateAllSessions("epoch_bump_promotion")
	// Refresh cached endpoints for new primary's view of replicas.
	c.CachedReplicaEndpoints = map[string]CachedEndpoint{}
	for _, id := range c.Coordinator.Members {
		if id == newPrimaryID {
			continue
		}
		if n := c.Nodes[id]; n != nil {
			c.CachedReplicaEndpoints[id] = CachedEndpoint{
				DataAddr:        n.DataAddr,
				EndpointVersion: n.EndpointVersion,
			}
		}
	}
	return nil
}

func (c *Cluster) RecoverReplicaFromPrimary(replicaID string, startExclusive, endInclusive uint64) error {
	primary := c.Primary()
	replica := c.Nodes[replicaID]
	if primary == nil || replica == nil {
		return fmt.Errorf("distsim: missing primary or replica")
	}
	if !primary.Running || !replica.Running {
		return fmt.Errorf("distsim: primary or replica not running")
	}
	for _, w := range writesInRange(primary.Storage.WAL, startExclusive, endInclusive) {
		replica.Storage.AppendWrite(w)
	}
	replica.Storage.AdvanceFlush(endInclusive)
	return nil
}

func (c *Cluster) RecoverReplicaFromPrimaryPartial(replicaID string, startExclusive, endInclusive uint64, maxWrites int) (uint64, error) {
	primary := c.Primary()
	replica := c.Nodes[replicaID]
	if primary == nil || replica == nil {
		return startExclusive, fmt.Errorf("distsim: missing primary or replica")
	}
	if !primary.Running || !replica.Running {
		return startExclusive, fmt.Errorf("distsim: primary or replica not running")
	}
	lastRecovered := startExclusive
	applied := 0
	for _, w := range writesInRange(primary.Storage.WAL, startExclusive, endInclusive) {
		if applied >= maxWrites {
			break
		}
		replica.Storage.AppendWrite(w)
		lastRecovered = w.LSN
		applied++
	}
	replica.Storage.AdvanceFlush(lastRecovered)
	return lastRecovered, nil
}

func (c *Cluster) RecoverReplicaFromPrimaryReserved(replicaID string, startExclusive, endInclusive, reservationExpiry uint64) error {
	primary := c.Primary()
	replica := c.Nodes[replicaID]
	if primary == nil || replica == nil {
		return fmt.Errorf("distsim: missing primary or replica")
	}
	if !primary.Running || !replica.Running {
		return fmt.Errorf("distsim: primary or replica not running")
	}
	lastRecovered := startExclusive
	for _, w := range writesInRange(primary.Storage.WAL, startExclusive, endInclusive) {
		if c.Now >= reservationExpiry {
			replica.Storage.AdvanceFlush(lastRecovered)
			return fmt.Errorf("distsim: recovery reservation expired at time %d before LSN %d", c.Now, w.LSN)
		}
		replica.Storage.AppendWrite(w)
		lastRecovered = w.LSN
		c.Tick()
	}
	replica.Storage.AdvanceFlush(lastRecovered)
	if lastRecovered != endInclusive {
		return fmt.Errorf("distsim: incomplete recovery range, got %d want %d", lastRecovered, endInclusive)
	}
	return nil
}

func (c *Cluster) RebuildReplicaFromSnapshot(replicaID, snapshotID string, targetLSN uint64) error {
	primary := c.Primary()
	replica := c.Nodes[replicaID]
	if primary == nil || replica == nil {
		return fmt.Errorf("distsim: missing primary or replica")
	}
	snap, ok := primary.Storage.Snapshots[snapshotID]
	if !ok {
		return fmt.Errorf("distsim: snapshot %s missing", snapshotID)
	}
	replica.Storage.LoadSnapshot(snap)
	for _, w := range writesInRange(primary.Storage.WAL, snap.LSN, targetLSN) {
		replica.Storage.AppendWrite(w)
	}
	replica.Storage.AdvanceFlush(targetLSN)
	return nil
}

func (c *Cluster) RebuildReplicaFromSnapshotPartial(replicaID, snapshotID string, targetLSN uint64, maxWrites int) (uint64, error) {
	primary := c.Primary()
	replica := c.Nodes[replicaID]
	if primary == nil || replica == nil {
		return 0, fmt.Errorf("distsim: missing primary or replica")
	}
	snap, ok := primary.Storage.Snapshots[snapshotID]
	if !ok {
		return 0, fmt.Errorf("distsim: snapshot %s missing", snapshotID)
	}
	replica.Storage.LoadSnapshot(snap)
	lastRecovered := snap.LSN
	applied := 0
	for _, w := range writesInRange(primary.Storage.WAL, snap.LSN, targetLSN) {
		if applied >= maxWrites {
			break
		}
		replica.Storage.AppendWrite(w)
		lastRecovered = w.LSN
		applied++
	}
	replica.Storage.AdvanceFlush(lastRecovered)
	return lastRecovered, nil
}

func (c *Cluster) InjectMessage(msg Message, deliverAt uint64) {
	c.enqueue(msg, deliverAt)
}

func FullyRecoverable(records []RecoveryRecord) bool {
	for _, r := range records {
		switch r.Class {
		case RecoveryClassWALInline:
			continue
		case RecoveryClassExtentReferenced:
			if !r.PayloadResolvable {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func ApplyRecoveryRecords(records []RecoveryRecord, startExclusive, endInclusive uint64) map[uint64]uint64 {
	state := map[uint64]uint64{}
	for _, r := range records {
		if r.LSN <= startExclusive {
			continue
		}
		if r.LSN > endInclusive {
			break
		}
		state[r.Block] = r.Value
	}
	return state
}

// PromotionCandidate represents a replica's suitability for primary promotion.
type PromotionCandidate struct {
	ID         string
	FlushedLSN uint64
	State      ReplicaNodeState
	Running    bool
}

// statePromotionRank returns priority rank for promotion. Lower = better.
func statePromotionRank(s ReplicaNodeState) int {
	switch s {
	case NodeStateInSync:
		return 0
	case NodeStateCatchingUp:
		return 1
	case NodeStateLagging:
		return 2
	case NodeStateRebuilding:
		return 3
	case NodeStateNeedsRebuild:
		return 4
	default:
		return 5
	}
}

// PromotionCandidates returns all replicas ranked by promotion suitability.
// Ranking: Running first, then by state rank (InSync > CatchingUp > ... > NeedsRebuild),
// then by FlushedLSN descending, then by ID (alphabetical tie-break).
func (c *Cluster) PromotionCandidates() []PromotionCandidate {
	var candidates []PromotionCandidate
	for _, id := range c.replicaIDs() {
		n := c.Nodes[id]
		if n == nil {
			continue
		}
		candidates = append(candidates, PromotionCandidate{
			ID:         id,
			FlushedLSN: n.Storage.FlushedLSN,
			State:      n.ReplicaState,
			Running:    n.Running,
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		ci, cj := candidates[i], candidates[j]
		if ci.Running != cj.Running {
			return ci.Running
		}
		ri, rj := statePromotionRank(ci.State), statePromotionRank(cj.State)
		if ri != rj {
			return ri < rj
		}
		if ci.FlushedLSN != cj.FlushedLSN {
			return ci.FlushedLSN > cj.FlushedLSN
		}
		return ci.ID < cj.ID
	})
	return candidates
}

// BestPromotionCandidate returns the best eligible candidate.
// Uses EvaluateCandidateEligibility: must be running, epoch-aligned,
// state-eligible (not NeedsRebuild/Rebuilding), and have flushed data.
// Returns "" if no eligible candidate exists.
func (c *Cluster) BestPromotionCandidate() string {
	eligible := c.EligiblePromotionCandidates()
	if len(eligible) == 0 {
		return ""
	}
	return eligible[0].ID
}

// BestPromotionCandidateDesperate returns the best running candidate regardless
// of state. Use only as explicit fallback when no safe candidate exists and
// availability is prioritized over consistency.
func (c *Cluster) BestPromotionCandidateDesperate() string {
	candidates := c.PromotionCandidates()
	if len(candidates) == 0 || !candidates[0].Running {
		return ""
	}
	return candidates[0].ID
}

// === Endpoint lifecycle ===

// RestartNodeWithNewAddress simulates a node restarting on a different address.
// The EndpointVersion bumps, making the primary's cached endpoint stale.
// Messages from primary to this node will be rejected as RejectStaleEndpoint
// until the control-plane flow updates the cached endpoint.
func (c *Cluster) RestartNodeWithNewAddress(id string) {
	n := c.Nodes[id]
	if n == nil {
		return
	}
	n.Running = true
	n.Epoch = c.Coordinator.Epoch
	n.EndpointVersion++
	n.DataAddr = fmt.Sprintf("%s:v%d", id, n.EndpointVersion)
	// Endpoint change invalidates any active session for this replica.
	c.InvalidateReplicaSession(id, "endpoint_changed")
}

// === Control-plane flow: heartbeat → detect → assignment → trigger ===

// HeartbeatReport represents what a node reports to the coordinator.
type HeartbeatReport struct {
	NodeID          string
	DataAddr        string
	EndpointVersion uint64
	FlushedLSN      uint64
	State           ReplicaNodeState
	Running         bool
}

// ReportHeartbeat returns the current heartbeat for a node.
func (c *Cluster) ReportHeartbeat(nodeID string) HeartbeatReport {
	n := c.Nodes[nodeID]
	if n == nil {
		return HeartbeatReport{NodeID: nodeID}
	}
	return HeartbeatReport{
		NodeID:          nodeID,
		DataAddr:        n.DataAddr,
		EndpointVersion: n.EndpointVersion,
		FlushedLSN:      n.Storage.FlushedLSN,
		State:           n.ReplicaState,
		Running:         n.Running,
	}
}

// AssignmentUpdate represents a master-driven update to the primary's replica view.
type AssignmentUpdate struct {
	TargetNodeID     string                   // which primary receives this
	UpdatedEndpoints map[string]CachedEndpoint // new cached endpoints
}

// CoordinatorDetectEndpointChange checks if a heartbeat reveals an address change.
// Returns an assignment update if the endpoint version differs from cached, nil otherwise.
func (c *Cluster) CoordinatorDetectEndpointChange(report HeartbeatReport) *AssignmentUpdate {
	cached, ok := c.CachedReplicaEndpoints[report.NodeID]
	if ok && cached.EndpointVersion == report.EndpointVersion {
		return nil
	}
	updated := map[string]CachedEndpoint{}
	for id, ep := range c.CachedReplicaEndpoints {
		updated[id] = ep
	}
	updated[report.NodeID] = CachedEndpoint{
		DataAddr:        report.DataAddr,
		EndpointVersion: report.EndpointVersion,
	}
	return &AssignmentUpdate{
		TargetNodeID:     c.Coordinator.PrimaryID,
		UpdatedEndpoints: updated,
	}
}

// ApplyAssignmentUpdate overwrites the cluster-global cached replica endpoints.
// The current model has a single primary view (CachedReplicaEndpoints is
// cluster-level, not per-node). TargetNodeID is carried for API shape but
// is not used for routing — the update is always applied to the global cache.
func (c *Cluster) ApplyAssignmentUpdate(update AssignmentUpdate) {
	c.CachedReplicaEndpoints = update.UpdatedEndpoints
}

// === Recovery session ownership ===

// RecoveryTrigger identifies how a recovery session was initiated.
type RecoveryTrigger string

const (
	TriggerNone               RecoveryTrigger = "none"
	TriggerBackgroundReconnect RecoveryTrigger = "background_reconnect" // V1.5
	TriggerReassignment        RecoveryTrigger = "reassignment"         // V2
)

// TrackedSession is an explicitly identified recovery session. Each replica
// has at most one active session. Sessions are invalidated by epoch bump,
// endpoint change, or explicit supersede. Stale session completions are
// rejected by ID.
type TrackedSession struct {
	ID        uint64
	ReplicaID string
	Epoch     uint64
	Trigger   RecoveryTrigger
	Active    bool
	Reason    string // non-empty when invalidated
}

// TriggerRecoverySession initiates a version-specific recovery session with
// explicit identity tracking. Rejects duplicate triggers while a session is
// active. Returns (trigger, sessionID, ok).
func (c *Cluster) TriggerRecoverySession(replicaID string) (RecoveryTrigger, uint64, bool) {
	node := c.Nodes[replicaID]
	if node == nil || !node.Running {
		return TriggerNone, 0, false
	}
	// Reject duplicate trigger while session active.
	if existing := c.Sessions[replicaID]; existing != nil && existing.Active {
		return TriggerNone, 0, false
	}
	cached := c.CachedReplicaEndpoints[replicaID]
	addrStable := cached.EndpointVersion == node.EndpointVersion

	var trigger RecoveryTrigger
	switch c.Protocol.Version {
	case ProtocolV1:
		return TriggerNone, 0, false
	case ProtocolV15:
		if !addrStable {
			return TriggerNone, 0, false
		}
		trigger = TriggerBackgroundReconnect
	case ProtocolV2:
		if !addrStable {
			return TriggerNone, 0, false
		}
		trigger = TriggerReassignment
	default:
		return TriggerNone, 0, false
	}

	c.nextSessionID++
	sess := &TrackedSession{
		ID:        c.nextSessionID,
		ReplicaID: replicaID,
		Epoch:     c.Coordinator.Epoch,
		Trigger:   trigger,
		Active:    true,
	}
	c.Sessions[replicaID] = sess
	c.SessionHistory = append(c.SessionHistory, sess)

	node.ReplicaState = NodeStateCatchingUp
	node.CatchupAttempts = 0
	return trigger, sess.ID, true
}

// CompleteRecoverySession marks the session as completed and transitions the
// replica to InSync. Returns false if the session ID doesn't match the active
// session (stale completion from an old/superseded session).
func (c *Cluster) CompleteRecoverySession(replicaID string, sessionID uint64) bool {
	sess := c.Sessions[replicaID]
	if sess == nil || !sess.Active || sess.ID != sessionID {
		return false // stale: old session, already completed, or wrong ID
	}
	sess.Active = false
	if node := c.Nodes[replicaID]; node != nil {
		node.ReplicaState = NodeStateInSync
		node.CatchupAttempts = 0
	}
	c.cancelRecoveryTimeouts(replicaID)
	return true
}

// InvalidateReplicaSession invalidates the active session for a replica.
func (c *Cluster) InvalidateReplicaSession(replicaID, reason string) {
	if sess := c.Sessions[replicaID]; sess != nil && sess.Active {
		sess.Active = false
		sess.Reason = reason
	}
}

// InvalidateAllSessions invalidates all active sessions (e.g., epoch bump).
func (c *Cluster) InvalidateAllSessions(reason string) int {
	count := 0
	for _, sess := range c.Sessions {
		if sess.Active {
			sess.Active = false
			sess.Reason = reason
			count++
		}
	}
	return count
}

// === Candidate eligibility ===

// CandidateEligibility describes why a node is or is not eligible for promotion.
type CandidateEligibility struct {
	ID       string
	Eligible bool
	Reasons  []string // non-empty when ineligible
}

// EvaluateCandidateEligibility checks all promotion prerequisites for a node.
// A candidate must have the full committed prefix (FlushedLSN >= CommittedLSN)
// to be eligible. Promoting a replica that is missing committed data would
// lose acknowledged writes.
func (c *Cluster) EvaluateCandidateEligibility(candidateID string) CandidateEligibility {
	n := c.Nodes[candidateID]
	if n == nil {
		return CandidateEligibility{ID: candidateID, Reasons: []string{"not_found"}}
	}
	var reasons []string
	if !n.Running {
		reasons = append(reasons, "not_running")
	}
	if n.Epoch != c.Coordinator.Epoch {
		reasons = append(reasons, "epoch_misaligned")
	}
	if n.ReplicaState == NodeStateNeedsRebuild || n.ReplicaState == NodeStateRebuilding {
		reasons = append(reasons, "state_ineligible")
	}
	if n.Storage.FlushedLSN < c.Coordinator.CommittedLSN {
		reasons = append(reasons, "insufficient_committed_prefix")
	}
	return CandidateEligibility{
		ID:       candidateID,
		Eligible: len(reasons) == 0,
		Reasons:  reasons,
	}
}

// EligiblePromotionCandidates returns only eligible candidates, ranked by suitability.
func (c *Cluster) EligiblePromotionCandidates() []PromotionCandidate {
	var eligible []PromotionCandidate
	for _, pc := range c.PromotionCandidates() {
		e := c.EvaluateCandidateEligibility(pc.ID)
		if e.Eligible {
			eligible = append(eligible, pc)
		}
	}
	return eligible
}

func (c *Cluster) AssertCommittedRecoverable(nodeID string) error {
	node := c.Nodes[nodeID]
	if node == nil {
		return fmt.Errorf("distsim: missing node %s", nodeID)
	}
	want := c.Reference.StateAt(c.Coordinator.CommittedLSN)
	got := node.Storage.StateAt(c.Coordinator.CommittedLSN)
	if !EqualState(got, want) {
		return fmt.Errorf("distsim: node %s mismatch at committed LSN %d: got=%v want=%v", nodeID, c.Coordinator.CommittedLSN, got, want)
	}
	return nil
}
