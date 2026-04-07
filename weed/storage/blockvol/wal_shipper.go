package blockvol

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrReplicaDegraded = errors.New("blockvol: replica degraded")
	ErrShipperStopped  = errors.New("blockvol: shipper stopped")
)

const barrierTimeout = 5 * time.Second

// ReplicaState tracks the replication state machine for one replica.
// Only InSync replicas are eligible for sync_all barrier participation.
type ReplicaState uint32

const (
	ReplicaDisconnected ReplicaState = 0 // no session (initial state)
	ReplicaConnecting   ReplicaState = 1 // socket open, handshake pending (CP13-5)
	ReplicaCatchingUp   ReplicaState = 2 // connected, replaying missed WAL (CP13-5)
	ReplicaInSync       ReplicaState = 3 // eligible for sync_all barriers
	ReplicaDegraded     ReplicaState = 4 // transient failure, retry allowed
	ReplicaNeedsRebuild ReplicaState = 5 // WAL gap too large, rebuild required (CP13-7)
)

func (s ReplicaState) String() string {
	switch s {
	case ReplicaDisconnected:
		return "disconnected"
	case ReplicaConnecting:
		return "connecting"
	case ReplicaCatchingUp:
		return "catching_up"
	case ReplicaInSync:
		return "in_sync"
	case ReplicaDegraded:
		return "degraded"
	case ReplicaNeedsRebuild:
		return "needs_rebuild"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// WALShipper streams WAL entries from the primary to a replica over TCP.
// Fire-and-forget: no per-entry ACK. Barriers provide durability confirmation.
type WALShipper struct {
	dataAddr    string
	controlAddr string
	replicaID   string
	epochFn     func() uint64
	wal         WALAccess // primary WAL access for reconnect catch-up
	metrics     *EngineMetrics

	mu       sync.Mutex // protects dataConn
	dataConn net.Conn

	ctrlMu   sync.Mutex // protects ctrlConn
	ctrlConn net.Conn

	shippedLSN         atomic.Uint64 // diagnostic: highest LSN sent to TCP socket
	replicaFlushedLSN  atomic.Uint64 // authoritative: highest LSN durably persisted on replica
	hasFlushedProgress atomic.Bool   // true once replica returns a valid (non-zero) FlushedLSN
	state              atomic.Uint32 // ReplicaState
	catchupFailures    int           // consecutive catch-up failures; reset on success
	lastContactTime    atomic.Value  // time.Time: last successful barrier/handshake/catch-up
	stopped            atomic.Bool

	// onStateChange is called when the shipper transitions between states.
	// Used to trigger immediate heartbeat on degradation/recovery.
	// Set via SetOnStateChange. Nil = no callback.
	onStateChange func(from, to ReplicaState)

	// onBarrierFailure reports the semantic reason for a failed barrier attempt.
	// Used by the host to surface bounded durability failures into diagnostics/core.
	onBarrierFailure func(reason string)

	// liveShippingPolicy gates whether this shipper may accept current live-tail
	// WAL entries. The host uses this to keep a replica in bounded catch-up until
	// the active session contract allows live streaming again.
	liveShippingPolicy func(replicaID string, entryLSN uint64) (allow bool, reason string)
}

// SetOnStateChange registers a callback for shipper state transitions.
// The callback is invoked synchronously from markDegraded/markInSync.
func (s *WALShipper) SetOnStateChange(fn func(from, to ReplicaState)) {
	s.onStateChange = fn
}

// SetOnBarrierFailure registers a callback for failed barrier attempts.
func (s *WALShipper) SetOnBarrierFailure(fn func(reason string)) {
	s.onBarrierFailure = fn
}

// SetReplicaID sets the stable replica identity carried from the host-side
// session contract. When empty, transport-level behavior still works but
// protocol-aware gating cannot make per-replica decisions.
func (s *WALShipper) SetReplicaID(replicaID string) {
	s.replicaID = replicaID
}

// ReplicaID returns the stable replica identity set via SetReplicaID.
func (s *WALShipper) ReplicaID() string {
	return s.replicaID
}

// SetLiveShippingPolicy installs a host-provided gate for current live-tail
// shipping. The callback is consulted before any network dial or send occurs.
func (s *WALShipper) SetLiveShippingPolicy(fn func(replicaID string, entryLSN uint64) (allow bool, reason string)) {
	s.liveShippingPolicy = fn
}

const maxCatchupRetries = 3

// NewWALShipper creates a WAL shipper. Connections are established lazily on
// first Ship/Barrier call. epochFn returns the current epoch for validation.
// wal provides WAL access for reconnect catch-up (nil disables catch-up).
// metrics is optional; if nil, no metrics are recorded.
func NewWALShipper(dataAddr, controlAddr string, epochFn func() uint64, walAccess WALAccess, metrics ...*EngineMetrics) *WALShipper {
	var m *EngineMetrics
	if len(metrics) > 0 {
		m = metrics[0]
	}
	return &WALShipper{
		dataAddr:    dataAddr,
		controlAddr: controlAddr,
		epochFn:     epochFn,
		wal:         walAccess,
		metrics:     m,
	}
}

// Ship sends a WAL entry to the replica over the data channel.
// On write error, the shipper enters degraded mode. Recovery requires
// the full reconnect protocol. See design/sync-all-reconnect-protocol.md.
func (s *WALShipper) Ship(entry *WALEntry) error {
	st := s.State()
	// Ship allowed from Disconnected (bootstrap: data must flow before first barrier)
	// and InSync (steady state). All other states reject.
	if s.stopped.Load() || (st != ReplicaInSync && st != ReplicaDisconnected) {
		return nil
	}
	if s.liveShippingPolicy != nil {
		if allow, reason := s.liveShippingPolicy(s.replicaID, entry.LSN); !allow {
			if reason == "" {
				reason = "live_shipping_blocked"
			}
			log.Printf("wal_shipper: live ship gated (replica=%s data=%s ctrl=%s lsn=%d reason=%s)",
				s.replicaID, s.dataAddr, s.controlAddr, entry.LSN, reason)
			return nil
		}
	}
	// Fresh or late-attached replicas must consume the retained backlog before
	// receiving a live-tail entry. This closes the LSN-gap path where the first
	// post-attach live write would otherwise arrive before the retained prefix.
	if st == ReplicaDisconnected && s.wal != nil && entry.LSN > 1 {
		if _, err := s.CatchUpTo(entry.LSN - 1); err != nil {
			log.Printf("wal_shipper: bounded catch-up before live ship failed (replica=%s data=%s ctrl=%s target=%d): %v",
				s.replicaID, s.dataAddr, s.controlAddr, entry.LSN-1, err)
			return nil
		}
	}
	if st == ReplicaDisconnected && s.shippedLSN.Load() == 0 {
		log.Printf("wal_shipper: bootstrap ship attempt (data=%s, ctrl=%s, lsn=%d, epoch=%d)",
			s.dataAddr, s.controlAddr, entry.LSN, entry.Epoch)
	}

	// Validate epoch: drop stale entries.
	if entry.Epoch != s.epochFn() {
		log.Printf("wal_shipper: dropping entry LSN=%d with stale epoch %d (current %d)",
			entry.LSN, entry.Epoch, s.epochFn())
		return nil
	}

	encoded, err := entry.Encode()
	if err != nil {
		return fmt.Errorf("wal_shipper: encode entry: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureDataConn(); err != nil {
		log.Printf("wal_shipper: data channel connect failed (data=%s, ctrl=%s, lsn=%d): %v",
			s.dataAddr, s.controlAddr, entry.LSN, err)
		s.markDegraded()
		return nil
	}

	// Set a write deadline so we don't block for the full TCP
	// retransmission timeout (~120s) if the replica is dead.
	s.dataConn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := WriteFrame(s.dataConn, MsgWALEntry, encoded); err != nil {
		s.dataConn.SetWriteDeadline(time.Time{}) // clear deadline
		s.markDegraded()
		return nil
	}
	s.dataConn.SetWriteDeadline(time.Time{}) // clear deadline

	s.shippedLSN.Store(entry.LSN)
	if s.metrics != nil {
		s.metrics.RecordWALShipped()
	}
	return nil
}

// CatchUpTo performs bounded WAL replay for one replica up to targetLSN before
// live tail resumes. It uses the replica's handshake-reported durable boundary
// as the authoritative replay start and never replays beyond targetLSN.
func (s *WALShipper) CatchUpTo(targetLSN uint64) (uint64, error) {
	if s.stopped.Load() {
		return 0, ErrShipperStopped
	}
	if s.wal == nil || targetLSN == 0 {
		return 0, nil
	}

	log.Printf("wal_shipper: catch-up start replica=%s target_lsn=%d state=%s flushed_lsn=%d data=%s ctrl=%s",
		s.replicaID, targetLSN, s.State(), s.replicaFlushedLSN.Load(), s.dataAddr, s.controlAddr)

	targetState, replicaFlushedLSN, err := s.reconnectWithHandshake()
	switch targetState {
	case ReplicaInSync:
		s.markInSync()
		s.resetCtrlConn()
		log.Printf("wal_shipper: catch-up not needed replica=%s handshake_state=%s replica_flushed=%d target_lsn=%d",
			s.replicaID, targetState, replicaFlushedLSN, targetLSN)
		if replicaFlushedLSN > targetLSN {
			return targetLSN, nil
		}
		return replicaFlushedLSN, nil
	case ReplicaCatchingUp:
		achievedLSN, catchErr := s.runCatchUpTo(replicaFlushedLSN, targetLSN)
		if catchErr != nil {
			log.Printf("wal_shipper: catch-up failed replica=%s from_lsn=%d target_lsn=%d err=%v",
				s.replicaID, replicaFlushedLSN, targetLSN, catchErr)
			s.catchupFailures++
			if s.catchupFailures >= maxCatchupRetries {
				s.state.Store(uint32(ReplicaNeedsRebuild))
				return achievedLSN, fmt.Errorf("catch-up failed %d times: %w", s.catchupFailures, catchErr)
			}
			s.markDegraded()
			return achievedLSN, ErrReplicaDegraded
		}
		s.markInSync()
		s.resetCtrlConn()
		log.Printf("wal_shipper: catch-up complete replica=%s achieved_lsn=%d target_lsn=%d",
			s.replicaID, achievedLSN, targetLSN)
		return achievedLSN, nil
	case ReplicaNeedsRebuild:
		s.state.Store(uint32(ReplicaNeedsRebuild))
		log.Printf("wal_shipper: catch-up escalated to rebuild replica=%s target_lsn=%d err=%v",
			s.replicaID, targetLSN, err)
		return replicaFlushedLSN, fmt.Errorf("reconnect: %w", err)
	default:
		s.markDegraded()
		log.Printf("wal_shipper: catch-up left replica degraded replica=%s target_lsn=%d err=%v",
			s.replicaID, targetLSN, err)
		if err != nil {
			return replicaFlushedLSN, err
		}
		return replicaFlushedLSN, ErrReplicaDegraded
	}
}

// Barrier sends a barrier request on the control channel and waits for the
// replica to confirm durability up to lsnMax. Returns ErrReplicaDegraded if
// the shipper is in degraded mode. Reconnection requires the full reconnect
// protocol (ResumeShipReq handshake + WAL catch-up), not just TCP retry.
// See design/sync-all-reconnect-protocol.md.
func (s *WALShipper) Barrier(lsnMax uint64) error {
	if s.stopped.Load() {
		return ErrShipperStopped
	}

	st := s.State()
	log.Printf("wal_shipper: barrier start replica=%s state=%s target_lsn=%d flushed_lsn=%d has_progress=%v data=%s ctrl=%s",
		s.replicaID, st, lsnMax, s.replicaFlushedLSN.Load(), s.hasFlushedProgress.Load(), s.dataAddr, s.controlAddr)
	switch st {
	case ReplicaInSync:
		// proceed normally to barrier
	case ReplicaDisconnected, ReplicaDegraded:
		if s.wal != nil && lsnMax > 0 {
			// Integrated bootstrap case: writes may have accumulated before the
			// shipper was configured. Replaying the retained prefix up to the
			// barrier target closes the "late-configured first fsync" gap.
			log.Printf("wal_shipper: barrier recovery via bounded catch-up replica=%s state=%s target_lsn=%d",
				s.replicaID, st, lsnMax)
			if _, err := s.CatchUpTo(lsnMax); err != nil {
				log.Printf("wal_shipper: barrier recovery catch-up failed replica=%s target_lsn=%d err=%v",
					s.replicaID, lsnMax, err)
				return err
			}
		} else if s.hasFlushedProgress.Load() && s.wal != nil {
			// Previously synced — reconnect handshake + catch-up path.
			log.Printf("wal_shipper: barrier recovery via reconnect replica=%s state=%s target_lsn=%d",
				s.replicaID, st, lsnMax)
			if err := s.doReconnectAndCatchUp(); err != nil {
				log.Printf("wal_shipper: barrier reconnect failed replica=%s target_lsn=%d err=%v",
					s.replicaID, lsnMax, err)
				return err
			}
		} else {
			// Fresh bootstrap with no retained target — reset connections for bare retry.
			log.Printf("wal_shipper: barrier reset connections for bootstrap retry replica=%s state=%s",
				s.replicaID, st)
			s.resetConnections()
		}
	default:
		// Connecting, CatchingUp, NeedsRebuild — reject immediately
		log.Printf("wal_shipper: barrier rejected replica=%s state=%s target_lsn=%d reason=state_not_ready",
			s.replicaID, st, lsnMax)
		return ErrReplicaDegraded
	}

	barrierStart := time.Now()

	req := EncodeBarrierRequest(BarrierRequest{
		LSN:   lsnMax,
		Epoch: s.epochFn(),
	})

	s.ctrlMu.Lock()
	defer s.ctrlMu.Unlock()

	if err := s.ensureCtrlConn(); err != nil {
		return s.failBarrier("barrier_ctrl_connect_failed", barrierStart, ErrReplicaDegraded)
	}

	s.ctrlConn.SetDeadline(time.Now().Add(barrierTimeout))

	if err := WriteFrame(s.ctrlConn, MsgBarrierReq, req); err != nil {
		return s.failBarrier("barrier_req_write_failed", barrierStart, ErrReplicaDegraded)
	}

	msgType, payload, err := ReadFrame(s.ctrlConn)
	if err != nil {
		return s.failBarrier("barrier_resp_read_failed", barrierStart, ErrReplicaDegraded)
	}

	if msgType != MsgBarrierResp || len(payload) < 1 {
		return s.failBarrier("barrier_bad_response", barrierStart, ErrReplicaDegraded)
	}

	resp := DecodeBarrierResponse(payload)

	switch resp.Status {
	case BarrierOK:
		// CP13-3: BarrierOK with FlushedLSN == 0 means the replica confirmed
		// receipt + fsync but did not report which LSN is durable (legacy 1-byte
		// response). This must NOT count as successful sync_all durability because
		// no authoritative durable progress was established.
		if resp.FlushedLSN == 0 {
			return s.failBarrier("barrier_missing_flushed_lsn", barrierStart,
				fmt.Errorf("wal_shipper: barrier OK but no FlushedLSN reported (legacy response)"))
		}
		// Barrier success with durable progress — transition to InSync.
		s.markInSync()
		// Update authoritative durable progress (monotonic: only advance).
		s.hasFlushedProgress.Store(true)
		for {
			cur := s.replicaFlushedLSN.Load()
			if resp.FlushedLSN <= cur {
				break
			}
			if s.replicaFlushedLSN.CompareAndSwap(cur, resp.FlushedLSN) {
				break
			}
		}
		s.recordBarrierMetric(barrierStart, false)
		log.Printf("wal_shipper: barrier success replica=%s target_lsn=%d flushed_lsn=%d",
			s.replicaID, lsnMax, resp.FlushedLSN)
		return nil
	case BarrierEpochMismatch:
		return s.failBarrier("barrier_epoch_mismatch", barrierStart,
			fmt.Errorf("wal_shipper: barrier epoch mismatch"))
	case BarrierTimeout:
		return s.failBarrier("barrier_timeout", barrierStart,
			fmt.Errorf("wal_shipper: barrier timeout on replica"))
	case BarrierFsyncFailed:
		return s.failBarrier("barrier_fsync_failed", barrierStart,
			fmt.Errorf("wal_shipper: barrier fsync failed on replica"))
	default:
		return s.failBarrier("barrier_unknown_status", barrierStart,
			fmt.Errorf("wal_shipper: unknown barrier status %d", payload[0]))
	}
}

func (s *WALShipper) recordBarrierMetric(start time.Time, failed bool) {
	if s.metrics != nil {
		s.metrics.RecordWALBarrier(time.Since(start), failed)
	}
}

func (s *WALShipper) notifyBarrierFailure(reason string) {
	if s.onBarrierFailure != nil {
		s.onBarrierFailure(reason)
	}
}

func (s *WALShipper) failBarrier(reason string, start time.Time, err error) error {
	s.markDegraded()
	s.recordBarrierMetric(start, true)
	s.notifyBarrierFailure(reason)
	log.Printf("wal_shipper: barrier failed replica=%s reason=%s target_flushed=%d err=%v data=%s ctrl=%s",
		s.replicaID, reason, s.replicaFlushedLSN.Load(), err, s.dataAddr, s.controlAddr)
	return err
}

// ShippedLSN returns the highest LSN successfully sent to the replica (diagnostic only).
// This is NOT authoritative for sync durability — use ReplicaFlushedLSN() instead.
func (s *WALShipper) ShippedLSN() uint64 {
	return s.shippedLSN.Load()
}

// ReplicaFlushedLSN returns the highest LSN durably persisted on the replica,
// as reported in the barrier response after fd.Sync(). This is the authoritative
// durable progress variable for sync_all correctness.
func (s *WALShipper) ReplicaFlushedLSN() uint64 {
	return s.replicaFlushedLSN.Load()
}

// HasFlushedProgress returns true if the replica has ever reported a valid
// (non-zero) FlushedLSN. Legacy replicas that only support 1-byte barrier
// responses will never set this, and must not count toward sync_all.
func (s *WALShipper) HasFlushedProgress() bool {
	return s.hasFlushedProgress.Load()
}

// HasTransportContact reports whether this shipper has established enough
// transport contact to treat the replication path as connected for bootstrap
// observability, even before barrier durability is proven.
func (s *WALShipper) HasTransportContact() bool {
	switch s.State() {
	case ReplicaDegraded, ReplicaNeedsRebuild:
		return false
	case ReplicaConnecting, ReplicaCatchingUp, ReplicaInSync:
		return true
	}
	if s.ShippedLSN() > 0 {
		return true
	}
	return !s.LastContactTime().IsZero()
}

// State returns the current replica state machine state.
func (s *WALShipper) State() ReplicaState {
	return ReplicaState(s.state.Load())
}

// LastContactTime returns the last time this replica had successful
// durable contact (barrier success, reconnect handshake, catch-up completion).
// Returns zero time if no contact has occurred.
func (s *WALShipper) LastContactTime() time.Time {
	if v := s.lastContactTime.Load(); v != nil {
		return v.(time.Time)
	}
	return time.Time{}
}

func (s *WALShipper) touchContactTime() {
	s.lastContactTime.Store(time.Now())
}

// IsDegraded returns true if the replica is not sync-eligible (any state
// other than InSync). This overloads Disconnected, Connecting, CatchingUp,
// NeedsRebuild, and Degraded into one "not healthy" shape for backward
// compatibility with existing metrics and callers.
func (s *WALShipper) IsDegraded() bool {
	return s.State() != ReplicaInSync
}

// Stop shuts down the shipper and closes connections.
func (s *WALShipper) Stop() {
	if s.stopped.Swap(true) {
		return
	}
	s.mu.Lock()
	if s.dataConn != nil {
		s.dataConn.Close()
		s.dataConn = nil
	}
	s.mu.Unlock()

	s.ctrlMu.Lock()
	if s.ctrlConn != nil {
		s.ctrlConn.Close()
		s.ctrlConn = nil
	}
	s.ctrlMu.Unlock()
}

func (s *WALShipper) ensureDataConn() error {
	if s.dataConn != nil {
		return nil
	}
	conn, err := net.DialTimeout("tcp", s.dataAddr, 3*time.Second)
	if err != nil {
		return err
	}
	s.dataConn = conn
	log.Printf("wal_shipper: data channel connected (data=%s, ctrl=%s)", s.dataAddr, s.controlAddr)
	return nil
}

func (s *WALShipper) ensureCtrlConn() error {
	if s.ctrlConn != nil {
		return nil
	}
	conn, err := net.DialTimeout("tcp", s.controlAddr, 3*time.Second)
	if err != nil {
		return err
	}
	s.ctrlConn = conn
	return nil
}

func (s *WALShipper) markDegraded() {
	prev := ReplicaState(s.state.Swap(uint32(ReplicaDegraded)))
	log.Printf("wal_shipper: replica degraded (data=%s, ctrl=%s, prev=%s)", s.dataAddr, s.controlAddr, prev)
	if prev != ReplicaDegraded && s.onStateChange != nil {
		s.onStateChange(prev, ReplicaDegraded)
	}
}

// resetConnections closes both data and control connections for a clean retry.
func (s *WALShipper) resetConnections() {
	s.mu.Lock()
	if s.dataConn != nil {
		s.dataConn.Close()
		s.dataConn = nil
	}
	s.mu.Unlock()
	s.ctrlMu.Lock()
	if s.ctrlConn != nil {
		s.ctrlConn.Close()
		s.ctrlConn = nil
	}
	s.ctrlMu.Unlock()
}

func (s *WALShipper) resetCtrlConn() {
	s.ctrlMu.Lock()
	if s.ctrlConn != nil {
		s.ctrlConn.Close()
		s.ctrlConn = nil
	}
	s.ctrlMu.Unlock()
}

// doReconnectAndCatchUp runs the full reconnect handshake + catch-up protocol.
// On success, transitions to InSync and resets ctrl connection for barrier.
func (s *WALShipper) doReconnectAndCatchUp() error {
	log.Printf("wal_shipper: reconnect start replica=%s state=%s flushed_lsn=%d data=%s ctrl=%s",
		s.replicaID, s.State(), s.replicaFlushedLSN.Load(), s.dataAddr, s.controlAddr)
	targetState, replicaFlushed, err := s.reconnectWithHandshake()
	switch targetState {
	case ReplicaInSync:
		s.markInSync()
		log.Printf("wal_shipper: reconnect complete replica=%s state=%s replica_flushed=%d",
			s.replicaID, targetState, replicaFlushed)
	case ReplicaCatchingUp:
		// Use the handshake-reported flushedLSN as catch-up start point,
		// NOT the shipper's cached value. The replica may have lost progress
		// since the shipper last heard from it.
		if catchErr := s.runCatchUp(replicaFlushed); catchErr != nil {
			log.Printf("wal_shipper: reconnect catch-up failed replica=%s from_lsn=%d err=%v",
				s.replicaID, replicaFlushed, catchErr)
			s.catchupFailures++
			if s.catchupFailures >= maxCatchupRetries {
				s.state.Store(uint32(ReplicaNeedsRebuild))
				return fmt.Errorf("catch-up failed %d times: %w", s.catchupFailures, catchErr)
			}
			s.markDegraded()
			return ErrReplicaDegraded
		}
		s.markInSync()
		log.Printf("wal_shipper: reconnect catch-up complete replica=%s from_lsn=%d",
			s.replicaID, replicaFlushed)
	case ReplicaNeedsRebuild:
		s.state.Store(uint32(ReplicaNeedsRebuild))
		log.Printf("wal_shipper: reconnect escalated to needs_rebuild replica=%s err=%v",
			s.replicaID, err)
		return fmt.Errorf("reconnect: %w", err)
	default:
		s.markDegraded()
		log.Printf("wal_shipper: reconnect left replica degraded replica=%s err=%v",
			s.replicaID, err)
		return ErrReplicaDegraded
	}
	// Reset ctrl connection so barrier creates a fresh one.
	s.resetCtrlConn()
	return nil
}

func (s *WALShipper) markInSync() {
	prev := ReplicaState(s.state.Swap(uint32(ReplicaInSync)))
	s.catchupFailures = 0
	s.touchContactTime()
	log.Printf("wal_shipper: replica in-sync (data=%s, ctrl=%s, prev=%s)", s.dataAddr, s.controlAddr, prev)
	if prev != ReplicaInSync && s.onStateChange != nil {
		s.onStateChange(prev, ReplicaInSync)
	}
}

const catchupTimeout = 30 * time.Second

// reconnectWithHandshake performs the CP13-5 reconnect protocol:
// connect data channel → send ResumeShipReq → read ResumeShipResp → decide.
// Returns the target state (InSync, CatchingUp, NeedsRebuild) and replica's flushed LSN.
// Caller must hold no locks. Must only be called when wal != nil.
func (s *WALShipper) reconnectWithHandshake() (targetState ReplicaState, replicaFlushedLSN uint64, err error) {
	s.state.Store(uint32(ReplicaConnecting))

	// Reset and establish data connection.
	s.mu.Lock()
	if s.dataConn != nil {
		s.dataConn.Close()
		s.dataConn = nil
	}
	if err := s.ensureDataConn(); err != nil {
		s.mu.Unlock()
		return ReplicaDegraded, 0, fmt.Errorf("reconnect dial: %w", err)
	}
	s.dataConn.SetDeadline(time.Now().Add(catchupTimeout))
	conn := s.dataConn
	s.mu.Unlock()

	// Gather primary state.
	retainStart, headLSN := s.wal.RetainedRange()
	epoch := s.epochFn()

	// Send ResumeShipReq.
	req := EncodeResumeShipReq(ResumeShipReq{
		Epoch:          epoch,
		PrimaryHeadLSN: headLSN,
		WalRetainStart: retainStart,
	})
	if err := WriteFrame(conn, MsgResumeShipReq, req); err != nil {
		return ReplicaDegraded, 0, fmt.Errorf("reconnect send req: %w", err)
	}

	// Read ResumeShipResp.
	msgType, payload, err := ReadFrame(conn)
	if err != nil {
		return ReplicaDegraded, 0, fmt.Errorf("reconnect read resp: %w", err)
	}
	if msgType != MsgResumeShipResp {
		return ReplicaDegraded, 0, fmt.Errorf("reconnect: unexpected msg type 0x%02x", msgType)
	}
	resp, err := DecodeResumeShipResp(payload)
	if err != nil {
		return ReplicaDegraded, 0, err
	}

	// Clear deadline for catch-up streaming.
	s.mu.Lock()
	if s.dataConn != nil {
		s.dataConn.SetDeadline(time.Time{})
	}
	s.mu.Unlock()

	// Decision matrix.
	switch resp.Status {
	case ResumeEpochMismatch:
		return ReplicaNeedsRebuild, resp.ReplicaFlushedLSN, fmt.Errorf("reconnect: epoch mismatch")
	case ResumeNeedsRebuild:
		return ReplicaNeedsRebuild, resp.ReplicaFlushedLSN, fmt.Errorf("reconnect: replica requests rebuild")
	case ResumeOK:
		// proceed to gap analysis
	default:
		return ReplicaDegraded, resp.ReplicaFlushedLSN, fmt.Errorf("reconnect: unknown status 0x%02x", resp.Status)
	}

	R := resp.ReplicaFlushedLSN
	H := headLSN
	S := retainStart

	if R > H {
		// Impossible: replica ahead of primary.
		log.Printf("wal_shipper: reconnect %s: impossible progress R=%d > H=%d", s.dataAddr, R, H)
		return ReplicaNeedsRebuild, R, fmt.Errorf("reconnect: impossible replica progress")
	}
	if R == H {
		// Already caught up.
		log.Printf("wal_shipper: reconnect %s: already caught up (R=H=%d)", s.dataAddr, R)
		return ReplicaInSync, R, nil
	}
	if R+1 >= S {
		// Recoverable gap: WAL still has entries from R+1.
		log.Printf("wal_shipper: reconnect %s: recoverable gap R=%d H=%d S=%d", s.dataAddr, R, H, S)
		return ReplicaCatchingUp, R, nil
	}
	// Gap exceeds retained WAL.
	log.Printf("wal_shipper: reconnect %s: gap too large R=%d H=%d S=%d", s.dataAddr, R, H, S)
	return ReplicaNeedsRebuild, R, fmt.Errorf("reconnect: gap exceeds retained WAL")
}

// runCatchUp streams WAL entries from fromLSN+1 to the replica on the data channel.
// Sends MsgCatchupDone when complete. Caller must hold no shipper locks.
func (s *WALShipper) runCatchUp(fromLSN uint64) error {
	_, err := s.runCatchUpTo(fromLSN, 0)
	return err
}

func (s *WALShipper) runCatchUpTo(fromLSN uint64, targetLSN uint64) (uint64, error) {
	s.state.Store(uint32(ReplicaCatchingUp))

	// Set a deadline for the entire catch-up operation.
	s.mu.Lock()
	if s.dataConn != nil {
		s.dataConn.SetDeadline(time.Now().Add(catchupTimeout))
	}
	conn := s.dataConn
	s.mu.Unlock()

	if conn == nil {
		return 0, fmt.Errorf("catch-up: no data connection")
	}

	// Stream entries from WAL.
	var lastSent uint64
	err := s.wal.StreamEntries(fromLSN+1, func(entry *WALEntry) error {
		if targetLSN > 0 && entry.LSN > targetLSN {
			return nil
		}
		encoded, encErr := entry.Encode()
		if encErr != nil {
			return encErr
		}
		if wErr := WriteFrame(conn, MsgWALEntry, encoded); wErr != nil {
			return wErr
		}
		lastSent = entry.LSN
		return nil
	})

	if err != nil {
		if errors.Is(err, ErrWALRecycled) {
			s.state.Store(uint32(ReplicaNeedsRebuild))
			return lastSent, fmt.Errorf("catch-up: WAL recycled: %w", err)
		}
		return lastSent, fmt.Errorf("catch-up: stream error: %w", err)
	}

	// Send CatchupDone marker.
	doneLSN := lastSent
	if doneLSN == 0 {
		doneLSN = fromLSN
	}
	if err := WriteFrame(conn, MsgCatchupDone, EncodeCatchupDone(doneLSN)); err != nil {
		return lastSent, fmt.Errorf("catch-up: send done: %w", err)
	}

	// Clear deadline.
	s.mu.Lock()
	if s.dataConn != nil {
		s.dataConn.SetDeadline(time.Time{})
	}
	s.mu.Unlock()

	if targetLSN > 0 && lastSent < targetLSN {
		return lastSent, fmt.Errorf("catch-up: target %d not reached (last=%d)", targetLSN, lastSent)
	}
	log.Printf("wal_shipper: catch-up complete %s: from=%d target=%d last=%d",
		s.dataAddr, fromLSN+1, targetLSN, lastSent)
	return lastSent, nil
}
