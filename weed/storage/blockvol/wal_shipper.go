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
	epochFn     func() uint64
	metrics     *EngineMetrics

	mu       sync.Mutex // protects dataConn
	dataConn net.Conn

	ctrlMu   sync.Mutex // protects ctrlConn
	ctrlConn net.Conn

	shippedLSN         atomic.Uint64 // diagnostic: highest LSN sent to TCP socket
	replicaFlushedLSN  atomic.Uint64 // authoritative: highest LSN durably persisted on replica
	hasFlushedProgress atomic.Bool   // true once replica returns a valid (non-zero) FlushedLSN
	state              atomic.Uint32 // ReplicaState
	stopped            atomic.Bool
}

// NewWALShipper creates a WAL shipper. Connections are established lazily on
// first Ship/Barrier call. epochFn returns the current epoch for validation.
// metrics is optional; if nil, no metrics are recorded.
func NewWALShipper(dataAddr, controlAddr string, epochFn func() uint64, metrics ...*EngineMetrics) *WALShipper {
	var m *EngineMetrics
	if len(metrics) > 0 {
		m = metrics[0]
	}
	return &WALShipper{
		dataAddr:    dataAddr,
		controlAddr: controlAddr,
		epochFn:     epochFn,
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
	switch st {
	case ReplicaInSync:
		// proceed normally
	case ReplicaDisconnected:
		// bootstrap path: attempt connect + barrier
	case ReplicaDegraded:
		// recovery path: reset both connections and attempt reconnect + barrier
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
	default:
		// Connecting, CatchingUp, NeedsRebuild — reject immediately
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
		s.markDegraded()
		s.recordBarrierMetric(barrierStart, true)
		return ErrReplicaDegraded
	}

	s.ctrlConn.SetDeadline(time.Now().Add(barrierTimeout))

	if err := WriteFrame(s.ctrlConn, MsgBarrierReq, req); err != nil {
		s.markDegraded()
		s.recordBarrierMetric(barrierStart, true)
		return ErrReplicaDegraded
	}

	msgType, payload, err := ReadFrame(s.ctrlConn)
	if err != nil {
		s.markDegraded()
		s.recordBarrierMetric(barrierStart, true)
		return ErrReplicaDegraded
	}

	if msgType != MsgBarrierResp || len(payload) < 1 {
		s.markDegraded()
		s.recordBarrierMetric(barrierStart, true)
		return ErrReplicaDegraded
	}

	resp := DecodeBarrierResponse(payload)

	switch resp.Status {
	case BarrierOK:
		// Barrier success — transition to InSync (only barrier success grants this).
		s.markInSync()
		// Update authoritative durable progress (monotonic: only advance).
		if resp.FlushedLSN > 0 {
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
		}
		s.recordBarrierMetric(barrierStart, false)
		return nil
	case BarrierEpochMismatch:
		s.markDegraded()
		s.recordBarrierMetric(barrierStart, true)
		return fmt.Errorf("wal_shipper: barrier epoch mismatch")
	case BarrierTimeout:
		s.markDegraded()
		s.recordBarrierMetric(barrierStart, true)
		return fmt.Errorf("wal_shipper: barrier timeout on replica")
	case BarrierFsyncFailed:
		s.markDegraded()
		s.recordBarrierMetric(barrierStart, true)
		return fmt.Errorf("wal_shipper: barrier fsync failed on replica")
	default:
		s.markDegraded()
		s.recordBarrierMetric(barrierStart, true)
		return fmt.Errorf("wal_shipper: unknown barrier status %d", payload[0])
	}
}

func (s *WALShipper) recordBarrierMetric(start time.Time, failed bool) {
	if s.metrics != nil {
		s.metrics.RecordWALBarrier(time.Since(start), failed)
	}
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

// State returns the current replica state machine state.
func (s *WALShipper) State() ReplicaState {
	return ReplicaState(s.state.Load())
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
	s.state.Store(uint32(ReplicaDegraded))
	log.Printf("wal_shipper: replica degraded (data=%s, ctrl=%s, state=%s)", s.dataAddr, s.controlAddr, s.State())
}

func (s *WALShipper) markInSync() {
	s.state.Store(uint32(ReplicaInSync))
	log.Printf("wal_shipper: replica in-sync (data=%s, ctrl=%s)", s.dataAddr, s.controlAddr)
}
