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

	shippedLSN atomic.Uint64
	degraded   atomic.Bool
	stopped    atomic.Bool
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
// On write error, the shipper enters degraded mode permanently.
func (s *WALShipper) Ship(entry *WALEntry) error {
	if s.stopped.Load() || s.degraded.Load() {
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
// the shipper is in degraded mode.
func (s *WALShipper) Barrier(lsnMax uint64) error {
	if s.stopped.Load() {
		return ErrShipperStopped
	}
	if s.degraded.Load() {
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

	switch payload[0] {
	case BarrierOK:
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

// ShippedLSN returns the highest LSN successfully sent to the replica.
func (s *WALShipper) ShippedLSN() uint64 {
	return s.shippedLSN.Load()
}

// IsDegraded returns true if the replica is unreachable.
func (s *WALShipper) IsDegraded() bool {
	return s.degraded.Load()
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
	s.degraded.Store(true)
	log.Printf("wal_shipper: replica degraded (data=%s, ctrl=%s)", s.dataAddr, s.controlAddr)
}
