package blockvol

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

var (
	ErrStaleEpoch   = errors.New("blockvol: stale epoch")
	ErrDuplicateLSN = errors.New("blockvol: duplicate or out-of-order LSN")
)

// ReplicaReceiver listens for WAL entries from a primary and applies them
// to the local BlockVol. It runs two listeners: one for the data channel
// (WAL entries) and one for the control channel (barrier requests).
type ReplicaReceiver struct {
	vol            *BlockVol
	barrierTimeout time.Duration
	advertisedHost string // canonical IP for address reporting; empty = auto-detect

	mu          sync.Mutex
	receivedLSN uint64
	flushedLSN  uint64 // highest LSN durably persisted (fd.Sync completed); updated only in handleBarrier
	cond        *sync.Cond

	connMu      sync.Mutex // protects activeConns
	activeConns map[net.Conn]struct{}

	dataListener net.Listener
	ctrlListener net.Listener
	stopCh       chan struct{}
	stopped      bool
	wg           sync.WaitGroup
}

const defaultBarrierTimeout = 5 * time.Second

// NewReplicaReceiver creates and starts listening on the data and control ports.
// advertisedHost is the canonical IP for this server (from VS listen addr or
// heartbeat identity). If empty, DataAddr()/CtrlAddr() will fall back to
// outbound-IP detection. On multi-NIC hosts, always provide advertisedHost.
func NewReplicaReceiver(vol *BlockVol, dataAddr, ctrlAddr string, advertisedHost ...string) (*ReplicaReceiver, error) {
	dataLn, err := net.Listen("tcp", dataAddr)
	if err != nil {
		return nil, fmt.Errorf("replica: listen data %s: %w", dataAddr, err)
	}
	ctrlLn, err := net.Listen("tcp", ctrlAddr)
	if err != nil {
		dataLn.Close()
		return nil, fmt.Errorf("replica: listen ctrl %s: %w", ctrlAddr, err)
	}

	var advHost string
	if len(advertisedHost) > 0 {
		advHost = advertisedHost[0]
	}
	r := &ReplicaReceiver{
		vol:            vol,
		barrierTimeout: defaultBarrierTimeout,
		advertisedHost: advHost,
		dataListener:   dataLn,
		ctrlListener:   ctrlLn,
		stopCh:         make(chan struct{}),
		activeConns:    make(map[net.Conn]struct{}),
	}
	r.cond = sync.NewCond(&r.mu)
	return r, nil
}

// Serve starts accept loops for both listeners. Call Stop() to shut down.
func (r *ReplicaReceiver) Serve() {
	r.wg.Add(2)
	go r.acceptDataLoop()
	go r.acceptCtrlLoop()
}

// Stop shuts down both listeners, closes active connections, and waits for goroutines.
func (r *ReplicaReceiver) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.mu.Unlock()

	close(r.stopCh)
	r.dataListener.Close()
	r.ctrlListener.Close()

	// Close all active connections to unblock ReadFrame calls.
	r.connMu.Lock()
	for conn := range r.activeConns {
		conn.Close()
	}
	r.connMu.Unlock()

	// Wake any barrier waiters so they can exit (must hold mu for cond).
	r.mu.Lock()
	r.cond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *ReplicaReceiver) trackConn(conn net.Conn) {
	r.connMu.Lock()
	r.activeConns[conn] = struct{}{}
	r.connMu.Unlock()
}

func (r *ReplicaReceiver) untrackConn(conn net.Conn) {
	r.connMu.Lock()
	delete(r.activeConns, conn)
	r.connMu.Unlock()
}

func (r *ReplicaReceiver) acceptDataLoop() {
	defer r.wg.Done()
	for {
		conn, err := r.dataListener.Accept()
		if err != nil {
			select {
			case <-r.stopCh:
				return
			default:
				log.Printf("replica: data accept error: %v", err)
				return
			}
		}
		r.trackConn(conn)
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer r.untrackConn(conn)
			r.handleDataConn(conn)
		}()
	}
}

func (r *ReplicaReceiver) acceptCtrlLoop() {
	defer r.wg.Done()
	for {
		conn, err := r.ctrlListener.Accept()
		if err != nil {
			select {
			case <-r.stopCh:
				return
			default:
				log.Printf("replica: ctrl accept error: %v", err)
				return
			}
		}
		r.trackConn(conn)
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer r.untrackConn(conn)
			r.handleControlConn(conn)
		}()
	}
}

// handleDataConn reads WAL entry frames and applies them to the local volume.
func (r *ReplicaReceiver) handleDataConn(conn net.Conn) {
	defer conn.Close()
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		msgType, payload, err := ReadFrame(conn)
		if err != nil {
			select {
			case <-r.stopCh:
			default:
				log.Printf("replica: data read error: %v", err)
			}
			return
		}

		if msgType != MsgWALEntry {
			log.Printf("replica: unexpected data message type 0x%02x", msgType)
			continue
		}

		if err := r.applyEntry(payload); err != nil {
			log.Printf("replica: apply entry error: %v", err)
		}
	}
}

// applyEntry decodes and applies a single WAL entry to the local volume.
// The entire apply (LSN check -> WAL append -> dirty map -> receivedLSN update)
// is serialized under mu to prevent TOCTOU races between concurrent entries.
func (r *ReplicaReceiver) applyEntry(payload []byte) error {
	entry, err := DecodeWALEntry(payload)
	if err != nil {
		return fmt.Errorf("decode WAL entry: %w", err)
	}

	// ioMu.RLock: protect WAL/dirtyMap mutation against exclusive restore/import.
	r.vol.ioMu.RLock()
	defer r.vol.ioMu.RUnlock()

	// Validate epoch: replicas must NOT accept epoch bumps from WAL stream.
	// Only the master can change epochs (via SetEpoch in CP3).
	localEpoch := r.vol.epoch.Load()
	if entry.Epoch != localEpoch {
		return fmt.Errorf("%w: entry epoch %d != local %d", ErrStaleEpoch, entry.Epoch, localEpoch)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Enforce contiguous LSN: only accept the next expected entry.
	// This prevents gaps that would let a barrier pass incorrectly.
	if entry.LSN <= r.receivedLSN {
		log.Printf("replica: skipping duplicate/old LSN %d (received %d)", entry.LSN, r.receivedLSN)
		return nil
	}
	if entry.LSN != r.receivedLSN+1 {
		return fmt.Errorf("%w: expected LSN %d, got %d (gap)", ErrDuplicateLSN, r.receivedLSN+1, entry.LSN)
	}

	// Append to local WAL (with retry on WAL full).
	walOff, err := r.replicaAppendWithRetry(&entry)
	if err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}

	// Update dirty map.
	switch entry.Type {
	case EntryTypeWrite, EntryTypeTrim:
		blocks := entry.Length / r.vol.super.BlockSize
		for i := uint32(0); i < blocks; i++ {
			r.vol.dirtyMap.Put(entry.LBA+uint64(i), walOff, entry.LSN, r.vol.super.BlockSize)
		}
	}

	// Update receivedLSN and signal barrier waiters.
	r.receivedLSN = entry.LSN
	r.cond.Broadcast()

	// Update vol.nextLSN so Status().WALHeadLSN reflects replicated state.
	// CAS loop: only advance, never regress.
	for {
		cur := r.vol.nextLSN.Load()
		next := entry.LSN + 1
		if next <= cur {
			break
		}
		if r.vol.nextLSN.CompareAndSwap(cur, next) {
			break
		}
	}

	return nil
}

// replicaAppendWithRetry appends a WAL entry, retrying on WAL-full by
// triggering the flusher. Caller must hold r.mu.
func (r *ReplicaReceiver) replicaAppendWithRetry(entry *WALEntry) (uint64, error) {
	walOff, err := r.vol.wal.Append(entry)
	if !errors.Is(err, ErrWALFull) {
		return walOff, err
	}

	deadline := time.After(r.vol.config.WALFullTimeout)
	for errors.Is(err, ErrWALFull) {
		select {
		case <-r.stopCh:
			return 0, fmt.Errorf("replica: stopped during WAL retry")
		default:
		}
		if r.vol.flusher != nil {
			r.vol.flusher.NotifyUrgent()
		}
		// Release mu briefly so barrier waiters can proceed and
		// the flusher can make progress (it may need dirty map lock).
		r.mu.Unlock()
		select {
		case <-deadline:
			r.mu.Lock()
			return 0, fmt.Errorf("replica: WAL full timeout: %w", ErrWALFull)
		case <-time.After(1 * time.Millisecond):
		}
		r.mu.Lock()
		walOff, err = r.vol.wal.Append(entry)
	}
	return walOff, err
}

// ReceivedLSN returns the highest LSN received and written to the local WAL.
func (r *ReplicaReceiver) ReceivedLSN() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.receivedLSN
}

// FlushedLSN returns the highest LSN durably persisted on this replica
// (after successful WAL fd.Sync). Updated only by handleBarrier.
func (r *ReplicaReceiver) FlushedLSN() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.flushedLSN
}

// DataAddr returns the data listener's canonical address (ip:port).
// Wildcard listener addresses are resolved using the advertised host
// or outbound-IP fallback.
func (r *ReplicaReceiver) DataAddr() string {
	return canonicalizeListenerAddr(r.dataListener.Addr(), r.advertisedHost)
}

// CtrlAddr returns the control listener's canonical address (ip:port).
func (r *ReplicaReceiver) CtrlAddr() string {
	return canonicalizeListenerAddr(r.ctrlListener.Addr(), r.advertisedHost)
}
