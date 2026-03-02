package blockvol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

const rebuildExtentChunkSize = 64 * 1024 // 64KB chunks for extent streaming

// ---------------------------------------------------------------------------
// Rebuild Server (primary side)
// ---------------------------------------------------------------------------

// RebuildServer serves WAL catch-up and full extent data to rebuilding replicas.
type RebuildServer struct {
	vol      *BlockVol
	listener net.Listener
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewRebuildServer creates a rebuild server listening on addr.
func NewRebuildServer(vol *BlockVol, addr string) (*RebuildServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("rebuild: listen %s: %w", addr, err)
	}
	return &RebuildServer{
		vol:      vol,
		listener: ln,
		stopCh:   make(chan struct{}),
	}, nil
}

// Serve starts accepting rebuild connections.
func (s *RebuildServer) Serve() {
	s.wg.Add(1)
	go s.acceptLoop()
}

// Stop shuts down the rebuild server.
func (s *RebuildServer) Stop() {
	select {
	case <-s.stopCh:
		return // already stopped
	default:
	}
	close(s.stopCh)
	s.listener.Close()
	s.wg.Wait()
}

// Addr returns the listener's address.
func (s *RebuildServer) Addr() string {
	return s.listener.Addr().String()
}

func (s *RebuildServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				log.Printf("rebuild: accept error: %v", err)
				return
			}
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

func (s *RebuildServer) handleConn(conn net.Conn) {
	defer conn.Close()

	msgType, payload, err := ReadFrame(conn)
	if err != nil {
		log.Printf("rebuild: read request: %v", err)
		return
	}
	if msgType != MsgRebuildReq {
		log.Printf("rebuild: unexpected message type 0x%02x", msgType)
		return
	}

	req, err := DecodeRebuildRequest(payload)
	if err != nil {
		log.Printf("rebuild: decode request: %v", err)
		return
	}

	// Validate epoch.
	if req.Epoch != s.vol.epoch.Load() {
		WriteFrame(conn, MsgRebuildError, []byte("EPOCH_MISMATCH"))
		return
	}

	switch req.Type {
	case RebuildWALCatchUp:
		s.handleWALCatchUp(conn, req)
	case RebuildFullExtent:
		s.handleFullExtent(conn)
	default:
		WriteFrame(conn, MsgRebuildError, []byte("UNKNOWN_TYPE"))
	}
}

func (s *RebuildServer) handleWALCatchUp(conn net.Conn, req RebuildRequest) {
	checkpointLSN := s.vol.flusher.CheckpointLSN()

	err := s.vol.wal.ScanFrom(s.vol.fd, s.vol.super.WALOffset,
		checkpointLSN, req.FromLSN, func(entry *WALEntry) error {
			encoded, err := entry.Encode()
			if err != nil {
				return err
			}
			return WriteFrame(conn, MsgRebuildEntry, encoded)
		})

	if errors.Is(err, ErrWALRecycled) {
		WriteFrame(conn, MsgRebuildError, []byte("WAL_RECYCLED"))
		return
	}
	if err != nil {
		WriteFrame(conn, MsgRebuildError, []byte(err.Error()))
		return
	}

	// Send done with the current nextLSN as the snapshot point.
	// The client uses this to know where to start a second catch-up
	// after a full extent copy.
	lsnBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lsnBuf, s.vol.nextLSN.Load())
	WriteFrame(conn, MsgRebuildDone, lsnBuf)
}

func (s *RebuildServer) handleFullExtent(conn net.Conn) {
	// Capture snapshot LSN before streaming — client will use this
	// for a second catch-up scan to capture writes during copy.
	snapshotLSN := s.vol.nextLSN.Load()

	extentStart := s.vol.super.WALOffset + s.vol.super.WALSize
	volumeSize := s.vol.super.VolumeSize

	buf := make([]byte, rebuildExtentChunkSize)
	var offset uint64
	for offset < volumeSize {
		chunkSize := uint64(rebuildExtentChunkSize)
		if offset+chunkSize > volumeSize {
			chunkSize = volumeSize - offset
		}
		n, err := s.vol.fd.ReadAt(buf[:chunkSize], int64(extentStart+offset))
		if err != nil && err != io.EOF {
			WriteFrame(conn, MsgRebuildError, []byte(err.Error()))
			return
		}
		if err := WriteFrame(conn, MsgRebuildExtent, buf[:n]); err != nil {
			return
		}
		offset += uint64(n)
	}

	// Send done with snapshot LSN.
	lsnBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lsnBuf, snapshotLSN)
	WriteFrame(conn, MsgRebuildDone, lsnBuf)
}

// ---------------------------------------------------------------------------
// Rebuild Client (rebuilding replica side)
// ---------------------------------------------------------------------------

// StartRebuild initiates rebuild from primary. The volume must already be
// in RoleRebuilding (set via HandleAssignment). On success, transitions
// the volume to RoleReplica.
func StartRebuild(vol *BlockVol, primaryAddr string, fromLSN uint64, epoch uint64) error {
	if vol.Role() != RoleRebuilding {
		return fmt.Errorf("rebuild: expected role Rebuilding, got %s", vol.Role())
	}

	conn, err := net.Dial("tcp", primaryAddr)
	if err != nil {
		return fmt.Errorf("rebuild: connect to %s: %w", primaryAddr, err)
	}
	defer conn.Close()

	// Try WAL catch-up first.
	req := RebuildRequest{
		Type:    RebuildWALCatchUp,
		FromLSN: fromLSN,
		Epoch:   epoch,
	}
	if err := WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(req)); err != nil {
		return fmt.Errorf("rebuild: send request: %w", err)
	}

	// Read responses.
	var snapshotLSN uint64
	for {
		msgType, payload, err := ReadFrame(conn)
		if err != nil {
			return fmt.Errorf("rebuild: read response: %w", err)
		}

		switch msgType {
		case MsgRebuildEntry:
			if err := applyRebuildEntry(vol, payload); err != nil {
				return fmt.Errorf("rebuild: apply entry: %w", err)
			}

		case MsgRebuildDone:
			if len(payload) >= 8 {
				snapshotLSN = binary.BigEndian.Uint64(payload[:8])
			}
			goto catchUpDone

		case MsgRebuildError:
			errMsg := string(payload)
			if errMsg == "WAL_RECYCLED" {
				// Fall back to full extent rebuild.
				conn.Close()
				return rebuildFullExtent(vol, primaryAddr, epoch)
			}
			return fmt.Errorf("rebuild: server error: %s", errMsg)

		default:
			return fmt.Errorf("rebuild: unexpected message type 0x%02x", msgType)
		}
	}

catchUpDone:
	conn.Close()

	// Second catch-up: capture writes that arrived on the primary after
	// the scan snapshot. Without this, those writes are lost.
	if err := rebuildSecondCatchUp(vol, primaryAddr, snapshotLSN, epoch); err != nil {
		return err
	}
	return vol.SetRole(RoleReplica)
}

// rebuildFullExtent streams the full extent from primary, then does a
// second WAL catch-up to capture writes during the copy.
func rebuildFullExtent(vol *BlockVol, primaryAddr string, epoch uint64) error {
	conn, err := net.Dial("tcp", primaryAddr)
	if err != nil {
		return fmt.Errorf("rebuild: connect for full extent: %w", err)
	}
	defer conn.Close()

	req := RebuildRequest{
		Type:  RebuildFullExtent,
		Epoch: epoch,
	}
	if err := WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(req)); err != nil {
		return fmt.Errorf("rebuild: send extent request: %w", err)
	}

	extentStart := vol.super.WALOffset + vol.super.WALSize
	var offset uint64
	var snapshotLSN uint64

	for {
		msgType, payload, err := ReadFrame(conn)
		if err != nil {
			return fmt.Errorf("rebuild: read extent response: %w", err)
		}

		switch msgType {
		case MsgRebuildExtent:
			if _, err := vol.fd.WriteAt(payload, int64(extentStart+offset)); err != nil {
				return fmt.Errorf("rebuild: write extent at offset %d: %w", offset, err)
			}
			offset += uint64(len(payload))

		case MsgRebuildDone:
			if len(payload) >= 8 {
				snapshotLSN = binary.BigEndian.Uint64(payload[:8])
			}
			goto extentDone

		case MsgRebuildError:
			return fmt.Errorf("rebuild: server error during extent: %s", string(payload))

		default:
			return fmt.Errorf("rebuild: unexpected message type 0x%02x during extent", msgType)
		}
	}

extentDone:
	// Fsync the extent data.
	if err := vol.fd.Sync(); err != nil {
		return fmt.Errorf("rebuild: fsync extent: %w", err)
	}

	// Clear dirty map -- all data now in extent, stale dirty entries invalid.
	vol.dirtyMap.Clear()

	// Reset WAL state -- no valid WAL entries for old data.
	vol.wal.mu.Lock()
	vol.wal.logicalHead = 0
	vol.wal.logicalTail = 0
	vol.wal.mu.Unlock()

	// Persist clean superblock state so crash recovery doesn't replay stale WAL.
	checkpointLSN := uint64(0)
	if snapshotLSN > 0 {
		checkpointLSN = snapshotLSN - 1
	}
	vol.mu.Lock()
	vol.super.WALHead = 0
	vol.super.WALTail = 0
	vol.super.WALCheckpointLSN = checkpointLSN
	if _, err := vol.fd.Seek(0, 0); err != nil {
		vol.mu.Unlock()
		return fmt.Errorf("rebuild: seek superblock: %w", err)
	}
	if _, err := vol.super.WriteTo(vol.fd); err != nil {
		vol.mu.Unlock()
		return fmt.Errorf("rebuild: write superblock: %w", err)
	}
	if err := vol.fd.Sync(); err != nil {
		vol.mu.Unlock()
		return fmt.Errorf("rebuild: sync superblock: %w", err)
	}
	vol.mu.Unlock()

	conn.Close()

	// Second catch-up scan: capture writes during extent copy.
	if err := rebuildSecondCatchUp(vol, primaryAddr, snapshotLSN, epoch); err != nil {
		return err
	}
	return vol.SetRole(RoleReplica)
}

// rebuildSecondCatchUp connects to the primary and streams WAL entries
// from snapshotLSN to capture writes that arrived after the initial scan
// or extent copy. Shared by both WAL catch-up and full-extent paths.
func rebuildSecondCatchUp(vol *BlockVol, primaryAddr string, snapshotLSN uint64, epoch uint64) error {
	if snapshotLSN == 0 {
		return nil
	}
	conn, err := net.Dial("tcp", primaryAddr)
	if err != nil {
		return fmt.Errorf("rebuild: connect for second catch-up: %w", err)
	}
	defer conn.Close()

	req := RebuildRequest{
		Type:    RebuildWALCatchUp,
		FromLSN: snapshotLSN,
		Epoch:   epoch,
	}
	if err := WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(req)); err != nil {
		return fmt.Errorf("rebuild: send second catch-up request: %w", err)
	}

	for {
		msgType, payload, err := ReadFrame(conn)
		if err != nil {
			return fmt.Errorf("rebuild: read second catch-up: %w", err)
		}

		switch msgType {
		case MsgRebuildEntry:
			if err := applyRebuildEntry(vol, payload); err != nil {
				return fmt.Errorf("rebuild: apply second catch-up entry: %w", err)
			}
		case MsgRebuildDone:
			return nil
		case MsgRebuildError:
			return fmt.Errorf("rebuild: server error during second catch-up: %s", string(payload))
		default:
			return fmt.Errorf("rebuild: unexpected message type 0x%02x during second catch-up", msgType)
		}
	}
}

// applyRebuildEntry decodes and applies a WAL entry during rebuild.
// Unlike ReplicaReceiver.applyEntry, no contiguous LSN enforcement
// (catch-up entries arrive in order but may have gaps from flushed entries).
func applyRebuildEntry(vol *BlockVol, payload []byte) error {
	entry, err := DecodeWALEntry(payload)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	walOff, err := vol.wal.Append(&entry)
	if errors.Is(err, ErrWALFull) {
		// Trigger flusher and retry.
		if vol.flusher != nil {
			vol.flusher.NotifyUrgent()
		}
		for i := 0; i < 100 && errors.Is(err, ErrWALFull); i++ {
			if vol.flusher != nil {
				vol.flusher.NotifyUrgent()
			}
			walOff, err = vol.wal.Append(&entry)
		}
	}
	if err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}

	switch entry.Type {
	case EntryTypeWrite, EntryTypeTrim:
		blocks := entry.Length / vol.super.BlockSize
		for i := uint32(0); i < blocks; i++ {
			vol.dirtyMap.Put(entry.LBA+uint64(i), walOff, entry.LSN, vol.super.BlockSize)
		}
	}

	// Update nextLSN if this entry has a higher LSN.
	for {
		current := vol.nextLSN.Load()
		next := entry.LSN + 1
		if next <= current {
			break
		}
		if vol.nextLSN.CompareAndSwap(current, next) {
			break
		}
	}

	return nil
}
