package blockvol

import (
	"log"
	"net"
	"sync"
	"time"
)

// handleControlConn reads barrier requests from the control channel and
// responds with barrier status after ensuring durability.
func (r *ReplicaReceiver) handleControlConn(conn net.Conn) {
	defer conn.Close()
	var writeMu sync.Mutex
	writeFrame := func(msgType byte, payload []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return WriteFrame(conn, msgType, payload)
	}
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
				log.Printf("replica: ctrl read error: %v", err)
			}
			return
		}

		switch msgType {
		case MsgBarrierReq:
			req, err := DecodeBarrierRequest(payload)
			if err != nil {
				log.Printf("replica: decode barrier request: %v", err)
				continue
			}

			resp := r.handleBarrier(req)

			respPayload := EncodeBarrierResponse(resp)
			if err := writeFrame(MsgBarrierResp, respPayload); err != nil {
				log.Printf("replica: write barrier response: %v", err)
				return
			}
		case MsgSessionControl:
			if err := r.handleSessionControl(conn, payload, writeFrame); err != nil {
				log.Printf("replica: session control error: %v", err)
				return
			}
		default:
			log.Printf("replica: unexpected ctrl message type 0x%02x", msgType)
		}
	}
}

func (r *ReplicaReceiver) handleSessionControl(conn net.Conn, payload []byte, writeFrame func(byte, []byte) error) error {
	ctrl, err := DecodeSessionControl(payload)
	if err != nil {
		return err
	}
	switch ctrl.Command {
	case SessionCmdStartRebuild:
		log.Printf("replica: handleSessionControl start_rebuild session=%d epoch=%d base=%d target=%d rebuildAddr=%q",
			ctrl.SessionID, ctrl.Epoch, ctrl.BaseLSN, ctrl.TargetLSN, ctrl.RebuildAddr)
		r.vol.SetOnRebuildSessionAck(func(ack SessionAckMsg) {
			if ack.SessionID != ctrl.SessionID {
				return
			}
			if err := writeFrame(MsgSessionAck, EncodeSessionAck(ack)); err != nil {
				log.Printf("replica: write session ack: %v", err)
			}
		})
		if err := r.vol.StartRebuildSession(RebuildSessionConfig{
			SessionID: ctrl.SessionID,
			Epoch:     ctrl.Epoch,
			BaseLSN:   ctrl.BaseLSN,
			TargetLSN: ctrl.TargetLSN,
		}); err != nil {
			_ = writeFrame(MsgSessionAck, EncodeSessionAck(SessionAckMsg{
				Epoch:         ctrl.Epoch,
				SessionID:     ctrl.SessionID,
				Phase:         SessionAckFailed,
				WALAppliedLSN: ctrl.BaseLSN,
			}))
			return err
		}
		// If the primary included a RebuildAddr trailer (v2 session control),
		// auto-start the base lane client so the replica pulls base blocks.
		if ctrl.RebuildAddr != "" {
			go r.runBaseLaneClient(ctrl.SessionID, ctrl.RebuildAddr)
		}
		return nil
	case SessionCmdCancel:
		if err := r.vol.CancelRebuildSession(ctrl.SessionID, "remote_cancel"); err != nil {
			return err
		}
		return nil
	default:
		return net.InvalidAddrError("unsupported session control command")
	}
}

// runBaseLaneClient connects to the primary's rebuild server and pulls base
// blocks for the active rebuild session. Runs as a background goroutine started
// by handleSessionControl when the primary includes a RebuildAddr in the v2
// session control message. On failure, the rebuild session will eventually time
// out or be cancelled by the primary.
func (r *ReplicaReceiver) runBaseLaneClient(sessionID uint64, rebuildAddr string) {
	log.Printf("replica: base lane client starting session=%d addr=%s", sessionID, rebuildAddr)
	conn, err := net.DialTimeout("tcp", rebuildAddr, 5*time.Second)
	if err != nil {
		log.Printf("replica: base lane dial %s: %v", rebuildAddr, err)
		return
	}
	defer conn.Close()

	// Send rebuild request so the RebuildServer dispatches to ServeBaseBlocks.
	epoch := r.vol.Epoch()
	req := RebuildRequest{
		Type:    RebuildSessionBase,
		Epoch:   epoch,
		FromLSN: 0, // full base
	}
	if err := WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(req)); err != nil {
		log.Printf("replica: base lane send request to %s: %v", rebuildAddr, err)
		return
	}

	client := NewRebuildTransportClient(r.vol, sessionID)
	blocks, err := client.ReceiveBaseBlocks(conn)
	if err != nil {
		log.Printf("replica: base lane receive from %s: %v", rebuildAddr, err)
		return
	}
	log.Printf("replica: base lane complete from %s: %d blocks", rebuildAddr, blocks)
}

// handleBarrier waits until all WAL entries up to req.LSN have been received,
// then fsyncs the WAL to ensure durability.
func (r *ReplicaReceiver) handleBarrier(req BarrierRequest) BarrierResponse {
	// Fail fast on epoch mismatch.
	localEpoch := r.vol.epoch.Load()
	if req.Epoch != localEpoch {
		return BarrierResponse{Status: BarrierEpochMismatch}
	}

	// Use a timer goroutine to wake us on deadline.
	timer := time.NewTimer(r.barrierTimeout)
	defer timer.Stop()
	timedOut := make(chan struct{})
	go func() {
		select {
		case <-timer.C:
			close(timedOut)
			// Broadcast to wake any cond.Wait blocked in the loop below.
			r.mu.Lock()
			r.cond.Broadcast()
			r.mu.Unlock()
		case <-r.stopCh:
		}
	}()

	r.mu.Lock()
	for r.receivedLSN < req.LSN {
		// Check if timed out or stopped.
		select {
		case <-timedOut:
			r.mu.Unlock()
			return BarrierResponse{Status: BarrierTimeout}
		case <-r.stopCh:
			r.mu.Unlock()
			return BarrierResponse{Status: BarrierTimeout}
		default:
		}

		// Block on cond.Wait -- woken by applyEntry or timeout goroutine.
		r.cond.Wait()
	}
	r.mu.Unlock()

	// fsync WAL AFTER confirming all entries received.
	if err := r.vol.fd.Sync(); err != nil {
		log.Printf("replica: barrier fsync error: %v", err)
		return BarrierResponse{Status: BarrierFsyncFailed}
	}

	// Advance durable progress. Only after fd.Sync() succeeds and contiguous
	// receipt through req.LSN has been proven (step 2 above).
	r.mu.Lock()
	if req.LSN > r.flushedLSN {
		r.flushedLSN = req.LSN
	}
	flushed := r.flushedLSN
	r.mu.Unlock()

	return BarrierResponse{Status: BarrierOK, FlushedLSN: flushed}
}
