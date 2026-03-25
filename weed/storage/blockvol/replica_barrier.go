package blockvol

import (
	"log"
	"net"
	"time"
)

// handleControlConn reads barrier requests from the control channel and
// responds with barrier status after ensuring durability.
func (r *ReplicaReceiver) handleControlConn(conn net.Conn) {
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
				log.Printf("replica: ctrl read error: %v", err)
			}
			return
		}

		if msgType != MsgBarrierReq {
			log.Printf("replica: unexpected ctrl message type 0x%02x", msgType)
			continue
		}

		req, err := DecodeBarrierRequest(payload)
		if err != nil {
			log.Printf("replica: decode barrier request: %v", err)
			continue
		}

		resp := r.handleBarrier(req)

		respPayload := EncodeBarrierResponse(resp)
		if err := WriteFrame(conn, MsgBarrierResp, respPayload); err != nil {
			log.Printf("replica: write barrier response: %v", err)
			return
		}
	}
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
