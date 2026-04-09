package weed_server

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// errRebuildAckFailed is returned by TransferFullBase when the rebuild failed
// via a replica SessionAckFailed. The ack observation path (ObserveReplicaRebuildSessionAck)
// already emitted engine.SessionFailed, so callers must NOT emit a second one.
var errRebuildAckFailed = errors.New("rebuild failed via session ack")

// RemoteRebuildIO implements engine.RebuildIO for the primary-side remote
// rebuild path. Instead of installing base blocks locally (which is what the
// v2bridge.Executor does on the replica), this implementation coordinates
// remotely: it sends a session control message to the replica and monitors
// acks until the replica reports completion or failure.
//
// The base blocks are served by the primary's existing RebuildServer.
// The replica auto-starts a base lane client on receiving the session control.
// WAL entries continue flowing through the shipper's data channel.
//
// Ack forwarding: each ack from the replica is forwarded to onAck, which
// routes through ObserveReplicaRebuildSessionAck for pin/watchdog/engine
// integration.
type RemoteRebuildIO struct {
	// ReplicaCtrlAddr is the replica's control channel address.
	ReplicaCtrlAddr string
	// RebuildAddr is the primary's rebuild server address (sent to replica
	// so it knows where to connect for the base lane).
	RebuildAddr string
	// BaseLSN is the flushed/checkpoint boundary the extent can serve.
	// Must be derived from the real checkpoint, not the plan target.
	BaseLSN uint64
	// Epoch is the current volume epoch for session validation.
	Epoch uint64
	// SessionID uniquely identifies this rebuild session.
	SessionID uint64
	// OnAck forwards each replica ack to the observation layer
	// (ObserveReplicaRebuildSessionAck). Returns error if the ack is
	// rejected (stale session, etc.). Shipper state transitions happen
	// only after successful observation.
	OnAck func(blockvol.SessionAckMsg) error
	// TransitionShipper changes the shipper state. Called only after
	// OnAck succeeds. Used for: NeedsRebuild → Rebuilding (on accepted),
	// Rebuilding → InSync (on completed), Rebuilding → NeedsRebuild (on failure).
	TransitionShipper func(blockvol.ReplicaState)
}

// TransferFullBase sends a session control to the replica and blocks until
// the rebuild completes or fails. committedLSN is the engine's frozen target
// from PlanRebuild — the replica must reach at least this LSN for completion.
//
// Protocol:
//  1. Dial replica ctrl addr (fresh connection, separate from barrier)
//  2. Send atomic SessionControlV2 with RebuildAddr trailer
//  3. Read acks in loop; forward each to OnAck for observation
//  4. On SessionAckAccepted: transition shipper to Rebuilding (live WAL lane opens)
//  5. On SessionAckCompleted: transition shipper to InSync, return achievedLSN
//  6. On SessionAckFailed or error: transition shipper to NeedsRebuild, return error
func (r *RemoteRebuildIO) TransferFullBase(committedLSN uint64) (uint64, error) {
	conn, err := net.DialTimeout("tcp", r.ReplicaCtrlAddr, 5*time.Second)
	if err != nil {
		return 0, fmt.Errorf("remote rebuild: dial ctrl %s: %w", r.ReplicaCtrlAddr, err)
	}
	defer conn.Close()

	// Set a generous deadline for the entire rebuild session.
	conn.SetDeadline(time.Now().Add(10 * time.Minute))

	// Send atomic v2 session control with RebuildAddr trailer.
	msg := blockvol.SessionControlMsg{
		Epoch:       r.Epoch,
		SessionID:   r.SessionID,
		Command:     blockvol.SessionCmdStartRebuild,
		BaseLSN:     r.BaseLSN,
		TargetLSN:   committedLSN, // engine's frozen rebuild target
		RebuildAddr: r.RebuildAddr,
	}
	if err := blockvol.SendSessionControlV2(conn, msg); err != nil {
		return 0, fmt.Errorf("remote rebuild: send session control: %w", err)
	}

	glog.V(0).Infof("remote rebuild: sent start_rebuild session=%d base=%d target=%d rebuild=%s → %s",
		r.SessionID, r.BaseLSN, committedLSN, r.RebuildAddr, r.ReplicaCtrlAddr)

	// Read acks until terminal phase.
	for {
		msgType, payload, err := blockvol.ReadFrame(conn)
		if err != nil {
			r.transitionOnFailure()
			return 0, fmt.Errorf("remote rebuild: read ack: %w", err)
		}
		if msgType != blockvol.MsgSessionAck {
			continue
		}
		ack, err := blockvol.DecodeSessionAck(payload)
		if err != nil {
			r.transitionOnFailure()
			return 0, fmt.Errorf("remote rebuild: decode ack: %w", err)
		}

		// Forward to observation layer. Only transition shipper state
		// if observation succeeds (Rule 1: ack-gated transitions).
		ackErr := r.forwardAck(ack)

		switch ack.Phase {
		case blockvol.SessionAckAccepted:
			if ackErr != nil {
				r.transitionOnFailure()
				return 0, fmt.Errorf("remote rebuild: accepted ack rejected by observation: %w", ackErr)
			}
			if r.TransitionShipper != nil {
				r.TransitionShipper(blockvol.ReplicaRebuilding)
			}
			glog.V(0).Infof("remote rebuild: session %d accepted by replica", r.SessionID)

		case blockvol.SessionAckRunning, blockvol.SessionAckBaseComplete:
			// Fail closed: if observation rejects a progress ack, the primary
			// no longer considers this session valid (stale ID, wrong kind, etc.).
			if ackErr != nil {
				r.transitionOnFailure()
				return 0, fmt.Errorf("remote rebuild: progress ack rejected by observation: %w", ackErr)
			}

		case blockvol.SessionAckCompleted:
			if ackErr != nil {
				r.transitionOnFailure()
				return 0, fmt.Errorf("remote rebuild: completed ack rejected by observation: %w", ackErr)
			}
			if r.TransitionShipper != nil {
				r.TransitionShipper(blockvol.ReplicaInSync)
			}
			achieved := ack.AchievedLSN
			if achieved == 0 {
				achieved = ack.WALAppliedLSN
			}
			glog.V(0).Infof("remote rebuild: session %d completed (achieved=%d)", r.SessionID, achieved)
			return achieved, nil

		case blockvol.SessionAckFailed:
			r.transitionOnFailure()
			if ackErr != nil {
				// Observation rejected the ack (stale session, etc.) — don't use
				// sentinel. Return a regular error so ExecutePendingRebuild emits
				// the fallback SessionFailed since observation didn't handle it.
				return 0, fmt.Errorf("remote rebuild: session %d failed (observation rejected: %w)", r.SessionID, ackErr)
			}
			// Observation accepted the ack and already emitted SessionFailed.
			// Use sentinel so ExecutePendingRebuild doesn't double-emit.
			return 0, fmt.Errorf("remote rebuild: session %d: %w", r.SessionID, errRebuildAckFailed)
		}
	}
}

// TransferSnapshot is not supported in the V1 remote rebuild path.
func (r *RemoteRebuildIO) TransferSnapshot(snapshotLSN uint64) error {
	return fmt.Errorf("remote rebuild: TransferSnapshot not supported (v1 full-base only)")
}

// StreamWALEntries is not supported — WAL flows through the shipper's data channel.
func (r *RemoteRebuildIO) StreamWALEntries(startExclusive, endInclusive uint64) (uint64, error) {
	return 0, fmt.Errorf("remote rebuild: StreamWALEntries not supported (WAL flows through shipper)")
}

func (r *RemoteRebuildIO) forwardAck(ack blockvol.SessionAckMsg) error {
	if r.OnAck == nil {
		return nil
	}
	return r.OnAck(ack)
}

func (r *RemoteRebuildIO) transitionOnFailure() {
	if r.TransitionShipper != nil {
		r.TransitionShipper(blockvol.ReplicaNeedsRebuild)
	}
}
