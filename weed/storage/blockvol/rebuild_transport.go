package blockvol

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

// Session control message types (on control channel, distinct from barrier).
const (
	MsgSessionControl byte = 0x10 // primary → replica: session command
	MsgSessionAck     byte = 0x11 // replica → primary: session progress/result
)

// Session control command kinds.
const (
	SessionCmdStartRebuild byte = 0x01
	SessionCmdCancel       byte = 0x02
)

// Session ack phase codes (wire representation of RebuildSessionPhase).
const (
	SessionAckAccepted     byte = 0x01
	SessionAckRunning      byte = 0x02
	SessionAckBaseComplete byte = 0x03
	SessionAckCompleted    byte = 0x04
	SessionAckFailed       byte = 0x05
)

// SessionControlMsg is the wire message for session control commands.
//
// Wire version: v1 (33 bytes). This is a new protocol with no deployed peers.
// The initial design had a 37-byte format with a SnapshotID field that was
// removed because the protocol contract uses flushed checkpoint boundaries,
// not explicit snapshot IDs. If future versions need additional fields, the
// decoder should check len(buf) and handle both sizes.
type SessionControlMsg struct {
	Epoch     uint64
	SessionID uint64
	Command   byte
	BaseLSN   uint64 // flushed/checkpoint boundary for start_rebuild
	TargetLSN uint64 // WAL target for start_rebuild
}

// EncodeSessionControl serializes a session control message.
// Wire: [8B epoch][8B sessionID][1B cmd][8B baseLSN][8B targetLSN] = 33 bytes.
func EncodeSessionControl(msg SessionControlMsg) []byte {
	buf := make([]byte, 33)
	binary.BigEndian.PutUint64(buf[0:8], msg.Epoch)
	binary.BigEndian.PutUint64(buf[8:16], msg.SessionID)
	buf[16] = msg.Command
	binary.BigEndian.PutUint64(buf[17:25], msg.BaseLSN)
	binary.BigEndian.PutUint64(buf[25:33], msg.TargetLSN)
	return buf
}

// DecodeSessionControl deserializes a session control message.
func DecodeSessionControl(buf []byte) (SessionControlMsg, error) {
	if len(buf) < 33 {
		return SessionControlMsg{}, fmt.Errorf("session control: short message (%d bytes)", len(buf))
	}
	return SessionControlMsg{
		Epoch:     binary.BigEndian.Uint64(buf[0:8]),
		SessionID: binary.BigEndian.Uint64(buf[8:16]),
		Command:   buf[16],
		BaseLSN:   binary.BigEndian.Uint64(buf[17:25]),
		TargetLSN: binary.BigEndian.Uint64(buf[25:33]),
	}, nil
}

// SessionAckMsg is the wire message for session progress/result.
type SessionAckMsg struct {
	Epoch         uint64
	SessionID     uint64
	Phase         byte
	WALAppliedLSN uint64
	BaseComplete  bool
	AchievedLSN   uint64 // on completion
}

// EncodeSessionAck serializes a session ack message.
// Wire: [8B epoch][8B sessionID][1B phase][8B walAppliedLSN][1B baseComplete][8B achievedLSN] = 34 bytes.
func EncodeSessionAck(msg SessionAckMsg) []byte {
	buf := make([]byte, 34)
	binary.BigEndian.PutUint64(buf[0:8], msg.Epoch)
	binary.BigEndian.PutUint64(buf[8:16], msg.SessionID)
	buf[16] = msg.Phase
	binary.BigEndian.PutUint64(buf[17:25], msg.WALAppliedLSN)
	if msg.BaseComplete {
		buf[25] = 1
	}
	binary.BigEndian.PutUint64(buf[26:34], msg.AchievedLSN)
	return buf
}

// DecodeSessionAck deserializes a session ack message.
func DecodeSessionAck(buf []byte) (SessionAckMsg, error) {
	if len(buf) < 34 {
		return SessionAckMsg{}, fmt.Errorf("session ack: short message (%d bytes)", len(buf))
	}
	return SessionAckMsg{
		Epoch:         binary.BigEndian.Uint64(buf[0:8]),
		SessionID:     binary.BigEndian.Uint64(buf[8:16]),
		Phase:         buf[16],
		WALAppliedLSN: binary.BigEndian.Uint64(buf[17:25]),
		BaseComplete:  buf[25] != 0,
		AchievedLSN:   binary.BigEndian.Uint64(buf[26:34]),
	}, nil
}

// RebuildTransportServer handles the primary-side rebuild data serving for one
// session. It streams snapshot base blocks to the replica over a dedicated TCP
// connection (the existing rebuild server path).
type RebuildTransportServer struct {
	vol       *BlockVol
	sessionID uint64
	epoch     uint64
	baseLSN   uint64
	targetLSN uint64
}

// NewRebuildTransportServer creates a primary-side rebuild transport server
// for one session.
func NewRebuildTransportServer(vol *BlockVol, sessionID, epoch, baseLSN, targetLSN uint64) *RebuildTransportServer {
	return &RebuildTransportServer{
		vol:       vol,
		sessionID: sessionID,
		epoch:     epoch,
		baseLSN:   baseLSN,
		targetLSN: targetLSN,
	}
}

// ServeBaseBlocks streams the current extent image to the replica connection.
// Each block is sent as MsgRebuildExtent with the LBA encoded in the first 8
// bytes. The stream ends with MsgRebuildDone.
//
// Contract: the base stream is the extent image anchored at or above base_lsn.
// It is NOT an exact point-in-time snapshot — concurrent flusher writes may
// advance some LBAs past base_lsn during a long rebuild. This is correct
// because the two-line model guarantees convergence: the WAL lane covers
// base_lsn+1 onward, and the bitmap ensures WAL-applied data always wins
// over base data regardless of ordering.
//
// Reads use readBlockFromExtent (bypassing dirty map) to avoid returning
// unflushed WAL data that the WAL lane will deliver separately.
func (s *RebuildTransportServer) ServeBaseBlocks(conn net.Conn) error {
	if s.vol == nil {
		return fmt.Errorf("rebuild transport: volume is nil")
	}

	conn.SetDeadline(time.Now().Add(10 * time.Minute))
	defer conn.SetDeadline(time.Time{})

	// Flush to ensure all WAL entries up to checkpoint are in the extent.
	if err := s.vol.ForceFlush(); err != nil {
		return fmt.Errorf("rebuild transport: flush before base stream: %v", err)
	}

	// Verify checkpoint meets the requested base_lsn.
	status := s.vol.Status()
	if s.baseLSN > 0 && status.CheckpointLSN < s.baseLSN {
		return fmt.Errorf("rebuild transport: checkpoint %d < requested base_lsn %d after flush",
			status.CheckpointLSN, s.baseLSN)
	}

	info := s.vol.Info()
	blockSize := uint64(info.BlockSize)
	totalLBAs := info.VolumeSize / blockSize

	var sentBlocks uint64
	for lba := uint64(0); lba < totalLBAs; lba++ {
		// Read directly from extent, bypassing dirty map. This avoids
		// returning unflushed WAL data that the WAL lane will deliver
		// separately. The extent may contain data flushed after base_lsn
		// on some LBAs — the two-line model handles this via bitmap.
		data, err := s.vol.readBlockFromExtent(lba)
		if err != nil {
			return fmt.Errorf("rebuild transport: read extent LBA %d: %w", lba, err)
		}

		// Encode: [8B LBA][block data]
		frame := make([]byte, 8+len(data))
		binary.BigEndian.PutUint64(frame[0:8], lba)
		copy(frame[8:], data)

		if err := WriteFrame(conn, MsgRebuildExtent, frame); err != nil {
			return fmt.Errorf("rebuild transport: send LBA %d: %w", lba, err)
		}
		sentBlocks++
	}

	// Send completion marker. The first 8 bytes remain totalBlocks for
	// compatibility; the optional second 8 bytes carry the current extent
	// boundary so session-controlled executors can surface achievedLSN > target.
	doneBuf := make([]byte, 16)
	binary.BigEndian.PutUint64(doneBuf[0:8], sentBlocks)
	achievedLSN := s.baseLSN
	if next := s.vol.nextLSN.Load(); next > 0 {
		achievedLSN = next - 1
	}
	binary.BigEndian.PutUint64(doneBuf[8:16], achievedLSN)
	if err := WriteFrame(conn, MsgRebuildDone, doneBuf); err != nil {
		return fmt.Errorf("rebuild transport: send done: %w", err)
	}

	log.Printf("rebuild transport: served %d base blocks for session %d", sentBlocks, s.sessionID)
	return nil
}

// RebuildTransportClient handles the replica-side rebuild data receiving for
// one session. It receives snapshot base blocks from the primary and routes
// them through the rebuild session's base lane.
type RebuildTransportClient struct {
	vol       *BlockVol
	sessionID uint64
}

// NewRebuildTransportClient creates a replica-side rebuild transport client
// for one session.
func NewRebuildTransportClient(vol *BlockVol, sessionID uint64) *RebuildTransportClient {
	return &RebuildTransportClient{
		vol:       vol,
		sessionID: sessionID,
	}
}

// ReceiveBaseBlocks reads base blocks from the primary connection and applies
// them through the rebuild session. Returns the total number of blocks processed.
func (c *RebuildTransportClient) ReceiveBaseBlocks(conn net.Conn) (uint64, error) {
	totalBlocks, _, err := c.ReceiveBaseBlocksWithStatus(conn)
	return totalBlocks, err
}

// ReceiveBaseBlocksWithStatus reads base blocks from the primary connection,
// applies them through the rebuild session, and returns both the total block
// count and the primary's authoritative achievedLSN if the sender included it.
func (c *RebuildTransportClient) ReceiveBaseBlocksWithStatus(conn net.Conn) (uint64, uint64, error) {
	if c.vol == nil {
		return 0, 0, fmt.Errorf("rebuild transport: volume is nil")
	}

	conn.SetDeadline(time.Now().Add(10 * time.Minute))
	defer conn.SetDeadline(time.Time{})

	var totalBlocks uint64
	var achievedLSN uint64
	for {
		msgType, payload, err := ReadFrame(conn)
		if err != nil {
			if err == io.EOF {
				break
			}
			return totalBlocks, achievedLSN, fmt.Errorf("rebuild transport: read frame: %w", err)
		}

		switch msgType {
		case MsgRebuildExtent:
			if len(payload) < 8 {
				return totalBlocks, achievedLSN, fmt.Errorf("rebuild transport: short extent frame")
			}
			lba := binary.BigEndian.Uint64(payload[0:8])
			data := payload[8:]
			if _, err := c.vol.ApplyRebuildSessionBaseBlock(c.sessionID, lba, data); err != nil {
				return totalBlocks, achievedLSN, fmt.Errorf("rebuild transport: apply base LBA %d: %w", lba, err)
			}
			totalBlocks++

		case MsgRebuildDone:
			if len(payload) >= 16 {
				achievedLSN = binary.BigEndian.Uint64(payload[8:16])
			}
			if err := c.vol.MarkRebuildSessionBaseComplete(c.sessionID, totalBlocks); err != nil {
				return totalBlocks, achievedLSN, fmt.Errorf("rebuild transport: mark base complete: %w", err)
			}
			log.Printf("rebuild transport: received %d base blocks for session %d", totalBlocks, c.sessionID)
			return totalBlocks, achievedLSN, nil

		case MsgRebuildError:
			return totalBlocks, achievedLSN, fmt.Errorf("rebuild transport: server error: %s", string(payload))

		default:
			return totalBlocks, achievedLSN, fmt.Errorf("rebuild transport: unexpected message type 0x%02x", msgType)
		}
	}
	return totalBlocks, achievedLSN, nil
}

// SendSessionControl sends a session control message on the control connection.
func SendSessionControl(conn net.Conn, msg SessionControlMsg) error {
	return WriteFrame(conn, MsgSessionControl, EncodeSessionControl(msg))
}

// SendSessionAck sends a session ack message on the control connection.
func SendSessionAck(conn net.Conn, msg SessionAckMsg) error {
	return WriteFrame(conn, MsgSessionAck, EncodeSessionAck(msg))
}
