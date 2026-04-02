package blockvol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Data channel message types.
const (
	MsgWALEntry       byte = 0x01
	MsgResumeShipReq  byte = 0x03 // CP13-5: reconnect handshake request
	MsgResumeShipResp byte = 0x04 // CP13-5: reconnect handshake response
	MsgCatchupDone    byte = 0x05 // CP13-5: end of catch-up stream
)

// ResumeShip status codes.
const (
	ResumeOK             byte = 0x00
	ResumeEpochMismatch  byte = 0x01
	ResumeNeedsRebuild   byte = 0x02
)

// Control channel message types.
const (
	MsgBarrierReq  byte = 0x01
	MsgBarrierResp byte = 0x02
)

// Barrier response status codes.
const (
	BarrierOK            byte = 0x00
	BarrierEpochMismatch byte = 0x01
	BarrierTimeout       byte = 0x02
	BarrierFsyncFailed   byte = 0x03
)

// BarrierRequest is sent by the primary to the replica on the control channel.
type BarrierRequest struct {
	Vid   uint32
	LSN   uint64
	Epoch uint64
}

// BarrierResponse is the replica's reply to a barrier request.
// Wire format: [1B status][8B flushedLSN] = 9 bytes.
// Legacy replicas send only 1 byte (FlushedLSN defaults to 0).
type BarrierResponse struct {
	Status     byte
	FlushedLSN uint64 // replica's durable WAL progress after this barrier
}

// EncodeBarrierResponse serializes a BarrierResponse (1+8 = 9 bytes).
func EncodeBarrierResponse(resp BarrierResponse) []byte {
	buf := make([]byte, 9)
	buf[0] = resp.Status
	binary.BigEndian.PutUint64(buf[1:9], resp.FlushedLSN)
	return buf
}

// DecodeBarrierResponse deserializes a BarrierResponse.
// Handles both 9-byte (new) and 1-byte (legacy) responses.
func DecodeBarrierResponse(buf []byte) BarrierResponse {
	if len(buf) < 1 {
		return BarrierResponse{}
	}
	resp := BarrierResponse{Status: buf[0]}
	if len(buf) >= 9 {
		resp.FlushedLSN = binary.BigEndian.Uint64(buf[1:9])
	}
	return resp
}

// Frame header: [1B type][4B payload_len].
const frameHeaderSize = 5

// maxFramePayload caps the payload size to prevent OOM on corrupt data.
const maxFramePayload = 256 * 1024 * 1024 // 256MB

var (
	ErrFrameTooLarge = errors.New("repl: frame payload exceeds maximum size")
	ErrFrameEmpty    = errors.New("repl: empty frame payload")
)

// WriteFrame writes a length-prefixed frame: [1B type][4B len][payload].
func WriteFrame(w io.Writer, msgType byte, payload []byte) error {
	hdr := make([]byte, frameHeaderSize)
	hdr[0] = msgType
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(payload)))
	if _, err := w.Write(hdr); err != nil {
		return fmt.Errorf("repl: write frame header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return fmt.Errorf("repl: write frame payload: %w", err)
		}
	}
	return nil
}

// ReadFrame reads a length-prefixed frame and returns message type and payload.
func ReadFrame(r io.Reader) (msgType byte, payload []byte, err error) {
	hdr := make([]byte, frameHeaderSize)
	if _, err = io.ReadFull(r, hdr); err != nil {
		return 0, nil, fmt.Errorf("repl: read frame header: %w", err)
	}
	msgType = hdr[0]
	payloadLen := binary.BigEndian.Uint32(hdr[1:5])
	if payloadLen > maxFramePayload {
		return 0, nil, ErrFrameTooLarge
	}
	payload = make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err = io.ReadFull(r, payload); err != nil {
			return 0, nil, fmt.Errorf("repl: read frame payload: %w", err)
		}
	}
	return msgType, payload, nil
}

// Rebuild message types (on rebuild channel).
const (
	MsgRebuildReq    byte = 0x10 // client -> server
	MsgRebuildEntry  byte = 0x11 // server -> client: WAL entry
	MsgRebuildExtent byte = 0x12 // server -> client: extent chunk
	MsgRebuildDone   byte = 0x13 // server -> client: stream complete
	MsgRebuildError  byte = 0x14 // server -> client: error
)

// Rebuild request types.
const (
	RebuildWALCatchUp  byte = 0x01
	RebuildFullExtent  byte = 0x02
	RebuildSnapshot    byte = 0x03 // P2: exact snapshot export at requested BaseLSN
)

// RebuildRequest is sent by the rebuilding replica to the primary.
type RebuildRequest struct {
	Type    byte   // RebuildWALCatchUp or RebuildFullExtent
	FromLSN uint64
	Epoch   uint64
}

// EncodeRebuildRequest serializes a RebuildRequest (1+8+8 = 17 bytes).
func EncodeRebuildRequest(req RebuildRequest) []byte {
	buf := make([]byte, 17)
	buf[0] = req.Type
	binary.BigEndian.PutUint64(buf[1:9], req.FromLSN)
	binary.BigEndian.PutUint64(buf[9:17], req.Epoch)
	return buf
}

// DecodeRebuildRequest deserializes a RebuildRequest.
func DecodeRebuildRequest(buf []byte) (RebuildRequest, error) {
	if len(buf) < 17 {
		return RebuildRequest{}, fmt.Errorf("repl: rebuild request too short: %d bytes", len(buf))
	}
	return RebuildRequest{
		Type:    buf[0],
		FromLSN: binary.BigEndian.Uint64(buf[1:9]),
		Epoch:   binary.BigEndian.Uint64(buf[9:17]),
	}, nil
}

// EncodeBarrierRequest serializes a BarrierRequest (4+8+8 = 20 bytes).
func EncodeBarrierRequest(req BarrierRequest) []byte {
	buf := make([]byte, 20)
	binary.BigEndian.PutUint32(buf[0:4], req.Vid)
	binary.BigEndian.PutUint64(buf[4:12], req.LSN)
	binary.BigEndian.PutUint64(buf[12:20], req.Epoch)
	return buf
}

// DecodeBarrierRequest deserializes a BarrierRequest.
func DecodeBarrierRequest(buf []byte) (BarrierRequest, error) {
	if len(buf) < 20 {
		return BarrierRequest{}, fmt.Errorf("repl: barrier request too short: %d bytes", len(buf))
	}
	return BarrierRequest{
		Vid:   binary.BigEndian.Uint32(buf[0:4]),
		LSN:   binary.BigEndian.Uint64(buf[4:12]),
		Epoch: binary.BigEndian.Uint64(buf[12:20]),
	}, nil
}

// --- CP13-5: Reconnect handshake messages ---

// ResumeShipReq is sent by the primary on the data channel after reconnect.
type ResumeShipReq struct {
	Epoch          uint64
	PrimaryHeadLSN uint64
	WalRetainStart uint64
}

// EncodeResumeShipReq serializes a ResumeShipReq (8+8+8 = 24 bytes).
func EncodeResumeShipReq(req ResumeShipReq) []byte {
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], req.Epoch)
	binary.BigEndian.PutUint64(buf[8:16], req.PrimaryHeadLSN)
	binary.BigEndian.PutUint64(buf[16:24], req.WalRetainStart)
	return buf
}

// DecodeResumeShipReq deserializes a ResumeShipReq.
func DecodeResumeShipReq(buf []byte) (ResumeShipReq, error) {
	if len(buf) < 24 {
		return ResumeShipReq{}, fmt.Errorf("repl: resume ship req too short: %d bytes", len(buf))
	}
	return ResumeShipReq{
		Epoch:          binary.BigEndian.Uint64(buf[0:8]),
		PrimaryHeadLSN: binary.BigEndian.Uint64(buf[8:16]),
		WalRetainStart: binary.BigEndian.Uint64(buf[16:24]),
	}, nil
}

// ResumeShipResp is the replica's reply on the data channel.
type ResumeShipResp struct {
	Status            byte
	ReplicaFlushedLSN uint64
}

// EncodeResumeShipResp serializes a ResumeShipResp (1+8 = 9 bytes).
func EncodeResumeShipResp(resp ResumeShipResp) []byte {
	buf := make([]byte, 9)
	buf[0] = resp.Status
	binary.BigEndian.PutUint64(buf[1:9], resp.ReplicaFlushedLSN)
	return buf
}

// DecodeResumeShipResp deserializes a ResumeShipResp.
func DecodeResumeShipResp(buf []byte) (ResumeShipResp, error) {
	if len(buf) < 9 {
		return ResumeShipResp{}, fmt.Errorf("repl: resume ship resp too short: %d bytes", len(buf))
	}
	return ResumeShipResp{
		Status:            buf[0],
		ReplicaFlushedLSN: binary.BigEndian.Uint64(buf[1:9]),
	}, nil
}

// EncodeCatchupDone serializes the catch-up done marker (8 bytes: snapshotLSN).
func EncodeCatchupDone(snapshotLSN uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:8], snapshotLSN)
	return buf
}

// DecodeCatchupDone deserializes the catch-up done marker.
func DecodeCatchupDone(buf []byte) (uint64, error) {
	if len(buf) < 8 {
		return 0, fmt.Errorf("repl: catchup done too short: %d bytes", len(buf))
	}
	return binary.BigEndian.Uint64(buf[0:8]), nil
}
