package blockvol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Data channel message types.
const (
	MsgWALEntry byte = 0x01
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
type BarrierResponse struct {
	Status byte
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
