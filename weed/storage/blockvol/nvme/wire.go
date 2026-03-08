package nvme

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// ---------- Reader ----------

// Reader decodes NVMe/TCP PDUs from a stream.
//
// Usage:
//
//	hdr, _ := r.Dequeue()            // read 8-byte CommonHeader
//	r.Receive(&capsuleCmd)           // read remaining specific header
//	if r.Length() > 0 {
//	    data := make([]byte, r.Length())
//	    r.ReceiveData(data)          // read payload
//	}
type Reader struct {
	rd     io.Reader
	CH     CommonHeader
	header [maxHeaderSize]byte
}

// NewReader wraps an io.Reader for NVMe/TCP PDU decoding.
func NewReader(r io.Reader) *Reader {
	return &Reader{rd: r}
}

// Dequeue reads the 8-byte CommonHeader, validates bounds, and returns it.
func (r *Reader) Dequeue() (*CommonHeader, error) {
	if _, err := io.ReadFull(r.rd, r.header[:commonHeaderSize]); err != nil {
		return nil, err
	}
	r.CH.Unmarshal(r.header[:commonHeaderSize])

	// Validate header bounds to prevent panics on malformed PDUs.
	if r.CH.HeaderLength < commonHeaderSize {
		return nil, fmt.Errorf("nvme: HeaderLength %d < minimum %d", r.CH.HeaderLength, commonHeaderSize)
	}
	if r.CH.HeaderLength > maxHeaderSize {
		return nil, fmt.Errorf("nvme: HeaderLength %d > maximum %d", r.CH.HeaderLength, maxHeaderSize)
	}
	if r.CH.DataOffset != 0 && r.CH.DataOffset < r.CH.HeaderLength {
		return nil, fmt.Errorf("nvme: DataOffset %d < HeaderLength %d", r.CH.DataOffset, r.CH.HeaderLength)
	}
	if r.CH.DataOffset != 0 && uint32(r.CH.DataOffset) > r.CH.DataLength {
		return nil, fmt.Errorf("nvme: DataOffset %d > DataLength %d", r.CH.DataOffset, r.CH.DataLength)
	}
	if r.CH.DataLength < uint32(r.CH.HeaderLength) {
		return nil, fmt.Errorf("nvme: DataLength %d < HeaderLength %d", r.CH.DataLength, r.CH.HeaderLength)
	}
	// DataOffset==0 means no inline data — DataLength must equal HeaderLength,
	// otherwise unconsumed bytes desynchronize the stream.
	if r.CH.DataOffset == 0 && r.CH.DataLength != uint32(r.CH.HeaderLength) {
		return nil, fmt.Errorf("nvme: DataOffset=0 but DataLength %d != HeaderLength %d", r.CH.DataLength, r.CH.HeaderLength)
	}

	return &r.CH, nil
}

// Receive reads the remaining PDU-specific header (HeaderLength - 8 bytes)
// and unmarshals it into pdu. It also skips any padding between header and
// data (DataOffset - HeaderLength bytes).
func (r *Reader) Receive(pdu PDU) error {
	remain := int(r.CH.HeaderLength) - commonHeaderSize
	if remain <= 0 {
		return nil
	}
	if _, err := io.ReadFull(r.rd, r.header[commonHeaderSize:r.CH.HeaderLength]); err != nil {
		return err
	}
	pdu.Unmarshal(r.header[commonHeaderSize:r.CH.HeaderLength])

	// Skip padding between header and data.
	pad := int(r.CH.DataOffset) - int(r.CH.HeaderLength)
	if pad > 0 {
		if _, err := io.ReadFull(r.rd, make([]byte, pad)); err != nil {
			return err
		}
	}
	return nil
}

// Length returns the payload size: DataLength - DataOffset (when DataOffset != 0).
func (r *Reader) Length() uint32 {
	if r.CH.DataOffset != 0 {
		return r.CH.DataLength - uint32(r.CH.DataOffset)
	}
	return 0
}

// ReceiveData reads exactly len(buf) bytes of payload data.
func (r *Reader) ReceiveData(buf []byte) error {
	_, err := io.ReadFull(r.rd, buf)
	return err
}

// ---------- Writer ----------

// Writer encodes NVMe/TCP PDUs to a stream.
type Writer struct {
	wr     *bufio.Writer
	CH     CommonHeader
	header [maxHeaderSize]byte
}

// NewWriter wraps an io.Writer for NVMe/TCP PDU encoding.
func NewWriter(w io.Writer) *Writer {
	return &Writer{wr: bufio.NewWriter(w)}
}

// PrepareHeaderOnly sets up a header-only PDU (no payload).
// Call Flush() to write it to the wire.
func (w *Writer) PrepareHeaderOnly(pduType uint8, pdu PDU, specificLen uint8) {
	w.CH.Type = pduType
	w.CH.Flags = 0
	w.CH.HeaderLength = commonHeaderSize + specificLen
	w.CH.DataOffset = 0
	w.CH.DataLength = uint32(w.CH.HeaderLength)
	pdu.Marshal(w.header[commonHeaderSize:])
}

// PrepareWithData sets up a PDU with payload data.
// Call Flush() to write it to the wire.
func (w *Writer) PrepareWithData(pduType, flags uint8, pdu PDU, specificLen uint8, data []byte) {
	w.CH.Type = pduType
	w.CH.Flags = flags
	w.CH.HeaderLength = commonHeaderSize + specificLen
	if data != nil {
		w.CH.DataOffset = w.CH.HeaderLength
		w.CH.DataLength = uint32(w.CH.HeaderLength) + uint32(len(data))
	} else {
		w.CH.DataOffset = 0
		w.CH.DataLength = uint32(w.CH.HeaderLength)
	}
	pdu.Marshal(w.header[commonHeaderSize:])
}

// Flush writes the prepared CommonHeader + specific header to the wire.
// If there was payload data (from PrepareWithData), call FlushData after.
func (w *Writer) Flush() error {
	w.CH.Marshal(w.header[:commonHeaderSize])
	if _, err := w.wr.Write(w.header[:w.CH.HeaderLength]); err != nil {
		return err
	}
	return nil
}

// FlushData writes payload data and flushes the underlying buffered writer.
func (w *Writer) FlushData(data []byte) error {
	if len(data) > 0 {
		if _, err := w.wr.Write(data); err != nil {
			return err
		}
	}
	return w.wr.Flush()
}

// SendHeaderOnly writes a complete header-only PDU (prepare + flush).
func (w *Writer) SendHeaderOnly(pduType uint8, pdu PDU, specificLen uint8) error {
	w.PrepareHeaderOnly(pduType, pdu, specificLen)
	if err := w.Flush(); err != nil {
		return err
	}
	return w.wr.Flush()
}

// SendWithData writes a complete PDU with payload data.
func (w *Writer) SendWithData(pduType, flags uint8, pdu PDU, specificLen uint8, data []byte) error {
	w.PrepareWithData(pduType, flags, pdu, specificLen, data)
	if err := w.Flush(); err != nil {
		return err
	}
	return w.FlushData(data)
}

// writeRaw writes raw bytes directly (used for ConnectData inline in capsule).
func (w *Writer) writeRaw(data []byte) error {
	_, err := w.wr.Write(data)
	return err
}

// flushBuf flushes the underlying buffered writer.
func (w *Writer) flushBuf() error {
	return w.wr.Flush()
}

// ---------- Helpers ----------

// putLE32 writes a uint32 in little-endian.
func putLE32(buf []byte, v uint32) {
	binary.LittleEndian.PutUint32(buf, v)
}

// putLE64 writes a uint64 in little-endian.
func putLE64(buf []byte, v uint64) {
	binary.LittleEndian.PutUint64(buf, v)
}
