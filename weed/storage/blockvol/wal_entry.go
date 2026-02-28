package blockvol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	EntryTypeWrite   = 0x01
	EntryTypeTrim    = 0x02
	EntryTypeBarrier = 0x03
	EntryTypePadding = 0xFF

	// walEntryHeaderSize is the fixed portion: LSN(8) + Epoch(8) + Type(1) +
	// Flags(1) + LBA(8) + Length(4) + CRC32(4) + EntrySize(4) = 38 bytes.
	walEntryHeaderSize = 38
)

var (
	ErrCRCMismatch    = errors.New("blockvol: CRC mismatch")
	ErrInvalidEntry   = errors.New("blockvol: invalid WAL entry")
	ErrEntryTruncated = errors.New("blockvol: WAL entry truncated")
)

// WALEntry is a variable-size record in the WAL region.
type WALEntry struct {
	LSN       uint64
	Epoch     uint64 // writer's epoch (Phase 1: always 0)
	Type      uint8  // EntryTypeWrite, EntryTypeTrim, EntryTypeBarrier
	Flags     uint8
	LBA       uint64 // in blocks
	Length    uint32 // data length in bytes (WRITE: data size, TRIM: trim size, BARRIER: 0)
	Data      []byte // present only for WRITE
	CRC32     uint32 // covers LSN through Data
	EntrySize uint32 // total serialized size
}

// Encode serializes the entry into a byte slice, computing CRC over all fields.
func (e *WALEntry) Encode() ([]byte, error) {
	switch e.Type {
	case EntryTypeWrite:
		if len(e.Data) == 0 {
			return nil, fmt.Errorf("%w: WRITE entry with no data", ErrInvalidEntry)
		}
		if uint32(len(e.Data)) != e.Length {
			return nil, fmt.Errorf("%w: data length %d != Length field %d", ErrInvalidEntry, len(e.Data), e.Length)
		}
	case EntryTypeTrim:
		if len(e.Data) != 0 {
			return nil, fmt.Errorf("%w: TRIM entry must have no data payload", ErrInvalidEntry)
		}
		// TRIM carries Length (trim size in bytes) but no Data payload.
	case EntryTypeBarrier:
		if e.Length != 0 || len(e.Data) != 0 {
			return nil, fmt.Errorf("%w: BARRIER entry must have no data", ErrInvalidEntry)
		}
	}

	totalSize := uint32(walEntryHeaderSize + len(e.Data))
	buf := make([]byte, totalSize)

	le := binary.LittleEndian
	off := 0
	le.PutUint64(buf[off:], e.LSN)
	off += 8
	le.PutUint64(buf[off:], e.Epoch)
	off += 8
	buf[off] = e.Type
	off++
	buf[off] = e.Flags
	off++
	le.PutUint64(buf[off:], e.LBA)
	off += 8
	le.PutUint32(buf[off:], e.Length)
	off += 4

	if len(e.Data) > 0 {
		copy(buf[off:], e.Data)
		off += len(e.Data)
	}

	// CRC covers everything from start through Data (offset 0 to off).
	checksum := crc32.ChecksumIEEE(buf[:off])
	le.PutUint32(buf[off:], checksum)
	off += 4
	le.PutUint32(buf[off:], totalSize)

	e.CRC32 = checksum
	e.EntrySize = totalSize
	return buf, nil
}

// DecodeWALEntry deserializes a WAL entry from buf, validating CRC.
func DecodeWALEntry(buf []byte) (WALEntry, error) {
	if len(buf) < walEntryHeaderSize {
		return WALEntry{}, fmt.Errorf("%w: need %d bytes, have %d", ErrEntryTruncated, walEntryHeaderSize, len(buf))
	}

	le := binary.LittleEndian
	var e WALEntry
	off := 0
	e.LSN = le.Uint64(buf[off:])
	off += 8
	e.Epoch = le.Uint64(buf[off:])
	off += 8
	e.Type = buf[off]
	off++
	e.Flags = buf[off]
	off++
	e.LBA = le.Uint64(buf[off:])
	off += 8
	e.Length = le.Uint32(buf[off:])
	off += 4

	// For WRITE entries, Length is the data payload size.
	// For TRIM entries, Length is the trim extent in bytes (no data payload).
	// For BARRIER/PADDING, Length is 0 (or padding size).
	var dataLen int
	if e.Type == EntryTypeWrite || e.Type == EntryTypePadding {
		dataLen = int(e.Length)
	}

	dataEnd := off + dataLen
	if dataEnd+8 > len(buf) { // +8 for CRC32 + EntrySize
		return WALEntry{}, fmt.Errorf("%w: need %d bytes for data+footer, have %d", ErrEntryTruncated, dataEnd+8, len(buf))
	}

	if dataLen > 0 {
		e.Data = make([]byte, dataLen)
		copy(e.Data, buf[off:dataEnd])
	}
	off = dataEnd

	e.CRC32 = le.Uint32(buf[off:])
	off += 4
	e.EntrySize = le.Uint32(buf[off:])

	// Verify CRC: covers LSN through Data.
	expected := crc32.ChecksumIEEE(buf[:dataEnd])
	if e.CRC32 != expected {
		return WALEntry{}, fmt.Errorf("%w: stored=%08x computed=%08x", ErrCRCMismatch, e.CRC32, expected)
	}

	// Verify EntrySize matches actual layout.
	expectedSize := uint32(walEntryHeaderSize) + uint32(dataLen)
	if e.EntrySize != expectedSize {
		return WALEntry{}, fmt.Errorf("%w: EntrySize=%d, expected=%d", ErrInvalidEntry, e.EntrySize, expectedSize)
	}

	return e, nil
}
