package blockvol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestWALEntry(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "wal_entry_roundtrip", run: testWALEntryRoundtrip},
		{name: "wal_entry_trim_no_data", run: testWALEntryTrimNoData},
		{name: "wal_entry_barrier", run: testWALEntryBarrier},
		{name: "wal_entry_crc_valid", run: testWALEntryCRCValid},
		{name: "wal_entry_crc_corrupt", run: testWALEntryCRCCorrupt},
		{name: "wal_entry_max_size", run: testWALEntryMaxSize},
		{name: "wal_entry_zero_length", run: testWALEntryZeroLength},
		{name: "wal_entry_trim_rejects_data", run: testWALEntryTrimRejectsData},
		{name: "wal_entry_barrier_rejects_data", run: testWALEntryBarrierRejectsData},
		{name: "wal_entry_bad_entry_size", run: testWALEntryBadEntrySize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func testWALEntryRoundtrip(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 251) // deterministic pattern
	}

	e := WALEntry{
		LSN:    1,
		Epoch:  0,
		Type:   EntryTypeWrite,
		Flags:  0,
		LBA:    100,
		Length: uint32(len(data)),
		Data:   data,
	}

	buf, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, err := DecodeWALEntry(buf)
	if err != nil {
		t.Fatalf("DecodeWALEntry: %v", err)
	}

	if got.LSN != e.LSN {
		t.Errorf("LSN = %d, want %d", got.LSN, e.LSN)
	}
	if got.Epoch != e.Epoch {
		t.Errorf("Epoch = %d, want %d", got.Epoch, e.Epoch)
	}
	if got.Type != e.Type {
		t.Errorf("Type = %d, want %d", got.Type, e.Type)
	}
	if got.LBA != e.LBA {
		t.Errorf("LBA = %d, want %d", got.LBA, e.LBA)
	}
	if got.Length != e.Length {
		t.Errorf("Length = %d, want %d", got.Length, e.Length)
	}
	if !bytes.Equal(got.Data, e.Data) {
		t.Errorf("Data mismatch")
	}
	if got.EntrySize != uint32(walEntryHeaderSize+len(data)) {
		t.Errorf("EntrySize = %d, want %d", got.EntrySize, walEntryHeaderSize+len(data))
	}
}

func testWALEntryTrimNoData(t *testing.T) {
	// TRIM carries Length (trim size in bytes) but no Data payload.
	e := WALEntry{
		LSN:    5,
		Epoch:  0,
		Type:   EntryTypeTrim,
		LBA:    200,
		Length: 4096,
	}

	buf, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, err := DecodeWALEntry(buf)
	if err != nil {
		t.Fatalf("DecodeWALEntry: %v", err)
	}

	if got.Type != EntryTypeTrim {
		t.Errorf("Type = %d, want %d", got.Type, EntryTypeTrim)
	}
	if got.Length != 4096 {
		t.Errorf("Length = %d, want 4096", got.Length)
	}
	if len(got.Data) != 0 {
		t.Errorf("Data should be empty, got %d bytes", len(got.Data))
	}
	// TRIM with Length but no Data: EntrySize = header only.
	if got.EntrySize != uint32(walEntryHeaderSize) {
		t.Errorf("EntrySize = %d, want %d (header only)", got.EntrySize, walEntryHeaderSize)
	}
}

func testWALEntryBarrier(t *testing.T) {
	e := WALEntry{
		LSN:   10,
		Epoch: 0,
		Type:  EntryTypeBarrier,
	}

	buf, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, err := DecodeWALEntry(buf)
	if err != nil {
		t.Fatalf("DecodeWALEntry: %v", err)
	}

	if got.Type != EntryTypeBarrier {
		t.Errorf("Type = %d, want %d", got.Type, EntryTypeBarrier)
	}
	if len(got.Data) != 0 {
		t.Errorf("Barrier should have no data, got %d bytes", len(got.Data))
	}
}

func testWALEntryCRCValid(t *testing.T) {
	data := []byte("hello blockvol WAL")
	e := WALEntry{
		LSN:    1,
		Type:   EntryTypeWrite,
		LBA:    0,
		Length: uint32(len(data)),
		Data:   data,
	}

	buf, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Decode should succeed with valid CRC.
	_, err = DecodeWALEntry(buf)
	if err != nil {
		t.Fatalf("valid entry should decode without error, got: %v", err)
	}
}

func testWALEntryCRCCorrupt(t *testing.T) {
	data := []byte("hello blockvol WAL")
	e := WALEntry{
		LSN:    1,
		Type:   EntryTypeWrite,
		LBA:    0,
		Length: uint32(len(data)),
		Data:   data,
	}

	buf, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Flip one bit in the data region.
	buf[walEntryHeaderSize-8] ^= 0x01 // flip bit in data area (before CRC/EntrySize footer)

	_, err = DecodeWALEntry(buf)
	if !errors.Is(err, ErrCRCMismatch) {
		t.Errorf("expected ErrCRCMismatch, got %v", err)
	}
}

func testWALEntryMaxSize(t *testing.T) {
	data := make([]byte, 64*1024) // 64KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	e := WALEntry{
		LSN:    99,
		Epoch:  0,
		Type:   EntryTypeWrite,
		LBA:    0,
		Length: uint32(len(data)),
		Data:   data,
	}

	buf, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode 64KB entry: %v", err)
	}

	got, err := DecodeWALEntry(buf)
	if err != nil {
		t.Fatalf("DecodeWALEntry: %v", err)
	}

	if !bytes.Equal(got.Data, data) {
		t.Errorf("64KB data mismatch after roundtrip")
	}
}

func testWALEntryZeroLength(t *testing.T) {
	e := WALEntry{
		LSN:    1,
		Type:   EntryTypeWrite,
		LBA:    0,
		Length: 0,
		Data:   nil,
	}

	_, err := e.Encode()
	if !errors.Is(err, ErrInvalidEntry) {
		t.Errorf("WRITE with zero data: expected ErrInvalidEntry, got %v", err)
	}
}

func testWALEntryTrimRejectsData(t *testing.T) {
	// TRIM allows Length (trim extent) but rejects Data payload.
	e := WALEntry{
		LSN:    1,
		Type:   EntryTypeTrim,
		LBA:    0,
		Length: 4096,
		Data:   []byte("bad!"),
	}
	_, err := e.Encode()
	if !errors.Is(err, ErrInvalidEntry) {
		t.Errorf("TRIM with data payload: expected ErrInvalidEntry, got %v", err)
	}
}

func testWALEntryBarrierRejectsData(t *testing.T) {
	e := WALEntry{
		LSN:    1,
		Type:   EntryTypeBarrier,
		Length: 1,
		Data:   []byte("x"),
	}
	_, err := e.Encode()
	if !errors.Is(err, ErrInvalidEntry) {
		t.Errorf("BARRIER with data: expected ErrInvalidEntry, got %v", err)
	}
}

func testWALEntryBadEntrySize(t *testing.T) {
	data := []byte("test data for entry size validation")
	e := WALEntry{
		LSN:    1,
		Type:   EntryTypeWrite,
		LBA:    0,
		Length: uint32(len(data)),
		Data:   data,
	}

	buf, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Corrupt EntrySize (last 4 bytes of buf).
	le := binary.LittleEndian
	le.PutUint32(buf[len(buf)-4:], 9999)

	_, err = DecodeWALEntry(buf)
	if !errors.Is(err, ErrInvalidEntry) {
		t.Errorf("bad EntrySize: expected ErrInvalidEntry, got %v", err)
	}
}
