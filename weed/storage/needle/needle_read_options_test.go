package needle

import (
	"bytes"
	"io"
	"reflect"
	"testing"
	"time"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type mockBackend struct {
	data []byte
}

func (m *mockBackend) ReadAt(p []byte, off int64) (n int, err error) {
	if int(off) >= len(m.data) {
		return 0, io.EOF
	}
	n = copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *mockBackend) GetStat() (int64, time.Time, error) {
	return int64(len(m.data)), time.Time{}, nil
}

func (m *mockBackend) Name() string {
	return "mock"
}

func (m *mockBackend) Close() error {
	return nil
}

func (m *mockBackend) Sync() error {
	return nil
}

func (m *mockBackend) Truncate(size int64) error {
	m.data = m.data[:size]
	return nil
}

func (m *mockBackend) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, nil
}

func TestReadFromFile_EquivalenceWithReadData(t *testing.T) {
	n := &Needle{
		Cookie:       0x12345678,
		Id:           0x1122334455667788,
		Data:         []byte("hello world"),
		Flags:        0xFF,
		Name:         []byte("filename.txt"),
		Mime:         []byte("text/plain"),
		LastModified: 0x1234567890,
		Ttl:          nil,
		Pairs:        []byte("key=value"),
		PairsSize:    9,
		Checksum:     0xCAFEBABE,
		AppendAtNs:   0xDEADBEEF,
	}
	buf := &bytes.Buffer{}
	_, _, err := writeNeedleV2(n, 0, buf)
	if err != nil {
		t.Fatalf("writeNeedleV2 failed: %v", err)
	}
	backend := &mockBackend{data: buf.Bytes()}
	size := Size(len(buf.Bytes()) - NeedleHeaderSize - NeedleChecksumSize - int(PaddingLength(Size(len(buf.Bytes())-NeedleHeaderSize-NeedleChecksumSize), Version2)))

	// Old method
	nOld := &Needle{}
	errOld := nOld.ReadData(backend, 0, size, Version2)

	// New method
	nNew := &Needle{}
	opts := NeedleReadOptions{ReadHeader: true, ReadData: true, ReadMeta: true}
	errNew := nNew.ReadFromFile(backend, 0, size, Version2, opts)

	if (errOld != nil) != (errNew != nil) || (errOld != nil && errOld.Error() != errNew.Error()) {
		t.Errorf("error mismatch: old=%v new=%v", errOld, errNew)
	}

	if !reflect.DeepEqual(nOld, nNew) {
		t.Errorf("needle mismatch: old=%+v new=%+v", nOld, nNew)
	}
}
