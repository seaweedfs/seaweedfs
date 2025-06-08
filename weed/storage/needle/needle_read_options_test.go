package needle

import (
	"bytes"
	"io"
	"reflect"
	"testing"
	"time"
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
		Checksum:     0,
		AppendAtNs:   0xDEADBEEF,
	}
	// remove the TTL bit in the flags
	n.Flags = n.Flags &^ FlagHasTtl
	//	n.Checksum = NewCRC(n.Data)

	buf := &bytes.Buffer{}
	_, _, err := writeNeedleV2(n, 0, buf)
	if err != nil {
		t.Fatalf("writeNeedleV2 failed: %v", err)
	}
	backend := &mockBackend{data: buf.Bytes()}
	size := n.Size

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

func TestReadFromFile_OptionsMatrix(t *testing.T) {
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
		Checksum:     0,
		AppendAtNs:   0xDEADBEEF,
	}
	n.Flags = n.Flags &^ FlagHasTtl
	n.Checksum = NewCRC(n.Data)

	buf := &bytes.Buffer{}
	_, _, err := writeNeedleV2(n, 0, buf)
	if err != nil {
		t.Fatalf("writeNeedleV2 failed: %v", err)
	}
	backend := &mockBackend{data: buf.Bytes()}
	size := n.Size

	t.Run("ReadHeader only", func(t *testing.T) {
		nHeader := &Needle{}
		opts := NeedleReadOptions{ReadHeader: true}
		err := nHeader.ReadFromFile(backend, 0, size, Version2, opts)
		if err != nil {
			t.Fatalf("ReadFromFile header only failed: %v", err)
		}
		if nHeader.Cookie != n.Cookie || nHeader.Id != n.Id || nHeader.Size != n.Size {
			t.Errorf("header fields mismatch: got %+v want %+v", nHeader, n)
		}
		if nHeader.Data != nil || nHeader.Name != nil || nHeader.Mime != nil || nHeader.Pairs != nil || nHeader.Checksum != 0 {
			t.Errorf("non-header fields should be zero, got %+v", nHeader)
		}
	})

	t.Run("ReadHeader+ReadMeta", func(t *testing.T) {
		nMeta := &Needle{}
		opts := NeedleReadOptions{ReadHeader: true, ReadMeta: true}
		err := nMeta.ReadFromFile(backend, 0, size, Version2, opts)
		if err != nil {
			t.Fatalf("ReadFromFile header+meta failed: %v", err)
		}
		if nMeta.Data != nil {
			t.Errorf("Data should not be set when only meta is read")
		}
		if nMeta.Name == nil || nMeta.Mime == nil || nMeta.Pairs == nil {
			t.Errorf("meta fields should be set, got %+v", nMeta)
		}
		if nMeta.Cookie != n.Cookie || nMeta.Id != n.Id || nMeta.Size != n.Size {
			t.Errorf("header fields mismatch: got %+v want %+v", nMeta, n)
		}
		if nMeta.Checksum != n.Checksum {
			t.Errorf("checksum mismatch: got %d want %d", nMeta.Checksum, n.Checksum)
		}
	})

	t.Run("ReadHeader+ReadData", func(t *testing.T) {
		// this is the same as ReadHeader+ReadData+ReadMeta
	})

	t.Run("ReadHeader+ReadData+ReadMeta", func(t *testing.T) {
		nFull := &Needle{}
		opts := NeedleReadOptions{ReadHeader: true, ReadData: true, ReadMeta: true}
		err := nFull.ReadFromFile(backend, 0, size, Version2, opts)
		if err != nil {
			t.Fatalf("ReadFromFile header+data+meta failed: %v", err)
		}
		nFull.AppendAtNs = n.AppendAtNs
		if !reflect.DeepEqual(nFull, n) {
			t.Errorf("needle mismatch: got %+v want %+v", nFull, n)
		}
	})
}
