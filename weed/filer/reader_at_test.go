package filer

import (
	"bytes"
	"io"
	"math"
	"strconv"
	"testing"
)

type mockChunkCache struct {
}

func (m *mockChunkCache) GetChunk(fileId string, minSize uint64) (data []byte) {
	x, _ := strconv.Atoi(fileId)
	data = make([]byte, minSize)
	for i := 0; i < int(minSize); i++ {
		data[i] = byte(x)
	}
	return data
}

func (m *mockChunkCache) ReadChunkAt(data []byte, fileId string, offset uint64) (n int, err error) {
	x, _ := strconv.Atoi(fileId)
	for i := 0; i < len(data); i++ {
		data[i] = byte(x)
	}
	return len(data), nil
}

func (m *mockChunkCache) SetChunk(fileId string, data []byte) {
}

func (m *mockChunkCache) GetMaxFilePartSizeInCache() uint64 {
	return 0
}

func (m *mockChunkCache) IsInCache(fileId string, lockNeeded bool) (answer bool) {
	return false
}

func TestReaderAt(t *testing.T) {

	visibles := NewIntervalList[*VisibleInterval]()
	addVisibleInterval(visibles, &VisibleInterval{
		start:     1,
		stop:      2,
		fileId:    "1",
		chunkSize: 9,
	})
	addVisibleInterval(visibles, &VisibleInterval{
		start:     3,
		stop:      4,
		fileId:    "3",
		chunkSize: 1,
	})
	addVisibleInterval(visibles, &VisibleInterval{
		start:     5,
		stop:      6,
		fileId:    "5",
		chunkSize: 2,
	})
	addVisibleInterval(visibles, &VisibleInterval{
		start:     7,
		stop:      9,
		fileId:    "7",
		chunkSize: 2,
	})
	addVisibleInterval(visibles, &VisibleInterval{
		start:     9,
		stop:      10,
		fileId:    "9",
		chunkSize: 2,
	})

	readerAt := &ChunkReadAt{
		chunkViews:    ViewFromVisibleIntervals(visibles, 0, math.MaxInt64),
		fileSize:      10,
		readerCache:   NewReaderCache(3, &mockChunkCache{}, nil),
		readerPattern: NewReaderPattern(),
	}

	testReadAt(t, readerAt, 0, 10, 10, io.EOF, nil, nil)
	testReadAt(t, readerAt, 0, 12, 10, io.EOF, nil, nil)
	testReadAt(t, readerAt, 2, 8, 8, io.EOF, nil, nil)
	testReadAt(t, readerAt, 3, 6, 6, nil, nil, nil)

}

func testReadAt(t *testing.T, readerAt *ChunkReadAt, offset int64, size int, expectedN int, expectedErr error, data, expectedData []byte) {
	if data == nil {
		data = make([]byte, size)
	}
	n, _, err := readerAt.doReadAt(data, offset)

	if expectedN != n {
		t.Errorf("unexpected read size: %d, expect: %d", n, expectedN)
	}
	if err != expectedErr {
		t.Errorf("unexpected read error: %v, expect: %v", err, expectedErr)
	}
	if expectedData != nil && !bytes.Equal(data, expectedData) {
		t.Errorf("unexpected read data: %v, expect: %v", data, expectedData)
	}
}

func TestReaderAt0(t *testing.T) {

	visibles := NewIntervalList[*VisibleInterval]()
	addVisibleInterval(visibles, &VisibleInterval{
		start:     2,
		stop:      5,
		fileId:    "1",
		chunkSize: 9,
	})
	addVisibleInterval(visibles, &VisibleInterval{
		start:     7,
		stop:      9,
		fileId:    "2",
		chunkSize: 9,
	})

	readerAt := &ChunkReadAt{
		chunkViews:    ViewFromVisibleIntervals(visibles, 0, math.MaxInt64),
		fileSize:      10,
		readerCache:   NewReaderCache(3, &mockChunkCache{}, nil),
		readerPattern: NewReaderPattern(),
	}

	testReadAt(t, readerAt, 0, 10, 10, io.EOF, nil, nil)
	testReadAt(t, readerAt, 3, 16, 7, io.EOF, nil, nil)
	testReadAt(t, readerAt, 3, 5, 5, nil, nil, nil)

	testReadAt(t, readerAt, 11, 5, 0, io.EOF, nil, nil)
	testReadAt(t, readerAt, 10, 5, 0, io.EOF, nil, nil)

}

func TestReaderAt1(t *testing.T) {

	visibles := NewIntervalList[*VisibleInterval]()
	addVisibleInterval(visibles, &VisibleInterval{
		start:     2,
		stop:      5,
		fileId:    "1",
		chunkSize: 9,
	})

	readerAt := &ChunkReadAt{
		chunkViews:    ViewFromVisibleIntervals(visibles, 0, math.MaxInt64),
		fileSize:      20,
		readerCache:   NewReaderCache(3, &mockChunkCache{}, nil),
		readerPattern: NewReaderPattern(),
	}

	testReadAt(t, readerAt, 0, 20, 20, io.EOF, nil, nil)
	testReadAt(t, readerAt, 1, 7, 7, nil, nil, nil)
	testReadAt(t, readerAt, 0, 1, 1, nil, nil, nil)
	testReadAt(t, readerAt, 18, 4, 2, io.EOF, nil, nil)
	testReadAt(t, readerAt, 12, 4, 4, nil, nil, nil)
	testReadAt(t, readerAt, 4, 20, 16, io.EOF, nil, nil)
	testReadAt(t, readerAt, 4, 10, 10, nil, nil, nil)
	testReadAt(t, readerAt, 1, 10, 10, nil, nil, nil)

}

func TestReaderAtGappedChunksDoNotLeak(t *testing.T) {
	visibles := NewIntervalList[*VisibleInterval]()
	addVisibleInterval(visibles, &VisibleInterval{
		start:     2,
		stop:      3,
		fileId:    "1",
		chunkSize: 5,
	})
	addVisibleInterval(visibles, &VisibleInterval{
		start:     7,
		stop:      9,
		fileId:    "1",
		chunkSize: 4,
	})

	readerAt := &ChunkReadAt{
		chunkViews:    ViewFromVisibleIntervals(visibles, 0, math.MaxInt64),
		fileSize:      9,
		readerCache:   NewReaderCache(3, &mockChunkCache{}, nil),
		readerPattern: NewReaderPattern(),
	}

	testReadAt(t, readerAt, 0, 9, 9, io.EOF, []byte{2, 2, 2, 2, 2, 2, 2, 2, 2}, []byte{0, 0, 1, 0, 0, 0, 0, 1, 1})
	testReadAt(t, readerAt, 1, 8, 8, io.EOF, []byte{2, 2, 2, 2, 2, 2, 2, 2}, []byte{0, 1, 0, 0, 0, 0, 1, 1})
}

func TestReaderAtSparseFileDoesNotLeak(t *testing.T) {
	readerAt := &ChunkReadAt{
		chunkViews:    ViewFromVisibleIntervals(NewIntervalList[*VisibleInterval](), 0, math.MaxInt64),
		fileSize:      3,
		readerCache:   NewReaderCache(3, &mockChunkCache{}, nil),
		readerPattern: NewReaderPattern(),
	}

	testReadAt(t, readerAt, 0, 3, 3, io.EOF, []byte{2, 2, 2}, []byte{0, 0, 0})
	testReadAt(t, readerAt, 1, 2, 2, io.EOF, []byte{2, 2}, []byte{0, 0})
}
