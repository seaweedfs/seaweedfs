package util

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ChunkSizes = []int{
		1 << 4,  // index 0, 16 bytes, inclusive
		1 << 6,  // index 1, 64 bytes
		1 << 8,  // index 2, 256 bytes
		1 << 10, // index 3, 1K bytes
		1 << 12, // index 4, 4K bytes
		1 << 14, // index 5, 16K bytes
		1 << 16, // index 6, 64K bytes
		1 << 18, // index 7, 256K bytes
		1 << 20, // index 8, 1M bytes
		1 << 22, // index 9, 4M bytes
		1 << 24, // index 10, 16M bytes
		1 << 26, // index 11, 64M bytes
		1 << 28, // index 12, 128M bytes
	}

	_DEBUG = false
)

type BytesPool struct {
	chunkPools []*byteChunkPool
}

func NewBytesPool() *BytesPool {
	var bp BytesPool
	for _, size := range ChunkSizes {
		bp.chunkPools = append(bp.chunkPools, newByteChunkPool(size))
	}
	ret := &bp
	if _DEBUG {
		t := time.NewTicker(10 * time.Second)
		go func() {
			for {
				println("buffer:", ret.String())
				<-t.C
			}
		}()
	}
	return ret
}

func (m *BytesPool) String() string {
	var buf bytes.Buffer
	for index, size := range ChunkSizes {
		if m.chunkPools[index].count > 0 {
			buf.WriteString(fmt.Sprintf("size:%d count:%d\n", size, m.chunkPools[index].count))
		}
	}
	return buf.String()
}

func findChunkPoolIndex(size int) int {
	if size <= 0 {
		return -1
	}
	size = (size - 1) >> 4
	ret := 0
	for size > 0 {
		size = size >> 2
		ret = ret + 1
	}
	if ret >= len(ChunkSizes) {
		return -1
	}
	return ret
}

func (m *BytesPool) Get(size int) []byte {
	index := findChunkPoolIndex(size)
	// println("get index:", index)
	if index < 0 {
		return make([]byte, size)
	}
	return m.chunkPools[index].Get()
}

func (m *BytesPool) Put(b []byte) {
	index := findChunkPoolIndex(len(b))
	// println("put index:", index)
	if index < 0 {
		return
	}
	m.chunkPools[index].Put(b)
}

// a pool of fix-sized []byte chunks. The pool size is managed by Go GC
type byteChunkPool struct {
	sync.Pool
	chunkSizeLimit int
	count          int64
}

var count int

func newByteChunkPool(chunkSizeLimit int) *byteChunkPool {
	var m byteChunkPool
	m.chunkSizeLimit = chunkSizeLimit
	m.Pool.New = func() interface{} {
		count++
		// println("creating []byte size", m.chunkSizeLimit, "new", count, "count", m.count)
		return make([]byte, m.chunkSizeLimit)
	}
	return &m
}

func (m *byteChunkPool) Get() []byte {
	// println("before get size:", m.chunkSizeLimit, "count:", m.count)
	atomic.AddInt64(&m.count, 1)
	return m.Pool.Get().([]byte)
}

func (m *byteChunkPool) Put(b []byte) {
	atomic.AddInt64(&m.count, -1)
	// println("after put get size:", m.chunkSizeLimit, "count:", m.count)
	m.Pool.Put(b)
}
