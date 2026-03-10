package nvme

import "sync"

// bufPool provides tiered buffer pools for NVMe I/O.
// Three tiers: 4KB (small I/O), 64KB (medium), 256KB (large).
var bufPool = struct {
	small  sync.Pool // 4KB
	medium sync.Pool // 64KB
	large  sync.Pool // 256KB
}{
	small:  sync.Pool{New: func() any { b := make([]byte, 4096); return &b }},
	medium: sync.Pool{New: func() any { b := make([]byte, 65536); return &b }},
	large:  sync.Pool{New: func() any { b := make([]byte, 262144); return &b }},
}

// getBuffer returns a buffer of at least size bytes from the pool.
func getBuffer(size int) []byte {
	switch {
	case size <= 4096:
		bp := bufPool.small.Get().(*[]byte)
		return (*bp)[:size]
	case size <= 65536:
		bp := bufPool.medium.Get().(*[]byte)
		return (*bp)[:size]
	case size <= 262144:
		bp := bufPool.large.Get().(*[]byte)
		return (*bp)[:size]
	default:
		return make([]byte, size) // oversized: don't pool
	}
}

// putBuffer returns a buffer to the appropriate pool.
func putBuffer(buf []byte) {
	c := cap(buf)
	buf = buf[:c]
	switch c {
	case 4096:
		bufPool.small.Put(&buf)
	case 65536:
		bufPool.medium.Put(&buf)
	case 262144:
		bufPool.large.Put(&buf)
	// Oversized or wrong-sized: let GC collect
	}
}
