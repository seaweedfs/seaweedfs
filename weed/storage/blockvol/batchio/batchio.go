// Package batchio provides a swappable I/O backend for batch pread/pwrite/fsync.
//
// The standard implementation uses sequential os.File calls (identical to current
// code). An optional io_uring implementation (Linux 5.6+, build-tagged) can batch
// these into fewer syscalls for higher throughput.
//
// Usage:
//
//	bio := batchio.NewStandard()  // or batchio.NewIOUring(256) on Linux
//	defer bio.Close()
//	bio.PreadBatch(fd, ops)
//	bio.PwriteBatch(fd, ops)
//	bio.Fsync(fd)
package batchio

import "os"

// Op represents a single I/O operation: read or write buf at offset.
type Op struct {
	Buf    []byte
	Offset int64
}

// BatchIO batches pread/pwrite/fsync operations.
// All methods are safe for concurrent use from a single goroutine.
// Callers must not share a BatchIO across goroutines without external locking.
type BatchIO interface {
	// PreadBatch reads multiple regions from fd. Each Op.Buf is filled with
	// data from Op.Offset. Returns the first error encountered.
	PreadBatch(fd *os.File, ops []Op) error

	// PwriteBatch writes multiple regions to fd. Each Op.Buf is written at
	// Op.Offset. Returns the first error encountered.
	PwriteBatch(fd *os.File, ops []Op) error

	// Fsync issues fdatasync on the file, flushing data to disk without
	// updating file metadata (mtime, size). On non-Linux platforms where
	// fdatasync is unavailable, falls back to fsync. Both backends use
	// identical sync semantics.
	Fsync(fd *os.File) error

	// LinkedWriteFsync writes buf at offset then issues fdatasync as a pair.
	// On io_uring this is a linked SQE chain (one io_uring_enter syscall).
	// On standard, this is sequential pwrite + fdatasync.
	LinkedWriteFsync(fd *os.File, buf []byte, offset int64) error

	// Close releases resources (io_uring ring, etc). No-op for standard.
	Close() error
}
