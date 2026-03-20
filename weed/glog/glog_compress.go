package glog

import (
	"compress/gzip"
	"io"
	"os"
	"strings"
	"sync/atomic"
)

// CompressRotated controls whether rotated log files are gzip-compressed.
// 0 = disabled (default), 1 = enabled.
var compressRotated int32

// SetCompressRotated enables or disables gzip compression of rotated log files.
// When enabled, after a log file is rotated the old file is compressed in a
// background goroutine (non-blocking to the logging path).
func SetCompressRotated(enabled bool) {
	if enabled {
		atomic.StoreInt32(&compressRotated, 1)
	} else {
		atomic.StoreInt32(&compressRotated, 0)
	}
}

// IsCompressRotated returns whether compression of rotated files is active.
func IsCompressRotated() bool {
	return atomic.LoadInt32(&compressRotated) == 1
}

// compressFile compresses the given file with gzip and removes the original.
// This runs in a background goroutine to avoid blocking the log write path.
// If any step fails the original file is kept intact.
func compressFile(path string) {
	// Skip if already compressed
	if strings.HasSuffix(path, ".gz") {
		return
	}

	src, err := os.Open(path)
	if err != nil {
		return
	}

	dstPath := path + ".gz"
	dst, err := os.Create(dstPath)
	if err != nil {
		src.Close()
		return
	}

	gz, err := gzip.NewWriterLevel(dst, gzip.BestSpeed)
	if err != nil {
		src.Close()
		dst.Close()
		os.Remove(dstPath)
		return
	}

	_, copyErr := io.Copy(gz, src)
	gzErr := gz.Close()
	src.Close()
	dst.Close()

	if copyErr != nil || gzErr != nil {
		os.Remove(dstPath) // cleanup corrupt/incomplete file
		return
	}

	// Only remove original after successful compression
	os.Remove(path)
}
