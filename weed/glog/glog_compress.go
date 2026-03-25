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
	defer src.Close()

	dstPath := path + ".gz"
	tmpPath := dstPath + ".tmp"
	dst, err := os.Create(tmpPath)
	if err != nil {
		return
	}

	var success bool
	defer func() {
		dst.Close()
		if !success {
			os.Remove(tmpPath)
		}
	}()

	gz, err := gzip.NewWriterLevel(dst, gzip.BestSpeed)
	if err != nil {
		return
	}

	if _, err = io.Copy(gz, src); err != nil {
		gz.Close()
		return
	}

	if err = gz.Close(); err != nil {
		return
	}

	if err = dst.Close(); err != nil {
		return
	}

	if err = os.Rename(tmpPath, dstPath); err != nil {
		return
	}

	success = true

	// Only remove original after successful compression
	os.Remove(path)
}
