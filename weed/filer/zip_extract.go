package filer

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	// ZipExtractMaxMembers bounds how many members one archive write fans out to.
	ZipExtractMaxMembers = 100000
	// ZipExtractMaxBytes bounds the total uncompressed bytes one archive may declare.
	ZipExtractMaxBytes = int64(1) << 40 // 1 TiB

	// ZipExtractMaxArchiveBytes bounds the compressed bytes one archive may
	// spool to local disk when the caller passes no tighter limit. It matches
	// the TUS default max upload size.
	ZipExtractMaxArchiveBytes = int64(5) << 30 // 5 GiB

)

// zipSpoolMemoryLimit caps how much of one archive is buffered in memory
// before the spool falls through to a temp file. It is a var rather than a
// const so tests can shrink it: exercising the temp-file path with the real
// 32 MiB threshold allocates hundreds of MiB, which tips the 32-bit test
// binary over its 4 GiB address space.
var zipSpoolMemoryLimit int64 = 32 << 20 // 32 MiB

// ErrZipTooLarge marks a spool failure caused by the archive exceeding the
// caller's size limit, so handlers can answer 4xx instead of 5xx.
var ErrZipTooLarge = errors.New("zip archive too large")

// SpoolZipReader buffers r into a seekable source for zip.NewReader: memory for
// small archives, a temp file otherwise. At most maxArchiveBytes are buffered;
// a non-positive limit falls back to ZipExtractMaxArchiveBytes. The returned
// cleanup releases the spool.
func SpoolZipReader(r io.Reader, maxArchiveBytes int64) (ra io.ReaderAt, n int64, cleanup func(), err error) {
	nop := func() {}
	if maxArchiveBytes <= 0 {
		maxArchiveBytes = ZipExtractMaxArchiveBytes
	}
	head, err := io.ReadAll(io.LimitReader(r, zipSpoolMemoryLimit+1))
	if err != nil {
		return nil, 0, nop, err
	}
	if int64(len(head)) > maxArchiveBytes {
		return nil, 0, nop, fmt.Errorf("%w: exceeds %d bytes", ErrZipTooLarge, maxArchiveBytes)
	}
	if int64(len(head)) <= zipSpoolMemoryLimit {
		return bytes.NewReader(head), int64(len(head)), nop, nil
	}
	f, err := os.CreateTemp("", "seaweedfs-extract-*.zip")
	if err != nil {
		return nil, 0, nop, err
	}
	cleanup = func() {
		name := f.Name()
		f.Close()
		os.Remove(name)
	}
	if _, err = f.Write(head); err == nil {
		var rest int64
		// One byte past the limit so an oversized archive errors out instead
		// of being silently truncated.
		if rest, err = io.Copy(f, io.LimitReader(r, maxArchiveBytes+1-int64(len(head)))); err == nil {
			n = int64(len(head)) + rest
		}
	}
	if err == nil && n > maxArchiveBytes {
		err = fmt.Errorf("%w: exceeds %d bytes", ErrZipTooLarge, maxArchiveBytes)
	}
	if err != nil {
		cleanup()
		return nil, 0, nop, err
	}
	return f, n, cleanup, nil
}

// CheckZipLimits enforces the member-count and total-size caps on an opened archive.
func CheckZipLimits(zr *zip.Reader) error {
	if len(zr.File) > ZipExtractMaxMembers {
		return fmt.Errorf("zip archive has %d members, limit %d", len(zr.File), ZipExtractMaxMembers)
	}
	var total uint64
	for _, f := range zr.File {
		if f.UncompressedSize64 > uint64(ZipExtractMaxBytes)-total {
			return fmt.Errorf("zip archive uncompressed size exceeds %d bytes", ZipExtractMaxBytes)
		}
		total += f.UncompressedSize64
	}
	return nil
}

// SanitizeZipMemberName converts a zip member name into a relative filer path.
// ok=false means the member must be skipped: absolute or escaping paths,
// non-regular files such as symlinks and devices.
func SanitizeZipMemberName(f *zip.File) (name string, ok bool) {
	mode := f.FileInfo().Mode()
	if mode&(os.ModeSymlink|os.ModeDevice|os.ModeNamedPipe|os.ModeSocket) != 0 {
		return "", false
	}
	name = strings.TrimSuffix(strings.ReplaceAll(f.Name, "\\", "/"), "/")
	if name == "" || strings.HasPrefix(name, "/") {
		return "", false
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "" || seg == "." || seg == ".." || strings.ContainsRune(seg, '\x00') {
			return "", false
		}
	}
	// filepath.IsLocal is the check static analysis recognizes: no element may
	// be "." or "..", the path may not be absolute or empty, and on Windows it
	// may not be a reserved name.
	if !filepath.IsLocal(name) {
		return "", false
	}
	return name, true
}
