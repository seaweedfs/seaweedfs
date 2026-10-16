package filer

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	// ZipExtractMaxMembers bounds how many members one archive write fans out to.
	ZipExtractMaxMembers = 100000
	// ZipExtractMaxBytes bounds the total uncompressed bytes one archive may declare.
	ZipExtractMaxBytes = int64(1) << 40 // 1 TiB

	zipSpoolMemoryLimit = 32 << 20 // 32 MiB; larger archives spool to a temp file
)

// SpoolZipReader buffers r into a seekable source for zip.NewReader: memory for
// small archives, a temp file otherwise. The returned cleanup releases it.
func SpoolZipReader(r io.Reader) (ra io.ReaderAt, n int64, cleanup func(), err error) {
	nop := func() {}
	head, err := io.ReadAll(io.LimitReader(r, zipSpoolMemoryLimit+1))
	if err != nil {
		return nil, 0, nop, err
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
		if rest, err = io.Copy(f, io.LimitReader(r, ZipExtractMaxBytes+1)); err == nil {
			n = int64(len(head)) + rest
		}
	}
	if err == nil && n > ZipExtractMaxBytes {
		err = fmt.Errorf("zip archive exceeds %d bytes", ZipExtractMaxBytes)
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
		total += f.UncompressedSize64
		if total > uint64(ZipExtractMaxBytes) {
			return fmt.Errorf("zip archive uncompressed size exceeds %d bytes", ZipExtractMaxBytes)
		}
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
	return name, true
}
