// sftp_helpers.go
package sftpd

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/pkg/sftp"
)

// FileInfo implements os.FileInfo.
type FileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *FileInfo) Name() string       { return fi.name }
func (fi *FileInfo) Size() int64        { return fi.size }
func (fi *FileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *FileInfo) ModTime() time.Time { return fi.modTime }
func (fi *FileInfo) IsDir() bool        { return fi.isDir }
func (fi *FileInfo) Sys() interface{}   { return nil }

// bufferReader wraps a byte slice to io.ReaderAt.
type bufferReader struct {
	b []byte
	i int64
}

func NewBufferReader(b []byte) *bufferReader { return &bufferReader{b: b} }

func (r *bufferReader) Read(p []byte) (int, error) {
	if r.i >= int64(len(r.b)) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += int64(n)
	return n, nil
}

func (r *bufferReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.b)) {
		return 0, io.EOF
	}
	n := copy(p, r.b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// listerat implements sftp.ListerAt.
type listerat []os.FileInfo

func (l listerat) ListAt(ls []os.FileInfo, offset int64) (int, error) {
	if offset >= int64(len(l)) {
		return 0, io.EOF
	}
	n := copy(ls, l[offset:])
	if n < len(ls) {
		return n, io.EOF
	}
	return n, nil
}

// SeaweedSftpFileWriter buffers writes and flushes on Close.
type SeaweedSftpFileWriter struct {
	fs          SftpServer
	req         *sftp.Request
	mu          sync.Mutex
	tmpFile     *os.File
	permissions os.FileMode
	uid         uint32
	gid         uint32
	offset      int64
}

func (w *SeaweedSftpFileWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.tmpFile.WriteAt(p, w.offset)
	w.offset += int64(n)
	return n, err
}

func (w *SeaweedSftpFileWriter) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.tmpFile.WriteAt(p, off)
}

func (w *SeaweedSftpFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	defer os.Remove(w.tmpFile.Name()) // Clean up temp file
	defer w.tmpFile.Close()

	if _, err := w.tmpFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Stream the file instead of loading it
	return w.fs.putFile(w.req.Filepath, w.tmpFile, w.fs.user)
}
