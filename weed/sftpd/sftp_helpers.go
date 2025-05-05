// sftp_helpers.go
package sftpd

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

// filerFileWriter buffers writes and flushes on Close.
type filerFileWriter struct {
	fs          SftpServer
	req         *sftp.Request
	mu          sync.Mutex
	data        []byte
	permissions os.FileMode
	uid         uint32
	gid         uint32
	offset      int64
}

func (w *filerFileWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	end := w.offset + int64(len(p))
	if end > int64(len(w.data)) {
		newBuf := make([]byte, end)
		copy(newBuf, w.data)
		w.data = newBuf
	}
	n := copy(w.data[w.offset:], p)
	w.offset += int64(n)
	return n, nil
}

func (w *filerFileWriter) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	end := int(off) + len(p)
	if end > len(w.data) {
		newBuf := make([]byte, end)
		copy(newBuf, w.data)
		w.data = newBuf
	}
	n := copy(w.data[off:], p)
	return n, nil
}

func (w *filerFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	dir, _ := util.FullPath(w.req.Filepath).DirAndName()

	// Check permissions based on file metadata and user permissions
	if err := w.fs.checkFilePermission(dir, "write"); err != nil {
		glog.Errorf("Permission denied for %s", dir)
		return err
	}

	// Call the extracted putFile method on SftpServer
	return w.fs.putFile(w.req.Filepath, w.data, w.fs.user)
}
