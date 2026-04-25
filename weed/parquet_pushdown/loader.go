package parquet_pushdown

import (
	"context"
	"fmt"
	"io"
	"os"
)

// FileHandle is what the service hands to the parser: a positional
// reader bound to a known size, with a Close so the caller can
// release any underlying resources.
type FileHandle interface {
	io.ReaderAt
	io.Closer
	Size() int64
}

// Loader resolves a Parquet file's path to a FileHandle. The split
// between "local file" and "filer-backed" implementations lives here
// so the service stays oblivious to the storage backend, and so
// integration tests can plug in deterministic local files instead of
// spinning up a filer.
type Loader interface {
	Open(ctx context.Context, path string) (FileHandle, error)
}

// LocalLoader treats `path` as a filesystem path. Used by integration
// tests and for local-development smoke runs against a Parquet file
// that is not under filer management.
type LocalLoader struct{}

func (LocalLoader) Open(_ context.Context, path string) (FileHandle, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat %q: %w", path, err)
	}
	if st.IsDir() {
		_ = f.Close()
		return nil, fmt.Errorf("%q is a directory", path)
	}
	return &localFile{f: f, size: st.Size()}, nil
}

type localFile struct {
	f    *os.File
	size int64
}

func (l *localFile) ReadAt(p []byte, off int64) (int, error) { return l.f.ReadAt(p, off) }
func (l *localFile) Close() error                            { return l.f.Close() }
func (l *localFile) Size() int64                             { return l.size }
