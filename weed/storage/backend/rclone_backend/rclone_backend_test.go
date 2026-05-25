//go:build rclone

package rclone_backend

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fstest/mockfs"
)

func TestUploadViaRcloneReturnsOpenErrorWithoutPanic(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.dat")

	_, err := uploadViaRclone(nil, missing, "key", nil)
	if err == nil {
		t.Fatal("expected open error")
	}
	if !os.IsNotExist(err) {
		t.Fatalf("expected not-exist error, got %v", err)
	}
}

func TestDownloadViaRcloneReturnsObjectOpenErrorWithoutPanic(t *testing.T) {
	openErr := errors.New("open failed")
	rfs := newTestRcloneFs(t, &testRcloneObject{remote: "key", openErr: openErr})

	_, err := downloadViaRclone(rfs, filepath.Join(t.TempDir(), "out.dat"), "key", nil)
	if !errors.Is(err, openErr) {
		t.Fatalf("expected %v, got %v", openErr, err)
	}
}

func TestDownloadViaRcloneReturnsCreateErrorWithoutPanic(t *testing.T) {
	rfs := newTestRcloneFs(t, &testRcloneObject{
		remote: "key",
		body:   "data",
	})

	_, err := downloadViaRclone(rfs, t.TempDir(), "key", nil)
	if err == nil {
		t.Fatal("expected create error")
	}
}

func TestDownloadViaRcloneReturnsFileCloseError(t *testing.T) {
	closeErr := errors.New("close failed")
	rfs := newTestRcloneFs(t, &testRcloneObject{
		remote: "key",
		body:   "data",
	})

	previousCreateFile := createRcloneDownloadFile
	createRcloneDownloadFile = func(string) (io.WriteCloser, error) {
		return &closeErrorWriter{closeErr: closeErr}, nil
	}
	t.Cleanup(func() {
		createRcloneDownloadFile = previousCreateFile
	})

	_, err := downloadViaRclone(rfs, "ignored", "key", func(int64, float32) error {
		return nil
	})
	if !errors.Is(err, closeErr) {
		t.Fatalf("expected %v, got %v", closeErr, err)
	}
}

func TestRcloneBackendStorageFileReadAtReturnsOpenErrorWithoutPanic(t *testing.T) {
	openErr := errors.New("range open failed")
	rfs := newTestRcloneFs(t, &testRcloneObject{remote: "key", openErr: openErr})
	storageFile := RcloneBackendStorageFile{
		backendStorage: &RcloneBackendStorage{fs: rfs},
		key:            "key",
	}

	_, err := storageFile.ReadAt(make([]byte, 4), 0)
	if !errors.Is(err, openErr) {
		t.Fatalf("expected %v, got %v", openErr, err)
	}
}

func newTestRcloneFs(t *testing.T, objects ...fs.Object) fs.Fs {
	t.Helper()

	rfs, err := mockfs.NewFs(context.Background(), "mock", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	mfs := rfs.(*mockfs.Fs)
	for _, obj := range objects {
		mfs.AddObject(obj)
	}
	return rfs
}

type testRcloneObject struct {
	remote  string
	body    string
	openErr error
	rfs     fs.Info
}

func (o *testRcloneObject) SetFs(rfs fs.Fs) {
	o.rfs = rfs
}

func (o *testRcloneObject) Fs() fs.Info {
	return o.rfs
}

func (o *testRcloneObject) String() string {
	return o.remote
}

func (o *testRcloneObject) Remote() string {
	return o.remote
}

func (o *testRcloneObject) ModTime(context.Context) time.Time {
	return time.Unix(0, 0)
}

func (o *testRcloneObject) Size() int64 {
	return int64(len(o.body))
}

func (o *testRcloneObject) Hash(context.Context, hash.Type) (string, error) {
	return "", nil
}

func (o *testRcloneObject) Storable() bool {
	return true
}

func (o *testRcloneObject) SetModTime(context.Context, time.Time) error {
	return nil
}

func (o *testRcloneObject) Open(context.Context, ...fs.OpenOption) (io.ReadCloser, error) {
	if o.openErr != nil {
		return nil, o.openErr
	}
	return io.NopCloser(strings.NewReader(o.body)), nil
}

func (o *testRcloneObject) Update(context.Context, io.Reader, fs.ObjectInfo, ...fs.OpenOption) error {
	return nil
}

func (o *testRcloneObject) Remove(context.Context) error {
	return nil
}

type closeErrorWriter struct {
	closeErr error
}

func (w *closeErrorWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (w *closeErrorWriter) Close() error {
	return w.closeErr
}
