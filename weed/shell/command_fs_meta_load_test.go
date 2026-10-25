package shell

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type stubMetaLoadClient struct {
	filer_pb.SeaweedFilerClient
	createEntry func(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error)
}

func (s *stubMetaLoadClient) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest, opts ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	return s.createEntry(ctx, req)
}

func writeMetaEntry(t *testing.T, buf *bytes.Buffer, dir, name string, isDir bool) {
	t.Helper()
	data, err := proto.Marshal(&filer_pb.FullEntry{
		Dir:   dir,
		Entry: &filer_pb.Entry{Name: name, IsDirectory: isDir},
	})
	if err != nil {
		t.Fatalf("marshal entry %s/%s: %v", dir, name, err)
	}
	sizeBuf := make([]byte, 4)
	util.Uint32toBytes(sizeBuf, uint32(len(data)))
	buf.Write(sizeBuf)
	buf.Write(data)
}

func runLoadEntries(c *commandFsMetaLoad, buf *bytes.Buffer, client filer_pb.SeaweedFilerClient) (dirCount, fileCount uint64, err error) {
	return c.loadEntries(buf, client, "", 4, false, io.Discard)
}

func TestFsMetaLoadCreatesAllEntries(t *testing.T) {
	var buf bytes.Buffer
	writeMetaEntry(t, &buf, "/dir", "sub", true)
	writeMetaEntry(t, &buf, "/dir", "file1", false)
	writeMetaEntry(t, &buf, "/dir", "file2", false)

	var mu sync.Mutex
	var created []string
	client := &stubMetaLoadClient{createEntry: func(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		mu.Lock()
		created = append(created, req.Directory+"/"+req.Entry.Name)
		mu.Unlock()
		return &filer_pb.CreateEntryResponse{}, nil
	}}

	c := &commandFsMetaLoad{}
	dirCount, fileCount, err := runLoadEntries(c, &buf, client)
	if err != nil {
		t.Fatalf("loadEntries: %v", err)
	}
	if dirCount != 1 || fileCount != 2 {
		t.Fatalf("got %d dirs %d files, want 1 dir 2 files", dirCount, fileCount)
	}
	if len(created) != 3 {
		t.Fatalf("got %d CreateEntry calls, want 3", len(created))
	}
}

func TestFsMetaLoadWaitsForInflightCreateAtEOF(t *testing.T) {
	var buf bytes.Buffer
	writeMetaEntry(t, &buf, "/dir", "file1", false)

	started := make(chan struct{})
	release := make(chan struct{})
	client := &stubMetaLoadClient{createEntry: func(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		close(started)
		<-release
		return &filer_pb.CreateEntryResponse{}, nil
	}}

	c := &commandFsMetaLoad{}
	done := make(chan error, 1)
	go func() {
		_, _, err := runLoadEntries(c, &buf, client)
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("CreateEntry was not called")
	}

	select {
	case err := <-done:
		t.Fatalf("loadEntries returned %v while CreateEntry was still in flight", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loadEntries: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loadEntries did not return after CreateEntry completed")
	}
}

func TestFsMetaLoadWaitsForInflightCreateOnCorruptData(t *testing.T) {
	var buf bytes.Buffer
	writeMetaEntry(t, &buf, "/dir", "file1", false)
	writeCorruptTail(&buf)

	started := make(chan struct{})
	release := make(chan struct{})
	client := &stubMetaLoadClient{createEntry: func(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		close(started)
		<-release
		return &filer_pb.CreateEntryResponse{}, nil
	}}

	c := &commandFsMetaLoad{}
	done := make(chan error, 1)
	go func() {
		_, _, err := runLoadEntries(c, &buf, client)
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("CreateEntry was not called")
	}

	select {
	case err := <-done:
		t.Fatalf("loadEntries returned %v while CreateEntry was still in flight", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("loadEntries returned nil on truncated input")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loadEntries did not return after CreateEntry completed")
	}
}

func TestFsMetaLoadReturnsWorkerError(t *testing.T) {
	var buf bytes.Buffer
	writeMetaEntry(t, &buf, "/dir", "file1", false)

	workerErr := errors.New("create entry rejected")
	client := &stubMetaLoadClient{createEntry: func(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		return nil, workerErr
	}}

	c := &commandFsMetaLoad{}
	_, _, err := runLoadEntries(c, &buf, client)
	if !errors.Is(err, workerErr) {
		t.Fatalf("got %v, want wrapped %v", err, workerErr)
	}
}

func TestFsMetaLoadPrefersWorkerErrorOverReadError(t *testing.T) {
	var buf bytes.Buffer
	writeMetaEntry(t, &buf, "/dir", "file1", false)
	writeCorruptTail(&buf)

	workerErr := errors.New("create entry rejected")
	started := make(chan struct{})
	release := make(chan struct{})
	client := &stubMetaLoadClient{createEntry: func(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		close(started)
		<-release
		return nil, workerErr
	}}

	c := &commandFsMetaLoad{}
	done := make(chan error, 1)
	go func() {
		_, _, err := runLoadEntries(c, &buf, client)
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("CreateEntry was not called")
	}
	close(release)

	select {
	case err := <-done:
		if !errors.Is(err, workerErr) {
			t.Fatalf("got %v, want wrapped %v", err, workerErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loadEntries did not return")
	}
}

func TestFsMetaLoadReturnsErrorOnCorruptData(t *testing.T) {
	client := &stubMetaLoadClient{createEntry: func(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
		return &filer_pb.CreateEntryResponse{}, nil
	}}
	c := &commandFsMetaLoad{}

	t.Run("truncated entry data", func(t *testing.T) {
		var buf bytes.Buffer
		writeMetaEntry(t, &buf, "/dir", "file1", false)
		writeCorruptTail(&buf)
		if _, _, err := runLoadEntries(c, &buf, client); err == nil {
			t.Fatal("loadEntries returned nil on truncated input")
		}
	})

	t.Run("malformed proto", func(t *testing.T) {
		var buf bytes.Buffer
		writeMetaEntry(t, &buf, "/dir", "file1", false)
		sizeBuf := make([]byte, 4)
		util.Uint32toBytes(sizeBuf, 4)
		buf.Write(sizeBuf)
		buf.Write([]byte{0xff, 0xff, 0xff, 0xff})
		if _, _, err := runLoadEntries(c, &buf, client); err == nil {
			t.Fatal("loadEntries returned nil on malformed proto")
		}
	})
}

func writeCorruptTail(buf *bytes.Buffer) {
	sizeBuf := make([]byte, 4)
	util.Uint32toBytes(sizeBuf, 100)
	buf.Write(sizeBuf)
	buf.Write([]byte{1, 2, 3})
}
