package operation

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeVolumeLocationServer struct {
	master_pb.UnimplementedSeaweedServer
	mu    sync.Mutex
	calls int
	urls  []string
}

func (s *fakeVolumeLocationServer) LookupVolume(_ context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	resp := &master_pb.LookupVolumeResponse{}
	for _, vid := range req.VolumeOrFileIds {
		var locs []*master_pb.Location
		for _, u := range s.urls {
			locs = append(locs, &master_pb.Location{Url: u})
		}
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeOrFileId: vid,
			Locations:      locs,
		})
	}
	return resp, nil
}

func (s *fakeVolumeLocationServer) lookups() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type chunkReplica struct {
	*httptest.Server
	mu     sync.Mutex
	ranges []string
}

func (c *chunkReplica) host() string {
	return strings.TrimPrefix(c.URL, "http://")
}

func (c *chunkReplica) requests() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.ranges...)
}

// newChunkReplica serves data; with cutAfter >= 0 it announces the full length
// but drops the connection after cutAfter bytes.
func newChunkReplica(t *testing.T, data []byte, cutAfter int) *chunkReplica {
	c := &chunkReplica{}
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		c.ranges = append(c.ranges, r.Header.Get("Range"))
		c.mu.Unlock()
		if cutAfter < 0 {
			http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		w.Write(data[:cutAfter])
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}))
	t.Cleanup(c.Close)
	return c
}

func deadAddress(t *testing.T) string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

func testChunkData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i * 7)
	}
	return data
}

func TestChunkedFileReaderResumesOnAnotherReplica(t *testing.T) {
	const vid = "91201"
	data := testChunkData(4096)
	broken := newChunkReplica(t, data, 1000)
	healthy := newChunkReplica(t, data, -1)
	fake := &fakeVolumeLocationServer{urls: []string{healthy.host()}}
	master := startFakeMasterServer(t, fake)
	dial := grpc.WithTransportCredentials(insecure.NewCredentials())

	vc.Set(vid, []Location{{Url: broken.host()}, {Url: healthy.host(), DataInRemote: true}}, time.Minute)
	t.Cleanup(func() { InvalidateVolumeIdLocationCache(vid) })

	chunks := []*ChunkInfo{{Fid: vid + ",01637037d6", Offset: 0, Size: int64(len(data))}}
	reader := NewChunkedFileReader(chunks, master, dial)
	got, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, want the %d bytes of the chunk", len(got), len(data))
	}
	if r := healthy.requests(); len(r) != 1 || r[0] != "bytes=1000-" {
		t.Fatalf("healthy replica requests = %q, want one resuming at byte 1000", r)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("client gone") }

func TestChunkedFileReaderStopsWhenTheClientGoesAway(t *testing.T) {
	const vid = "91204"
	data := testChunkData(1024)
	a, b := newChunkReplica(t, data, -1), newChunkReplica(t, data, -1)
	vc.Set(vid, []Location{{Url: a.host()}, {Url: b.host()}}, time.Minute)
	t.Cleanup(func() { InvalidateVolumeIdLocationCache(vid) })

	chunks := []*ChunkInfo{{Fid: vid + ",01637037d6", Offset: 0, Size: int64(len(data))}}
	reader := NewChunkedFileReader(chunks, pb.ServerAddress("127.0.0.1:1"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if _, err := reader.WriteTo(failingWriter{}); err == nil || err.Error() != "client gone" {
		t.Fatalf("WriteTo error = %v, want the writer's error", err)
	}
	if n := len(a.requests()) + len(b.requests()); n != 1 {
		t.Fatalf("replica requests = %d, want 1", n)
	}
}
