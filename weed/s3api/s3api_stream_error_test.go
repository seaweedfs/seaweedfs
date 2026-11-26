package s3api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func TestWriteStreamErrorResponse(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{"context canceled", context.Canceled, http.StatusInternalServerError},
		{"wrapped context canceled", fmt.Errorf("resolve chunks: %w", context.Canceled), http.StatusInternalServerError},
		{"grpc canceled", status.Error(codes.Canceled, "client connection is closing"), http.StatusInternalServerError},
		{"deadline exceeded", context.DeadlineExceeded, http.StatusInternalServerError},
		{"unwritten stream error", &StreamError{Err: context.Canceled}, http.StatusInternalServerError},
		{"too many requests", util_http.ErrTooManyRequests, http.StatusServiceUnavailable},
		// the recorder's default code stands for "nothing written"
		{"response already written", newStreamErrorWithResponse(context.Canceled), http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeStreamErrorResponse(rec, httptest.NewRequest(http.MethodGet, "/bucket/object", nil), tt.err)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}

// TestIdleTimeoutCancelStillAnswersConnectedClient checks that a request whose
// context the idle timeout canceled can still answer its connected client, so
// a canceled stream must not return without a response.
func TestIdleTimeoutCancelStillAnswersConnectedClient(t *testing.T) {
	listener, err := util.NewListener("127.0.0.1:0", 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{ConnContext: util.ConnContext, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// a first read stalled past the idle timeout
		<-r.Context().Done()
		// only the read side should be short; a write timing out would fail the
		// test on a slow runner
		if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Error(err)
		}
		writeStreamErrorResponse(w, r, r.Context().Err())
	})}
	go server.Serve(listener)
	defer server.Close()

	// bounded so a broken idle cancel fails here instead of hanging the package
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listener.Addr().String()+"/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
}

// TestClientGoneBeforeFirstByteIsNotAnInternalError checks that a canceled
// stream whose client went away is answered ClientDisconnected, so an aborted
// download does not count as a server error.
func TestClientGoneBeforeFirstByteIsNotAnInternalError(t *testing.T) {
	clientCancels := func(cancel context.CancelFunc, _ *httptest.Server) { cancel() }
	serverCloses := func(_ context.CancelFunc, server *httptest.Server) { server.CloseClientConnections() }
	clientDisconnected := s3err.GetAPIError(s3err.ErrClientDisconnected).HTTPStatusCode
	tests := []struct {
		name  string
		start func(*httptest.Server)
		abort func(context.CancelFunc, *httptest.Server)
		want  int
	}{
		{"client closes the connection", (*httptest.Server).Start, clientCancels, clientDisconnected},
		{"client closes the TLS connection", (*httptest.Server).StartTLS, clientCancels, clientDisconnected},
		{"client resets the HTTP/2 stream", func(server *httptest.Server) {
			server.EnableHTTP2 = true
			server.StartTLS()
		}, clientCancels, clientDisconnected},
		{"server closes the connection", (*httptest.Server).Start, serverCloses, http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := util.NewListener("127.0.0.1:0", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			entered := make(chan struct{})
			status := make(chan int, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-r.Context().Done()
				rec := httptest.NewRecorder()
				writeStreamErrorResponse(rec, r, r.Context().Err())
				status <- rec.Code
			}))
			server.Listener.Close()
			server.Listener = listener
			server.Config.ConnContext = util.ConnContext
			tt.start(server)
			defer server.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/bucket/object", nil)
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				if resp, err := server.Client().Do(req); err == nil {
					resp.Body.Close()
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the request never reached the handler")
			}
			tt.abort(cancel, server)

			select {
			case got := <-status:
				if got != tt.want {
					t.Fatalf("status = %d, want %d", got, tt.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the request context was never canceled")
			}
		})
	}
}

// cancelingLookupFiler cancels the request context while the first chunk
// lookup is in flight, as the idle timeout does to a client still connected.
type cancelingLookupFiler struct {
	anonymousReadFiler
	cancel context.CancelFunc
}

func (f *cancelingLookupFiler) LookupVolume(context.Context, *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	f.cancel()
	return nil, status.Error(codes.Canceled, "lookup canceled")
}

func TestGetObjectAnswersRequestCanceledBeforeFirstByte(t *testing.T) {
	f := &cancelingLookupFiler{anonymousReadFiler: anonymousReadFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b/o": {
			Name:       "o",
			Attributes: &filer_pb.FuseAttributes{FileSize: 11, Mtime: 1700000000},
			Chunks:     []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 11, ModifiedTsNs: 1}},
		},
	}}}
	s3a := newPutTestServer(t, startFakeFiler(t, f))
	s3a.readerCache = filer.NewReaderCache(8, (*chunk_cache.TieredChunkCache)(nil), s3a.filerClient.GetLookupFileIdFunction(), s3a.filerClient)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.cancel = cancel
	r := mux.SetURLVars(httptest.NewRequestWithContext(ctx, http.MethodGet, "/b/o", nil), map[string]string{"bucket": "b", "object": "o"})
	w := httptest.NewRecorder()
	s3a.GetObjectHandler(w, r)
	if err := ctx.Err(); err != context.Canceled {
		t.Fatalf("context error = %v, want %v: the lookup was never reached", err, context.Canceled)
	}
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d with %d body bytes, want %d", w.Code, w.Body.Len(), http.StatusInternalServerError)
	}
}

type volumeLookupFiler struct {
	anonymousReadFiler
	volumeServer string
}

func (f *volumeLookupFiler) LookupVolume(_ context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	locations := make(map[string]*filer_pb.Locations, len(req.VolumeIds))
	for _, volumeID := range req.VolumeIds {
		locations[volumeID] = &filer_pb.Locations{Locations: []*filer_pb.Location{{Url: f.volumeServer}}}
	}
	return &filer_pb.LookupVolumeResponse{LocationsMap: locations}, nil
}

// A client that hangs up mid-download fails the next write with a broken pipe
// or a connection reset; that is the client leaving, not a server error.
func TestGetObjectClientHangupIsNotAnError(t *testing.T) {
	const chunkSize = 4 << 20
	chunkData := bytes.Repeat([]byte("v"), chunkSize)
	volumeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(chunkData)
	}))
	t.Cleanup(volumeServer.Close)

	const chunkCount = 16
	chunks := make([]*filer_pb.FileChunk, chunkCount)
	for i := range chunks {
		chunks[i] = &filer_pb.FileChunk{FileId: fmt.Sprintf("%d,01637037d6", i+1), Offset: int64(i) * chunkSize, Size: chunkSize, ModifiedTsNs: 1}
	}
	f := &volumeLookupFiler{volumeServer: strings.TrimPrefix(volumeServer.URL, "http://")}
	f.entries = map[string]*filer_pb.Entry{"/buckets/b/o": {
		Name:       "o",
		Attributes: &filer_pb.FuseAttributes{FileSize: chunkCount * chunkSize, Mtime: 1700000000},
		Chunks:     chunks,
	}}
	s3a := newPutTestServer(t, startFakeFiler(t, f))
	s3a.readerCache = filer.NewReaderCache(8, (*chunk_cache.TieredChunkCache)(nil), s3a.filerClient.GetLookupFileIdFunction(), s3a.filerClient)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})

	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		s3a.GetObjectHandler(w, mux.SetURLVars(r, map[string]string{"bucket": "b", "object": "o"}))
	}))
	t.Cleanup(server.Close)

	errorsBefore := glog.Stats.Error.Lines()
	resp, err := server.Client().Get(server.URL + "/b/o")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if _, err := io.ReadFull(resp.Body, make([]byte, 64*1024)); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the handler kept streaming after the client hung up")
	}
	if n := glog.Stats.Error.Lines() - errorsBefore; n != 0 {
		t.Fatalf("a client hang-up logged %d ERROR lines", n)
	}
}

func TestStreamCanceled(t *testing.T) {
	live := context.Background()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	brokenPipe := &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}
	for _, tt := range []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"volume server failure under a live request", live, brokenPipe, false},
		{"client write failed and net/http canceled the request", canceled, brokenPipe, true},
		{"shared work canceled under a live request", live, context.Canceled, true},
		{"server-side deadline", live, context.DeadlineExceeded, false},
	} {
		if got := streamCanceled(tt.ctx, tt.err); got != tt.want {
			t.Errorf("%s: streamCanceled = %v, want %v", tt.name, got, tt.want)
		}
	}
}
