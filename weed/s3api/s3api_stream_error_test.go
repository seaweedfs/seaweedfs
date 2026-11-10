package s3api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
