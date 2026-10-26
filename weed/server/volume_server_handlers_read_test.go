package weed_server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestInternalErrorWritesTheReadError(t *testing.T) {
	readErr := errors.New("invalid CRC for needle 2a (got 0a1b2c3d, want 4e5f6071), data on disk corrupted: needle data corrupted")
	w := httptest.NewRecorder()
	InternalError(w, httptest.NewRequest(http.MethodGet, "/1,abc", nil), readErr)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", w.Code)
	}
	if body := strings.TrimSpace(w.Body.String()); body != `{"error":"`+readErr.Error()+`"}` {
		t.Fatalf("body %q", body)
	}

	w = httptest.NewRecorder()
	InternalError(w, httptest.NewRequest(http.MethodGet, "/1,abc", nil), nil)
	if w.Code != http.StatusInternalServerError || w.Body.Len() != 0 {
		t.Fatalf("without an error: status %d body %q, want a bare 500", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	InternalError(w, httptest.NewRequest(http.MethodHead, "/1,abc", nil), readErr)
	if w.Code != http.StatusInternalServerError || w.Body.Len() != 0 {
		t.Fatalf("on a HEAD request: status %d body %q, want a bare 500", w.Code, w.Body.String())
	}
}

// A HEAD only attempts a metadata-only read, like a GET: a needle up to
// PagedReadLimit is read whole. Demanding metadata only would turn that one
// read into a header read followed by a trailer read.
func TestShouldAttemptStreamWrite(t *testing.T) {
	for _, tc := range []struct {
		name         string
		method       string
		target       string
		ext          string
		hasVolume    bool
		wantAttempt  bool
		wantMustMeta bool
	}{
		{"HEAD", http.MethodHead, "/1,abc.jpg", ".jpg", true, true, false},
		{"GET", http.MethodGet, "/1,abc.jpg", ".jpg", true, true, false},
		{"GET of a resized image", http.MethodGet, "/1,abc.jpg?width=100", ".jpg", true, false, false},
		{"HEAD of a resized image", http.MethodHead, "/1,abc.jpg?width=100", ".jpg", true, false, false},
		{"HEAD of a cropped image", http.MethodHead, "/1,abc.jpg?crop_x1=0&crop_y1=0&crop_x2=100&crop_y2=100", ".jpg", true, false, false},
		{"HEAD without a local volume", http.MethodHead, "/1,abc.jpg", ".jpg", false, false, false},
	} {
		r := httptest.NewRequest(tc.method, tc.target, nil)
		attempt, mustMeta := shouldAttemptStreamWrite(tc.hasVolume, tc.ext, r)
		if attempt != tc.wantAttempt || mustMeta != tc.wantMustMeta {
			t.Errorf("%s: attempt %v, must be metadata only %v; want %v, %v", tc.name, attempt, mustMeta, tc.wantAttempt, tc.wantMustMeta)
		}
	}
}

func TestNotModifiedChecksIfNoneMatchFirst(t *testing.T) {
	const etag = "353dd8be"
	const lastModified = 1700000000
	before := time.Unix(lastModified-60, 0).UTC().Format(http.TimeFormat)
	after := time.Unix(lastModified+60, 0).UTC().Format(http.TimeFormat)

	tests := []struct {
		name    string
		headers [][2]string
		want    bool
	}{
		{"matching etag", [][2]string{{"If-None-Match", `"353dd8be"`}}, true},
		{"weak etag", [][2]string{{"If-None-Match", `W/"353dd8be"`}}, true},
		{"etag in a list", [][2]string{{"If-None-Match", `"x", "353dd8be"`}}, true},
		{"star", [][2]string{{"If-None-Match", `*`}}, true},
		{"other etag", [][2]string{{"If-None-Match", `"x"`}}, false},
		{"etag across repeated lines", [][2]string{{"If-None-Match", `"x"`}, {"If-None-Match", `"353dd8be"`}}, true},
		{"comma inside quotes", [][2]string{{"If-None-Match", `"353dd8be,old"`}}, false},
		{"other etag ignores If-Modified-Since", [][2]string{{"If-None-Match", `"x"`}, {"If-Modified-Since", after}}, false},
		{"empty If-None-Match ignores If-Modified-Since", [][2]string{{"If-None-Match", ``}, {"If-Modified-Since", after}}, false},
		{"not modified since", [][2]string{{"If-Modified-Since", after}}, true},
		{"modified since", [][2]string{{"If-Modified-Since", before}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/3,01637037d6", nil)
			for _, kv := range tt.headers {
				r.Header.Add(kv[0], kv[1])
			}
			if got := notModified(r, etag, lastModified); got != tt.want {
				t.Errorf("notModified = %v, want %v", got, tt.want)
			}
		})
	}
}

type proxyTestMaster struct {
	master_pb.UnimplementedSeaweedServer
	lookup func(context.Context, *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error)
}

func (m *proxyTestMaster) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	return m.lookup(ctx, req)
}

func newProxyTestHTTPServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: handler}}
	server.Start()
	t.Cleanup(server.Close)
	return server
}

type proxyTestResponseWriter struct {
	http.ResponseWriter
	statusWritten chan struct{}
}

func (w proxyTestResponseWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	w.statusWritten <- struct{}{}
}

func newProxyTestVolumeServer(t *testing.T, volumeID string, locations []*master_pb.Location, lookupErr error) *VolumeServer {
	t.Helper()
	operation.InvalidateVolumeIdLocationCache(volumeID)

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	masterAddress := pb.NewServerAddress("127.0.0.1", 9333, listener.Addr().(*net.TCPAddr).Port)
	masterServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(masterServer, &proxyTestMaster{lookup: func(_ context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
		if lookupErr != nil {
			return nil, lookupErr
		}
		if len(req.VolumeOrFileIds) == 0 {
			return nil, errors.New("lookup request contains no volume IDs")
		}
		return &master_pb.LookupVolumeResponse{VolumeIdLocations: []*master_pb.LookupVolumeResponse_VolumeIdLocation{{
			VolumeOrFileId: req.VolumeOrFileIds[0],
			Locations:      locations,
		}}}, nil
	}})
	go func() {
		_ = masterServer.Serve(listener)
	}()

	vs := &VolumeServer{
		store:          &storage.Store{Ip: "192.0.2.10", Port: 8080},
		ReadMode:       "proxy",
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	vs.setCurrentMaster(masterAddress)
	t.Cleanup(func() {
		operation.InvalidateVolumeIdLocationCache(volumeID)
		pb.InvalidateGrpcConnection(masterAddress.ToGrpcAddress())
		masterServer.Stop()
	})
	return vs
}

func TestProxyReqToTargetServerPreservesMethodAndResponse(t *testing.T) {
	const volumeID = "849300101"
	type observedRequest struct {
		method       string
		rangeHeader  string
		ifNoneMatch  string
		ifModifiedAt string
	}
	observed := make(chan observedRequest, 2)
	peer := newProxyTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- observedRequest{
			method:       r.Method,
			rangeHeader:  r.Header.Get("Range"),
			ifNoneMatch:  r.Header.Get("If-None-Match"),
			ifModifiedAt: r.Header.Get("If-Modified-Since"),
		}
		w.Header().Set("ETag", `"peer-etag"`)
		w.Header().Set("Content-Range", "bytes 0-3/10")
		w.Header().Set("X-Peer-Header", "preserved")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "data")
	}))

	remoteAddress := strings.TrimPrefix(peer.URL, "http://")
	vs := newProxyTestVolumeServer(t, volumeID, []*master_pb.Location{{Url: remoteAddress}}, nil)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			r := httptest.NewRequest(method, "/"+volumeID+",abcdef", nil)
			r.Header.Set("Range", "bytes=0-3")
			r.Header.Set("If-None-Match", `"client-etag"`)
			r.Header.Set("If-Modified-Since", "Wed, 21 Oct 2015 07:28:00 GMT")
			w := httptest.NewRecorder()
			vs.proxyReqToTargetServer(w, r)

			got := <-observed
			if got.method != method {
				t.Errorf("peer received method %q, want %q", got.method, method)
			}
			if got.rangeHeader != "bytes=0-3" || got.ifNoneMatch != `"client-etag"` || got.ifModifiedAt != "Wed, 21 Oct 2015 07:28:00 GMT" {
				t.Errorf("peer received conditional/range headers %+v", got)
			}
			if w.Code != http.StatusPartialContent {
				t.Errorf("status %d, want %d", w.Code, http.StatusPartialContent)
			}
			if w.Header().Get("ETag") != `"peer-etag"` || w.Header().Get("Content-Range") != "bytes 0-3/10" || w.Header().Get("X-Peer-Header") != "preserved" {
				t.Errorf("peer response headers were not preserved: %v", w.Header())
			}
			wantBody := "data"
			if method == http.MethodHead {
				wantBody = ""
			}
			if gotBody := w.Body.String(); gotBody != wantBody {
				t.Errorf("response body %q, want %q", gotBody, wantBody)
			}
		})
	}
}

func TestProxyReqToTargetServerCancelsUpstreamWithInboundRequest(t *testing.T) {
	const volumeID = "849300102"
	peerStarted := make(chan struct{})
	peerCanceled := make(chan struct{})
	releasePeer := make(chan struct{})
	var releasePeerOnce sync.Once
	peer := newProxyTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(peerStarted)
		select {
		case <-r.Context().Done():
			close(peerCanceled)
		case <-releasePeer:
		}
	}))
	defer releasePeerOnce.Do(func() { close(releasePeer) })

	remoteAddress := strings.TrimPrefix(peer.URL, "http://")
	vs := newProxyTestVolumeServer(t, volumeID, []*master_pb.Location{{Url: remoteAddress}}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, "/"+volumeID+",abcdef", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	proxyDone := make(chan struct{})
	go func() {
		vs.proxyReqToTargetServer(w, r)
		close(proxyDone)
	}()

	select {
	case <-peerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("peer did not receive the proxied request")
	}
	cancel()
	select {
	case <-peerCanceled:
	case <-time.After(time.Second):
		t.Error("canceling the inbound request did not cancel the peer request")
	}
	releasePeerOnce.Do(func() { close(releasePeer) })
	select {
	case <-proxyDone:
	case <-time.After(2 * time.Second):
		t.Error("proxy handler did not finish after inbound cancellation")
	}
}

func TestProxyReqToTargetServerCancelsUpstreamBodyReadWithInboundRequest(t *testing.T) {
	const volumeID = "849300106"
	peerCanceled := make(chan struct{})
	releasePeer := make(chan struct{})
	var releasePeerOnce sync.Once
	peer := newProxyTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			close(peerCanceled)
		case <-releasePeer:
		}
	}))
	defer releasePeerOnce.Do(func() { close(releasePeer) })

	remoteAddress := strings.TrimPrefix(peer.URL, "http://")
	vs := newProxyTestVolumeServer(t, volumeID, []*master_pb.Location{{Url: remoteAddress}}, nil)
	r := httptest.NewRequest(http.MethodGet, "/"+volumeID+",abcdef", nil)
	ctx, cancel := context.WithCancel(r.Context())
	r = r.WithContext(ctx)
	recorder := httptest.NewRecorder()
	statusWritten := make(chan struct{}, 1)
	w := proxyTestResponseWriter{ResponseWriter: recorder, statusWritten: statusWritten}
	proxyDone := make(chan struct{})
	go func() {
		vs.proxyReqToTargetServer(w, r)
		close(proxyDone)
	}()

	select {
	case <-statusWritten:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("proxy did not receive the peer response headers")
	}
	cancel()
	select {
	case <-peerCanceled:
	case <-time.After(time.Second):
		t.Error("canceling the inbound request did not cancel the peer response body")
	}
	releasePeerOnce.Do(func() { close(releasePeer) })
	select {
	case <-proxyDone:
	case <-time.After(2 * time.Second):
		t.Error("proxy handler did not finish reading after inbound cancellation")
	}
}

func TestProxyReqToTargetServerDistinguishesLookupFailureFromNoLocation(t *testing.T) {
	t.Run("master lookup failure is internal error", func(t *testing.T) {
		const volumeID = "849300103"
		vs := newProxyTestVolumeServer(t, volumeID, nil, status.Error(codes.Unavailable, "master unavailable"))
		w := httptest.NewRecorder()
		vs.proxyReqToTargetServer(w, httptest.NewRequest(http.MethodGet, "/"+volumeID+",abcdef", nil))
		if w.Code != http.StatusInternalServerError {
			t.Errorf("status %d, want %d", w.Code, http.StatusInternalServerError)
		}
	})

	for _, tc := range []struct {
		name      string
		locations []*master_pb.Location
	}{
		{name: "lookup returned no replica locations"},
		{name: "only local replica is not a usable proxy target", locations: []*master_pb.Location{{Url: "192.0.2.10:8080"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			volumeID := "84930010"
			if tc.name == "lookup returned no replica locations" {
				volumeID += "4"
			} else {
				volumeID += "5"
			}
			vs := newProxyTestVolumeServer(t, volumeID, tc.locations, nil)
			w := httptest.NewRecorder()
			vs.proxyReqToTargetServer(w, httptest.NewRequest(http.MethodGet, "/"+volumeID+",abcdef", nil))
			if w.Code != http.StatusNotFound {
				t.Errorf("status %d, want %d", w.Code, http.StatusNotFound)
			}
		})
	}
}
