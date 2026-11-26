package weed_server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newUploadLimitVolumeServer(limit, inFlight int64, timeout time.Duration) *VolumeServer {
	return &VolumeServer{
		concurrentUploadLimit:       limit,
		inFlightUploadDataSize:      inFlight,
		inflightUploadDataTimeout:   timeout,
		inFlightUploadDataLimitCond: sync.NewCond(new(sync.Mutex)),
		fileSizeLimitBytes:          1 << 20,
	}
}

// countingBody records how many body bytes the handler read.
type countingBody struct {
	data io.Reader
	read atomic.Int64
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.data.Read(p)
	b.read.Add(int64(n))
	return n, err
}

func (b *countingBody) Close() error { return nil }

// uploadRequest builds a POST whose body is size bytes; a negative size
// sends an unknown length (as a chunked upload does).
func uploadRequest(ctx context.Context, target string, size int64) (*http.Request, *countingBody) {
	n := size
	if n < 0 {
		n = 1 << 10
	}
	body := &countingBody{data: io.LimitReader(zeroReader{}, n)}
	r := httptest.NewRequest(http.MethodPost, target, nil).WithContext(ctx)
	r.Body = body
	r.ContentLength = size
	if size >= 0 {
		r.Header.Set("Content-Length", strconv.FormatInt(size, 10))
	}
	r.Header.Set("Content-Type", "application/octet-stream")
	return r, body
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func admitUpload(vs *VolumeServer, w http.ResponseWriter, r *http.Request) (bool, int64) {
	charged := vs.uploadCharge(r)
	return vs.waitForUploadSlot(w, r, charged), charged
}

func TestUploadLimitRejectsWith503AfterTimeout(t *testing.T) {
	for _, tc := range []struct {
		name      string
		size      int64
		wantClose bool
	}{
		{name: "small body keeps the connection", size: 4, wantClose: false},
		{name: "large body closes the connection", size: 1 << 20, wantClose: true},
		{name: "unknown length closes the connection", size: -1, wantClose: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vs := newUploadLimitVolumeServer(10, 10, 50*time.Millisecond)
			r, body := uploadRequest(context.Background(), "/1,0123456789abcdef", tc.size)
			w := httptest.NewRecorder()

			ok, _ := admitUpload(vs, w, r)

			require.False(t, ok)
			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			require.Equal(t, "1", w.Header().Get("Retry-After"))
			if tc.wantClose {
				require.Equal(t, "close", w.Header().Get("Connection"))
			} else {
				require.Empty(t, w.Header().Get("Connection"))
			}
			require.Zero(t, body.read.Load(), "rejected upload must not read the body")
			require.Equal(t, int64(10), atomic.LoadInt64(&vs.inFlightUploadDataSize))
		})
	}
}

func TestUploadLimitDoesNotOvershoot(t *testing.T) {
	vs := newUploadLimitVolumeServer(10, 8, 50*time.Millisecond)
	r, _ := uploadRequest(context.Background(), "/1,0123456789abcdef", 4)
	w := httptest.NewRecorder()

	ok, _ := admitUpload(vs, w, r)

	require.False(t, ok)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, int64(8), atomic.LoadInt64(&vs.inFlightUploadDataSize))
}

func TestUploadLimitAdmitsWhenSlotFrees(t *testing.T) {
	vs := newUploadLimitVolumeServer(10, 10, 5*time.Second)
	r, _ := uploadRequest(context.Background(), "/1,0123456789abcdef", 4)
	w := httptest.NewRecorder()

	got := make(chan bool, 1)
	go func() {
		ok, _ := admitUpload(vs, w, r)
		got <- ok
	}()
	time.Sleep(20 * time.Millisecond)
	vs.releaseUploadSlot(10)

	select {
	case ok := <-got:
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("waiter missed the released upload slot")
	}
	require.Equal(t, int64(4), atomic.LoadInt64(&vs.inFlightUploadDataSize))
}

func TestUploadLimitAdmitsOversizedWhenIdle(t *testing.T) {
	vs := newUploadLimitVolumeServer(10, 0, 50*time.Millisecond)
	r, _ := uploadRequest(context.Background(), "/1,0123456789abcdef", 100)
	w := httptest.NewRecorder()

	start := time.Now()
	ok, charged := admitUpload(vs, w, r)

	require.True(t, ok)
	require.Less(t, time.Since(start), 40*time.Millisecond)
	require.Equal(t, int64(100), charged)
	require.Equal(t, int64(100), atomic.LoadInt64(&vs.inFlightUploadDataSize))
}

func TestUploadLimitCountsGatewayHolderWrites(t *testing.T) {
	vs := newUploadLimitVolumeServer(10, 10, 50*time.Millisecond)
	r, _ := uploadRequest(context.Background(), "/1,0123456789abcdef?type=replicate&src=client", 4)
	w := httptest.NewRecorder()

	ok, _ := admitUpload(vs, w, r)

	require.False(t, ok)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestUploadLimitLetsPrimaryForwardsThrough(t *testing.T) {
	vs := newUploadLimitVolumeServer(10, 10, 50*time.Millisecond)
	r, _ := uploadRequest(context.Background(), "/1,0123456789abcdef?type=replicate", 4)
	w := httptest.NewRecorder()

	start := time.Now()
	ok, charged := admitUpload(vs, w, r)

	require.True(t, ok)
	require.Less(t, time.Since(start), 40*time.Millisecond)
	require.Equal(t, int64(4), charged)
	require.Equal(t, int64(14), atomic.LoadInt64(&vs.inFlightUploadDataSize))
}

func TestUploadLimitChargesUnknownLength(t *testing.T) {
	vs := newUploadLimitVolumeServer(1<<30, 0, 50*time.Millisecond)
	vs.fileSizeLimitBytes = 64
	r, _ := uploadRequest(context.Background(), "/1,0123456789abcdef", -1)
	w := httptest.NewRecorder()

	ok, charged := admitUpload(vs, w, r)

	require.True(t, ok)
	require.Equal(t, int64(64), charged)
	require.Equal(t, int64(64), atomic.LoadInt64(&vs.inFlightUploadDataSize))
}

func TestUploadLimitCapsUnknownLengthChargeAtLimit(t *testing.T) {
	vs := newUploadLimitVolumeServer(10, 0, 50*time.Millisecond)
	vs.fileSizeLimitBytes = 1 << 20
	r, _ := uploadRequest(context.Background(), "/1,0123456789abcdef", -1)

	require.Equal(t, int64(10), vs.uploadCharge(r))
}

func TestUploadLimitZeroCountsWithoutWaiting(t *testing.T) {
	vs := newUploadLimitVolumeServer(0, 1<<40, 50*time.Millisecond)
	r, _ := uploadRequest(context.Background(), "/1,0123456789abcdef", 4)
	w := httptest.NewRecorder()

	ok, charged := admitUpload(vs, w, r)
	require.True(t, ok)
	require.Equal(t, int64(1<<40+4), atomic.LoadInt64(&vs.inFlightUploadDataSize))

	vs.releaseUploadSlot(charged)
	require.Equal(t, int64(1<<40), atomic.LoadInt64(&vs.inFlightUploadDataSize))
}

func TestUploadLimitClientGoneAnswers499(t *testing.T) {
	vs := newUploadLimitVolumeServer(10, 10, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	r, body := uploadRequest(ctx, "/1,0123456789abcdef", 4)
	w := httptest.NewRecorder()

	ok, _ := admitUpload(vs, w, r)

	require.False(t, ok)
	require.Equal(t, 499, w.Code)
	require.Zero(t, body.read.Load())
	require.Equal(t, int64(10), atomic.LoadInt64(&vs.inFlightUploadDataSize))
}

func TestUploadLimitClientWaitIsCapped(t *testing.T) {
	for _, tc := range []struct {
		timeout time.Duration
		want    time.Duration
	}{
		{timeout: 0, want: volumeClientUploadWaitCap},
		{timeout: 50 * time.Millisecond, want: 50 * time.Millisecond},
		{timeout: time.Minute, want: volumeClientUploadWaitCap},
	} {
		vs := newUploadLimitVolumeServer(10, 0, tc.timeout)
		require.Equal(t, tc.want, vs.clientUploadWait(), "timeout %v", tc.timeout)
	}
}

func TestUploadLimit503ReachesGoClientForLargeBody(t *testing.T) {
	vs := newUploadLimitVolumeServer(10, 10, 50*time.Millisecond)
	srv := httptest.NewServer(http.HandlerFunc(vs.handleUploadRequest))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/1,0123456789abcdef", bytes.NewReader(make([]byte, 8<<20)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := srv.Client().Do(req)
	require.NoError(t, err, "client must see the 503, not a write error")
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, "1", resp.Header.Get("Retry-After"))
}
