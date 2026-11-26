package weed_server

import (
	"bufio"
	"context"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func filerPutRequest(ctx context.Context) *http.Request {
	return httptest.NewRequest(http.MethodPut, "/dir/file", strings.NewReader("data")).WithContext(ctx)
}

func TestFilerWaitForUploadSlot(t *testing.T) {
	newFiler := func(timeout time.Duration) *FilerServer {
		fs := &FilerServer{
			option:                &FilerOption{ConcurrentFileUploadLimit: 1, InflightUploadDataTimeout: timeout},
			inFlightDataLimitCond: sync.NewCond(new(sync.Mutex)),
		}
		if !fs.waitForUploadSlot(httptest.NewRecorder(), filerPutRequest(context.Background()), 4) {
			t.Fatal("first upload was not admitted")
		}
		return fs
	}

	t.Run("times out with 503 and Retry-After", func(t *testing.T) {
		fs := newFiler(100 * time.Millisecond)
		rec := httptest.NewRecorder()
		start := time.Now()
		if fs.waitForUploadSlot(rec, filerPutRequest(context.Background()), 4) {
			t.Fatal("upload admitted without a free slot")
		}
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
			t.Fatalf("got %d Retry-After %q, want 503 and 1", rec.Code, rec.Header().Get("Retry-After"))
		}
		if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
			t.Fatalf("rejected after %v, before the wait timeout", elapsed)
		}
		if fs.inFlightUploads != 1 || fs.inFlightDataSize != 4 {
			t.Fatalf("in-flight uploads %d bytes %d, want 1 and 4", fs.inFlightUploads, fs.inFlightDataSize)
		}
	})

	t.Run("stalled body still gets the 503", func(t *testing.T) {
		fs := newFiler(100 * time.Millisecond)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if fs.waitForUploadSlot(w, r, r.ContentLength) {
				t.Error("upload admitted without a free slot")
			}
		}))
		defer srv.Close()
		conn, err := net.Dial("tcp", srv.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := io.WriteString(conn, "PUT /dir/file HTTP/1.1\r\nHost: filer\r\nContent-Length: 10\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("no reply while the body is unsent: %v", err)
		}
		if resp.StatusCode != http.StatusServiceUnavailable || !resp.Close || resp.Header.Get("Retry-After") != "1" {
			t.Fatalf("got %d close=%v, want 503 with Retry-After and Connection: close", resp.StatusCode, resp.Close)
		}
	})

	t.Run("cancelled request stops waiting", func(t *testing.T) {
		fs := newFiler(0)
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		done := make(chan int)
		go func() {
			rec := httptest.NewRecorder()
			if fs.waitForUploadSlot(rec, filerPutRequest(ctx), 4) {
				t.Error("cancelled upload admitted")
			}
			done <- rec.Code
		}()
		select {
		case code := <-done:
			if code != util.HttpStatusCancelled {
				t.Fatalf("got %d, want %d", code, util.HttpStatusCancelled)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled upload kept waiting for a slot")
		}
	})
}

func newLimitedFiler(limit, inFlight int64) *FilerServer {
	return &FilerServer{
		option:                &FilerOption{ConcurrentUploadLimit: limit, MaxMB: 4, InflightUploadDataTimeout: 50 * time.Millisecond},
		inFlightDataLimitCond: sync.NewCond(new(sync.Mutex)),
		inFlightDataSize:      inFlight,
	}
}

func TestFilerUploadSlotNoOvershoot(t *testing.T) {
	fs := newLimitedFiler(10, 8)
	rec := httptest.NewRecorder()
	if fs.waitForUploadSlot(rec, filerPutRequest(context.Background()), 4) {
		t.Fatal("upload admitted beyond the limit")
	}
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("got %d Retry-After %q, want 503 and 1", rec.Code, rec.Header().Get("Retry-After"))
	}
	if fs.inFlightDataSize != 8 {
		t.Fatalf("in-flight bytes %d, want 8", fs.inFlightDataSize)
	}
}

func TestFilerUploadSlotChargesUnknownLength(t *testing.T) {
	const limit = 1 << 20
	unknown := func() *http.Request {
		req := filerPutRequest(context.Background())
		req.ContentLength = -1
		return req
	}
	fs := newLimitedFiler(limit, 600<<10)
	rec := httptest.NewRecorder()
	if fs.waitForUploadSlot(rec, unknown(), fs.uploadCharge(unknown())) {
		t.Fatal("unknown-length upload admitted beside 600 KiB in flight")
	}
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("got %d Retry-After %q, want 503 and 1", rec.Code, rec.Header().Get("Retry-After"))
	}
	fs = newLimitedFiler(limit, 0)
	if !fs.waitForUploadSlot(httptest.NewRecorder(), unknown(), fs.uploadCharge(unknown())) || fs.inFlightDataSize != limit/2 {
		t.Fatalf("idle: in-flight %d, want admitted with %d", fs.inFlightDataSize, limit/2)
	}
}

func TestFilerUploadSlotAdmitsHugeObjectBesideSmallOnes(t *testing.T) {
	fs := newLimitedFiler(64<<20, 32<<20)
	req := filerPutRequest(context.Background())
	req.ContentLength = 5 << 30
	if !fs.waitForUploadSlot(httptest.NewRecorder(), req, fs.uploadCharge(req)) || fs.inFlightDataSize != 48<<20 {
		t.Fatalf("in-flight %d, want admitted with 48 MiB", fs.inFlightDataSize)
	}
}

// A filer started with -maxMB 0 still charges a chunk for an unknown length
// and caps a known one at four chunks: the chunk size is clamped as S3 does,
// so the charge never collapses to 0 and admits without counting.
func TestFilerUploadChargeClampsNonPositiveMaxMB(t *testing.T) {
	for _, maxMB := range []int{0, -1} {
		fs := &FilerServer{option: &FilerOption{MaxMB: maxMB}}
		req := filerPutRequest(context.Background())
		req.ContentLength = -1
		if got := fs.uploadCharge(req); got != filerDefaultChunkSizeMB<<20 {
			t.Fatalf("maxMB %d, unknown length: charge %d, want %d", maxMB, got, filerDefaultChunkSizeMB<<20)
		}
		req.ContentLength = 5 << 30
		if got := fs.uploadCharge(req); got != 4*filerDefaultChunkSizeMB<<20 {
			t.Fatalf("maxMB %d, 5 GiB: charge %d, want %d", maxMB, got, 4*filerDefaultChunkSizeMB<<20)
		}
	}
	fs := &FilerServer{option: &FilerOption{MaxMB: 1 << 20}}
	req := filerPutRequest(context.Background())
	req.ContentLength = -1
	if got := fs.uploadCharge(req); got != filerMaxChunkSizeMB<<20 {
		t.Fatalf("huge maxMB: charge %d, want %d", got, int64(filerMaxChunkSizeMB)<<20)
	}
}

// A request's ?maxMB sets the chunk the upload loop cuts, so it sets the charge
// too: four 64 MiB buffers are not four 4 MiB ones.
func TestFilerUploadChargeFollowsRequestMaxMB(t *testing.T) {
	fs := newLimitedFiler(1<<30, 0)
	req := httptest.NewRequest(http.MethodPut, "/dir/file?maxMB=64", strings.NewReader("data"))
	req.ContentLength = 256 << 20
	if got := fs.uploadCharge(req); got != 4*64<<20 {
		t.Fatalf("256 MiB with maxMB=64: charge %d, want %d", got, 4*64<<20)
	}
	req.ContentLength = -1
	if got := fs.uploadCharge(req); got != 64<<20 {
		t.Fatalf("unknown length with maxMB=64: charge %d, want %d", got, 64<<20)
	}
}

// An upload stored inside the filer entry is buffered whole, so it reserves its
// whole length; two such uploads that together exceed the budget do not run
// side by side.
func TestFilerUploadChargeCoversInlineUploads(t *testing.T) {
	const limit = 64 << 20
	for _, target := range []string{"/dir/file?saveInside=true", "/etc/config.toml"} {
		t.Run(target, func(t *testing.T) {
			fs := newLimitedFiler(limit, 0)
			req := httptest.NewRequest(http.MethodPost, target, strings.NewReader("data"))
			req.ContentLength = 80 << 20
			// The whole body, capped at the whole budget: over the budget it
			// runs only on an idle filer either way.
			if got := fs.uploadCharge(req); got != limit {
				t.Fatalf("80 MiB inline: charge %d, want the whole budget %d", got, limit)
			}
			req.ContentLength = 40 << 20
			if got := fs.uploadCharge(req); got != 40<<20 {
				t.Fatalf("40 MiB inline: charge %d, want the whole body", got)
			}
			req.ContentLength = 80 << 20
			// Alone it is admitted, as any oversized request is on an idle filer.
			if !fs.waitForUploadSlot(httptest.NewRecorder(), req, fs.uploadCharge(req)) {
				t.Fatal("an oversized inline upload was refused on an idle filer")
			}
			second := httptest.NewRequest(http.MethodPost, target, strings.NewReader("data"))
			second.ContentLength = 80 << 20
			if fs.waitForUploadSlot(httptest.NewRecorder(), second, fs.uploadCharge(second)) {
				t.Fatal("a second 80 MiB inline upload was admitted beside the first")
			}

			req.ContentLength = -1
			if got := fs.uploadCharge(req); got != limit {
				t.Fatalf("unknown-length inline: charge %d, want the whole budget %d", got, limit)
			}
			// A size near MaxInt64 is charged the whole budget, so the
			// in-flight counter cannot wrap.
			req.ContentLength = math.MaxInt64
			if got := fs.uploadCharge(req); got != limit {
				t.Fatalf("huge inline: charge %d, want the whole budget %d", got, limit)
			}
		})
	}
}

// A PUT to an inline path still goes through the chunk loop, so it is charged by
// chunks: charged its whole body it would only run on an idle filer.
func TestFilerUploadChargeKeepsChunksForInlinePut(t *testing.T) {
	fs := newLimitedFiler(64<<20, 0)
	req := httptest.NewRequest(http.MethodPut, "/etc/config.toml", strings.NewReader("data"))
	req.ContentLength = 1 << 30
	if got := fs.uploadCharge(req); got != 4*4<<20 {
		t.Fatalf("1 GiB PUT under /etc: charge %d, want four 4 MiB chunks", got)
	}
}
