package weed_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/security"
)

const (
	proxyTestWriteKey = "cluster-write-key"
	proxyTestReadKey  = "cluster-read-key"
	proxyTestVid      = "3"
	proxyTestFid      = "01637037d6"
	proxyTestFileId   = proxyTestVid + "," + proxyTestFid
)

// The volume server strips a _N delta suffix before comparing the fid claim, so
// a token minted for the suffixed form would never validate.
func TestProxyReadTokenMatchesDeltaFid(t *testing.T) {
	const deltaFileId = proxyTestFileId + "_1"

	fs := &FilerServer{volumeGuard: security.NewGuard([]string{}, proxyTestWriteKey, 10, proxyTestReadKey, 10)}
	jwt := fs.maybeGetVolumeReadJwtAuthorizationToken(deltaFileId)
	if jwt == "" {
		t.Fatal("no read token minted for a delta fid")
	}

	vs := &VolumeServer{guard: security.NewGuard([]string{}, proxyTestWriteKey, 10, proxyTestReadKey, 10)}
	r := httptest.NewRequest(http.MethodGet, "http://volume:8080/"+deltaFileId, nil)
	r.Header.Set("Authorization", security.BearerPrefix+jwt)
	if !vs.maybeCheckJwtAuthorization(r, proxyTestVid, proxyTestFid+"_1", false) {
		t.Fatal("token minted for a delta fid did not authorize the read")
	}
}

func TestValidateProxyChunkId(t *testing.T) {
	for _, tc := range []struct {
		fileId string
		ok     bool
	}{
		{"3,01637037d6", true},
		{"1,0c2b3f2f0f", true},
		{"12,04f0e6ba1d", true},
		{"3,01637037d6_1", true},  // batch-assign delta form
		{"3,01637037d6_12", true}, // multi-digit delta
		{"3,x/../../status", false},
		{"3,01637037d6/../../status", false},
		{"3,01637037d6/../../stats/counter", false},
		{"3,../../status", false},
		{"3,01637037d6/../../status_1", false}, // traversal wearing a delta suffix
		// The suffix must be digits only, or stripping it would reduce a
		// traversal payload to a valid fid and let it through.
		{"3,01637037d6_1/../../status", false},
		{"3,01637037d6_../../status", false},
		{"3,01637037d6_1/../../stats/counter", false},
		{"3,01637037d6_", false},
		{"3,01637037d6_abc", false},
		{"3,01637037d6_1a", false},
		{"3,01637037d6?readDeleted=true", false},
		{"3,01637037d6#frag", false},
		{"3,", false},
		{"3,abc", false},
		{"3", false},
		{"", false},
	} {
		err := validateProxyChunkId(tc.fileId)
		if tc.ok && err != nil {
			t.Errorf("validateProxyChunkId(%q) rejected a valid fid: %v", tc.fileId, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("validateProxyChunkId(%q) accepted a malformed fid", tc.fileId)
		}
	}
}

// A fid carrying dot segments must be rejected before the lookup, so it can
// never be pasted into a volume server URL. Asserting on 400 (not merely "no
// traversal") also proves the request never left the filer.
func TestProxyRejectsTraversalBeforeLookup(t *testing.T) {
	fs := &FilerServer{}

	for _, fileId := range []string{
		"3,x/../../status",
		"3,01637037d6/../../status",
		"3,01637037d6/../../stats/counter",
		"3,01637037d6_1/../../status",
		"3,01637037d6_../../status",
	} {
		r := httptest.NewRequest(http.MethodGet, "http://filer:8888/?proxyChunkId="+fileId, nil)
		w := httptest.NewRecorder()

		// fs.filer is nil: reaching the lookup would panic, so surviving this
		// call is itself proof the fid was rejected first.
		fs.proxyToVolumeServer(w, r, fileId)

		if w.Code != http.StatusBadRequest {
			t.Errorf("proxyChunkId=%q returned %d, want 400", fileId, w.Code)
		}
	}
}

func TestProxySemaphore_LimitsConcurrency(t *testing.T) {
	host := "test-volume:8080"
	defer proxySemaphores.Delete(host)

	var running atomic.Int32
	var maxSeen atomic.Int32
	var wg sync.WaitGroup

	// Launch more goroutines than the semaphore allows
	total := proxyReadConcurrencyPerVolumeServer * 3
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := acquireProxySemaphore(context.Background(), host); err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			defer releaseProxySemaphore(host)

			cur := running.Add(1)
			// Track peak concurrency
			for {
				old := maxSeen.Load()
				if cur <= old || maxSeen.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			running.Add(-1)
		}()
	}
	wg.Wait()

	peak := maxSeen.Load()
	if peak > int32(proxyReadConcurrencyPerVolumeServer) {
		t.Fatalf("peak concurrency %d exceeded limit %d", peak, proxyReadConcurrencyPerVolumeServer)
	}
	if peak == 0 {
		t.Fatal("no goroutines ran")
	}
}

func TestProxySemaphore_ContextCancellation(t *testing.T) {
	host := "test-cancel:8080"
	defer proxySemaphores.Delete(host)

	// Fill the semaphore
	for i := 0; i < proxyReadConcurrencyPerVolumeServer; i++ {
		if err := acquireProxySemaphore(context.Background(), host); err != nil {
			t.Fatalf("fill acquire: %v", err)
		}
	}

	// Try to acquire with a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := acquireProxySemaphore(ctx, host)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}

	// Clean up
	for i := 0; i < proxyReadConcurrencyPerVolumeServer; i++ {
		releaseProxySemaphore(host)
	}
}

func TestProxySemaphore_PerHostIsolation(t *testing.T) {
	hostA := "volume-a:8080"
	hostB := "volume-b:8080"
	defer proxySemaphores.Delete(hostA)
	defer proxySemaphores.Delete(hostB)

	// Fill hostA's semaphore
	for i := 0; i < proxyReadConcurrencyPerVolumeServer; i++ {
		if err := acquireProxySemaphore(context.Background(), hostA); err != nil {
			t.Fatalf("fill hostA: %v", err)
		}
	}

	// hostB should still be acquirable
	if err := acquireProxySemaphore(context.Background(), hostB); err != nil {
		t.Fatalf("hostB should not be blocked by hostA: %v", err)
	}
	releaseProxySemaphore(hostB)

	// Clean up hostA
	for i := 0; i < proxyReadConcurrencyPerVolumeServer; i++ {
		releaseProxySemaphore(hostA)
	}
}
