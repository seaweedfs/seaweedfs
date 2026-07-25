package weed_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
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

// proxyTestVolume is a stand-in volume server that records what the filer
// actually sent. Recording arrival separately from the header is what keeps the
// negative assertions honest: an absent Authorization and a request that never
// left the filer are otherwise indistinguishable.
type proxyTestVolume struct {
	*httptest.Server
	hits atomic.Int32
	auth atomic.Value // string
}

func newProxyTestVolume(t *testing.T) *proxyTestVolume {
	t.Helper()
	v := &proxyTestVolume{}
	v.auth.Store("")
	v.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v.hits.Add(1)
		v.auth.Store(r.Header.Get("Authorization"))
	}))
	t.Cleanup(func() {
		v.Close()
		// proxyToVolumeServerURL keys the semaphore map by host, and every
		// httptest server binds a fresh port; drop ours so -count=N runs do not
		// grow the map without bound.
		if u, err := url.Parse(v.URL); err == nil {
			proxySemaphores.Delete(u.Host)
		}
	})
	return v
}

func (v *proxyTestVolume) seenAuth() string { return v.auth.Load().(string) }

func (v *proxyTestVolume) requireReached(t *testing.T) {
	t.Helper()
	if v.hits.Load() == 0 {
		t.Fatal("request never reached the volume server, so the assertion below proves nothing")
	}
}

// Everything the filer can hand a caller on the proxy path is reachable without
// authentication, because the branch runs ahead of the filer's JWT gate. With
// only a write key configured it must therefore mint nothing at all.
func TestProxyMintsNothingWithoutReadKey(t *testing.T) {
	fs := &FilerServer{volumeGuard: security.NewGuard([]string{}, proxyTestWriteKey, 10, "", 10)}

	if jwt := fs.maybeGetVolumeReadJwtAuthorizationToken(proxyTestFileId); jwt != "" {
		t.Fatalf("minted %q with no read key configured", jwt)
	}
}

// A configured read key still yields a read token, and it stays read-only.
func TestProxyReadTokenIsReadOnly(t *testing.T) {
	fs := &FilerServer{volumeGuard: security.NewGuard([]string{}, proxyTestWriteKey, 10, proxyTestReadKey, 10)}

	jwt := fs.maybeGetVolumeReadJwtAuthorizationToken(proxyTestFileId)
	if jwt == "" {
		t.Fatal("no read token minted despite a configured read key")
	}

	vs := &VolumeServer{guard: security.NewGuard([]string{}, proxyTestWriteKey, 10, proxyTestReadKey, 10)}

	read := httptest.NewRequest(http.MethodGet, "http://volume:8080/"+proxyTestFileId, nil)
	read.Header.Set("Authorization", security.BearerPrefix+jwt)
	if !vs.maybeCheckJwtAuthorization(read, proxyTestVid, proxyTestFid, false) {
		t.Fatal("read token rejected on a read")
	}

	write := httptest.NewRequest(http.MethodDelete, "http://volume:8080/"+proxyTestFileId, nil)
	write.Header.Set("Authorization", security.BearerPrefix+jwt)
	if vs.maybeCheckJwtAuthorization(write, proxyTestVid, proxyTestFid, true) {
		t.Fatal("read token authorized a write")
	}
}

// Writes must reach the volume server carrying the caller's own AssignVolume
// token and nothing else. POST is the method every in-tree proxied uploader
// actually sends, so it leads the table.
func TestProxyWriteCarriesOnlyCallerCredential(t *testing.T) {
	callerToken := security.BearerPrefix + string(security.GenJwtForVolumeServer(security.SigningKey(proxyTestWriteKey), 10, proxyTestFileId))

	for _, tc := range []struct {
		name    string
		method  string
		readKey string
		sent    string
		want    string
	}{
		{"anonymous post", http.MethodPost, "", "", ""},
		{"anonymous post with read key", http.MethodPost, proxyTestReadKey, "", ""},
		{"anonymous delete", http.MethodDelete, "", "", ""},
		{"anonymous delete with read key", http.MethodDelete, proxyTestReadKey, "", ""},
		{"anonymous put with read key", http.MethodPut, proxyTestReadKey, "", ""},
		{"caller token forwarded on post", http.MethodPost, proxyTestReadKey, callerToken, callerToken},
		{"caller token forwarded on delete", http.MethodDelete, "", callerToken, callerToken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			volume := newProxyTestVolume(t)
			fs := &FilerServer{volumeGuard: security.NewGuard([]string{}, proxyTestWriteKey, 10, tc.readKey, 10)}

			r := httptest.NewRequest(tc.method, "http://filer:8888/?proxyChunkId="+proxyTestFileId, nil)
			if tc.sent != "" {
				r.Header.Set("Authorization", tc.sent)
			}
			fs.proxyToVolumeServerURL(httptest.NewRecorder(), r, proxyTestFileId, volume.URL+"/"+proxyTestFileId)

			volume.requireReached(t)
			if got := volume.seenAuth(); got != tc.want {
				t.Fatalf("volume server saw Authorization %q, want %q", got, tc.want)
			}
		})
	}
}

// Reads keep the minted token so weed mount can read through the proxy against a
// volume server that enforces read JWTs -- and the minted token must *replace*
// whatever the caller sent, not be appended alongside it.
func TestProxyReadReplacesCallerCredential(t *testing.T) {
	volume := newProxyTestVolume(t)
	fs := &FilerServer{volumeGuard: security.NewGuard([]string{}, proxyTestWriteKey, 10, proxyTestReadKey, 10)}

	r := httptest.NewRequest(http.MethodGet, "http://filer:8888/?proxyChunkId="+proxyTestFileId, nil)
	r.Header.Set("Authorization", security.BearerPrefix+"caller-supplied-token")
	fs.proxyToVolumeServerURL(httptest.NewRecorder(), r, proxyTestFileId, volume.URL+"/"+proxyTestFileId)

	volume.requireReached(t)
	seen := volume.seenAuth()
	if seen == security.BearerPrefix+"caller-supplied-token" {
		t.Fatal("caller's token reached the volume server instead of the minted one")
	}

	vs := &VolumeServer{guard: security.NewGuard([]string{}, proxyTestWriteKey, 10, proxyTestReadKey, 10)}
	check := httptest.NewRequest(http.MethodGet, "http://volume:8080/"+proxyTestFileId, nil)
	check.Header.Set("Authorization", seen)
	if !vs.maybeCheckJwtAuthorization(check, proxyTestVid, proxyTestFid, false) {
		t.Fatalf("forwarded token %q did not authorize the read", seen)
	}
}

// With no read key there is nothing to mint, and the caller's Authorization on
// the read path is a filer credential -- it must be dropped, not relayed to a
// volume server that has no business seeing it.
func TestProxyReadDropsCallerCredentialWhenNothingMinted(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			volume := newProxyTestVolume(t)
			fs := &FilerServer{volumeGuard: security.NewGuard([]string{}, proxyTestWriteKey, 10, "", 10)}

			r := httptest.NewRequest(method, "http://filer:8888/?proxyChunkId="+proxyTestFileId, nil)
			r.Header.Set("Authorization", security.BearerPrefix+"filer-credential")
			fs.proxyToVolumeServerURL(httptest.NewRecorder(), r, proxyTestFileId, volume.URL+"/"+proxyTestFileId)

			volume.requireReached(t)
			if got := volume.seenAuth(); got != "" {
				t.Fatalf("volume server saw Authorization %q, want it dropped", got)
			}
		})
	}
}

// Writes must not queue behind the read semaphore: a proxied write carries an
// AssignVolume token that expires 10s after the assign by default, and waiting
// for a read slot can push it past expiry.
func TestProxyWriteBypassesReadSemaphore(t *testing.T) {
	volume := newProxyTestVolume(t)
	host := volume.Listener.Addr().String()

	// Fill every read slot for this host and never release them.
	for i := 0; i < proxyReadConcurrencyPerVolumeServer; i++ {
		if err := acquireProxySemaphore(context.Background(), host); err != nil {
			t.Fatalf("fill slot %d: %v", i, err)
		}
	}
	defer func() {
		for i := 0; i < proxyReadConcurrencyPerVolumeServer; i++ {
			releaseProxySemaphore(host)
		}
	}()

	fs := &FilerServer{volumeGuard: security.NewGuard([]string{}, proxyTestWriteKey, 10, "", 10)}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := httptest.NewRequest(http.MethodPost, "http://filer:8888/?proxyChunkId="+proxyTestFileId, nil).WithContext(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		fs.proxyToVolumeServerURL(httptest.NewRecorder(), r, proxyTestFileId, volume.URL+"/"+proxyTestFileId)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("proxied write blocked on the read semaphore")
	}
	volume.requireReached(t)
}

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
