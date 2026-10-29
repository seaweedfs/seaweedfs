package remote_storage

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
)

// a client that is only an identity: the cache never calls it
type cacheTestClient struct {
	RemoteStorageClient
	id int64
}

// a fresh cache for one test, swapped in for the process-wide one and restored after
func useTestCache(t *testing.T, max int) (*clientCache, *[]time.Duration) {
	t.Helper()
	c := newClientCache(max, time.Minute)
	var delayed []time.Duration
	var mu sync.Mutex
	c.afterFunc = func(d time.Duration, f func()) { mu.Lock(); delayed = append(delayed, d); mu.Unlock(); f() }
	prev := remoteClients
	remoteClients = c
	t.Cleanup(func() { remoteClients = prev })
	return c, &delayed
}

type counter struct {
	builds atomic.Int64
	mu     sync.Mutex
	closed map[int64]int
	confs  []*remote_pb.RemoteConf
}

func (k *counter) build(conf *remote_pb.RemoteConf) (RemoteStorageClient, func(), error) {
	id := k.builds.Add(1)
	k.mu.Lock()
	k.confs = append(k.confs, conf)
	k.mu.Unlock()
	return &cacheTestClient{id: id}, func() {
		k.mu.Lock()
		defer k.mu.Unlock()
		if k.closed == nil {
			k.closed = map[int64]int{}
		}
		k.closed[id]++
	}, nil
}

func (k *counter) closes(id int64) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.closed[id]
}

func s3conf(name, secret string) *remote_pb.RemoteConf {
	return &remote_pb.RemoteConf{Name: name, Type: "s3", S3Endpoint: "https://s3.example", S3AccessKey: "k", S3SecretKey: secret}
}

// One client per configuration: the same configuration (another copy of it) gets the same client, built once.
func TestRemoteClientCache_SameConfigurationOneClient(t *testing.T) {
	useTestCache(t, 64)
	var k counter
	first, err := GetRemoteStorageWith("guarded", s3conf("a", "x"), "", k.build)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		again, _ := GetRemoteStorageWith("guarded", s3conf("a", "x"), "", k.build)
		if again != first {
			t.Fatalf("call %d built a new client for the same configuration", i)
		}
	}
	if k.builds.Load() != 1 {
		t.Fatalf("builds = %d, want 1", k.builds.Load())
	}
	plain, _ := GetRemoteStorageWith("", s3conf("a", "x"), "", k.build)
	if plain == first {
		t.Fatal("the plain and the guarded client of one configuration must not be shared")
	}
}

// Two configurations under one name (a key being rotated while the old one is still in use) coexist: alternating between
// them builds each once, never thrashing.
func TestRemoteClientCache_TwoConfigurationsUnderOneNameCoexist(t *testing.T) {
	useTestCache(t, 64)
	var k counter
	for i := 0; i < 10; i++ {
		secret := "old"
		if i%2 == 1 {
			secret = "new"
		}
		if _, err := GetRemoteStorageWith("guarded", s3conf("a", secret), "", k.build); err != nil {
			t.Fatal(err)
		}
	}
	if k.builds.Load() != 2 {
		t.Fatalf("builds = %d for two alternating configurations under one name, want 2", k.builds.Load())
	}
	if k.closes(1) != 0 || k.closes(2) != 0 {
		t.Fatal("neither client may be closed while both are in use")
	}
}

// The credentials version is part of the key: a changed credentials file is a new client.
func TestRemoteClientCache_VersionIsPartOfTheKey(t *testing.T) {
	useTestCache(t, 64)
	var k counter
	a, _ := GetRemoteStorageWith("guarded", s3conf("a", ""), "v1", k.build)
	b, _ := GetRemoteStorageWith("guarded", s3conf("a", ""), "v1", k.build)
	c, _ := GetRemoteStorageWith("guarded", s3conf("a", ""), "v2", k.build)
	if a != b || a == c || k.builds.Load() != 2 {
		t.Fatalf("same version must share, another version must not (builds = %d)", k.builds.Load())
	}
}

// The cache is bounded: past its size the least recently used client is evicted and its idle connections closed, at once
// and again after the grace period.
func TestRemoteClientCache_BoundedLRU(t *testing.T) {
	c, delayed := useTestCache(t, 4)
	var k counter
	for i := 0; i < 4; i++ {
		_, _ = GetRemoteStorageWith("guarded", s3conf(fmt.Sprintf("r%d", i), "x"), "", k.build)
	}
	_, _ = GetRemoteStorageWith("guarded", s3conf("r0", "x"), "", k.build) // r0 is now the most recently used
	_, _ = GetRemoteStorageWith("guarded", s3conf("r4", "x"), "", k.build) // evicts r1 (client 2)
	if n := c.len(); n != 4 {
		t.Fatalf("cache holds %d clients, want 4", n)
	}
	if k.closes(2) != 2 {
		t.Fatalf("the evicted client must be closed now and after the grace period: closed %d time(s)", k.closes(2))
	}
	if len(*delayed) != 1 || (*delayed)[0] != time.Minute {
		t.Fatalf("one delayed close after the grace period, got %v", *delayed)
	}
	if k.closes(1) != 0 {
		t.Fatal("the recently used client was evicted instead of the least recently used one")
	}
	again, _ := GetRemoteStorageWith("guarded", s3conf("r0", "x"), "", k.build)
	if again.(*cacheTestClient).id != 1 {
		t.Fatal("r0 must still be cached")
	}
}

// RetainRemoteStorages drops and closes the clients of every configuration not named, of every kind.
func TestRemoteClientCache_RetainDropsWhatIsGone(t *testing.T) {
	useTestCache(t, 64)
	var k counter
	_, _ = GetRemoteStorageWith("guarded", s3conf("keep", "x"), "", k.build)
	_, _ = GetRemoteStorageWith("guarded", s3conf("gone", "x"), "", k.build)
	_, _ = GetRemoteStorageWith("", s3conf("gone", "x"), "", k.build)
	RetainRemoteStorages(map[string]bool{"keep": true})
	if k.closes(1) != 0 || k.closes(2) != 2 || k.closes(3) != 2 {
		t.Fatalf("closes: keep %d, gone %d and %d", k.closes(1), k.closes(2), k.closes(3))
	}
	_, _ = GetRemoteStorageWith("guarded", s3conf("gone", "x"), "", k.build)
	if k.builds.Load() != 4 {
		t.Fatal("a dropped configuration is built again on its next use")
	}
}

// Builds run outside the cache's lock, once per key: a slow build does not stall another key, and concurrent callers of
// one key share one build.
func TestRemoteClientCache_BuildsOutsideTheLockOncePerKey(t *testing.T) {
	useTestCache(t, 64)
	var k counter
	release := make(chan struct{})
	slow := func(conf *remote_pb.RemoteConf) (RemoteStorageClient, func(), error) {
		<-release
		return k.build(conf)
	}
	var wg sync.WaitGroup
	results := make([]RemoteStorageClient, 20)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], _ = GetRemoteStorageWith("guarded", s3conf("slow", "x"), "", slow)
		}(i)
	}
	time.Sleep(50 * time.Millisecond) // the slow build is in progress
	done := make(chan struct{})
	go func() {
		_, _ = GetRemoteStorageWith("guarded", s3conf("other", "x"), "", k.build)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("another key waited for a slow build: the build holds the cache's lock")
	}
	close(release)
	wg.Wait()
	for i, r := range results {
		if r != results[0] || r == nil {
			t.Fatalf("caller %d got another client: concurrent callers of one key must share one build", i)
		}
	}
	if k.builds.Load() != 2 {
		t.Fatalf("builds = %d, want 2 (one per key)", k.builds.Load())
	}
}

// The client is built from a copy of the configuration: the caller changing its value afterwards changes nothing.
func TestRemoteClientCache_BuildsFromACopy(t *testing.T) {
	useTestCache(t, 64)
	var k counter
	conf := s3conf("a", "x")
	_, _ = GetRemoteStorageWith("guarded", conf, "", k.build)
	if k.confs[0] == conf {
		t.Fatal("the build was given the caller's own configuration")
	}
	conf.S3SecretKey = "changed"
	if k.confs[0].S3SecretKey != "x" || !proto.Equal(k.confs[0], s3conf("a", "x")) {
		t.Fatal("the cached client's configuration changed with the caller's value")
	}
}

// The process-wide cache can be emptied (tests in other packages use it).
func TestRemoteClientCache_Reset(t *testing.T) {
	useTestCache(t, 64)
	var k counter
	_, _ = GetRemoteStorageWith("guarded", s3conf("a", "x"), "", k.build)
	ResetRemoteStorageClients()
	if remoteClients.len() != 0 || k.closes(1) != 2 {
		t.Fatalf("after a reset: %d cached, closed %d time(s)", remoteClients.len(), k.closes(1))
	}
}

// A reload (RetainRemoteStorages) keeps, per kind and name, only the newest client: the old configuration of a rotated
// key, or the old version of a credentials file, leaves no client and no pool behind; another kind keeps its own.
func TestRemoteClientCache_RetainKeepsOnlyTheNewestPerName(t *testing.T) {
	useTestCache(t, 64)
	var k counter
	_, _ = GetRemoteStorageWith("guarded", s3conf("a", "old"), "", k.build) // 1
	_, _ = GetRemoteStorageWith("guarded", s3conf("a", "new"), "", k.build) // 2: the rotated key
	_, _ = GetRemoteStorageWith("guarded", s3conf("a", ""), "v1", k.build)  // 3
	_, _ = GetRemoteStorageWith("guarded", s3conf("a", ""), "v2", k.build)  // 4: the credentials file changed
	_, _ = GetRemoteStorageWith("", s3conf("a", "old"), "", k.build)        // 5: the plain kind's own
	RetainRemoteStorages(map[string]bool{"a": true})
	if k.closes(1) != 2 || k.closes(2) != 2 || k.closes(3) != 2 {
		t.Fatalf("superseded clients must be closed: %d %d %d", k.closes(1), k.closes(2), k.closes(3))
	}
	if k.closes(4) != 0 || k.closes(5) != 0 {
		t.Fatalf("the newest of each kind must stay: guarded %d, plain %d", k.closes(4), k.closes(5))
	}
	again, _ := GetRemoteStorageWith("guarded", s3conf("a", ""), "v2", k.build)
	if again.(*cacheTestClient).id != 4 {
		t.Fatal("the newest client must still be cached")
	}
}

// A build that finishes after a reload removed its remote must not put the client back: the caller still gets it, but
// it never enters the cache, and the next use builds again.
func TestRemoteClientCache_BuildSpanningRetainIsDropped(t *testing.T) {
	useTestCache(t, 64)
	var k counter
	started := make(chan struct{})
	release := make(chan struct{})
	slow := func(conf *remote_pb.RemoteConf) (RemoteStorageClient, func(), error) {
		close(started)
		<-release
		return k.build(conf)
	}
	built := make(chan RemoteStorageClient, 1)
	go func() {
		client, _ := GetRemoteStorageWith("guarded", s3conf("gone", "x"), "", slow)
		built <- client
	}()
	<-started
	RetainRemoteStorages(map[string]bool{"keep": true})
	close(release)
	if client := <-built; client == nil {
		t.Fatal("the caller still gets its built client")
	}
	if k.closes(1) != 2 {
		t.Fatalf("the dropped client must be retired: closes %d", k.closes(1))
	}
	_, _ = GetRemoteStorageWith("guarded", s3conf("gone", "x"), "", k.build)
	if k.builds.Load() != 2 {
		t.Fatalf("builds = %d, want 2: the dropped client was not cached", k.builds.Load())
	}
}

// A remote the local filer never named — another filer's, proxied through this volume server — keeps connection reuse:
// retention only ever drops what was built, never what a later get is for.
func TestRemoteClientCache_UnknownNameStillCaches(t *testing.T) {
	useTestCache(t, 64)
	var k counter
	RetainRemoteStorages(map[string]bool{"keep": true})
	first, _ := GetRemoteStorageWith("guarded", s3conf("peer", "x"), "", k.build)
	again, _ := GetRemoteStorageWith("guarded", s3conf("peer", "x"), "", k.build)
	if first == nil || first != again || k.builds.Load() != 1 {
		t.Fatal("a remote the local filer does not name must still be cached")
	}
}

// The plain gcs client's version follows its credentials file: a conf path or GOOGLE_APPLICATION_CREDENTIALS, while
// inline JSON needs none (it is inside the configuration's hash already).
func TestPlainClientGcsCredentialsVersion(t *testing.T) {
	dir := t.TempDir()
	creds := filepath.Join(dir, "creds.json")
	if err := os.WriteFile(creds, []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	conf := &remote_pb.RemoteConf{Name: "g", Type: "gcs", GcsGoogleApplicationCredentials: creds}
	v1 := CredentialsVersion(conf)
	if v1 == "" {
		t.Fatal("a credentials path must produce a version")
	}
	if err := os.WriteFile(creds, []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	if CredentialsVersion(conf) == v1 {
		t.Fatal("a rotated credentials file must change the version")
	}
	if CredentialsVersion(&remote_pb.RemoteConf{Type: "gcs", GcsGoogleApplicationCredentials: "{}"}) != "" {
		t.Fatal("inline JSON credentials need no version")
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", creds)
	if CredentialsVersion(&remote_pb.RemoteConf{Type: "gcs"}) == "" {
		t.Fatal("the environment's credentials path must produce a version")
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", `{"type":"one"}`)
	vEnvJSON := CredentialsVersion(&remote_pb.RemoteConf{Type: "gcs"})
	if vEnvJSON == "" {
		t.Fatal("the environment's inline JSON must produce a version")
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", `{"type":"two"}`)
	if CredentialsVersion(&remote_pb.RemoteConf{Type: "gcs"}) == vEnvJSON {
		t.Fatal("rotated inline JSON credentials must change the version")
	}
	if CredentialsVersion(&remote_pb.RemoteConf{Type: "s3"}) != "" {
		t.Fatal("a non-gcs remote has no credentials file version")
	}
}

// With neither the configuration nor GOOGLE_APPLICATION_CREDENTIALS naming credentials, the version follows the
// well-known application default credentials file the SDK reads.
func TestPlainClientGcsWellKnownCredentialsVersion(t *testing.T) {
	dir := t.TempDir()
	if runtime.GOOS == "windows" {
		t.Setenv("APPDATA", dir)
	} else {
		t.Setenv("HOME", dir)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	adc := wellKnownCredentialsFile()
	if err := os.MkdirAll(filepath.Dir(adc), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(adc, []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	conf := &remote_pb.RemoteConf{Type: "gcs"}
	v1 := CredentialsVersion(conf)
	if v1 == "" {
		t.Fatal("the well-known credentials file must produce a version")
	}
	if err := os.WriteFile(adc, []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	if CredentialsVersion(conf) == v1 {
		t.Fatal("a rotated well-known credentials file must change the version")
	}
}

// FileVersion reads "~/…" as the gcs client does (util.ResolvePath), so a rotated file under the home directory is seen.
func TestFileVersionResolvesHome(t *testing.T) {
	u, err := user.Current()
	if err != nil {
		t.Skip("no current user")
	}
	name := fmt.Sprintf(".seaweedfs-fileversion-test-%d.json", os.Getpid())
	path := filepath.Join(u.HomeDir, name)
	if err := os.WriteFile(path, []byte("one"), 0o600); err != nil {
		t.Skip("home directory not writable")
	}
	t.Cleanup(func() { os.Remove(path) })
	v1 := FileVersion("~/" + name)
	if v1 == "" {
		t.Fatal(`FileVersion("~/...") is empty: "~" was not resolved`)
	}
	_ = os.WriteFile(path, []byte("two"), 0o600)
	if v2 := FileVersion("~/" + name); v2 == v1 {
		t.Fatal("a rotated file must change the version")
	}
}

// A failed plain build names the remote configuration in its error.
func TestGetRemoteStorageErrorNamesTheRemote(t *testing.T) {
	useTestCache(t, 64)
	_, err := GetRemoteStorage(&remote_pb.RemoteConf{Name: "mybucketremote", Type: "no-such-type"})
	if err == nil || !strings.Contains(err.Error(), "make remote storage client mybucketremote") {
		t.Fatalf("error %v must name the remote", err)
	}
}
