package remote_storage

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Remote storage clients are cached, one per (kind, configuration,
// credentials version).
//
// Each client carries its own HTTP transport. A client built per request keeps
// its connection open for the transport's idle timeout and never reuses it, so
// a steady stream of lazy remote stats, cold reads or remote deletes holds one
// open connection per request; a storage endpoint that caps connections per
// source then refuses every new one. So clients are cached:
//   - the key is the kind ("" for the plain client of GetRemoteStorage,
//     "guarded" for the servers' SSRF-guarded one; the two are never shared),
//     the configuration's name, a hash of the whole configuration and a
//     credentials version (the content hash of a credentials file the
//     configuration points at, which can change while the configuration does
//     not). Two configurations under one name are two entries; neither evicts
//     the other.
//   - the cache is an LRU of WEED_REMOTE_CLIENT_CACHE_MAX entries (default 64).
//   - a client is built outside the cache's lock, once per key at a time
//     (singleflight): a slow build (a DNS check, a credentials file) never
//     stalls the lookups of other keys.
//   - a client leaving the cache (evicted, or dropped by RetainRemoteStorages)
//     has its idle connections closed at once and again after a grace period,
//     for the connections its in-flight requests return to the pool afterwards.
//   - RetainRemoteStorages (every filer reload) also retires, per kind and
//     name, every entry but the newest: a key rotation, a new endpoint or a new
//     credentials file leaves no client of the old configuration behind. A
//     build finishing after a reload never enters the cache either: it is
//     retired instead when a reload passed while it was running.
//   - the cached client is built from a copy of the configuration, so a caller
//     changing its value later changes nothing.

// ClientBuild builds the client of a configuration and, optionally, a function that closes its idle connections.
type ClientBuild func(conf *remote_pb.RemoteConf) (client RemoteStorageClient, closeIdle func(), err error)

type cachedClient struct {
	key        string
	kind       string
	name       string
	born       uint64 // insertion order: retain keeps only the newest per kind and name
	generation uint64 // the retain generation the build started under
	client     RemoteStorageClient
	closeIdle  func()
}

type clientCache struct {
	mu         sync.Mutex
	max        int
	order      *list.List // front: most recently used
	entries    map[string]*list.Element
	building   singleflight.Group
	births     uint64
	generation uint64 // bumped by every retain: a build started under an older one is stale
	grace      time.Duration
	afterFunc  func(time.Duration, func()) // tests
}

var remoteClients = newClientCache(clientCacheMaxFromEnv(), time.Minute)

func newClientCache(max int, grace time.Duration) *clientCache {
	return &clientCache{max: max, order: list.New(), entries: map[string]*list.Element{}, grace: grace,
		afterFunc: func(d time.Duration, f func()) { time.AfterFunc(d, f) }}
}

func clientCacheMaxFromEnv() int {
	if v := os.Getenv("WEED_REMOTE_CLIENT_CACHE_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
		glog.Warningf("WEED_REMOTE_CLIENT_CACHE_MAX=%q is not a positive number; using 64", v)
	}
	return 64
}

// clientKey names a configuration exactly: kind, name, a hash of its deterministic encoding, and the credentials version.
func clientKey(kind string, conf *remote_pb.RemoteConf, version string) string {
	encoded, _ := proto.MarshalOptions{Deterministic: true}.Marshal(conf)
	sum := sha256.Sum256(encoded)
	return kind + "\x00" + conf.GetName() + "\x00" + hex.EncodeToString(sum[:16]) + "\x00" + version
}

func (c *clientCache) get(kind string, conf *remote_pb.RemoteConf, version string, build ClientBuild) (RemoteStorageClient, error) {
	key := clientKey(kind, conf, version)
	if client := c.lookup(key); client != nil {
		return client, nil
	}
	own := proto.Clone(conf).(*remote_pb.RemoteConf)
	v, err, _ := c.building.Do(key, func() (interface{}, error) {
		if client := c.lookup(key); client != nil { // another caller's build finished between the lookup and Do
			return client, nil
		}
		generation := c.generationSnapshot()
		client, closeIdle, err := build(own)
		if err != nil {
			return nil, err
		}
		c.insert(&cachedClient{key: key, kind: kind, name: own.GetName(), generation: generation, client: client, closeIdle: closeIdle})
		return client, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(RemoteStorageClient), nil
}

func (c *clientCache) lookup(key string) RemoteStorageClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		c.order.MoveToFront(el)
		return el.Value.(*cachedClient).client
	}
	return nil
}

func (c *clientCache) generationSnapshot() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

func (c *clientCache) insert(e *cachedClient) {
	var evicted []*cachedClient
	c.mu.Lock()
	if el, ok := c.entries[e.key]; ok { // cannot happen under singleflight; keep the first, retire the newcomer
		c.order.MoveToFront(el)
		evicted = append(evicted, e)
	} else if e.generation < c.generation {
		// a reload ran while the build was in flight: the caller keeps the
		// client but it never enters the cache
		evicted = append(evicted, e)
	} else {
		c.births++
		e.born = c.births
		c.entries[e.key] = c.order.PushFront(e)
		for c.order.Len() > c.max {
			back := c.order.Back()
			old := back.Value.(*cachedClient)
			c.order.Remove(back)
			delete(c.entries, old.key)
			evicted = append(evicted, old)
		}
	}
	c.mu.Unlock()
	for _, old := range evicted {
		c.retire(old)
	}
}

// retire closes a client's idle connections now and again after the grace period (requests in flight when it left the
// cache return their connections to its pool afterwards).
func (c *clientCache) retire(e *cachedClient) {
	if e.closeIdle == nil {
		return
	}
	e.closeIdle()
	c.afterFunc(c.grace, e.closeIdle)
}

func (c *clientCache) retain(names map[string]bool) {
	var dropped []*cachedClient
	c.mu.Lock()
	c.generation++
	newest := map[string]uint64{} // kind and name -> the newest entry's birth
	for _, el := range c.entries {
		e := el.Value.(*cachedClient)
		if k := e.kind + "\x00" + e.name; e.born > newest[k] {
			newest[k] = e.born
		}
	}
	for key, el := range c.entries {
		e := el.Value.(*cachedClient)
		if names[e.name] && e.born == newest[e.kind+"\x00"+e.name] {
			continue
		}
		c.order.Remove(el)
		delete(c.entries, key)
		dropped = append(dropped, e)
	}
	c.mu.Unlock()
	for _, e := range dropped {
		c.retire(e)
	}
}

func (c *clientCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// GetRemoteStorage returns the plain client of a configuration (one per configuration).
func GetRemoteStorage(remoteConf *remote_pb.RemoteConf) (RemoteStorageClient, error) {
	return GetRemoteStorageWith("", remoteConf, CredentialsVersion(remoteConf), func(conf *remote_pb.RemoteConf) (RemoteStorageClient, func(), error) {
		client, err := makeRemoteStorageClient(conf)
		if err != nil {
			return nil, nil, fmt.Errorf("make remote storage client %s: %w", conf.GetName(), err)
		}
		return client, nil, nil
	})
}

// GetRemoteStorageWith returns the cached client of one kind for a configuration and credentials version, building it with
// build (given a copy of the configuration) when there is none. version names whatever the client reads at build time
// that the configuration does not hold (a credentials file's content hash); "" when nothing.
func GetRemoteStorageWith(kind string, remoteConf *remote_pb.RemoteConf, version string, build ClientBuild) (RemoteStorageClient, error) {
	return remoteClients.get(kind, remoteConf, version, build)
}

// CredentialsVersion names what a gcs client, plain or guarded, reads at
// build time that its configuration does not hold: a credentials file's
// content, inline JSON from GOOGLE_APPLICATION_CREDENTIALS, or the well-known
// application default credentials file when neither names one. The
// configuration's own inline JSON needs none; it is already inside the
// configuration's hash. "" for a non-gcs remote.
func CredentialsVersion(remoteConf *remote_pb.RemoteConf) string {
	if remoteConf.GetType() != "gcs" {
		return ""
	}
	path := remoteConf.GetGcsGoogleApplicationCredentials()
	fromEnv := false
	if path == "" {
		path, fromEnv = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"), true
	}
	if strings.HasPrefix(path, "{") {
		if !fromEnv {
			return ""
		}
		sum := sha256.Sum256([]byte(path))
		return hex.EncodeToString(sum[:16])
	}
	if path == "" {
		path = wellKnownCredentialsFile()
	}
	return FileVersion(path)
}

// wellKnownCredentialsFile is the application default credentials file the
// gcs SDK reads when neither the configuration nor
// GOOGLE_APPLICATION_CREDENTIALS names one.
func wellKnownCredentialsFile() string {
	const f = "application_default_credentials.json"
	if runtime.GOOS == "windows" {
		return filepath.Join(os.Getenv("APPDATA"), "gcloud", f)
	}
	home := os.Getenv("HOME")
	if home == "" {
		if u, err := user.Current(); err == nil {
			home = u.HomeDir
		}
	}
	return filepath.Join(home, ".config", "gcloud", f)
}

// RetainRemoteStorages drops, and closes the idle connections of, every cached client whose configuration name is not in
// names, and every client of a name but its newest: a remote removed from /etc/remote, or the old configuration of a
// changed one, keeps no client and no pool. The filer calls it on every reload.
func RetainRemoteStorages(names map[string]bool) {
	remoteClients.retain(names)
}

// FileVersion is the credentials version of a file: a hash of its content, "" when it cannot be read (the build then
// reads it, or fails, as it would without the cache).
func FileVersion(path string) string {
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(util.ResolvePath(path)) // "~/…" as the gcs client reads it
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:16])
}

// ResetRemoteStorageClients empties the cache, retiring every client. For tests.
func ResetRemoteStorageClients() {
	remoteClients.retain(nil)
}
