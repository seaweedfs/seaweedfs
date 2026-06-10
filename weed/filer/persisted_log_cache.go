package filer

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

const (
	// persistedLogCacheMaxBytes bounds retained entries regardless of subscriber count.
	persistedLogCacheMaxBytes = 256 << 20
	// persistedLogCacheMaxLoads bounds how many chunks fetch and decode at once.
	persistedLogCacheMaxLoads = 8
	// persistedLogCacheIdleTTL frees entries no replay has touched recently, so
	// the cache holds memory only while subscribers actually replay.
	persistedLogCacheIdleTTL = 5 * time.Minute
	// maxLogEntrySize guards the per-entry allocation against a corrupt size prefix.
	maxLogEntrySize = 1 << 30
)

// errLogChunkIncomplete reports a chunk that does not start and end on record
// boundaries; the file is then only readable as a whole byte stream.
var errLogChunkIncomplete = errors.New("log chunk does not hold whole records")

// persistedLogCache shares decoded metadata-log chunks across concurrent
// SubscribeMetadata replays. Chunks are immutable (each log flush uploads one
// whole buffer of complete records as a new chunk), so even the actively
// written current file shares its flushed chunks. Cached entries are shared
// read-only; callers must not mutate them.
type persistedLogCache struct {
	mu       sync.Mutex
	ll       *list.List // front = most recently used; values are *logCacheItem
	index    map[string]*list.Element
	curBytes int64
	maxBytes int64
	sf       singleflight.Group
	loadSem  chan struct{}
}

type logCacheItem struct {
	key      string // chunk file id
	entries  []*filer_pb.LogEntry
	bytes    int64
	lastUsed time.Time
}

func newPersistedLogCache(maxBytes int64) *persistedLogCache {
	c := &persistedLogCache{
		ll:       list.New(),
		index:    make(map[string]*list.Element),
		maxBytes: maxBytes,
		loadSem:  make(chan struct{}, persistedLogCacheMaxLoads),
	}
	// the filer's cache lives for the process lifetime
	go c.loopEvictIdle()
	return c
}

func (c *persistedLogCache) loopEvictIdle() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		c.evictIdle(time.Now().Add(-persistedLogCacheIdleTTL))
	}
}

// evictIdle drops every entry last used at or before cutoff. Recency order
// makes the idle entries exactly the tail of the LRU list.
func (c *persistedLogCache) evictIdle(cutoff time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.ll.Len() > 0 {
		el := c.ll.Back()
		if el.Value.(*logCacheItem).lastUsed.After(cutoff) {
			break
		}
		c.removeElement(el)
	}
}

type logLoadResult struct {
	entries []*filer_pb.LogEntry
	err     error
}

// getOrLoad returns the decoded entries for a chunk, loading once on miss and
// coalescing concurrent misses. Only a clean, complete decode is cached: a
// chunk-not-found read must be re-probed on later replays, and an incomplete
// chunk stays with the streaming fallback.
func (c *persistedLogCache) getOrLoad(fileId string, load func() ([]*filer_pb.LogEntry, bool, error)) ([]*filer_pb.LogEntry, error) {
	if entries, ok := c.lookup(fileId); ok {
		return entries, nil
	}
	v, _, _ := c.sf.Do(fileId, func() (interface{}, error) {
		if entries, ok := c.lookup(fileId); ok {
			return logLoadResult{entries: entries}, nil
		}
		entries, cacheable, loadErr := c.loadGuarded(load)
		if loadErr == nil && cacheable {
			c.store(fileId, entries)
		}
		return logLoadResult{entries: entries, err: loadErr}, nil
	})
	res := v.(logLoadResult)
	return res.entries, res.err
}

func (c *persistedLogCache) loadGuarded(load func() ([]*filer_pb.LogEntry, bool, error)) ([]*filer_pb.LogEntry, bool, error) {
	c.loadSem <- struct{}{}
	defer func() { <-c.loadSem }()
	return load()
}

func (c *persistedLogCache) lookup(fileId string) ([]*filer_pb.LogEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.index[fileId]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	item := el.Value.(*logCacheItem)
	item.lastUsed = time.Now()
	return item.entries, true
}

func (c *persistedLogCache) store(fileId string, entries []*filer_pb.LogEntry) {
	bytes := estimateEntriesBytes(entries)
	if bytes > c.maxBytes {
		// would evict everything else and still not fit; serve unretained
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.index[fileId]; ok {
		c.removeElement(el)
	}
	el := c.ll.PushFront(&logCacheItem{key: fileId, entries: entries, bytes: bytes, lastUsed: time.Now()})
	c.index[fileId] = el
	c.curBytes += bytes
	for c.curBytes > c.maxBytes && c.ll.Len() > 1 {
		c.removeElement(c.ll.Back())
	}
}

// removeElement drops an element from both the list and the index. Caller holds mu.
func (c *persistedLogCache) removeElement(el *list.Element) {
	item := el.Value.(*logCacheItem)
	c.ll.Remove(el)
	delete(c.index, item.key)
	c.curBytes -= item.bytes
}

// estimateEntriesBytes is deliberately generous so curBytes does not run under
// the real retained heap.
func estimateEntriesBytes(entries []*filer_pb.LogEntry) int64 {
	total := int64(len(entries)) * 128
	for _, e := range entries {
		total += int64(len(e.Data)+len(e.Key)) + 16
	}
	return total
}

// loadLogFileEntries reads one log file chunk from volume servers and decodes
// its records. fetchWholeChunk handles lookup, retries, cipher and gzip.
func loadLogFileEntries(masterClient *wdclient.MasterClient, chunk *filer_pb.FileChunk) (entries []*filer_pb.LogEntry, cacheable bool, err error) {
	bytesBuffer := bytesBufferPool.Get().(*bytes.Buffer)
	bytesBuffer.Reset()
	defer bytesBufferPool.Put(bytesBuffer)
	lookupFileIdFn := func(ctx context.Context, fileId string) (targetUrls []string, err error) {
		return masterClient.LookupFileId(ctx, fileId)
	}
	if fetchErr := fetchWholeChunk(context.Background(), bytesBuffer, lookupFileIdFn, chunk.GetFileIdString(), chunk.CipherKey, chunk.IsCompressed); fetchErr != nil {
		return nil, false, fetchErr
	}
	return decodeLogRecords(bytesBuffer.Bytes())
}

// decodeLogRecords parses size-prefixed LogEntry records. A buffer that stops
// mid-record, or whose size prefix is garbage (also the symptom of starting
// mid-record), reports errLogChunkIncomplete with the cleanly decoded prefix.
// proto.Unmarshal copies all bytes, so the entries do not alias data.
func decodeLogRecords(data []byte) (entries []*filer_pb.LogEntry, cacheable bool, err error) {
	for pos := 0; pos < len(data); {
		if pos+4 > len(data) {
			return entries, false, errLogChunkIncomplete
		}
		size32 := util.BytesToUint32(data[pos : pos+4])
		if size32 > maxLogEntrySize {
			return entries, false, errLogChunkIncomplete
		}
		size := int(size32)
		if pos+4+size > len(data) {
			return entries, false, errLogChunkIncomplete
		}
		logEntry := &filer_pb.LogEntry{}
		if unmarshalErr := proto.Unmarshal(data[pos+4:pos+4+size], logEntry); unmarshalErr != nil {
			return entries, false, errLogChunkIncomplete
		}
		entries = append(entries, logEntry)
		pos += 4 + size
	}
	return entries, true, nil
}
