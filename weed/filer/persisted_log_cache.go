package filer

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

const (
	// persistedLogCacheMaxBytes bounds retained entries regardless of subscriber
	// count.
	persistedLogCacheMaxBytes = 256 << 20
	// persistedLogCacheMinAge keeps the actively-written current file out of the
	// cache to avoid reload churn. It is a heuristic, not a correctness gate:
	// every hit is validated against the file's current chunk set (a fingerprint),
	// so a file that turns out to have grown is reloaded rather than served stale.
	persistedLogCacheMinAge = 2 * LogFlushInterval
	// persistedLogCacheMaxLoads bounds how many distinct files decode at once, so
	// the transient peak (an 8MB chunk reader plus the decoded slice per load)
	// stays bounded no matter how many positions the subscriber fleet spans.
	persistedLogCacheMaxLoads = 8
	// maxLogEntrySize guards the per-entry allocation against a corrupt size
	// prefix. The write path caps a single entry well under this, so it never
	// rejects a legitimate event.
	maxLogEntrySize = 1 << 30
)

// persistedLogCache shares the decoded entries of completed metadata log files
// across concurrent SubscribeMetadata replays, so N replays of the same history
// read and decode each file once instead of N times. Cached entries are shared
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
	key string
	// fingerprint identifies the file's chunk set when it was decoded. A cached
	// item is only served when the caller's fingerprint still matches; an
	// appended-to file (e.g. a delayed flush landed after the file was first read)
	// produces a new fingerprint and is reloaded.
	fingerprint string
	entries     []*filer_pb.LogEntry
	bytes       int64
}

func newPersistedLogCache(maxBytes int64) *persistedLogCache {
	return &persistedLogCache{
		ll:       list.New(),
		index:    make(map[string]*list.Element),
		maxBytes: maxBytes,
		loadSem:  make(chan struct{}, persistedLogCacheMaxLoads),
	}
}

// logFileIsCacheable reports whether a log file identified by its minute
// timestamp is old enough that caching it is worthwhile (it is unlikely to still
// be receiving appends). Correctness does not depend on this being exact.
func logFileIsCacheable(fileTsNs int64) bool {
	return time.Since(time.Unix(0, fileTsNs)) > persistedLogCacheMinAge
}

// chunksFingerprint summarizes a log file's append-only chunk set; it changes
// whenever the file grows.
func chunksFingerprint(chunks []*filer_pb.FileChunk) string {
	if len(chunks) == 0 {
		return "empty"
	}
	var total uint64
	for _, c := range chunks {
		total += c.Size
	}
	return fmt.Sprintf("%d/%d/%s", len(chunks), total, chunks[len(chunks)-1].GetFileIdString())
}

// getOrLoad returns the decoded entries for key, loading them once on miss and
// coalescing concurrent misses for the same (key, fingerprint). A cached item
// whose fingerprint no longer matches is treated as a miss and replaced. The
// returned slice is shared and must be treated as read-only.
type logLoadResult struct {
	entries []*filer_pb.LogEntry
	err     error
}

// load returns the decoded entries, whether the result is stable enough to
// cache, and any error. A read that stopped on a missing chunk is delivered to
// this caller but not cached: the truncation point depends on transient volume
// availability, not on the (cached) fingerprint, so it must be re-probed on
// later replays. On a genuine error the partially-read entries are still
// returned alongside the error so the caller can deliver them before failing,
// matching the streaming path; only a clean, complete read is cached.
func (c *persistedLogCache) getOrLoad(key, fingerprint string, load func() ([]*filer_pb.LogEntry, bool, error)) ([]*filer_pb.LogEntry, error) {
	if entries, ok := c.lookup(key, fingerprint); ok {
		return entries, nil
	}
	v, _, _ := c.sf.Do(key+"\x00"+fingerprint, func() (interface{}, error) {
		if entries, ok := c.lookup(key, fingerprint); ok {
			return logLoadResult{entries: entries}, nil
		}
		entries, cacheable, loadErr := c.loadGuarded(load)
		if loadErr == nil && cacheable {
			c.store(key, fingerprint, entries)
		}
		return logLoadResult{entries: entries, err: loadErr}, nil
	})
	res := v.(logLoadResult)
	return res.entries, res.err
}

// loadGuarded runs load under the concurrent-load semaphore, releasing the slot
// via defer so an abnormal exit cannot leak slots and wedge future loads.
func (c *persistedLogCache) loadGuarded(load func() ([]*filer_pb.LogEntry, bool, error)) ([]*filer_pb.LogEntry, bool, error) {
	c.loadSem <- struct{}{}
	defer func() { <-c.loadSem }()
	return load()
}

func (c *persistedLogCache) lookup(key, fingerprint string) ([]*filer_pb.LogEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.index[key]
	if !ok {
		return nil, false
	}
	item := el.Value.(*logCacheItem)
	if item.fingerprint != fingerprint {
		// The file changed (grew) since it was cached; drop the stale snapshot so
		// the caller reloads and stores the current version.
		c.removeElement(el)
		return nil, false
	}
	c.ll.MoveToFront(el)
	return item.entries, true
}

func (c *persistedLogCache) store(key, fingerprint string, entries []*filer_pb.LogEntry) {
	bytes := estimateEntriesBytes(entries)
	if bytes > c.maxBytes {
		// A single file larger than the whole budget would evict everything else
		// and still not fit; serve it this round but do not retain it.
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.index[key]; ok {
		c.removeElement(el) // replace any older version of this file
	}
	el := c.ll.PushFront(&logCacheItem{key: key, fingerprint: fingerprint, entries: entries, bytes: bytes})
	c.index[key] = el
	c.curBytes += bytes
	// Evict least-recently-used until under budget, but always keep what we just
	// inserted (a single file larger than the budget still serves this round).
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

func estimateEntriesBytes(entries []*filer_pb.LogEntry) int64 {
	// LogEntry struct (~112B) + slice slot (8B) + a little slack, plus the
	// payload backing arrays. Deliberately generous so curBytes does not run
	// under the real retained heap.
	total := int64(len(entries)) * 128
	for _, e := range entries {
		total += int64(len(e.Data)+len(e.Key)) + 16
	}
	return total
}

// loadLogFileEntries reads a whole persisted log file from volume servers and
// decodes every LogEntry. The second result reports whether the read reached a
// clean end (cacheable). A chunk-not-found stop returns the readable prefix with
// cacheable=false: the file's chunk list (the cache fingerprint) is unchanged by
// a momentarily-unreachable volume, so caching the prefix would pin a truncation
// that a transient outage should self-heal. Such reads mirror the streaming
// path for this round but are re-probed on the next replay instead.
func loadLogFileEntries(masterClient *wdclient.MasterClient, filePath string, chunks []*filer_pb.FileChunk) (entries []*filer_pb.LogEntry, cacheable bool, err error) {
	r := NewChunkStreamReaderFromFiler(context.Background(), masterClient, chunks)
	defer r.Close()

	sizeBuf := make([]byte, 4)
	for {
		if _, readErr := io.ReadFull(r, sizeBuf); readErr != nil {
			if readErr == io.EOF {
				// clean end exactly at a record boundary: every record was read
				return entries, true, nil
			}
			if readErr == io.ErrUnexpectedEOF {
				// stopped mid-frame (a partial size prefix): ambiguous, possibly a
				// transient short read, so deliver the prefix but do not cache it
				glog.V(1).Infof("log file %s ends mid-record, not caching", filePath)
				return entries, false, nil
			}
			if isChunkNotFoundError(readErr) {
				glog.V(1).Infof("log file %s has an unreadable chunk, not caching: %v", filePath, readErr)
				return entries, false, nil
			}
			return entries, false, readErr
		}
		size := util.BytesToUint32(sizeBuf)
		if size > maxLogEntrySize {
			return entries, false, fmt.Errorf("log file %s entry size %d exceeds %d", filePath, size, maxLogEntrySize)
		}
		entryData := make([]byte, size)
		if _, readErr := io.ReadFull(r, entryData); readErr != nil {
			if isChunkNotFoundError(readErr) {
				glog.V(1).Infof("log file %s has an unreadable chunk, not caching: %v", filePath, readErr)
				return entries, false, nil
			}
			return entries, false, readErr
		}
		logEntry := &filer_pb.LogEntry{}
		if unmarshalErr := proto.Unmarshal(entryData, logEntry); unmarshalErr != nil {
			return entries, false, unmarshalErr
		}
		entries = append(entries, logEntry)
	}
}
