package filer

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

func logEntriesAt(tsNs ...int64) []*filer_pb.LogEntry {
	out := make([]*filer_pb.LogEntry, 0, len(tsNs))
	for _, ts := range tsNs {
		out = append(out, &filer_pb.LogEntry{TsNs: ts, Data: []byte("x")})
	}
	return out
}

func encodeLogRecords(t *testing.T, entries []*filer_pb.LogEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	sizeBuf := make([]byte, 4)
	for _, e := range entries {
		data, err := proto.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		util.Uint32toBytes(sizeBuf, uint32(len(data)))
		buf.Write(sizeBuf)
		buf.Write(data)
	}
	return buf.Bytes()
}

func TestPersistedLogCacheHitMiss(t *testing.T) {
	c := newPersistedLogCache(persistedLogCacheMaxBytes)
	var loads int32
	load := func() ([]*filer_pb.LogEntry, bool, error) {
		atomic.AddInt32(&loads, 1)
		return logEntriesAt(1, 2, 3), true, nil
	}

	e1, err := c.getOrLoad("3,01", 1, load)
	if err != nil || len(e1) != 3 {
		t.Fatalf("first getOrLoad: err=%v len=%d", err, len(e1))
	}
	e2, err := c.getOrLoad("3,01", 1, load)
	if err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&loads); n != 1 {
		t.Fatalf("expected loader called once, got %d", n)
	}
	if &e1[0] != &e2[0] {
		t.Fatal("cache should return the same shared slice")
	}
}

func TestPersistedLogCacheNotCachedWhenUncacheable(t *testing.T) {
	c := newPersistedLogCache(persistedLogCacheMaxBytes)
	var loads int32
	// cacheable=false models a chunk-not-found stop: deliver the prefix this
	// round but never pin it, so a transient outage is re-probed next replay.
	load := func() ([]*filer_pb.LogEntry, bool, error) {
		atomic.AddInt32(&loads, 1)
		return logEntriesAt(1, 2), false, nil
	}

	if e, err := c.getOrLoad("3,01", 1, load); err != nil || len(e) != 2 {
		t.Fatalf("first: err=%v len=%d", err, len(e))
	}
	if e, err := c.getOrLoad("3,01", 1, load); err != nil || len(e) != 2 {
		t.Fatalf("second: err=%v len=%d", err, len(e))
	}
	if n := atomic.LoadInt32(&loads); n != 2 {
		t.Fatalf("uncacheable result must re-load every time, got %d loads", n)
	}
}

func TestPersistedLogCacheSingleFlight(t *testing.T) {
	c := newPersistedLogCache(persistedLogCacheMaxBytes)
	var loads int32
	release := make(chan struct{})
	started := make(chan struct{})
	load := func() ([]*filer_pb.LogEntry, bool, error) {
		if atomic.AddInt32(&loads, 1) == 1 {
			close(started)
		}
		<-release // hold the flight open so concurrent callers coalesce
		return logEntriesAt(1), true, nil
	}

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.getOrLoad("3,01", 1, load); err != nil {
				t.Error(err)
			}
		}()
	}
	<-started // the flight is provably open before anyone is released
	time.Sleep(10 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&loads); got != 1 {
		t.Fatalf("expected 1 coalesced load across %d callers, got %d", n, got)
	}
}

func TestPersistedLogCacheEviction(t *testing.T) {
	// One entry estimates to 128 + 100 + 16 = 244 bytes; budget holds one, not two.
	c := newPersistedLogCache(300)
	mk := func(ts int64) func() ([]*filer_pb.LogEntry, bool, error) {
		return func() ([]*filer_pb.LogEntry, bool, error) {
			return []*filer_pb.LogEntry{{TsNs: ts, Data: make([]byte, 100)}}, true, nil
		}
	}

	if _, err := c.getOrLoad("a", 1, mk(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.getOrLoad("b", 1, mk(2)); err != nil { // pushes over budget, evicts LRU "a"
		t.Fatal(err)
	}

	c.mu.Lock()
	_, hasA := c.index["a"]
	_, hasB := c.index["b"]
	c.mu.Unlock()
	if hasA {
		t.Error("least-recently-used entry a should have been evicted")
	}
	if !hasB {
		t.Error("most-recently-used entry b should remain")
	}
}

func TestDecodeLogRecords(t *testing.T) {
	want := logEntriesAt(10, 20, 30)
	entries, cacheable, err := decodeLogRecords(encodeLogRecords(t, want))
	if err != nil || !cacheable {
		t.Fatalf("clean chunk: err=%v cacheable=%v", err, cacheable)
	}
	if len(entries) != 3 || entries[0].TsNs != 10 || entries[2].TsNs != 30 {
		t.Fatalf("decoded %v", entries)
	}

	if entries, cacheable, err := decodeLogRecords(nil); err != nil || !cacheable || len(entries) != 0 {
		t.Fatalf("empty chunk: entries=%v cacheable=%v err=%v", entries, cacheable, err)
	}
}

func TestDecodeLogRecordsIncomplete(t *testing.T) {
	full := encodeLogRecords(t, logEntriesAt(10, 20))
	rec1 := len(encodeLogRecords(t, logEntriesAt(10)))

	// stops mid-record: the second record's bytes are cut short
	entries, cacheable, err := decodeLogRecords(full[:len(full)-2])
	if err != errLogChunkIncomplete || cacheable {
		t.Fatalf("mid-record: err=%v cacheable=%v", err, cacheable)
	}
	if len(entries) != 1 || entries[0].TsNs != 10 {
		t.Fatalf("mid-record prefix: %v", entries)
	}

	// stops mid-size-prefix
	if _, _, err := decodeLogRecords(full[:rec1+2]); err != errLogChunkIncomplete {
		t.Fatalf("mid-prefix: err=%v", err)
	}

	// garbage size prefix, e.g. a chunk that starts mid-record
	garbage := make([]byte, 8)
	util.Uint32toBytes(garbage, uint32(maxLogEntrySize)+1)
	if _, _, err := decodeLogRecords(garbage); err != errLogChunkIncomplete {
		t.Fatalf("garbage size: err=%v", err)
	}
}

func logFileEntry(chunks ...*filer_pb.FileChunk) *Entry {
	return &Entry{
		FullPath: "/topics/.system/log/2026-06-10/00-01.f1",
		Attr:     Attr{Mode: 0644},
		Chunks:   chunks,
	}
}

func stubChunkLoader(t *testing.T, chunkData map[string][]byte) *int32 {
	t.Helper()
	var loads int32
	prev := loadLogFileEntriesFn
	loadLogFileEntriesFn = func(_ *wdclient.MasterClient, chunk *filer_pb.FileChunk) ([]*filer_pb.LogEntry, bool, error) {
		atomic.AddInt32(&loads, 1)
		data, ok := chunkData[chunk.GetFileIdString()]
		if !ok {
			t.Errorf("unexpected load of chunk %s", chunk.GetFileIdString())
			return nil, false, fmt.Errorf("unexpected chunk %s", chunk.GetFileIdString())
		}
		return decodeLogRecords(data)
	}
	t.Cleanup(func() { loadLogFileEntriesFn = prev })
	return &loads
}

func collectTs(t *testing.T, iter *LogFileIterator) (got []int64) {
	t.Helper()
	for {
		e, err := iter.getNext()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, e.TsNs)
	}
}

func TestLogFileIteratorChunkedFiltering(t *testing.T) {
	stubChunkLoader(t, map[string][]byte{
		"3,01": encodeLogRecords(t, logEntriesAt(10, 20)),
		"3,02": encodeLogRecords(t, logEntriesAt(30, 40)),
	})
	entry := logFileEntry(
		&filer_pb.FileChunk{FileId: "3,01", Offset: 0, Size: 100, ModifiedTsNs: 25},
		&filer_pb.FileChunk{FileId: "3,02", Offset: 100, Size: 100, ModifiedTsNs: 45},
	)

	iter := newLogFileIterator(nil, newPersistedLogCache(persistedLogCacheMaxBytes), entry, 15, 35)
	got := collectTs(t, iter)
	want := []int64{20, 30} // 10 filtered by startTsNs, 40 cut off by stopTsNs
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("chunked filtering: got %v, want %v", got, want)
	}
}

func TestLogFileIteratorSkipsColdChunks(t *testing.T) {
	// chunk "3,01" is not in the stub map: loading it would fail the test. Its
	// flush time (ModifiedTsNs) is a full flush interval before startTsNs, so it
	// must be skipped without being read.
	start := int64(20) + int64(LogFlushInterval)
	stubChunkLoader(t, map[string][]byte{
		"3,02": encodeLogRecords(t, logEntriesAt(start+10, start+20)),
	})
	entry := logFileEntry(
		&filer_pb.FileChunk{FileId: "3,01", Offset: 0, Size: 100, ModifiedTsNs: 20},
		&filer_pb.FileChunk{FileId: "3,02", Offset: 100, Size: 100, ModifiedTsNs: start + 25},
	)

	iter := newLogFileIterator(nil, newPersistedLogCache(persistedLogCacheMaxBytes), entry, start, 0)
	got := collectTs(t, iter)
	if fmt.Sprint(got) != fmt.Sprint([]int64{start + 10, start + 20}) {
		t.Fatalf("cold-chunk skip: got %v", got)
	}
}

func TestLogFileIteratorSharesDecodedChunks(t *testing.T) {
	loads := stubChunkLoader(t, map[string][]byte{
		"3,01": encodeLogRecords(t, logEntriesAt(10, 20)),
	})
	entry := logFileEntry(&filer_pb.FileChunk{FileId: "3,01", Offset: 0, Size: 100, ModifiedTsNs: 25})
	cache := newPersistedLogCache(persistedLogCacheMaxBytes)

	for i := 0; i < 3; i++ { // three replays of the same chunk
		iter := newLogFileIterator(nil, cache, entry, 0, 0)
		if got := collectTs(t, iter); fmt.Sprint(got) != fmt.Sprint([]int64{10, 20}) {
			t.Fatalf("replay %d: got %v", i, got)
		}
	}
	if n := atomic.LoadInt32(loads); n != 1 {
		t.Fatalf("expected one shared decode across replays, got %d", n)
	}
}

func TestLogFileIteratorStreamsWhenRecordsSpanChunks(t *testing.T) {
	// One file of four records, flushed in a way that splits the third record
	// across two chunks. The first chunk decodes records 10 and 20 cleanly; the
	// second starts mid-record. The iterator must yield every record exactly
	// once by falling back to a whole-file byte stream.
	all := logEntriesAt(10, 20, 30, 40)
	full := encodeLogRecords(t, all)
	cut := len(encodeLogRecords(t, all[:2])) + 3 // 3 bytes into record 30

	stubChunkLoader(t, map[string][]byte{
		"3,01": full[:cut],
		"3,02": full[cut:],
	})
	prev := newLogFileStreamReader
	newLogFileStreamReader = func(_ *wdclient.MasterClient, _ []*filer_pb.FileChunk) io.Reader {
		return bytes.NewReader(full)
	}
	t.Cleanup(func() { newLogFileStreamReader = prev })

	entry := logFileEntry(
		&filer_pb.FileChunk{FileId: "3,01", Offset: 0, Size: uint64(cut), ModifiedTsNs: 45},
		&filer_pb.FileChunk{FileId: "3,02", Offset: int64(cut), Size: uint64(len(full) - cut), ModifiedTsNs: 45},
	)

	iter := newLogFileIterator(nil, newPersistedLogCache(persistedLogCacheMaxBytes), entry, 0, 0)
	got := collectTs(t, iter)
	if fmt.Sprint(got) != fmt.Sprint([]int64{10, 20, 30, 40}) {
		t.Fatalf("spanning fallback: got %v", got)
	}
}

func TestLogFileIteratorStreamFallbackResumesAfterYielded(t *testing.T) {
	// The first chunk decodes standalone and its records are yielded before the
	// second chunk turns out to be incomplete. The stream fallback re-reads the
	// file from the start, so the already-yielded records must be skipped.
	all := logEntriesAt(10, 20, 30, 40)
	full := encodeLogRecords(t, all)
	chunk1 := len(encodeLogRecords(t, all[:2]))

	stubChunkLoader(t, map[string][]byte{
		"3,01": full[:chunk1],
		"3,02": full[chunk1 : len(full)-2], // truncated mid-record
	})
	prev := newLogFileStreamReader
	newLogFileStreamReader = func(_ *wdclient.MasterClient, _ []*filer_pb.FileChunk) io.Reader {
		return bytes.NewReader(full)
	}
	t.Cleanup(func() { newLogFileStreamReader = prev })

	entry := logFileEntry(
		&filer_pb.FileChunk{FileId: "3,01", Offset: 0, Size: uint64(chunk1), ModifiedTsNs: 45},
		&filer_pb.FileChunk{FileId: "3,02", Offset: int64(chunk1), Size: uint64(len(full) - chunk1), ModifiedTsNs: 45},
	)

	iter := newLogFileIterator(nil, newPersistedLogCache(persistedLogCacheMaxBytes), entry, 0, 0)
	got := collectTs(t, iter)
	if fmt.Sprint(got) != fmt.Sprint([]int64{10, 20, 30, 40}) {
		t.Fatalf("fallback resume: got %v", got)
	}
}

func TestLogFileIteratorLoadErrorPropagates(t *testing.T) {
	boom := fmt.Errorf("boom")
	prev := loadLogFileEntriesFn
	loadLogFileEntriesFn = func(_ *wdclient.MasterClient, _ *filer_pb.FileChunk) ([]*filer_pb.LogEntry, bool, error) {
		return nil, false, boom
	}
	t.Cleanup(func() { loadLogFileEntriesFn = prev })

	entry := logFileEntry(&filer_pb.FileChunk{FileId: "3,01", Offset: 0, Size: 100, ModifiedTsNs: 45})
	iter := newLogFileIterator(nil, newPersistedLogCache(persistedLogCacheMaxBytes), entry, 0, 0)
	if _, err := iter.getNext(); err == nil || err.Error() != "boom" {
		t.Fatalf("expected load error surfaced, got %v", err)
	}
}

func TestPersistedLogCacheIdleEviction(t *testing.T) {
	c := newPersistedLogCache(persistedLogCacheMaxBytes)
	load := func() ([]*filer_pb.LogEntry, bool, error) {
		return logEntriesAt(1), true, nil
	}
	if _, err := c.getOrLoad("idle", 1, load); err != nil {
		t.Fatal(err)
	}
	if _, err := c.getOrLoad("hot", 1, load); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	c.index["idle"].Value.(*logCacheItem).lastUsed = time.Now().Add(-2 * persistedLogCacheIdleTTL)
	c.mu.Unlock()

	c.evictIdle(time.Now().Add(-persistedLogCacheIdleTTL))

	c.mu.Lock()
	_, hasIdle := c.index["idle"]
	_, hasHot := c.index["hot"]
	bytesLeft := c.curBytes
	c.mu.Unlock()
	if hasIdle {
		t.Error("idle entry should have been evicted")
	}
	if !hasHot {
		t.Error("recently used entry should remain")
	}
	if want := estimateEntriesBytes(logEntriesAt(1)); bytesLeft != want {
		t.Errorf("curBytes=%d, want %d", bytesLeft, want)
	}
}

func TestDecodeLogRecordsRejectsImplausibleRecords(t *testing.T) {
	// zeroed region: parses as endless empty records without the size guard
	if _, _, err := decodeLogRecords(make([]byte, 16)); err != errLogChunkIncomplete {
		t.Fatalf("zeroed data: err=%v", err)
	}

	// the writer never produces a zero timestamp
	if _, _, err := decodeLogRecords(encodeLogRecords(t, logEntriesAt(0))); err != errLogChunkIncomplete {
		t.Fatalf("zero ts: err=%v", err)
	}

	// timestamps are strictly increasing within one flushed buffer
	entries, cacheable, err := decodeLogRecords(encodeLogRecords(t, logEntriesAt(10, 10)))
	if err != errLogChunkIncomplete || cacheable {
		t.Fatalf("non-increasing ts: err=%v cacheable=%v", err, cacheable)
	}
	if len(entries) != 1 || entries[0].TsNs != 10 {
		t.Fatalf("non-increasing prefix: %v", entries)
	}
}

func TestPersistedLogCacheLoadLargerThanBudget(t *testing.T) {
	// a load weight above the semaphore size could never be acquired; it must
	// clamp and still run
	c := newPersistedLogCache(persistedLogCacheMaxBytes)
	e, err := c.getOrLoad("3,01", persistedLogCacheLoadBudget*4, func() ([]*filer_pb.LogEntry, bool, error) {
		return logEntriesAt(1), true, nil
	})
	if err != nil || len(e) != 1 {
		t.Fatalf("oversized load: err=%v len=%d", err, len(e))
	}
}
