package weed_server

// The persisted log may end before a subscriber's start position (a filer
// whose own journal is older than the position a backup client resumes from).
// With nothing on disk and every ring entry held by a peer watermark, the
// aggregated loop used to re-list and re-read the persisted log on every wake
// - about a full CPU core per such subscriber. The disk pass may only re-run
// when something it cannot miss has changed.

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type countingStore struct {
	filer.FilerStore
	logLists *atomic.Int64
}

func (s *countingStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	if strings.HasPrefix(string(dirPath), filer.SystemLogDir) {
		s.logLists.Add(1)
	}
	return s.FilerStore.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, prefix, eachEntryFunc)
}

func TestSubscribeLoop_AggregatedNoPersistedEntryAfterStart(t *testing.T) {
	h := newSubscribeHarness(t)

	lists := &atomic.Int64{}
	h.f.SetStore(&countingStore{FilerStore: h.f.GetStore(), logLists: lists})

	// Persisted log ends at T1: one flushed window, nothing after.
	h.append(h.tsAt(0, 0))
	h.append(h.tsAt(0, 1))
	h.f.LocalMetaLogBuffer.ForceFlush()
	waitForFlushedFiles(t, h, h.tsAt(0, 1))

	// Client cursor sits just past the last persisted entry.
	cursor := h.tsAt(0, 1) + int64(time.Millisecond)

	ma := h.startAggregator()

	// Aggregated ring holds only much newer entries (peer events).
	recent := time.Now().UnixNano()
	h.appendAggregated(recent)
	h.appendAggregated(recent + int64(time.Millisecond))

	// Peers' watermarks are stuck at the old log tail.
	reportPeersAt(ma, h.tsAt(0, 1), h.tsAt(0, 1))

	r := h.subscribeAggregated(cursor)

	// Warm up: the first pass plus the cursor-move re-read after the gap
	// machinery re-arms the cursor are legitimate.
	time.Sleep(150 * time.Millisecond)

	before := lists.Load()
	time.Sleep(500 * time.Millisecond)
	rate := float64(lists.Load()-before) / 0.5
	if rate > 4 {
		t.Fatalf("%.1f persisted-log listings per second while parked; the disk pass re-ran on every wake", rate)
	}

	// Peer progress through the held entries releases the read: the held
	// events are delivered without another disk pass.
	lists.Store(0)
	reportPeersAt(ma, recent+int64(time.Millisecond), h.tsAt(0, 1))
	waitForEvents(t, r, []int64{recent, recent + int64(time.Millisecond)}, 3*time.Second)
	if got := lists.Load(); got > 4 {
		t.Fatalf("%d listings while draining held entries; delivery should come from the ring", got)
	}
}
