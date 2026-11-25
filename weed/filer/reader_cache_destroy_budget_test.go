package filer

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// budgetTestViews returns one whole-chunk view per file id. The views lie past
// the mock chunk cache's 1 MiB cutoff, so prefetches do not land in it: the
// mock keeps the downloader's pooled buffer rather than a copy, and that buffer
// is reused once the chunk is dropped.
func budgetTestViews(chunkSize int, fids ...string) *Interval[*ChunkView] {
	var views *Interval[*ChunkView]
	for i := len(fids) - 1; i >= 0; i-- {
		views = &Interval[*ChunkView]{Value: &ChunkView{FileId: fids[i], ViewOffset: 2<<20 + int64(i*chunkSize), ViewSize: uint64(chunkSize), ChunkSize: uint64(chunkSize)}, Next: views}
	}
	return views
}

// waitBudgetDrained waits until the budget holds nothing. A chunk dropped while
// its download still runs is destroyed once that download gives up.
func waitBudgetDrained(t *testing.T, b *ReaderCacheBudget) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		b.Lock()
		used, reserved, idle := b.used, len(b.reservations), b.idle.Len()
		b.Unlock()
		if used == 0 && reserved == 0 && idle == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("closed files still reserve %d bytes (%d reservations, %d idle)", used, reserved, idle)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestReaderCacheDestroyWhileDownloadWaitsForBudget closes a file while one of
// its downloads waits for budget that two other downloads of the file hold.
// destroy() takes every chunk out of the map and then destroys them one at a
// time. When it reaches the waiting download first, it waits for it, and that
// download can only get budget the file's own chunks give back.
//   - prefetch: the waiting download is a prefetch nobody read, which
//     destroy() cancels.
//   - pinned, holders cancelled: a read started the waiting download and gave
//     up (its process was killed), so its stream is still positioned in it.
//     Closing a ChunkGroup does not release the streams, so destroy() cannot
//     cancel it. The holders are prefetches that destroy() cancels; they end
//     as failed downloads, which are never evictable, so only their
//     reservations can make room.
//   - pinned, holders pinned: the holders were also started by reads that gave
//     up, so they are not cancelled. They finish after the close, as chunks a
//     stream is positioned in that are no longer in the map, and eviction has
//     to free them.
func TestReaderCacheDestroyWhileDownloadWaitsForBudget(t *testing.T) {
	const chunkSize = 3 << 10 // pooled as 4 KiB, so the budget holds two
	for _, tc := range []struct {
		name          string
		pinned        bool // the waiting download was started by a read that gave up
		holdersPinned bool // so were the two downloads holding the budget
	}{
		{name: "prefetch"},
		{name: "pinned, holders cancelled", pinned: true},
		{name: "pinned, holders pinned", pinned: true, holdersPinned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for round := 0; round < 30; round++ {
				budget := NewReaderCacheBudget(8 << 10)
				rc := NewReaderCache(256, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) { return []string{"unused"}, nil }, nil, budget)
				gate := make(chan struct{})
				started := make(chan string, 3)
				rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, fileId string, _ util_http.RefreshUrlsFunc) (int, error) {
					started <- fileId
					<-gate
					buffer[0] = 42
					return len(buffer), nil
				}
				// giveUp starts reading fid through a stream of its own and gives
				// up once wait has passed; the stream stays positioned in the chunk
				giveUp := func(fid string, wait func()) {
					ctx, cancel := context.WithCancel(context.Background())
					readDone := make(chan error, 1)
					go func() {
						_, err := rc.readChunkAt(ctx, &chunkStream{}, make([]byte, 1), fid, nil, false, 0, chunkSize, false)
						readDone <- err
					}()
					wait()
					cancel()
					select {
					case <-readDone:
					case <-time.After(5 * time.Second):
						t.Fatalf("round %d: the cancelled read of %s did not return", round, fid)
					}
				}
				holdersStarted := func() {
					select {
					case <-started:
					case <-time.After(5 * time.Second):
						t.Fatalf("round %d: a download did not start", round)
					}
				}

				switch {
				case tc.holdersPinned:
					giveUp("a", holdersStarted)
					giveUp("b", holdersStarted)
				case tc.pinned:
					rc.MaybeCache(budgetTestViews(chunkSize, "a", "b"), 2)
					holdersStarted()
					holdersStarted()
				default:
					// any two of the three take the budget and the third waits
					rc.MaybeCache(budgetTestViews(chunkSize, "a", "b", "c"), 3)
					holdersStarted()
					holdersStarted()
				}
				if tc.pinned {
					giveUp("c", func() { time.Sleep(20 * time.Millisecond) })
				}
				select {
				case fid := <-started:
					t.Fatalf("round %d: %s downloaded beyond the budget", round, fid)
				case <-time.After(20 * time.Millisecond):
				}

				// the file is closed while the third download waits, then the two finish
				destroyed := make(chan struct{})
				go func() {
					rc.destroy()
					close(destroyed)
				}()
				time.Sleep(10 * time.Millisecond)
				close(gate)

				select {
				case <-destroyed:
				case <-time.After(3 * time.Second):
					t.Fatalf("round %d: destroy is stuck: the waiting download cannot get budget the closed file's own chunks hold", round)
				}
				budget.Lock()
				used := budget.used
				budget.Unlock()
				if used != 0 {
					t.Fatalf("round %d: %d bytes still reserved after destroy", round, used)
				}
			}
		})
	}
}

// TestReaderCacheBudgetStress runs reads (some given up midway), prefetches,
// failing downloads and file close/reopen concurrently on several files that
// share a budget of four chunks. Run it with -race. Every read that returns
// data must return the chunk's bytes, nothing may hang, and once every file is
// closed no budget may stay reserved.
func TestReaderCacheBudgetStress(t *testing.T) {
	const (
		chunkSize = 3 << 10
		chunks    = 12
		files     = 4
		workers   = 16
	)
	duration := 2 * time.Second
	if testing.Short() {
		duration = 300 * time.Millisecond
	}
	budget := NewReaderCacheBudget(16 << 10)
	lookup := func(context.Context, string) ([]string, error) { return []string{"unused"}, nil }
	content := func(fid string) byte { return fid[1] }
	fetch := func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, fileId string, _ util_http.RefreshUrlsFunc) (int, error) {
		time.Sleep(time.Duration(rand.Intn(1500)) * time.Microsecond)
		if rand.Intn(20) == 0 {
			return 0, errors.New("injected fetch failure")
		}
		for i := range buffer {
			buffer[i] = content(fileId)
		}
		return len(buffer), nil
	}
	fids := make([]string, chunks)
	for i := range fids {
		fids[i] = string([]byte{'f', byte('A' + i)})
	}

	var mu sync.Mutex
	var all []*ReaderCache
	open := func() *ReaderCache {
		rc := NewReaderCache(4, newMockChunkCacheForReaderCache(), lookup, nil, budget)
		rc.fetchChunkDataFn = fetch
		mu.Lock()
		all = append(all, rc)
		mu.Unlock()
		return rc
	}
	var slots [files]atomic.Pointer[ReaderCache]
	for i := range slots {
		slots[i].Store(open())
	}

	stop := make(chan struct{})
	var wrongData, reads atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			streams := map[*ReaderCache]*chunkStream{}
			for {
				select {
				case <-stop:
					return
				default:
				}
				rc := slots[r.Intn(files)].Load()
				first := r.Intn(chunks - 2)
				if r.Intn(4) == 0 {
					rc.MaybeCache(budgetTestViews(chunkSize, fids[first:first+3]...), 1+r.Intn(3))
					continue
				}
				stream := streams[rc]
				if stream == nil {
					stream = &chunkStream{}
					streams[rc] = stream
				}
				ctx, cancel := context.WithCancel(context.Background())
				if r.Intn(5) == 0 {
					time.AfterFunc(time.Duration(r.Intn(800))*time.Microsecond, cancel)
				}
				offset := int64(r.Intn(chunkSize))
				buf := make([]byte, 1+r.Intn(chunkSize))
				n, err := rc.readChunkAt(ctx, stream, buf, fids[first], nil, false, offset, chunkSize, false)
				cancel()
				if err == nil && n > 0 {
					reads.Add(1)
					for _, b := range buf[:n] {
						if b != content(fids[first]) {
							wrongData.Add(1)
							break
						}
					}
				}
			}
		}(int64(w))
	}
	// close and reopen files while the workers read them
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(99))
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(1+r.Intn(4)) * time.Millisecond):
			}
			slot := &slots[r.Intn(files)]
			old := slot.Swap(open())
			old.destroy()
		}
	}()

	time.Sleep(duration)
	close(stop)
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		mu.Lock()
		closing := append([]*ReaderCache(nil), all...)
		mu.Unlock()
		for _, rc := range closing {
			rc.destroy()
		}
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		t.Fatal("reads, prefetches and closes are stuck")
	}
	if wrongData.Load() != 0 {
		t.Fatalf("%d of %d reads returned bytes of another chunk", wrongData.Load(), reads.Load())
	}
	if reads.Load() == 0 {
		t.Fatal("no read returned data")
	}
	waitBudgetDrained(t, budget)
	t.Logf("%d reads with data", reads.Load())
}

// TestReaderCacheBudgetStreamsStress: streams moving between the chunks of one
// file under a budget of two chunks, with reads given up midway. A read that
// drops the chunk its stream left must not wait for that chunk's download
// while it holds its new chunk: the download can be evicting the new chunk
// for budget, waiting for that read. Run it with -race.
func TestReaderCacheBudgetStreamsStress(t *testing.T) {
	const (
		chunkSize = 3 << 10
		chunks    = 4
		workers   = 8
	)
	duration := 2 * time.Second
	if testing.Short() {
		duration = 300 * time.Millisecond
	}
	budget := NewReaderCacheBudget(8 << 10)
	rc := NewReaderCache(chunks, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) { return []string{"unused"}, nil }, nil, budget)
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, fileId string, _ util_http.RefreshUrlsFunc) (int, error) {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
		for i := range buffer {
			buffer[i] = fileId[1]
		}
		return len(buffer), nil
	}
	fids := make([]string, chunks)
	for i := range fids {
		fids[i] = string([]byte{'s', byte('A' + i)})
	}
	stop := make(chan struct{})
	var wrongData, reads atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			stream := &chunkStream{}
			defer rc.releaseStream(stream)
			for {
				select {
				case <-stop:
					return
				default:
				}
				fid := fids[r.Intn(chunks)]
				ctx, cancel := context.WithCancel(context.Background())
				if r.Intn(3) == 0 {
					time.AfterFunc(time.Duration(r.Intn(500))*time.Microsecond, cancel)
				}
				buf := make([]byte, 1+r.Intn(chunkSize))
				n, err := rc.readChunkAt(ctx, stream, buf, fid, nil, false, int64(r.Intn(chunkSize)), chunkSize, false)
				cancel()
				if err == nil && n > 0 {
					reads.Add(1)
					for _, b := range buf[:n] {
						if b != fid[1] {
							wrongData.Add(1)
							break
						}
					}
				}
			}
		}(int64(w))
	}
	time.Sleep(duration)
	close(stop)
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		rc.destroy()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		t.Fatal("streams are stuck")
	}
	if wrongData.Load() != 0 {
		t.Fatalf("%d of %d reads returned bytes of another chunk", wrongData.Load(), reads.Load())
	}
	if reads.Load() == 0 {
		t.Fatal("no read returned data")
	}
	waitBudgetDrained(t, budget)
	t.Logf("%d reads with data", reads.Load())
}
