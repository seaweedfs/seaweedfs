package component

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// These tests verify that multi-block writes (4MB from mkfs.ext4) work
// correctly and do not cause excessive memory usage in the flusher.
//
// Bug: flusher.go allocated make([]byte, 4MB) per dirty block instead of
// per unique WAL entry. A 4MB write (1024 blocks) caused 1024 × 4MB = 4GB.
// Fix: deduplicate WAL reads by WalOffset.

func createLargeWriteVol(t *testing.T, volSize, walSize uint64) *blockvol.BlockVol {
	t.Helper()
	path := t.TempDir() + "/test.blk"
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: volSize,
		BlockSize:  4096,
		WALSize:    walSize,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	t.Cleanup(func() { vol.Close() })
	return vol
}

// heapMB returns current heap usage in MB as int64 (safe for subtraction).
func heapMB() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc) / (1024 * 1024)
}

// Test 1: Single 4MB write — does it succeed at all?
func TestLargeWrite_Single4MB(t *testing.T) {
	vol := createLargeWriteVol(t, 64*1024*1024, 64*1024*1024)

	data := make([]byte, 4*1024*1024)
	for i := range data {
		data[i] = 0xAA
	}

	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("4MB WriteLBA failed: %v", err)
	}

	readBack, err := vol.ReadLBA(0, 4*1024*1024)
	if err != nil {
		t.Fatalf("4MB ReadLBA failed: %v", err)
	}
	if readBack[0] != 0xAA || readBack[4*1024*1024-1] != 0xAA {
		t.Fatal("read data mismatch")
	}
	t.Log("single 4MB write+read: OK")
}

// Test 2: Sequential 4MB writes — simulates mkfs journal creation.
func TestLargeWrite_Sequential4MB_MkfsSim(t *testing.T) {
	vol := createLargeWriteVol(t, 128*1024*1024, 64*1024*1024)

	data := make([]byte, 4*1024*1024)
	for i := range data {
		data[i] = byte(i & 0xFF)
	}

	before := heapMB()
	t.Logf("heap before: %d MB", before)

	start := time.Now()
	const writeCount = 20

	for i := range writeCount {
		lba := uint64(i) * 1024
		if err := vol.WriteLBA(lba, data); err != nil {
			t.Fatalf("write %d at LBA %d failed after %s: %v", i, lba, time.Since(start), err)
		}
		if i > 0 && i%5 == 0 {
			t.Logf("  write %d/%d: elapsed=%s heap=%dMB", i, writeCount, time.Since(start), heapMB())
		}
	}

	after := heapMB()
	delta := after - before
	t.Logf("heap after: %d MB (delta=%d MB)", after, delta)
	t.Logf("%d × 4MB writes in %s (%.1f MB/s)", writeCount, time.Since(start),
		float64(writeCount*4)/time.Since(start).Seconds())

	if delta > 500 {
		t.Errorf("MEMORY BLOWUP: heap grew by %d MB for %d MB of writes", delta, writeCount*4)
	}
}

// Test 3: Concurrent 4MB writes — worst case for WAL admission.
func TestLargeWrite_Concurrent4MB(t *testing.T) {
	vol := createLargeWriteVol(t, 512*1024*1024, 64*1024*1024)

	data := make([]byte, 4*1024*1024)
	for i := range data {
		data[i] = byte(i & 0xFF)
	}

	before := heapMB()
	t.Logf("heap before concurrent: %d MB", before)

	const writers = 16
	const writesPerWriter = 5
	var wg sync.WaitGroup
	errs := make([]error, writers)

	start := time.Now()
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range writesPerWriter {
				lba := uint64(w*writesPerWriter+i) * 1024
				if err := vol.WriteLBA(lba, data); err != nil {
					errs[w] = fmt.Errorf("writer %d write %d: %v", w, i, err)
					return
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("TIMEOUT: concurrent 4MB writes hung for 60s")
	}

	for _, err := range errs {
		if err != nil {
			t.Error(err)
		}
	}

	after := heapMB()
	delta := after - before
	t.Logf("heap after concurrent: %d MB (delta=%d MB)", after, delta)
	t.Logf("%d writers × %d × 4MB = %d MB in %s",
		writers, writesPerWriter, writers*writesPerWriter*4, time.Since(start))

	if delta > 1000 {
		t.Errorf("MEMORY BLOWUP: heap grew by %d MB", delta)
	}
}

// Test 4: WAL size vs entry size — 4MB entry in 1MB WAL should fail, not hang.
func TestLargeWrite_EntryLargerThanWAL(t *testing.T) {
	vol := createLargeWriteVol(t, 64*1024*1024, 1*1024*1024)

	err := vol.WriteLBA(0, make([]byte, 4*1024*1024))
	if err == nil {
		t.Log("4MB write succeeded in 1MB WAL — entry was split or WAL grew")
	} else {
		t.Logf("4MB write in 1MB WAL returned error (expected): %v", err)
	}
}

// Test 5: Mixed 4K+1M+4M writes — simulates real mkfs pattern.
func TestLargeWrite_MixedSizes(t *testing.T) {
	vol := createLargeWriteVol(t, 128*1024*1024, 64*1024*1024)

	small := make([]byte, 4096)
	large := make([]byte, 1<<20)
	huge := make([]byte, 4<<20)
	for i := range small {
		small[i] = 0x11
	}
	for i := range large {
		large[i] = 0x22
	}
	for i := range huge {
		huge[i] = 0x33
	}

	start := time.Now()
	for i := range 100 {
		if err := vol.WriteLBA(uint64(i), small); err != nil {
			t.Fatalf("small write %d: %v", i, err)
		}
	}
	t.Logf("phase 1: 100 × 4KB in %s", time.Since(start))

	phase2 := time.Now()
	for i := range 10 {
		if err := vol.WriteLBA(uint64(1000+i*256), large); err != nil {
			t.Fatalf("large write %d: %v", i, err)
		}
	}
	t.Logf("phase 2: 10 × 1MB in %s", time.Since(phase2))

	phase3 := time.Now()
	for i := range 10 {
		if err := vol.WriteLBA(uint64(10000+i*1024), huge); err != nil {
			t.Fatalf("huge write %d: %v (elapsed %s)", i, err, time.Since(phase3))
		}
	}
	t.Logf("phase 3: 10 × 4MB in %s", time.Since(phase3))
	t.Logf("total: %s, final heap: %d MB", time.Since(start), heapMB())
}

// Test 6: Production-sized volume (2GB) — memory stays bounded.
func TestLargeWrite_ProductionVolumeMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: creates 2GB file on disk")
	}

	runtime.GC()
	before := heapMB()
	t.Logf("heap before create: %d MB", before)

	vol := createLargeWriteVol(t, 2*1024*1024*1024, 64*1024*1024)

	runtime.GC()
	afterCreate := heapMB()
	t.Logf("heap after create: %d MB (delta=%d MB)", afterCreate, afterCreate-before)

	small := make([]byte, 4096)
	for i := range 1000 {
		if err := vol.WriteLBA(uint64(i)*100, small); err != nil {
			t.Fatalf("small write %d: %v", i, err)
		}
	}

	runtime.GC()
	afterSmall := heapMB()
	t.Logf("heap after 1000 × 4KB: %d MB (delta=%d MB)", afterSmall, afterSmall-before)

	large := make([]byte, 4*1024*1024)
	for i := range 10 {
		if err := vol.WriteLBA(uint64(200000+i*1024), large); err != nil {
			t.Logf("large write %d failed: %v", i, err)
			break
		}
	}

	runtime.GC()
	afterLarge := heapMB()
	delta := afterLarge - before
	t.Logf("heap after 4MB writes: %d MB (delta=%d MB)", afterLarge, delta)

	if delta > 500 {
		t.Errorf("EXCESSIVE MEMORY: heap grew %d MB for 2GB volume", delta)
	}
}

// Test 7: Sustained 4MB writes for 30s — flusher keeps up.
func TestLargeWrite_FlusherThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("30s sustained write test")
	}
	vol := createLargeWriteVol(t, 256*1024*1024, 64*1024*1024)

	data := make([]byte, 4*1024*1024)
	start := time.Now()
	deadline := start.Add(30 * time.Second)

	writes := 0
	for time.Now().Before(deadline) {
		lba := uint64(writes%50) * 1024
		if err := vol.WriteLBA(lba, data); err != nil {
			t.Fatalf("write %d failed after %s: %v", writes, time.Since(start), err)
		}
		writes++
	}

	t.Logf("flusher throughput: %d × 4MB in %s (%.1f writes/s, %.1f MB/s)",
		writes, time.Since(start), float64(writes)/time.Since(start).Seconds(),
		float64(writes*4)/time.Since(start).Seconds())
	t.Logf("final heap: %d MB", heapMB())
}
