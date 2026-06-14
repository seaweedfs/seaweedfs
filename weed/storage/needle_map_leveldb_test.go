package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A mid-commit crash can leave an old .ldb (with a high watermark) beside a
// freshly swapped, shorter .idx. generateLevelDbFile must distrust the stale
// watermark and rebuild from offset 0 instead of walking past EOF and
// replaying zero entries, which would leave the needle map empty and make
// every live needle a phantom 404.
func TestGenerateLevelDbFileStaleWatermarkRebuilds(t *testing.T) {
	dir := t.TempDir()

	idxPath := filepath.Join(dir, "1.idx")
	idxFile, err := os.OpenFile(idxPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("create idx: %v", err)
	}
	defer idxFile.Close()

	// A short .idx: three live needles.
	const liveCount = 3
	for i := uint64(1); i <= liveCount; i++ {
		entry := needle_map.ToBytes(types.Uint64ToNeedleId(i), types.ToOffset(int64(i*1024)), types.Size(512))
		if _, err := idxFile.Write(entry); err != nil {
			t.Fatalf("write idx entry: %v", err)
		}
	}
	if err := idxFile.Sync(); err != nil {
		t.Fatalf("sync idx: %v", err)
	}

	// Seed an .ldb whose stored watermark sits far past the short .idx, mimicking
	// the leftover db from a pre-crash, much larger index.
	dbPath := filepath.Join(dir, "1.ldb")
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		t.Fatalf("open ldb: %v", err)
	}
	if err := setWatermark(db, watermarkBatchSize); err != nil {
		t.Fatalf("set stale watermark: %v", err)
	}
	db.Close()

	if err := generateLevelDbFile(dbPath, idxFile); err != nil {
		t.Fatalf("generateLevelDbFile: %v", err)
	}

	db, err = leveldb.OpenFile(dbPath, nil)
	if err != nil {
		t.Fatalf("reopen ldb: %v", err)
	}
	defer db.Close()
	for i := uint64(1); i <= liveCount; i++ {
		keyBytes := make([]byte, types.NeedleIdSize)
		types.NeedleIdToBytes(keyBytes, types.Uint64ToNeedleId(i))
		if _, err := db.Get(keyBytes, nil); err != nil {
			t.Fatalf("needle %d missing after rebuild (stale watermark poisoned the map): %v", i, err)
		}
	}
}

func TestLevelDbNeedleMap_Concurrency(t *testing.T) {
	dir, err := os.MkdirTemp("", "test_leveldb_concurrency")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	prefix := "test"
	indexFile, err := os.Create(filepath.Join(dir, prefix+".idx"))
	if err != nil {
		t.Fatalf("create index file: %v", err)
	}
	dbFileName := filepath.Join(dir, prefix+".ldb")

	// Create and initialize map
	m, err := NewLevelDbNeedleMap(dbFileName, indexFile, nil, 1, needle.GetCurrentVersion())
	if err != nil {
		t.Fatalf("NewLevelDbNeedleMap: %v", err)
	}
	defer m.Close()

	// Pre-populate some data
	key := types.NeedleId(1)
	if err := m.Put(key, types.ToOffset(100), types.Size(200)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Force unload to start from nil state
	if err := unloadLdb(m); err != nil {
		t.Fatalf("unloadLdb: %v", err)
	}

	var wg sync.WaitGroup
	startCh := make(chan struct{})
	errCh := make(chan error, 100)

	// Spawn multiple goroutines to trigger the race
	// Multiple readers will see m.db == nil and try to reload concurrently
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-startCh

			// Try multiple times to increase chance of collision
			for j := 0; j < 2; j++ {
				_, ok := m.Get(key)
				if !ok {
					// Get failed, possibly due to race in reload.
					// But we also put data concurrently, so maybe it's missing if deleted?
					// In this test, we only Put, never Delete. So Key 1 should be there.
					// However, if DB reload fails, Get returns false!
					errCh <- fmt.Errorf("routine %d iter %d: Get returned false", id, j)
				}

				// Also try Put concurrently
				err := m.Put(types.NeedleId(2+id), types.ToOffset(100), types.Size(200))
				if err != nil {
					errCh <- fmt.Errorf("routine %d iter %d: Put failed: %v", id, j, err)
				}

				// Manually unload occasionally to reset the state
				if j%2 == 0 {
					// This might fail if locked, but that's fine
					unloadLdb(m)
				}
			}
		}(i)
	}

	close(startCh)
	wg.Wait()
	close(errCh)

	for e := range errCh {
		t.Errorf("Error encountered: %v", e)
	}
}
