//go:build foundationdb
// +build foundationdb

package foundationdb

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/foundationdb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestFoundationDBStore_ConcurrentInserts(t *testing.T) {
	store := createTestStore(t)
	defer store.Shutdown()

	ctx := context.Background()
	numGoroutines := 10
	entriesPerGoroutine := 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*entriesPerGoroutine)

	// Launch concurrent insert operations
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < entriesPerGoroutine; i++ {
				entry := &filer.Entry{
					FullPath: util.NewFullPath("/concurrent", fmt.Sprintf("g%d_file%d.txt", goroutineID, i)),
					Attr: filer.Attr{
						Mode:  0644,
						Uid:   uint32(goroutineID),
						Gid:   1000,
						Mtime: time.Now(),
					},
				}

				err := store.InsertEntry(ctx, entry)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d, entry %d: %v", goroutineID, i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent insert error: %v", err)
	}

	// Verify all entries were inserted
	expectedTotal := numGoroutines * entriesPerGoroutine
	actualCount := 0

	_, err := store.ListDirectoryEntries(ctx, "/concurrent", "", true, 10000, func(entry *filer.Entry) bool {
		actualCount++
		return true
	})
	if err != nil {
		t.Fatalf("ListDirectoryEntries failed: %v", err)
	}

	if actualCount != expectedTotal {
		t.Errorf("Expected %d entries, found %d", expectedTotal, actualCount)
	}
}

func TestFoundationDBStore_ConcurrentReadsAndWrites(t *testing.T) {
	store := createTestStore(t)
	defer store.Shutdown()

	ctx := context.Background()
	numReaders := 5
	numWriters := 5
	operationsPerGoroutine := 50
	testFile := "/concurrent/rw_test_file.txt"

	// Insert initial file
	initialEntry := &filer.Entry{
		FullPath: testFile,
		Attr: filer.Attr{
			Mode:  0644,
			Uid:   1000,
			Gid:   1000,
			Mtime: time.Now(),
		},
	}
	err := store.InsertEntry(ctx, initialEntry)
	if err != nil {
		t.Fatalf("Initial InsertEntry failed: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, (numReaders+numWriters)*operationsPerGoroutine)

	// Launch reader goroutines
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for i := 0; i < operationsPerGoroutine; i++ {
				_, err := store.FindEntry(ctx, testFile)
				if err != nil {
					errors <- fmt.Errorf("reader %d, operation %d: %v", readerID, i, err)
					return
				}

				// Small delay to allow interleaving with writes
				time.Sleep(1 * time.Millisecond)
			}
		}(r)
	}

	// Launch writer goroutines
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for i := 0; i < operationsPerGoroutine; i++ {
				entry := &filer.Entry{
					FullPath: testFile,
					Attr: filer.Attr{
						Mode:  0644,
						Uid:   uint32(writerID + 1000),
						Gid:   uint32(i),
						Mtime: time.Now(),
					},
				}

				err := store.UpdateEntry(ctx, entry)
				if err != nil {
					errors <- fmt.Errorf("writer %d, operation %d: %v", writerID, i, err)
					return
				}

				// Small delay to allow interleaving with reads
				time.Sleep(1 * time.Millisecond)
			}
		}(w)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent read/write error: %v", err)
	}

	// Verify final state
	finalEntry, err := store.FindEntry(ctx, testFile)
	if err != nil {
		t.Fatalf("Final FindEntry failed: %v", err)
	}

	if finalEntry.FullPath != testFile {
		t.Errorf("Expected final path %s, got %s", testFile, finalEntry.FullPath)
	}
}

func TestFoundationDBStore_ConcurrentTransactions(t *testing.T) {
	store := createTestStore(t)
	defer store.Shutdown()

	ctx := context.Background()
	numTransactions := 5
	entriesPerTransaction := 10

	var wg sync.WaitGroup
	errors := make(chan error, numTransactions)
	successfulTx := make(chan int, numTransactions)

	// Launch concurrent transactions
	for tx := 0; tx < numTransactions; tx++ {
		wg.Add(1)
		go func(txID int) {
			defer wg.Done()

			// Note: FoundationDB has optimistic concurrency control
			// Some transactions may need to retry due to conflicts
			maxRetries := 3
			for attempt := 0; attempt < maxRetries; attempt++ {
				txCtx, err := store.BeginTransaction(ctx)
				if err != nil {
					if attempt == maxRetries-1 {
						errors <- fmt.Errorf("tx %d: failed to begin after %d attempts: %v", txID, maxRetries, err)
					}
					time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
					continue
				}

				// Insert multiple entries in transaction
				success := true
				for i := 0; i < entriesPerTransaction; i++ {
					entry := &filer.Entry{
						FullPath: util.NewFullPath("/transactions", fmt.Sprintf("tx%d_file%d.txt", txID, i)),
						Attr: filer.Attr{
							Mode:  0644,
							Uid:   uint32(txID),
							Gid:   uint32(i),
							Mtime: time.Now(),
						},
					}

					err = store.InsertEntry(txCtx, entry)
					if err != nil {
						errors <- fmt.Errorf("tx %d, entry %d: insert failed: %v", txID, i, err)
						store.RollbackTransaction(txCtx)
						success = false
						break
					}
				}

				if success {
					err = store.CommitTransaction(txCtx)
					if err != nil {
						if attempt == maxRetries-1 {
							errors <- fmt.Errorf("tx %d: commit failed after %d attempts: %v", txID, maxRetries, err)
						}
						time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
						continue
					}
					successfulTx <- txID
					return
				}
			}
		}(tx)
	}

	wg.Wait()
	close(errors)
	close(successfulTx)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent transaction error: %v", err)
	}

	// Count successful transactions
	successCount := 0
	successfulTxIDs := make([]int, 0)
	for txID := range successfulTx {
		successCount++
		successfulTxIDs = append(successfulTxIDs, txID)
	}

	t.Logf("Successful transactions: %d/%d (IDs: %v)", successCount, numTransactions, successfulTxIDs)

	// Verify entries from successful transactions
	totalExpectedEntries := successCount * entriesPerTransaction
	actualCount := 0

	_, err := store.ListDirectoryEntries(ctx, "/transactions", "", true, 10000, func(entry *filer.Entry) bool {
		actualCount++
		return true
	})
	if err != nil {
		t.Fatalf("ListDirectoryEntries failed: %v", err)
	}

	if actualCount != totalExpectedEntries {
		t.Errorf("Expected %d entries from successful transactions, found %d", totalExpectedEntries, actualCount)
	}
}

func TestFoundationDBStore_ConcurrentDirectoryOperations(t *testing.T) {
	store := createTestStore(t)
	defer store.Shutdown()

	ctx := context.Background()
	numWorkers := 10
	directoriesPerWorker := 20
	filesPerDirectory := 5

	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*directoriesPerWorker*filesPerDirectory)

	// Launch workers that create directories with files
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for d := 0; d < directoriesPerWorker; d++ {
				dirPath := fmt.Sprintf("/worker%d/dir%d", workerID, d)

				// Create files in directory
				for f := 0; f < filesPerDirectory; f++ {
					entry := &filer.Entry{
						FullPath: util.NewFullPath(dirPath, fmt.Sprintf("file%d.txt", f)),
						Attr: filer.Attr{
							Mode:  0644,
							Uid:   uint32(workerID),
							Gid:   uint32(d),
							Mtime: time.Now(),
						},
					}

					err := store.InsertEntry(ctx, entry)
					if err != nil {
						errors <- fmt.Errorf("worker %d, dir %d, file %d: %v", workerID, d, f, err)
						return
					}
				}
			}
		}(w)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent directory operation error: %v", err)
	}

	// Verify directory structure
	for w := 0; w < numWorkers; w++ {
		for d := 0; d < directoriesPerWorker; d++ {
			dirPath := fmt.Sprintf("/worker%d/dir%d", w, d)

			fileCount := 0
			_, err := store.ListDirectoryEntries(ctx, dirPath, "", true, 1000, func(entry *filer.Entry) bool {
				fileCount++
				return true
			})
			if err != nil {
				t.Errorf("ListDirectoryEntries failed for %s: %v", dirPath, err)
				continue
			}

			if fileCount != filesPerDirectory {
				t.Errorf("Expected %d files in %s, found %d", filesPerDirectory, dirPath, fileCount)
			}
		}
	}
}

func TestFoundationDBStore_ConcurrentKVOperations(t *testing.T) {
	store := createTestStore(t)
	defer store.Shutdown()

	ctx := context.Background()
	numWorkers := 8
	operationsPerWorker := 100

	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*operationsPerWorker)

	// Launch workers performing KV operations
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < operationsPerWorker; i++ {
				key := []byte(fmt.Sprintf("worker%d_key%d", workerID, i))
				value := []byte(fmt.Sprintf("worker%d_value%d_timestamp%d", workerID, i, time.Now().UnixNano()))

				// Put operation
				err := store.KvPut(ctx, key, value)
				if err != nil {
					errors <- fmt.Errorf("worker %d, operation %d: KvPut failed: %v", workerID, i, err)
					continue
				}

				// Get operation
				retrievedValue, err := store.KvGet(ctx, key)
				if err != nil {
					errors <- fmt.Errorf("worker %d, operation %d: KvGet failed: %v", workerID, i, err)
					continue
				}

				if string(retrievedValue) != string(value) {
					errors <- fmt.Errorf("worker %d, operation %d: value mismatch", workerID, i)
					continue
				}

				// Delete operation (for some keys)
				if i%5 == 0 {
					err = store.KvDelete(ctx, key)
					if err != nil {
						errors <- fmt.Errorf("worker %d, operation %d: KvDelete failed: %v", workerID, i, err)
					}
				}
			}
		}(w)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Errorf("Concurrent KV operation error: %v", err)
		errorCount++
	}

	if errorCount > 0 {
		t.Errorf("Total errors in concurrent KV operations: %d", errorCount)
	}
}

func createTestStore(t *testing.T) *foundationdb.FoundationDBStore {
	// Skip test if FoundationDB cluster file doesn't exist
	clusterFile := os.Getenv("FDB_CLUSTER_FILE")
	if clusterFile == "" {
		clusterFile = "/var/fdb/config/fdb.cluster"
	}

	if _, err := os.Stat(clusterFile); os.IsNotExist(err) {
		t.Skip("FoundationDB cluster file not found, skipping test")
	}

	config := util.GetViper()
	config.Set("foundationdb.cluster_file", clusterFile)
	config.Set("foundationdb.api_version", 740)
	config.Set("foundationdb.timeout", "10s")
	config.Set("foundationdb.max_retry_delay", "2s")
	config.Set("foundationdb.directory_prefix", fmt.Sprintf("seaweedfs_concurrent_test_%d", time.Now().UnixNano()))

	store := &foundationdb.FoundationDBStore{}
	err := store.Initialize(config, "foundationdb.")
	if err != nil {
		t.Fatalf("Failed to initialize FoundationDB store: %v", err)
	}

	return store
}
