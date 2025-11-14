//go:build foundationdb
// +build foundationdb

package foundationdb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/foundationdb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestFoundationDBStore_BasicOperations(t *testing.T) {
	store := createTestStore(t)
	defer store.Shutdown()

	ctx := context.Background()

	// Test InsertEntry
	entry := &filer.Entry{
		FullPath: "/test/file1.txt",
		Attr: filer.Attr{
			Mode:  0644,
			Uid:   1000,
			Gid:   1000,
			Mtime: time.Now(),
		},
	}

	err := store.InsertEntry(ctx, entry)
	if err != nil {
		t.Fatalf("InsertEntry failed: %v", err)
	}

	// Test FindEntry
	foundEntry, err := store.FindEntry(ctx, "/test/file1.txt")
	if err != nil {
		t.Fatalf("FindEntry failed: %v", err)
	}

	if foundEntry.FullPath != entry.FullPath {
		t.Errorf("Expected path %s, got %s", entry.FullPath, foundEntry.FullPath)
	}

	if foundEntry.Attr.Mode != entry.Attr.Mode {
		t.Errorf("Expected mode %o, got %o", entry.Attr.Mode, foundEntry.Attr.Mode)
	}

	// Test UpdateEntry
	foundEntry.Attr.Mode = 0755
	err = store.UpdateEntry(ctx, foundEntry)
	if err != nil {
		t.Fatalf("UpdateEntry failed: %v", err)
	}

	updatedEntry, err := store.FindEntry(ctx, "/test/file1.txt")
	if err != nil {
		t.Fatalf("FindEntry after update failed: %v", err)
	}

	if updatedEntry.Attr.Mode != 0755 {
		t.Errorf("Expected updated mode 0755, got %o", updatedEntry.Attr.Mode)
	}

	// Test DeleteEntry
	err = store.DeleteEntry(ctx, "/test/file1.txt")
	if err != nil {
		t.Fatalf("DeleteEntry failed: %v", err)
	}

	_, err = store.FindEntry(ctx, "/test/file1.txt")
	if err == nil {
		t.Error("Expected entry to be deleted, but it was found")
	}
	if err != filer_pb.ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestFoundationDBStore_DirectoryOperations(t *testing.T) {
	store := createTestStore(t)
	defer store.Shutdown()

	ctx := context.Background()

	// Create multiple entries in a directory
	testDir := "/test/dir"
	files := []string{"file1.txt", "file2.txt", "file3.txt", "subdir/"}

	for _, fileName := range files {
		entry := &filer.Entry{
			FullPath: util.NewFullPath(testDir, fileName),
			Attr: filer.Attr{
				Mode:  0644,
				Uid:   1000,
				Gid:   1000,
				Mtime: time.Now(),
			},
		}
		if fileName == "subdir/" {
			entry.Attr.Mode = 0755 | os.ModeDir
		}

		err := store.InsertEntry(ctx, entry)
		if err != nil {
			t.Fatalf("InsertEntry failed for %s: %v", fileName, err)
		}
	}

	// Test ListDirectoryEntries
	var listedFiles []string
	lastFileName, err := store.ListDirectoryEntries(ctx, testDir, "", true, 100, func(entry *filer.Entry) bool {
		listedFiles = append(listedFiles, entry.Name())
		return true
	})
	if err != nil {
		t.Fatalf("ListDirectoryEntries failed: %v", err)
	}

	t.Logf("Last file name: %s", lastFileName)
	t.Logf("Listed files: %v", listedFiles)

	if len(listedFiles) != len(files) {
		t.Errorf("Expected %d files, got %d", len(files), len(listedFiles))
	}

	// Test ListDirectoryPrefixedEntries
	var prefixedFiles []string
	_, err = store.ListDirectoryPrefixedEntries(ctx, testDir, "", true, 100, "file", func(entry *filer.Entry) bool {
		prefixedFiles = append(prefixedFiles, entry.Name())
		return true
	})
	if err != nil {
		t.Fatalf("ListDirectoryPrefixedEntries failed: %v", err)
	}

	expectedPrefixedCount := 3 // file1.txt, file2.txt, file3.txt
	if len(prefixedFiles) != expectedPrefixedCount {
		t.Errorf("Expected %d prefixed files, got %d: %v", expectedPrefixedCount, len(prefixedFiles), prefixedFiles)
	}

	// Test DeleteFolderChildren
	err = store.DeleteFolderChildren(ctx, testDir)
	if err != nil {
		t.Fatalf("DeleteFolderChildren failed: %v", err)
	}

	// Verify children are deleted
	var remainingFiles []string
	_, err = store.ListDirectoryEntries(ctx, testDir, "", true, 100, func(entry *filer.Entry) bool {
		remainingFiles = append(remainingFiles, entry.Name())
		return true
	})
	if err != nil {
		t.Fatalf("ListDirectoryEntries after delete failed: %v", err)
	}

	if len(remainingFiles) != 0 {
		t.Errorf("Expected no files after DeleteFolderChildren, got %d: %v", len(remainingFiles), remainingFiles)
	}
}

func TestFoundationDBStore_TransactionOperations(t *testing.T) {
	store := createTestStore(t)
	defer store.Shutdown()

	ctx := context.Background()

	// Begin transaction
	txCtx, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Insert entry in transaction
	entry := &filer.Entry{
		FullPath: "/test/tx_file.txt",
		Attr: filer.Attr{
			Mode:  0644,
			Uid:   1000,
			Gid:   1000,
			Mtime: time.Now(),
		},
	}

	err = store.InsertEntry(txCtx, entry)
	if err != nil {
		t.Fatalf("InsertEntry in transaction failed: %v", err)
	}

	// Entry should not be visible outside transaction yet
	_, err = store.FindEntry(ctx, "/test/tx_file.txt")
	if err == nil {
		t.Error("Entry should not be visible before transaction commit")
	}

	// Commit transaction
	err = store.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Entry should now be visible
	foundEntry, err := store.FindEntry(ctx, "/test/tx_file.txt")
	if err != nil {
		t.Fatalf("FindEntry after commit failed: %v", err)
	}

	if foundEntry.FullPath != entry.FullPath {
		t.Errorf("Expected path %s, got %s", entry.FullPath, foundEntry.FullPath)
	}

	// Test rollback
	txCtx2, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction for rollback test failed: %v", err)
	}

	entry2 := &filer.Entry{
		FullPath: "/test/rollback_file.txt",
		Attr: filer.Attr{
			Mode:  0644,
			Uid:   1000,
			Gid:   1000,
			Mtime: time.Now(),
		},
	}

	err = store.InsertEntry(txCtx2, entry2)
	if err != nil {
		t.Fatalf("InsertEntry for rollback test failed: %v", err)
	}

	// Rollback transaction
	err = store.RollbackTransaction(txCtx2)
	if err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}

	// Entry should not exist after rollback
	_, err = store.FindEntry(ctx, "/test/rollback_file.txt")
	if err == nil {
		t.Error("Entry should not exist after rollback")
	}
	if err != filer_pb.ErrNotFound {
		t.Errorf("Expected ErrNotFound after rollback, got %v", err)
	}
}

func TestFoundationDBStore_KVOperations(t *testing.T) {
	store := createTestStore(t)
	defer store.Shutdown()

	ctx := context.Background()

	// Test KvPut
	key := []byte("test_key")
	value := []byte("test_value")

	err := store.KvPut(ctx, key, value)
	if err != nil {
		t.Fatalf("KvPut failed: %v", err)
	}

	// Test KvGet
	retrievedValue, err := store.KvGet(ctx, key)
	if err != nil {
		t.Fatalf("KvGet failed: %v", err)
	}

	if string(retrievedValue) != string(value) {
		t.Errorf("Expected value %s, got %s", value, retrievedValue)
	}

	// Test KvDelete
	err = store.KvDelete(ctx, key)
	if err != nil {
		t.Fatalf("KvDelete failed: %v", err)
	}

	// Verify key is deleted
	_, err = store.KvGet(ctx, key)
	if err == nil {
		t.Error("Expected key to be deleted")
	}
	if err != filer.ErrKvNotFound {
		t.Errorf("Expected ErrKvNotFound, got %v", err)
	}
}

func TestFoundationDBStore_LargeEntry(t *testing.T) {
	store := createTestStore(t)
	defer store.Shutdown()

	ctx := context.Background()

	// Create entry with many chunks (to test compression)
	entry := &filer.Entry{
		FullPath: "/test/large_file.txt",
		Attr: filer.Attr{
			Mode:  0644,
			Uid:   1000,
			Gid:   1000,
			Mtime: time.Now(),
		},
	}

	// Add many chunks to trigger compression
	for i := 0; i < filer.CountEntryChunksForGzip+10; i++ {
		chunk := &filer_pb.FileChunk{
			FileId: util.Uint64toHex(uint64(i)),
			Offset: int64(i * 1024),
			Size:   1024,
		}
		entry.Chunks = append(entry.Chunks, chunk)
	}

	err := store.InsertEntry(ctx, entry)
	if err != nil {
		t.Fatalf("InsertEntry with large chunks failed: %v", err)
	}

	// Retrieve and verify
	foundEntry, err := store.FindEntry(ctx, "/test/large_file.txt")
	if err != nil {
		t.Fatalf("FindEntry for large file failed: %v", err)
	}

	if len(foundEntry.Chunks) != len(entry.Chunks) {
		t.Errorf("Expected %d chunks, got %d", len(entry.Chunks), len(foundEntry.Chunks))
	}

	// Verify some chunk data
	if foundEntry.Chunks[0].FileId != entry.Chunks[0].FileId {
		t.Errorf("Expected first chunk FileId %s, got %s", entry.Chunks[0].FileId, foundEntry.Chunks[0].FileId)
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
	config.Set("foundationdb.directory_prefix", "seaweedfs_test")

	store := &foundationdb.FoundationDBStore{}
	err := store.Initialize(config, "foundationdb.")
	if err != nil {
		t.Fatalf("Failed to initialize FoundationDB store: %v", err)
	}

	return store
}
