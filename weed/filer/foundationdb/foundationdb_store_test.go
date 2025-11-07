//go:build foundationdb
// +build foundationdb

package foundationdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestFoundationDBStore_Initialize(t *testing.T) {
	// Test with default configuration
	config := util.GetViper()
	config.Set("foundationdb.cluster_file", getTestClusterFile())
	config.Set("foundationdb.api_version", 740)

	store := &FoundationDBStore{}
	err := store.Initialize(config, "foundationdb.")
	if err != nil {
		t.Skip("FoundationDB not available for testing, skipping")
	}

	defer store.Shutdown()

	if store.GetName() != "foundationdb" {
		t.Errorf("Expected store name 'foundationdb', got '%s'", store.GetName())
	}

	if store.directoryPrefix != "seaweedfs" {
		t.Errorf("Expected default directory prefix 'seaweedfs', got '%s'", store.directoryPrefix)
	}
}

func TestFoundationDBStore_InitializeWithCustomConfig(t *testing.T) {
	config := util.GetViper()
	config.Set("foundationdb.cluster_file", getTestClusterFile())
	config.Set("foundationdb.api_version", 740)
	config.Set("foundationdb.timeout", "10s")
	config.Set("foundationdb.max_retry_delay", "2s")
	config.Set("foundationdb.directory_prefix", "custom_prefix")

	store := &FoundationDBStore{}
	err := store.Initialize(config, "foundationdb.")
	if err != nil {
		t.Skip("FoundationDB not available for testing, skipping")
	}

	defer store.Shutdown()

	if store.directoryPrefix != "custom_prefix" {
		t.Errorf("Expected custom directory prefix 'custom_prefix', got '%s'", store.directoryPrefix)
	}

	if store.timeout != 10*time.Second {
		t.Errorf("Expected timeout 10s, got %v", store.timeout)
	}

	if store.maxRetryDelay != 2*time.Second {
		t.Errorf("Expected max retry delay 2s, got %v", store.maxRetryDelay)
	}
}

func TestFoundationDBStore_InitializeInvalidConfig(t *testing.T) {
	tests := []struct {
		name     string
		config   map[string]interface{}
		errorMsg string
	}{
		{
			name: "invalid timeout",
			config: map[string]interface{}{
				"foundationdb.cluster_file":     getTestClusterFile(),
				"foundationdb.api_version":      740,
				"foundationdb.timeout":          "invalid",
				"foundationdb.directory_prefix": "test",
			},
			errorMsg: "invalid timeout duration",
		},
		{
			name: "invalid max_retry_delay",
			config: map[string]interface{}{
				"foundationdb.cluster_file":     getTestClusterFile(),
				"foundationdb.api_version":      740,
				"foundationdb.timeout":          "5s",
				"foundationdb.max_retry_delay":  "invalid",
				"foundationdb.directory_prefix": "test",
			},
			errorMsg: "invalid max_retry_delay duration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := util.GetViper()
			for key, value := range tt.config {
				config.Set(key, value)
			}

			store := &FoundationDBStore{}
			err := store.Initialize(config, "foundationdb.")
			if err == nil {
				store.Shutdown()
				t.Errorf("Expected initialization to fail, but it succeeded")
			} else if !containsString(err.Error(), tt.errorMsg) {
				t.Errorf("Expected error message to contain '%s', got '%s'", tt.errorMsg, err.Error())
			}
		})
	}
}

func TestFoundationDBStore_KeyGeneration(t *testing.T) {
	store := &FoundationDBStore{}
	err := store.initialize(getTestClusterFile(), 740)
	if err != nil {
		t.Skip("FoundationDB not available for testing, skipping")
	}
	defer store.Shutdown()

	// Test key generation for different paths
	testCases := []struct {
		dirPath  string
		fileName string
		desc     string
	}{
		{"/", "file.txt", "root directory file"},
		{"/dir", "file.txt", "subdirectory file"},
		{"/deep/nested/dir", "file.txt", "deep nested file"},
		{"/dir with spaces", "file with spaces.txt", "paths with spaces"},
		{"/unicode/测试", "文件.txt", "unicode paths"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			key := store.genKey(tc.dirPath, tc.fileName)
			if len(key) == 0 {
				t.Error("Generated key should not be empty")
			}

			// Test that we can extract filename back
			// Note: This tests internal consistency
			if tc.fileName != "" {
				extractedName, err := store.extractFileName(key)
				if err != nil {
					t.Errorf("extractFileName failed: %v", err)
				}
				if extractedName != tc.fileName {
					t.Errorf("Expected extracted filename '%s', got '%s'", tc.fileName, extractedName)
				}
			}
		})
	}
}

func TestFoundationDBStore_ErrorHandling(t *testing.T) {
	store := &FoundationDBStore{}
	err := store.initialize(getTestClusterFile(), 740)
	if err != nil {
		t.Skip("FoundationDB not available for testing, skipping")
	}
	defer store.Shutdown()

	ctx := context.Background()

	// Test FindEntry with non-existent path
	_, err = store.FindEntry(ctx, "/non/existent/file.txt")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
	if !errors.Is(err, filer_pb.ErrNotFound) {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}

	// Test KvGet with non-existent key
	_, err = store.KvGet(ctx, []byte("non_existent_key"))
	if err == nil {
		t.Error("Expected error for non-existent key")
	}
	if !errors.Is(err, filer.ErrKvNotFound) {
		t.Errorf("Expected ErrKvNotFound, got %v", err)
	}

	// Test transaction state errors
	err = store.CommitTransaction(ctx)
	if err == nil {
		t.Error("Expected error when committing without active transaction")
	}

	err = store.RollbackTransaction(ctx)
	if err == nil {
		t.Error("Expected error when rolling back without active transaction")
	}
}

func TestFoundationDBStore_TransactionState(t *testing.T) {
	store := &FoundationDBStore{}
	err := store.initialize(getTestClusterFile(), 740)
	if err != nil {
		t.Skip("FoundationDB not available for testing, skipping")
	}
	defer store.Shutdown()

	ctx := context.Background()

	// Test double transaction begin
	txCtx, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Try to begin another transaction on the same context
	_, err = store.BeginTransaction(txCtx)
	if err == nil {
		t.Error("Expected error when beginning transaction while one is active")
	}

	// Commit the transaction
	err = store.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Now should be able to begin a new transaction
	txCtx2, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction after commit failed: %v", err)
	}

	// Rollback this time
	err = store.RollbackTransaction(txCtx2)
	if err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}
}

// Benchmark tests
func BenchmarkFoundationDBStore_InsertEntry(b *testing.B) {
	store := createBenchmarkStore(b)
	defer store.Shutdown()

	ctx := context.Background()
	entry := &filer.Entry{
		FullPath: "/benchmark/file.txt",
		Attr: filer.Attr{
			Mode:  0644,
			Uid:   1000,
			Gid:   1000,
			Mtime: time.Now(),
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.FullPath = util.NewFullPath("/benchmark", fmt.Sprintf("%x", uint64(i))+".txt")
		err := store.InsertEntry(ctx, entry)
		if err != nil {
			b.Fatalf("InsertEntry failed: %v", err)
		}
	}
}

func BenchmarkFoundationDBStore_FindEntry(b *testing.B) {
	store := createBenchmarkStore(b)
	defer store.Shutdown()

	ctx := context.Background()

	// Pre-populate with test entries
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		entry := &filer.Entry{
			FullPath: util.NewFullPath("/benchmark", fmt.Sprintf("%x", uint64(i))+".txt"),
			Attr: filer.Attr{
				Mode:  0644,
				Uid:   1000,
				Gid:   1000,
				Mtime: time.Now(),
			},
		}
		err := store.InsertEntry(ctx, entry)
		if err != nil {
			b.Fatalf("Pre-population InsertEntry failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := util.NewFullPath("/benchmark", fmt.Sprintf("%x", uint64(i%numEntries))+".txt")
		_, err := store.FindEntry(ctx, path)
		if err != nil {
			b.Fatalf("FindEntry failed: %v", err)
		}
	}
}

func BenchmarkFoundationDBStore_KvOperations(b *testing.B) {
	store := createBenchmarkStore(b)
	defer store.Shutdown()

	ctx := context.Background()
	key := []byte("benchmark_key")
	value := []byte("benchmark_value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Put
		err := store.KvPut(ctx, key, value)
		if err != nil {
			b.Fatalf("KvPut failed: %v", err)
		}

		// Get
		_, err = store.KvGet(ctx, key)
		if err != nil {
			b.Fatalf("KvGet failed: %v", err)
		}
	}
}

// Helper functions
func getTestClusterFile() string {
	clusterFile := os.Getenv("FDB_CLUSTER_FILE")
	if clusterFile == "" {
		clusterFile = "/var/fdb/config/fdb.cluster"
	}
	return clusterFile
}

func createBenchmarkStore(b *testing.B) *FoundationDBStore {
	clusterFile := getTestClusterFile()
	if _, err := os.Stat(clusterFile); os.IsNotExist(err) {
		b.Skip("FoundationDB cluster file not found, skipping benchmark")
	}

	store := &FoundationDBStore{}
	err := store.initialize(clusterFile, 740)
	if err != nil {
		b.Skipf("Failed to initialize FoundationDB store: %v", err)
	}

	return store
}

func containsString(s, substr string) bool {
	return strings.Contains(s, substr)
}

func TestFoundationDBStore_DeleteFolderChildrenWithBatching(t *testing.T) {
	// This test validates that DeleteFolderChildren always uses batching
	// to safely handle large directories, regardless of transaction context
	
	store := getTestStore(t)
	defer store.Shutdown()
	
	ctx := context.Background()
	testDir := util.FullPath(fmt.Sprintf("/test_batch_delete_%d", time.Now().UnixNano()))
	
	// Create a large directory (> 100 entries to trigger batching)
	const NUM_ENTRIES = 250
	
	t.Logf("Creating %d test entries...", NUM_ENTRIES)
	for i := 0; i < NUM_ENTRIES; i++ {
		entry := &filer.Entry{
			FullPath: util.NewFullPath(string(testDir), fmt.Sprintf("file_%04d.txt", i)),
			Attr: filer.Attr{
				Mode:  0644,
				Uid:   1000,
				Gid:   1000,
				Mtime: time.Now(),
			},
		}
		if err := store.InsertEntry(ctx, entry); err != nil {
			t.Fatalf("Failed to insert test entry %d: %v", i, err)
		}
	}
	
	// Test 1: DeleteFolderChildren outside transaction should succeed
	t.Run("OutsideTransaction", func(t *testing.T) {
		testDir1 := util.FullPath(fmt.Sprintf("/test_batch_1_%d", time.Now().UnixNano()))
		
		// Create entries
		for i := 0; i < NUM_ENTRIES; i++ {
			entry := &filer.Entry{
				FullPath: util.NewFullPath(string(testDir1), fmt.Sprintf("file_%04d.txt", i)),
				Attr: filer.Attr{
					Mode:  0644,
					Uid:   1000,
					Gid:   1000,
					Mtime: time.Now(),
				},
			}
			store.InsertEntry(ctx, entry)
		}
		
		// Delete with batching
		err := store.DeleteFolderChildren(ctx, testDir1)
		if err != nil {
			t.Errorf("DeleteFolderChildren outside transaction should succeed, got error: %v", err)
		}
		
		// Verify all entries deleted
		var count int
		store.ListDirectoryEntries(ctx, testDir1, "", true, 1000, func(entry *filer.Entry) bool {
			count++
			return true
		})
		if count != 0 {
			t.Errorf("Expected all entries to be deleted, found %d", count)
		}
	})
	
	// Test 2: DeleteFolderChildren with transaction context - uses its own batched transactions
	t.Run("WithTransactionContext", func(t *testing.T) {
		testDir2 := util.FullPath(fmt.Sprintf("/test_batch_2_%d", time.Now().UnixNano()))
		
		// Create entries
		for i := 0; i < NUM_ENTRIES; i++ {
			entry := &filer.Entry{
				FullPath: util.NewFullPath(string(testDir2), fmt.Sprintf("file_%04d.txt", i)),
				Attr: filer.Attr{
					Mode:  0644,
					Uid:   1000,
					Gid:   1000,
					Mtime: time.Now(),
				},
			}
			store.InsertEntry(ctx, entry)
		}
		
		// Start a transaction (DeleteFolderChildren will ignore it and use its own batching)
		txCtx, err := store.BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		
		// Delete large directory - should succeed with batching
		err = store.DeleteFolderChildren(txCtx, testDir2)
		if err != nil {
			t.Errorf("DeleteFolderChildren should succeed with batching even when transaction context present, got: %v", err)
		}
		
		// Rollback transaction (DeleteFolderChildren used its own transactions, so this doesn't affect deletions)
		store.RollbackTransaction(txCtx)
		
		// Verify entries are still deleted (because DeleteFolderChildren managed its own transactions)
		var count int
		store.ListDirectoryEntries(ctx, testDir2, "", true, 1000, func(entry *filer.Entry) bool {
			count++
			return true
		})
		
		if count != 0 {
			t.Errorf("Expected all entries to be deleted, found %d (DeleteFolderChildren uses its own transactions)", count)
		}
	})
	
	// Test 3: Nested directories with batching
	t.Run("NestedDirectories", func(t *testing.T) {
		testDir3 := util.FullPath(fmt.Sprintf("/test_batch_3_%d", time.Now().UnixNano()))
		
		// Create nested structure
		for i := 0; i < 50; i++ {
			// Files in root
			entry := &filer.Entry{
				FullPath: util.NewFullPath(string(testDir3), fmt.Sprintf("file_%02d.txt", i)),
				Attr: filer.Attr{
					Mode:  0644,
					Uid:   1000,
					Gid:   1000,
					Mtime: time.Now(),
				},
			}
			store.InsertEntry(ctx, entry)
			
			// Subdirectory
			subDir := &filer.Entry{
				FullPath: util.NewFullPath(string(testDir3), fmt.Sprintf("dir_%02d", i)),
				Attr: filer.Attr{
					Mode:  0755 | os.ModeDir,
					Uid:   1000,
					Gid:   1000,
					Mtime: time.Now(),
				},
			}
			store.InsertEntry(ctx, subDir)
			
			// Files in subdirectory
			for j := 0; j < 3; j++ {
				subEntry := &filer.Entry{
					FullPath: util.NewFullPath(string(testDir3)+"/"+fmt.Sprintf("dir_%02d", i), fmt.Sprintf("subfile_%02d.txt", j)),
					Attr: filer.Attr{
						Mode:  0644,
						Uid:   1000,
						Gid:   1000,
						Mtime: time.Now(),
					},
				}
				store.InsertEntry(ctx, subEntry)
			}
		}
		
		// Delete all with batching
		err := store.DeleteFolderChildren(ctx, testDir3)
		if err != nil {
			t.Errorf("DeleteFolderChildren should handle nested directories, got: %v", err)
		}
		
		// Verify all deleted
		var count int
		store.ListDirectoryEntries(ctx, testDir3, "", true, 1000, func(entry *filer.Entry) bool {
			count++
			return true
		})
		if count != 0 {
			t.Errorf("Expected all nested entries to be deleted, found %d", count)
		}
	})
	
	// Cleanup
	store.DeleteFolderChildren(ctx, testDir)
}
