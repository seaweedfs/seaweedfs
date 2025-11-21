package foundationdb

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// MockFoundationDBStore provides a simple mock implementation for testing
type MockFoundationDBStore struct {
	data          map[string][]byte
	kvStore       map[string][]byte
	inTransaction bool
}

func NewMockFoundationDBStore() *MockFoundationDBStore {
	return &MockFoundationDBStore{
		data:    make(map[string][]byte),
		kvStore: make(map[string][]byte),
	}
}

func (store *MockFoundationDBStore) GetName() string {
	return "foundationdb_mock"
}

func (store *MockFoundationDBStore) Initialize(configuration util.Configuration, prefix string) error {
	return nil
}

func (store *MockFoundationDBStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	store.inTransaction = true
	return ctx, nil
}

func (store *MockFoundationDBStore) CommitTransaction(ctx context.Context) error {
	store.inTransaction = false
	return nil
}

func (store *MockFoundationDBStore) RollbackTransaction(ctx context.Context) error {
	store.inTransaction = false
	return nil
}

func (store *MockFoundationDBStore) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	return store.UpdateEntry(ctx, entry)
}

func (store *MockFoundationDBStore) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	key := string(entry.FullPath)

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return err
	}

	store.data[key] = value
	return nil
}

func (store *MockFoundationDBStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {
	key := string(fullpath)

	data, exists := store.data[key]
	if !exists {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}

	err = entry.DecodeAttributesAndChunks(data)
	return entry, err
}

func (store *MockFoundationDBStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) error {
	key := string(fullpath)
	delete(store.data, key)
	return nil
}

func (store *MockFoundationDBStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) error {
	prefix := string(fullpath)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	for key := range store.data {
		if strings.HasPrefix(key, prefix) {
			delete(store.data, key)
		}
	}
	return nil
}

func (store *MockFoundationDBStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *MockFoundationDBStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	dirPrefix := string(dirPath)
	if !strings.HasSuffix(dirPrefix, "/") {
		dirPrefix += "/"
	}

	var entries []string
	for key := range store.data {
		if strings.HasPrefix(key, dirPrefix) {
			relativePath := strings.TrimPrefix(key, dirPrefix)
			// Only direct children (no subdirectories)
			if !strings.Contains(relativePath, "/") && strings.HasPrefix(relativePath, prefix) {
				entries = append(entries, key)
			}
		}
	}

	// Sort entries for consistent ordering
	sort.Strings(entries)

	// Apply startFileName filter
	startIndex := 0
	if startFileName != "" {
		for i, entryPath := range entries {
			fileName := strings.TrimPrefix(entryPath, dirPrefix)
			if fileName == startFileName {
				if includeStartFile {
					startIndex = i
				} else {
					startIndex = i + 1
				}
				break
			} else if fileName > startFileName {
				startIndex = i
				break
			}
		}
	}

	// Iterate through sorted entries with limit
	count := int64(0)
	for i := startIndex; i < len(entries) && count < limit; i++ {
		entryPath := entries[i]
		data := store.data[entryPath]
		entry := &filer.Entry{
			FullPath: util.FullPath(entryPath),
		}

		if err := entry.DecodeAttributesAndChunks(data); err != nil {
			continue
		}

		resEachEntryFunc, resEachEntryFuncErr := eachEntryFunc(entry)
		if resEachEntryFuncErr != nil {
			err = fmt.Errorf("failed to process eachEntryFunc: %w", resEachEntryFuncErr)
			break
		}
		if !resEachEntryFunc {
			break
		}

		lastFileName = entry.Name()
		count++
	}

	return lastFileName, err
}

func (store *MockFoundationDBStore) KvPut(ctx context.Context, key []byte, value []byte) error {
	store.kvStore[string(key)] = value
	return nil
}

func (store *MockFoundationDBStore) KvGet(ctx context.Context, key []byte) ([]byte, error) {
	value, exists := store.kvStore[string(key)]
	if !exists {
		return nil, filer.ErrKvNotFound
	}
	return value, nil
}

func (store *MockFoundationDBStore) KvDelete(ctx context.Context, key []byte) error {
	delete(store.kvStore, string(key))
	return nil
}

func (store *MockFoundationDBStore) Shutdown() {
	// Nothing to do for mock
}

// TestMockFoundationDBStore_BasicOperations tests basic store operations with mock
func TestMockFoundationDBStore_BasicOperations(t *testing.T) {
	store := NewMockFoundationDBStore()
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
	t.Log("✅ InsertEntry successful")

	// Test FindEntry
	foundEntry, err := store.FindEntry(ctx, "/test/file1.txt")
	if err != nil {
		t.Fatalf("FindEntry failed: %v", err)
	}

	if foundEntry.FullPath != entry.FullPath {
		t.Errorf("Expected path %s, got %s", entry.FullPath, foundEntry.FullPath)
	}
	t.Log("✅ FindEntry successful")

	// Test UpdateEntry
	foundEntry.Attr.Mode = 0755
	err = store.UpdateEntry(ctx, foundEntry)
	if err != nil {
		t.Fatalf("UpdateEntry failed: %v", err)
	}
	t.Log("✅ UpdateEntry successful")

	// Test DeleteEntry
	err = store.DeleteEntry(ctx, "/test/file1.txt")
	if err != nil {
		t.Fatalf("DeleteEntry failed: %v", err)
	}
	t.Log("✅ DeleteEntry successful")

	// Test entry is deleted
	_, err = store.FindEntry(ctx, "/test/file1.txt")
	if err == nil {
		t.Error("Expected entry to be deleted, but it was found")
	}
	if err != filer_pb.ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
	t.Log("✅ Entry deletion verified")
}

// TestMockFoundationDBStore_TransactionOperations tests transaction handling
func TestMockFoundationDBStore_TransactionOperations(t *testing.T) {
	store := NewMockFoundationDBStore()
	defer store.Shutdown()

	ctx := context.Background()

	// Test transaction workflow
	txCtx, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	t.Log("✅ BeginTransaction successful")

	if !store.inTransaction {
		t.Error("Expected to be in transaction")
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
	t.Log("✅ InsertEntry in transaction successful")

	// Commit transaction
	err = store.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}
	t.Log("✅ CommitTransaction successful")

	if store.inTransaction {
		t.Error("Expected to not be in transaction after commit")
	}

	// Test rollback
	txCtx2, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction for rollback test failed: %v", err)
	}

	err = store.RollbackTransaction(txCtx2)
	if err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}
	t.Log("✅ RollbackTransaction successful")

	if store.inTransaction {
		t.Error("Expected to not be in transaction after rollback")
	}
}

// TestMockFoundationDBStore_KVOperations tests key-value operations
func TestMockFoundationDBStore_KVOperations(t *testing.T) {
	store := NewMockFoundationDBStore()
	defer store.Shutdown()

	ctx := context.Background()

	// Test KvPut
	key := []byte("test_key")
	value := []byte("test_value")

	err := store.KvPut(ctx, key, value)
	if err != nil {
		t.Fatalf("KvPut failed: %v", err)
	}
	t.Log("✅ KvPut successful")

	// Test KvGet
	retrievedValue, err := store.KvGet(ctx, key)
	if err != nil {
		t.Fatalf("KvGet failed: %v", err)
	}

	if string(retrievedValue) != string(value) {
		t.Errorf("Expected value %s, got %s", value, retrievedValue)
	}
	t.Log("✅ KvGet successful")

	// Test KvDelete
	err = store.KvDelete(ctx, key)
	if err != nil {
		t.Fatalf("KvDelete failed: %v", err)
	}
	t.Log("✅ KvDelete successful")

	// Verify key is deleted
	_, err = store.KvGet(ctx, key)
	if err == nil {
		t.Error("Expected key to be deleted")
	}
	if err != filer.ErrKvNotFound {
		t.Errorf("Expected ErrKvNotFound, got %v", err)
	}
	t.Log("✅ Key deletion verified")
}

// TestMockFoundationDBStore_DirectoryOperations tests directory operations
func TestMockFoundationDBStore_DirectoryOperations(t *testing.T) {
	store := NewMockFoundationDBStore()
	defer store.Shutdown()

	ctx := context.Background()

	// Create multiple entries in a directory
	testDir := util.FullPath("/test/dir/")
	files := []string{"file1.txt", "file2.txt", "file3.txt"}

	for _, fileName := range files {
		entry := &filer.Entry{
			FullPath: util.NewFullPath(string(testDir), fileName),
			Attr: filer.Attr{
				Mode:  0644,
				Uid:   1000,
				Gid:   1000,
				Mtime: time.Now(),
			},
		}

		err := store.InsertEntry(ctx, entry)
		if err != nil {
			t.Fatalf("InsertEntry failed for %s: %v", fileName, err)
		}
	}
	t.Log("✅ Directory entries created")

	// Test ListDirectoryEntries
	var listedFiles []string
	lastFileName, err := store.ListDirectoryEntries(ctx, testDir, "", true, 100, func(entry *filer.Entry) (bool, error) {
		listedFiles = append(listedFiles, entry.Name())
		return true, nil
	})
	if err != nil {
		t.Fatalf("ListDirectoryEntries failed: %v", err)
	}
	t.Logf("✅ ListDirectoryEntries successful, last file: %s", lastFileName)
	t.Logf("Listed files: %v", listedFiles)

	// Test DeleteFolderChildren
	err = store.DeleteFolderChildren(ctx, testDir)
	if err != nil {
		t.Fatalf("DeleteFolderChildren failed: %v", err)
	}
	t.Log("✅ DeleteFolderChildren successful")

	// Verify children are deleted
	var remainingFiles []string
	_, err = store.ListDirectoryEntries(ctx, testDir, "", true, 100, func(entry *filer.Entry) (bool, error) {
		remainingFiles = append(remainingFiles, entry.Name())
		return true, nil
	})
	if err != nil {
		t.Fatalf("ListDirectoryEntries after delete failed: %v", err)
	}

	if len(remainingFiles) != 0 {
		t.Errorf("Expected no files after DeleteFolderChildren, got %d: %v", len(remainingFiles), remainingFiles)
	}
	t.Log("✅ Folder children deletion verified")
}
