package empty_folder_cleanup

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type mockFilerOps struct {
	countFn  func(path util.FullPath) (int, error)
	deleteFn func(path util.FullPath) error
	attrsFn  func(path util.FullPath) (map[string][]byte, error)
}

func (m *mockFilerOps) CountDirectoryEntries(_ context.Context, dirPath util.FullPath, _ int) (int, error) {
	if m.countFn == nil {
		return 0, nil
	}
	return m.countFn(dirPath)
}

func (m *mockFilerOps) DeleteEntryMetaAndData(_ context.Context, p util.FullPath, _, _, _, _ bool, _ []int32, _ int64) error {
	if m.deleteFn == nil {
		return nil
	}
	return m.deleteFn(p)
}

func (m *mockFilerOps) GetEntryAttributes(_ context.Context, p util.FullPath) (map[string][]byte, error) {
	if m.attrsFn == nil {
		return nil, nil
	}
	return m.attrsFn(p)
}

func Test_isUnderPath(t *testing.T) {
	tests := []struct {
		name     string
		child    string
		parent   string
		expected bool
	}{
		{"child under parent", "/buckets/mybucket/folder/file.txt", "/buckets", true},
		{"child is parent", "/buckets", "/buckets", true},
		{"child not under parent", "/other/path", "/buckets", false},
		{"empty parent", "/any/path", "", true},
		{"root parent", "/any/path", "/", true},
		{"parent with trailing slash", "/buckets/mybucket", "/buckets/", true},
		{"similar prefix but not under", "/buckets-other/file", "/buckets", false},
		{"deeply nested", "/buckets/a/b/c/d/e/f", "/buckets/a/b", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isUnderPath(tt.child, tt.parent)
			if result != tt.expected {
				t.Errorf("isUnderPath(%q, %q) = %v, want %v", tt.child, tt.parent, result, tt.expected)
			}
		})
	}
}

func Test_isUnderBucketPath(t *testing.T) {
	tests := []struct {
		name       string
		directory  string
		bucketPath string
		expected   bool
	}{
		// Should NOT process - bucket path itself
		{"bucket path itself", "/buckets", "/buckets", false},
		// Should NOT process - bucket directory (immediate child)
		{"bucket directory", "/buckets/mybucket", "/buckets", false},
		// Should process - folder inside bucket
		{"folder in bucket", "/buckets/mybucket/folder", "/buckets", true},
		// Should process - nested folder
		{"nested folder", "/buckets/mybucket/a/b/c", "/buckets", true},
		// Should NOT process - outside buckets
		{"outside buckets", "/other/path", "/buckets", false},
		// Empty bucket path allows all
		{"empty bucket path", "/any/path", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isUnderBucketPath(tt.directory, tt.bucketPath)
			if result != tt.expected {
				t.Errorf("isUnderBucketPath(%q, %q) = %v, want %v", tt.directory, tt.bucketPath, result, tt.expected)
			}
		})
	}
}

func TestEmptyFolderCleaner_ownsFolder(t *testing.T) {
	// Create a LockRing with multiple servers
	lockRing := lock_manager.NewLockRing(5 * time.Second)

	servers := []pb.ServerAddress{
		"filer1:8888",
		"filer2:8888",
		"filer3:8888",
	}
	lockRing.SetSnapshot(servers)

	// Create cleaner for filer1
	cleaner1 := &EmptyFolderCleaner{
		lockRing: lockRing,
		host:     "filer1:8888",
	}

	// Create cleaner for filer2
	cleaner2 := &EmptyFolderCleaner{
		lockRing: lockRing,
		host:     "filer2:8888",
	}

	// Create cleaner for filer3
	cleaner3 := &EmptyFolderCleaner{
		lockRing: lockRing,
		host:     "filer3:8888",
	}

	// Test that exactly one filer owns each folder
	testFolders := []string{
		"/buckets/mybucket/folder1",
		"/buckets/mybucket/folder2",
		"/buckets/mybucket/folder3",
		"/buckets/mybucket/a/b/c",
		"/buckets/otherbucket/x",
	}

	for _, folder := range testFolders {
		ownCount := 0
		if cleaner1.ownsFolder(folder) {
			ownCount++
		}
		if cleaner2.ownsFolder(folder) {
			ownCount++
		}
		if cleaner3.ownsFolder(folder) {
			ownCount++
		}

		if ownCount != 1 {
			t.Errorf("folder %q owned by %d filers, expected exactly 1", folder, ownCount)
		}
	}
}

func TestEmptyFolderCleaner_ownsFolder_singleServer(t *testing.T) {
	// Create a LockRing with a single server
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing: lockRing,
		host:     "filer1:8888",
	}

	// Single filer should own all folders
	testFolders := []string{
		"/buckets/mybucket/folder1",
		"/buckets/mybucket/folder2",
		"/buckets/otherbucket/x",
	}

	for _, folder := range testFolders {
		if !cleaner.ownsFolder(folder) {
			t.Errorf("single filer should own folder %q", folder)
		}
	}
}

func TestEmptyFolderCleaner_ownsFolder_emptyRing(t *testing.T) {
	// Create an empty LockRing
	lockRing := lock_manager.NewLockRing(5 * time.Second)

	cleaner := &EmptyFolderCleaner{
		lockRing: lockRing,
		host:     "filer1:8888",
	}

	// With empty ring, should own all folders
	if !cleaner.ownsFolder("/buckets/mybucket/folder") {
		t.Error("should own folder with empty ring")
	}
}

func TestEmptyFolderCleaner_OnCreateEvent_cancelsCleanup(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:     lockRing,
		host:         "filer1:8888",
		bucketPath:   "/buckets",
		enabled:      true,
		folderCounts: make(map[string]*folderState),
		cleanupQueue: NewCleanupQueue(1000, 10*time.Minute),
		stopCh:       make(chan struct{}),
	}

	folder := "/buckets/mybucket/testfolder"
	now := time.Now()

	// Simulate delete event
	cleaner.OnDeleteEvent(folder, "file.txt", false, now)

	// Check that cleanup is queued
	if cleaner.GetPendingCleanupCount() != 1 {
		t.Errorf("expected 1 pending cleanup, got %d", cleaner.GetPendingCleanupCount())
	}

	// Simulate create event
	cleaner.OnCreateEvent(folder, "newfile.txt", false)

	// Check that cleanup is cancelled
	if cleaner.GetPendingCleanupCount() != 0 {
		t.Errorf("expected 0 pending cleanups after create, got %d", cleaner.GetPendingCleanupCount())
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_OnDeleteEvent_deduplication(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:     lockRing,
		host:         "filer1:8888",
		bucketPath:   "/buckets",
		enabled:      true,
		folderCounts: make(map[string]*folderState),
		cleanupQueue: NewCleanupQueue(1000, 10*time.Minute),
		stopCh:       make(chan struct{}),
	}

	folder := "/buckets/mybucket/testfolder"
	now := time.Now()

	// Simulate multiple delete events for same folder
	for i := 0; i < 5; i++ {
		cleaner.OnDeleteEvent(folder, "file"+string(rune('0'+i))+".txt", false, now.Add(time.Duration(i)*time.Second))
	}

	// Check that only 1 cleanup is queued (deduplicated)
	if cleaner.GetPendingCleanupCount() != 1 {
		t.Errorf("expected 1 pending cleanup after deduplication, got %d", cleaner.GetPendingCleanupCount())
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_OnDeleteEvent_multipleFolders(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:     lockRing,
		host:         "filer1:8888",
		bucketPath:   "/buckets",
		enabled:      true,
		folderCounts: make(map[string]*folderState),
		cleanupQueue: NewCleanupQueue(1000, 10*time.Minute),
		stopCh:       make(chan struct{}),
	}

	now := time.Now()

	// Delete files in different folders
	cleaner.OnDeleteEvent("/buckets/mybucket/folder1", "file.txt", false, now)
	cleaner.OnDeleteEvent("/buckets/mybucket/folder2", "file.txt", false, now.Add(1*time.Second))
	cleaner.OnDeleteEvent("/buckets/mybucket/folder3", "file.txt", false, now.Add(2*time.Second))

	// Each folder should be queued
	if cleaner.GetPendingCleanupCount() != 3 {
		t.Errorf("expected 3 pending cleanups, got %d", cleaner.GetPendingCleanupCount())
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_OnDeleteEvent_notOwner(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888", "filer2:8888"})

	// Create cleaner for filer that doesn't own the folder
	cleaner := &EmptyFolderCleaner{
		lockRing:     lockRing,
		host:         "filer1:8888",
		bucketPath:   "/buckets",
		enabled:      true,
		folderCounts: make(map[string]*folderState),
		cleanupQueue: NewCleanupQueue(1000, 10*time.Minute),
		stopCh:       make(chan struct{}),
	}

	now := time.Now()

	// Try many folders, looking for one that filer1 doesn't own
	foundNonOwned := false
	for i := 0; i < 100; i++ {
		folder := "/buckets/mybucket/folder" + string(rune('0'+i%10)) + string(rune('0'+i/10))
		if !cleaner.ownsFolder(folder) {
			// This folder is not owned by filer1
			cleaner.OnDeleteEvent(folder, "file.txt", false, now)
			if cleaner.GetPendingCleanupCount() != 0 {
				t.Errorf("non-owner should not queue cleanup for folder %s", folder)
			}
			foundNonOwned = true
			break
		}
	}

	if !foundNonOwned {
		t.Skip("could not find a folder not owned by filer1")
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_OnDeleteEvent_disabled(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:     lockRing,
		host:         "filer1:8888",
		bucketPath:   "/buckets",
		enabled:      false, // Disabled
		folderCounts: make(map[string]*folderState),
		cleanupQueue: NewCleanupQueue(1000, 10*time.Minute),
		stopCh:       make(chan struct{}),
	}

	folder := "/buckets/mybucket/testfolder"
	now := time.Now()

	// Simulate delete event
	cleaner.OnDeleteEvent(folder, "file.txt", false, now)

	// Check that no cleanup is queued when disabled
	if cleaner.GetPendingCleanupCount() != 0 {
		t.Errorf("disabled cleaner should not queue cleanup, got %d", cleaner.GetPendingCleanupCount())
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_OnDeleteEvent_directoryDeletion(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:     lockRing,
		host:         "filer1:8888",
		bucketPath:   "/buckets",
		enabled:      true,
		folderCounts: make(map[string]*folderState),
		cleanupQueue: NewCleanupQueue(1000, 10*time.Minute),
		stopCh:       make(chan struct{}),
	}

	folder := "/buckets/mybucket/testfolder"
	now := time.Now()

	// Simulate directory delete event - should trigger cleanup
	// because subdirectory deletion also makes parent potentially empty
	cleaner.OnDeleteEvent(folder, "subdir", true, now)

	// Check that cleanup IS queued for directory deletion
	if cleaner.GetPendingCleanupCount() != 1 {
		t.Errorf("directory deletion should trigger cleanup, got %d", cleaner.GetPendingCleanupCount())
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_cachedCounts(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:     lockRing,
		host:         "filer1:8888",
		bucketPath:   "/buckets",
		enabled:      true,
		folderCounts: make(map[string]*folderState),
		cleanupQueue: NewCleanupQueue(1000, 10*time.Minute),
		stopCh:       make(chan struct{}),
	}

	folder := "/buckets/mybucket/testfolder"

	// Initialize cached count
	cleaner.folderCounts[folder] = &folderState{roughCount: 5}

	// Simulate create events
	cleaner.OnCreateEvent(folder, "newfile1.txt", false)
	cleaner.OnCreateEvent(folder, "newfile2.txt", false)

	// Check cached count increased
	count, exists := cleaner.GetCachedFolderCount(folder)
	if !exists {
		t.Error("cached folder count should exist")
	}
	if count != 7 {
		t.Errorf("expected cached count 7, got %d", count)
	}

	// Simulate delete events
	now := time.Now()
	cleaner.OnDeleteEvent(folder, "file1.txt", false, now)
	cleaner.OnDeleteEvent(folder, "file2.txt", false, now.Add(1*time.Second))

	// Check cached count decreased
	count, exists = cleaner.GetCachedFolderCount(folder)
	if !exists {
		t.Error("cached folder count should exist")
	}
	if count != 5 {
		t.Errorf("expected cached count 5, got %d", count)
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_Stop(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:     lockRing,
		host:         "filer1:8888",
		bucketPath:   "/buckets",
		enabled:      true,
		folderCounts: make(map[string]*folderState),
		cleanupQueue: NewCleanupQueue(1000, 10*time.Minute),
		stopCh:       make(chan struct{}),
	}

	now := time.Now()

	// Queue some cleanups
	cleaner.OnDeleteEvent("/buckets/mybucket/folder1", "file1.txt", false, now)
	cleaner.OnDeleteEvent("/buckets/mybucket/folder2", "file2.txt", false, now.Add(1*time.Second))
	cleaner.OnDeleteEvent("/buckets/mybucket/folder3", "file3.txt", false, now.Add(2*time.Second))

	// Verify cleanups are queued
	if cleaner.GetPendingCleanupCount() < 1 {
		t.Error("expected at least 1 pending cleanup before stop")
	}

	// Stop the cleaner
	cleaner.Stop()

	// Verify all cleanups are cancelled
	if cleaner.GetPendingCleanupCount() != 0 {
		t.Errorf("expected 0 pending cleanups after stop, got %d", cleaner.GetPendingCleanupCount())
	}
}

func TestEmptyFolderCleaner_cacheEviction(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:     lockRing,
		host:         "filer1:8888",
		bucketPath:   "/buckets",
		enabled:      true,
		folderCounts: make(map[string]*folderState),
		cleanupQueue: NewCleanupQueue(1000, 10*time.Minute),
		cacheExpiry:  100 * time.Millisecond, // Short expiry for testing
		stopCh:       make(chan struct{}),
	}

	folder1 := "/buckets/mybucket/folder1"
	folder2 := "/buckets/mybucket/folder2"
	folder3 := "/buckets/mybucket/folder3"

	// Add some cache entries with old timestamps
	oldTime := time.Now().Add(-1 * time.Hour)
	cleaner.folderCounts[folder1] = &folderState{roughCount: 5, lastCheck: oldTime}
	cleaner.folderCounts[folder2] = &folderState{roughCount: 3, lastCheck: oldTime}
	// folder3 has recent activity
	cleaner.folderCounts[folder3] = &folderState{roughCount: 2, lastCheck: time.Now()}

	// Verify all entries exist
	if len(cleaner.folderCounts) != 3 {
		t.Errorf("expected 3 cache entries, got %d", len(cleaner.folderCounts))
	}

	// Run eviction
	cleaner.evictStaleCacheEntries()

	// Verify stale entries are evicted
	if len(cleaner.folderCounts) != 1 {
		t.Errorf("expected 1 cache entry after eviction, got %d", len(cleaner.folderCounts))
	}

	// Verify the recent entry still exists
	if _, exists := cleaner.folderCounts[folder3]; !exists {
		t.Error("expected folder3 to still exist in cache")
	}

	// Verify stale entries are removed
	if _, exists := cleaner.folderCounts[folder1]; exists {
		t.Error("expected folder1 to be evicted")
	}
	if _, exists := cleaner.folderCounts[folder2]; exists {
		t.Error("expected folder2 to be evicted")
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_cacheEviction_skipsEntriesInQueue(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:     lockRing,
		host:         "filer1:8888",
		bucketPath:   "/buckets",
		enabled:      true,
		folderCounts: make(map[string]*folderState),
		cleanupQueue: NewCleanupQueue(1000, 10*time.Minute),
		cacheExpiry:  100 * time.Millisecond,
		stopCh:       make(chan struct{}),
	}

	folder := "/buckets/mybucket/folder"
	oldTime := time.Now().Add(-1 * time.Hour)

	// Add a stale cache entry
	cleaner.folderCounts[folder] = &folderState{roughCount: 0, lastCheck: oldTime}
	// Also add to cleanup queue
	cleaner.cleanupQueue.Add(folder, time.Now())

	// Run eviction
	cleaner.evictStaleCacheEntries()

	// Verify entry is NOT evicted because it's in cleanup queue
	if _, exists := cleaner.folderCounts[folder]; !exists {
		t.Error("expected folder to still exist in cache (is in cleanup queue)")
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_queueFIFOOrder(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:     lockRing,
		host:         "filer1:8888",
		bucketPath:   "/buckets",
		enabled:      true,
		folderCounts: make(map[string]*folderState),
		cleanupQueue: NewCleanupQueue(1000, 10*time.Minute),
		stopCh:       make(chan struct{}),
	}

	now := time.Now()

	// Add folders in order
	folders := []string{
		"/buckets/mybucket/folder1",
		"/buckets/mybucket/folder2",
		"/buckets/mybucket/folder3",
	}
	for i, folder := range folders {
		cleaner.OnDeleteEvent(folder, "file.txt", false, now.Add(time.Duration(i)*time.Second))
	}

	// Verify queue length
	if cleaner.GetPendingCleanupCount() != 3 {
		t.Errorf("expected 3 queued folders, got %d", cleaner.GetPendingCleanupCount())
	}

	// Verify time-sorted order by popping
	for i, expected := range folders {
		folder, ok := cleaner.cleanupQueue.Pop()
		if !ok || folder != expected {
			t.Errorf("expected folder %s at index %d, got %s", expected, i, folder)
		}
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_processCleanupQueue_drainsAllOnceTriggered(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	var deleted []string
	mock := &mockFilerOps{
		countFn: func(_ util.FullPath) (int, error) {
			return 0, nil
		},
		deleteFn: func(path util.FullPath) error {
			deleted = append(deleted, string(path))
			return nil
		},
		attrsFn: func(_ util.FullPath) (map[string][]byte, error) {
			return map[string][]byte{s3_constants.ExtS3ImplicitDir: []byte("true")}, nil
		},
	}

	cleaner := &EmptyFolderCleaner{
		filer:          mock,
		lockRing:       lockRing,
		host:           "filer1:8888",
		bucketPath:     "/buckets",
		enabled:        true,
		folderCounts:   make(map[string]*folderState),
		cleanupQueue:   NewCleanupQueue(2, time.Hour),
		maxCountCheck:  1000,
		cacheExpiry:    time.Minute,
		processorSleep: time.Second,
		stopCh:         make(chan struct{}),
	}

	now := time.Now()
	cleaner.cleanupQueue.Add("/buckets/test/folder1", now)
	cleaner.cleanupQueue.Add("/buckets/test/folder2", now.Add(time.Millisecond))
	cleaner.cleanupQueue.Add("/buckets/test/folder3", now.Add(2*time.Millisecond))

	cleaner.processCleanupQueue()

	if got := cleaner.cleanupQueue.Len(); got != 0 {
		t.Fatalf("expected queue to be drained, got len=%d", got)
	}
	if len(deleted) != 3 {
		t.Fatalf("expected 3 deleted folders, got %d", len(deleted))
	}
}

func TestEmptyFolderCleaner_executeCleanup_missingImplicitAttributeStillDeletes(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	var deleted []string
	mock := &mockFilerOps{
		countFn: func(_ util.FullPath) (int, error) {
			return 0, nil
		},
		deleteFn: func(path util.FullPath) error {
			deleted = append(deleted, string(path))
			return nil
		},
		attrsFn: func(_ util.FullPath) (map[string][]byte, error) {
			return map[string][]byte{}, nil
		},
	}

	cleaner := &EmptyFolderCleaner{
		filer:          mock,
		lockRing:       lockRing,
		host:           "filer1:8888",
		bucketPath:     "/buckets",
		enabled:        true,
		folderCounts:   make(map[string]*folderState),
		cleanupQueue:   NewCleanupQueue(1000, time.Minute),
		maxCountCheck:  1000,
		cacheExpiry:    time.Minute,
		processorSleep: time.Second,
		stopCh:         make(chan struct{}),
	}

	folder := "/buckets/test/folder"
	cleaner.executeCleanup(folder)

	if len(deleted) != 1 || deleted[0] != folder {
		t.Fatalf("expected folder %s to be deleted, got %v", folder, deleted)
	}
}
