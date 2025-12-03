package filer

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestIsUnderPath(t *testing.T) {
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

func TestIsUnderBucketPath(t *testing.T) {
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
		lockRing:        lockRing,
		host:            "filer1:8888",
		bucketPath:      "/buckets",
		enabled:         true,
		folderCounts:    make(map[string]*folderState),
		pendingCleanups: make(map[string]*cleanupTask),
		cleanupDelay:    10 * time.Second,
		maxCountCheck:   1000,
		stopCh:          make(chan struct{}),
	}

	folder := "/buckets/mybucket/testfolder"

	// Simulate delete event
	cleaner.OnDeleteEvent(folder, "file.txt", false)

	// Check that cleanup is scheduled
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

func TestEmptyFolderCleaner_OnDeleteEvent_debouncing(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:        lockRing,
		host:            "filer1:8888",
		bucketPath:      "/buckets",
		enabled:         true,
		folderCounts:    make(map[string]*folderState),
		pendingCleanups: make(map[string]*cleanupTask),
		cleanupDelay:    100 * time.Millisecond, // Short delay for testing
		maxCountCheck:   1000,
		stopCh:          make(chan struct{}),
	}

	folder := "/buckets/mybucket/testfolder"

	// Simulate multiple delete events
	for i := 0; i < 5; i++ {
		cleaner.OnDeleteEvent(folder, "file"+string(rune('0'+i))+".txt", false)
	}

	// Check that only 1 cleanup is scheduled (debounced)
	if cleaner.GetPendingCleanupCount() != 1 {
		t.Errorf("expected 1 pending cleanup after debouncing, got %d", cleaner.GetPendingCleanupCount())
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_OnDeleteEvent_notOwner(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888", "filer2:8888"})

	// Create cleaner for filer that doesn't own the folder
	// We need to find a folder that hashes to filer2
	cleaner := &EmptyFolderCleaner{
		lockRing:        lockRing,
		host:            "filer1:8888",
		bucketPath:      "/buckets",
		enabled:         true,
		folderCounts:    make(map[string]*folderState),
		pendingCleanups: make(map[string]*cleanupTask),
		cleanupDelay:    10 * time.Second,
		maxCountCheck:   1000,
		stopCh:          make(chan struct{}),
	}

	// Try many folders, looking for one that filer1 doesn't own
	foundNonOwned := false
	for i := 0; i < 100; i++ {
		folder := "/buckets/mybucket/folder" + string(rune('0'+i%10)) + string(rune('0'+i/10))
		if !cleaner.ownsFolder(folder) {
			// This folder is not owned by filer1
			cleaner.OnDeleteEvent(folder, "file.txt", false)
			if cleaner.GetPendingCleanupCount() != 0 {
				t.Errorf("non-owner should not schedule cleanup for folder %s", folder)
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
		lockRing:        lockRing,
		host:            "filer1:8888",
		bucketPath:      "/buckets",
		enabled:         false, // Disabled
		folderCounts:    make(map[string]*folderState),
		pendingCleanups: make(map[string]*cleanupTask),
		cleanupDelay:    10 * time.Second,
		maxCountCheck:   1000,
		stopCh:          make(chan struct{}),
	}

	folder := "/buckets/mybucket/testfolder"

	// Simulate delete event
	cleaner.OnDeleteEvent(folder, "file.txt", false)

	// Check that no cleanup is scheduled when disabled
	if cleaner.GetPendingCleanupCount() != 0 {
		t.Errorf("disabled cleaner should not schedule cleanup, got %d", cleaner.GetPendingCleanupCount())
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_OnDeleteEvent_directoryDeletion(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:        lockRing,
		host:            "filer1:8888",
		bucketPath:      "/buckets",
		enabled:         true,
		folderCounts:    make(map[string]*folderState),
		pendingCleanups: make(map[string]*cleanupTask),
		cleanupDelay:    10 * time.Second,
		maxCountCheck:   1000,
		stopCh:          make(chan struct{}),
	}

	folder := "/buckets/mybucket/testfolder"

	// Simulate directory delete event - should trigger cleanup
	// because subdirectory deletion also makes parent potentially empty
	cleaner.OnDeleteEvent(folder, "subdir", true)

	// Check that cleanup IS scheduled for directory deletion
	if cleaner.GetPendingCleanupCount() != 1 {
		t.Errorf("directory deletion should trigger cleanup, got %d", cleaner.GetPendingCleanupCount())
	}

	cleaner.Stop()
}

func TestEmptyFolderCleaner_cachedCounts(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:        lockRing,
		host:            "filer1:8888",
		bucketPath:      "/buckets",
		enabled:         true,
		folderCounts:    make(map[string]*folderState),
		pendingCleanups: make(map[string]*cleanupTask),
		cleanupDelay:    10 * time.Second,
		maxCountCheck:   1000,
		stopCh:          make(chan struct{}),
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
	cleaner.OnDeleteEvent(folder, "file1.txt", false)
	cleaner.OnDeleteEvent(folder, "file2.txt", false)

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
		lockRing:        lockRing,
		host:            "filer1:8888",
		bucketPath:      "/buckets",
		enabled:         true,
		folderCounts:    make(map[string]*folderState),
		pendingCleanups: make(map[string]*cleanupTask),
		cleanupDelay:    1 * time.Hour, // Long delay to ensure timers are active
		maxCountCheck:   1000,
		stopCh:          make(chan struct{}),
	}

	// Schedule some cleanups
	cleaner.OnDeleteEvent("/buckets/mybucket/folder1", "file1.txt", false)
	cleaner.OnDeleteEvent("/buckets/mybucket/folder2", "file2.txt", false)
	cleaner.OnDeleteEvent("/buckets/mybucket/folder3", "file3.txt", false)

	// Verify cleanups are scheduled
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
		lockRing:        lockRing,
		host:            "filer1:8888",
		bucketPath:      "/buckets",
		enabled:         true,
		folderCounts:    make(map[string]*folderState),
		pendingCleanups: make(map[string]*cleanupTask),
		cleanupDelay:    1 * time.Hour,
		maxCountCheck:   1000,
		cacheExpiry:     100 * time.Millisecond, // Short expiry for testing
		stopCh:          make(chan struct{}),
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

func TestEmptyFolderCleaner_cacheEviction_skipsEntriesWithPendingCleanup(t *testing.T) {
	lockRing := lock_manager.NewLockRing(5 * time.Second)
	lockRing.SetSnapshot([]pb.ServerAddress{"filer1:8888"})

	cleaner := &EmptyFolderCleaner{
		lockRing:        lockRing,
		host:            "filer1:8888",
		bucketPath:      "/buckets",
		enabled:         true,
		folderCounts:    make(map[string]*folderState),
		pendingCleanups: make(map[string]*cleanupTask),
		cleanupDelay:    1 * time.Hour,
		maxCountCheck:   1000,
		cacheExpiry:     100 * time.Millisecond,
		stopCh:          make(chan struct{}),
	}

	folder := "/buckets/mybucket/folder"
	oldTime := time.Now().Add(-1 * time.Hour)

	// Add a stale cache entry
	cleaner.folderCounts[folder] = &folderState{roughCount: 0, lastCheck: oldTime}
	// But also schedule a pending cleanup for it
	cleaner.pendingCleanups[folder] = &cleanupTask{
		folder:        folder,
		scheduledTime: time.Now().Add(1 * time.Hour),
		timer:         time.NewTimer(1 * time.Hour),
	}

	// Run eviction
	cleaner.evictStaleCacheEntries()

	// Verify entry is NOT evicted because it has a pending cleanup
	if _, exists := cleaner.folderCounts[folder]; !exists {
		t.Error("expected folder to still exist in cache (has pending cleanup)")
	}

	cleaner.Stop()
}

