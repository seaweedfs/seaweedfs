package page_writer

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSwapFile_NewSwapFileChunk_Concurrent(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "seaweedfs_swap_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sf := NewSwapFile(filepath.Join(tempDir, "swap"), 1024)
	defer sf.FreeResource()

	var wg sync.WaitGroup
	numConcurrent := 10
	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			chunk := sf.NewSwapFileChunk(LogicChunkIndex(idx))
			if chunk == nil {
				t.Errorf("failed to create chunk %d", idx)
			}
		}(i)
	}
	wg.Wait()

	if sf.file == nil {
		t.Error("sf.file should not be nil")
	}
}

func TestSwapFile_MkdirAll_Permissions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "seaweedfs_swap_perm_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	swapDir := filepath.Join(tempDir, "swap_subdir")
	sf := NewSwapFile(swapDir, 1024)
	defer sf.FreeResource()

	// This should trigger os.MkdirAll
	sf.NewSwapFileChunk(0)

	info, err := os.Stat(swapDir)
	if err != nil {
		t.Fatalf("failed to stat swap dir: %v", err)
	}

	if !info.IsDir() {
		t.Errorf("expected %s to be a directory", swapDir)
	}

	// Check permissions - should be 0700
	if info.Mode().Perm() != 0700 {
		t.Errorf("expected permissions 0700, got %o", info.Mode().Perm())
	}
}

func TestSwapFile_RecreateDir(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "seaweedfs_swap_recreate_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	swapDir := filepath.Join(tempDir, "swap_recreate")
	sf := NewSwapFile(swapDir, 1024)

	// Create first chunk
	sf.NewSwapFileChunk(0)
	sf.FreeResource()

	// Delete the directory
	os.RemoveAll(swapDir)

	// Create second chunk - should recreate directory
	chunk := sf.NewSwapFileChunk(1)
	if chunk == nil {
		t.Error("failed to create chunk after directory deletion")
	}

	if _, err := os.Stat(swapDir); os.IsNotExist(err) {
		t.Error("swap directory was not recreated")
	}
	sf.FreeResource()
}
