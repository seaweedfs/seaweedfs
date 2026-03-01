package blockvol

import (
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

func TestAdapterImplementsInterface(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "adapter_test.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1024 * 4096, // 1024 blocks
		BlockSize:  4096,
		ExtentSize: 65536,
		WALSize:    1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	adapter := NewBlockVolAdapter(vol)

	// Verify it satisfies the interface.
	var _ iscsi.BlockDevice = adapter

	// Test basic operations through the adapter.
	if adapter.BlockSize() != 4096 {
		t.Fatalf("BlockSize: got %d, want 4096", adapter.BlockSize())
	}
	if adapter.VolumeSize() != 1024*4096 {
		t.Fatalf("VolumeSize: got %d, want %d", adapter.VolumeSize(), 1024*4096)
	}
	if !adapter.IsHealthy() {
		t.Fatal("expected healthy")
	}

	// Write and read back through adapter.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAB
	}
	if err := adapter.WriteAt(0, data); err != nil {
		t.Fatal(err)
	}

	got, err := adapter.ReadAt(0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4096 {
		t.Fatalf("ReadAt length: got %d, want 4096", len(got))
	}
	if got[0] != 0xAB || got[4095] != 0xAB {
		t.Fatal("data mismatch")
	}

	// SyncCache through adapter.
	if err := adapter.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Trim through adapter.
	if err := adapter.Trim(0, 4096); err != nil {
		t.Fatal(err)
	}
}
