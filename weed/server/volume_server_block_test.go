package weed_server

import (
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func createTestBlockVolFile(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1024 * 4096,
		BlockSize:  4096,
		ExtentSize: 65536,
		WALSize:    1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	vol.Close()
	return path
}

func TestBlockServiceDisabledByDefault(t *testing.T) {
	// Empty blockDir means feature is disabled.
	bs := StartBlockService("0.0.0.0:3260", "", "")
	if bs != nil {
		bs.Shutdown()
		t.Fatal("expected nil BlockService when blockDir is empty")
	}

	// Shutdown on nil should be safe (no panic).
	var nilBS *BlockService
	nilBS.Shutdown()
}

func TestBlockServiceStartAndShutdown(t *testing.T) {
	dir := t.TempDir()
	createTestBlockVolFile(t, dir, "testvol.blk")

	bs := StartBlockService("127.0.0.1:0", dir, "iqn.2024-01.com.test:vol.")
	if bs == nil {
		t.Fatal("expected non-nil BlockService")
	}
	defer bs.Shutdown()

	// Verify the volume was registered.
	paths := bs.blockStore.ListBlockVolumes()
	if len(paths) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(paths))
	}

	expected := filepath.Join(dir, "testvol.blk")
	if paths[0] != expected {
		t.Fatalf("expected path %s, got %s", expected, paths[0])
	}
}
