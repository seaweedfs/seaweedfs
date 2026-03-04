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

func TestAdapterALUAProvider(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "alua_test.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1024 * 4096,
		BlockSize:  4096,
		ExtentSize: 65536,
		WALSize:    1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	adapter := &BlockVolAdapter{Vol: vol, TPGID: 42}

	// Verify it satisfies the ALUA interface.
	var _ iscsi.ALUAProvider = adapter

	if adapter.TPGroupID() != 42 {
		t.Fatalf("TPGroupID: got %d, want 42", adapter.TPGroupID())
	}

	// RoleNone -> Active/Optimized
	if adapter.ALUAState() != iscsi.ALUAActiveOptimized {
		t.Fatalf("ALUAState: got %d, want %d", adapter.ALUAState(), iscsi.ALUAActiveOptimized)
	}

	// DeviceNAA should have NAA=6 in high nibble.
	naa := adapter.DeviceNAA()
	if naa[0]&0xF0 != 0x60 {
		t.Fatalf("DeviceNAA high nibble: got 0x%02x, want 0x6x", naa[0])
	}
}

func TestRoleToALUA(t *testing.T) {
	tests := []struct {
		role Role
		want uint8
	}{
		{RoleNone, iscsi.ALUAActiveOptimized},
		{RolePrimary, iscsi.ALUAActiveOptimized},
		{RoleReplica, iscsi.ALUAStandby},
		{RoleStale, iscsi.ALUAUnavailable},
		{RoleRebuilding, iscsi.ALUATransitioning},
		{RoleDraining, iscsi.ALUATransitioning},
	}
	for _, tt := range tests {
		got := RoleToALUA(tt.role)
		if got != tt.want {
			t.Errorf("RoleToALUA(%v): got %d, want %d", tt.role, got, tt.want)
		}
	}
}

func TestUUIDToNAA(t *testing.T) {
	var uuid [16]byte
	for i := range uuid {
		uuid[i] = byte(i + 1)
	}
	naa := UUIDToNAA(uuid)
	// High nibble must be 0x6
	if naa[0]&0xF0 != 0x60 {
		t.Fatalf("NAA high nibble: got 0x%02x, want 0x6x", naa[0])
	}
	// Low nibble of first byte comes from uuid[0]
	if naa[0]&0x0F != uuid[0]&0x0F {
		t.Fatalf("NAA low nibble: got 0x%02x, want 0x%02x", naa[0]&0x0F, uuid[0]&0x0F)
	}
	// Bytes 1-7 come from uuid[1:8]
	for i := 1; i < 8; i++ {
		if naa[i] != uuid[i] {
			t.Fatalf("NAA[%d]: got 0x%02x, want 0x%02x", i, naa[i], uuid[i])
		}
	}
}
