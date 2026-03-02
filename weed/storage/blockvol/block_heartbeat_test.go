package blockvol

import (
	"math"
	"path/filepath"
	"testing"
	"time"
)

func TestBlockHeartbeat(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "to_info_message_fields", run: testToInfoMessageFields},
		{name: "to_short_info_message", run: testToShortInfoMessage},
		{name: "role_wire_roundtrip", run: testRoleWireRoundtrip},
		{name: "role_from_wire_unknown_maps_to_none", run: testRoleFromWireUnknown},
		{name: "lease_ttl_wire_roundtrip", run: testLeaseTTLWireRoundtrip},
		{name: "lease_ttl_overflow_clamps", run: testLeaseTTLOverflowClamps},
		{name: "disk_type_propagates", run: testDiskTypePropagates},
		{name: "collect_heartbeat_empty", run: testCollectHeartbeatEmpty},
		{name: "collect_heartbeat_multiple", run: testCollectHeartbeatMultiple},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// testToInfoMessageFields creates a vol, writes data, and verifies all fields.
func testToInfoMessageFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hb.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 << 20, // 1MB
		BlockSize:  4096,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer vol.Close()

	// Write one block so WAL head LSN advances.
	data := make([]byte, 4096)
	data[0] = 0xAB
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("write: %v", err)
	}

	msg := ToBlockVolumeInfoMessage(path, "ssd", vol)

	if msg.Path != path {
		t.Errorf("Path = %q, want %q", msg.Path, path)
	}
	if msg.VolumeSize != 1<<20 {
		t.Errorf("VolumeSize = %d, want %d", msg.VolumeSize, 1<<20)
	}
	if msg.BlockSize != 4096 {
		t.Errorf("BlockSize = %d, want 4096", msg.BlockSize)
	}
	if msg.WalHeadLsn == 0 {
		t.Error("WalHeadLsn should be > 0 after a write")
	}
	// Role should be RoleNone (0) for a fresh volume.
	if msg.Role != RoleToWire(RoleNone) {
		t.Errorf("Role = %d, want %d", msg.Role, RoleToWire(RoleNone))
	}
	if msg.DiskType != "ssd" {
		t.Errorf("DiskType = %q, want %q", msg.DiskType, "ssd")
	}
}

// testToShortInfoMessage verifies short message has path/size/blockSize/diskType.
func testToShortInfoMessage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hb_short.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 << 20,
		BlockSize:  4096,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer vol.Close()

	msg := ToBlockVolumeShortInfoMessage(path, "hdd", vol)

	if msg.Path != path {
		t.Errorf("Path = %q, want %q", msg.Path, path)
	}
	if msg.VolumeSize != 1<<20 {
		t.Errorf("VolumeSize = %d, want %d", msg.VolumeSize, 1<<20)
	}
	if msg.BlockSize != 4096 {
		t.Errorf("BlockSize = %d, want 4096", msg.BlockSize)
	}
	if msg.DiskType != "hdd" {
		t.Errorf("DiskType = %q, want %q", msg.DiskType, "hdd")
	}
}

// testRoleWireRoundtrip verifies all Role values roundtrip through wire conversion.
func testRoleWireRoundtrip(t *testing.T) {
	roles := []Role{RoleNone, RolePrimary, RoleReplica, RoleStale, RoleRebuilding, RoleDraining}
	for _, r := range roles {
		wire := RoleToWire(r)
		back := RoleFromWire(wire)
		if back != r {
			t.Errorf("RoleFromWire(RoleToWire(%s)) = %s, want %s", r, back, r)
		}
	}
}

// testRoleFromWireUnknown verifies unknown wire values map to RoleNone.
func testRoleFromWireUnknown(t *testing.T) {
	unknowns := []uint32{100, 255, math.MaxUint32}
	for _, u := range unknowns {
		got := RoleFromWire(u)
		if got != RoleNone {
			t.Errorf("RoleFromWire(%d) = %s, want %s", u, got, RoleNone)
		}
	}
}

// testLeaseTTLWireRoundtrip verifies various durations roundtrip correctly.
func testLeaseTTLWireRoundtrip(t *testing.T) {
	cases := []time.Duration{
		0,
		1 * time.Millisecond,
		500 * time.Millisecond,
		5 * time.Second,
		30 * time.Second,
		10 * time.Minute,
	}
	for _, d := range cases {
		wire := LeaseTTLToWire(d)
		back := LeaseTTLFromWire(wire)
		if back != d {
			t.Errorf("LeaseTTLFromWire(LeaseTTLToWire(%v)) = %v, want %v", d, back, d)
		}
	}
}

// testLeaseTTLOverflowClamps verifies large durations clamp to MaxUint32.
func testLeaseTTLOverflowClamps(t *testing.T) {
	huge := 50 * 24 * time.Hour // 50 days > ~49.7 day uint32 ms limit
	wire := LeaseTTLToWire(huge)
	if wire != math.MaxUint32 {
		t.Errorf("LeaseTTLToWire(50 days) = %d, want %d", wire, uint32(math.MaxUint32))
	}

	// Negative duration should clamp to 0.
	wire = LeaseTTLToWire(-1 * time.Second)
	if wire != 0 {
		t.Errorf("LeaseTTLToWire(-1s) = %d, want 0", wire)
	}
}

// testDiskTypePropagates verifies DiskType flows through both message types.
func testDiskTypePropagates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dt.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 << 20,
		BlockSize:  4096,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer vol.Close()

	info := ToBlockVolumeInfoMessage(path, "nvme", vol)
	if info.DiskType != "nvme" {
		t.Errorf("info DiskType = %q, want %q", info.DiskType, "nvme")
	}

	short := ToBlockVolumeShortInfoMessage(path, "nvme", vol)
	if short.DiskType != "nvme" {
		t.Errorf("short DiskType = %q, want %q", short.DiskType, "nvme")
	}

	// Empty diskType should be allowed (unknown disk).
	info2 := ToBlockVolumeInfoMessage(path, "", vol)
	if info2.DiskType != "" {
		t.Errorf("info DiskType = %q, want empty", info2.DiskType)
	}
}

// testCollectHeartbeatEmpty verifies empty store returns empty slice.
func testCollectHeartbeatEmpty(t *testing.T) {
	store := &testBlockVolumeStore{
		volumes:   make(map[string]*BlockVol),
		diskTypes: make(map[string]string),
	}
	msgs := collectBlockVolumeHeartbeat(store)
	if len(msgs) != 0 {
		t.Errorf("expected empty slice, got %d messages", len(msgs))
	}
}

// testCollectHeartbeatMultiple verifies store with 3 vols returns 3 messages.
func testCollectHeartbeatMultiple(t *testing.T) {
	dir := t.TempDir()
	store := &testBlockVolumeStore{
		volumes:   make(map[string]*BlockVol),
		diskTypes: make(map[string]string),
	}

	paths := []string{"a.blk", "b.blk", "c.blk"}
	dtypes := []string{"ssd", "hdd", "nvme"}
	for i, name := range paths {
		p := filepath.Join(dir, name)
		vol, err := CreateBlockVol(p, CreateOptions{
			VolumeSize: 1 << 20,
			BlockSize:  4096,
		})
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		defer vol.Close()
		store.volumes[p] = vol
		store.diskTypes[p] = dtypes[i]
	}

	msgs := collectBlockVolumeHeartbeat(store)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	// Verify each message has correct fields.
	seen := make(map[string]BlockVolumeInfoMessage)
	for _, m := range msgs {
		seen[m.Path] = m
		if m.VolumeSize != 1<<20 {
			t.Errorf("msg %s: VolumeSize = %d, want %d", m.Path, m.VolumeSize, 1<<20)
		}
		if m.BlockSize != 4096 {
			t.Errorf("msg %s: BlockSize = %d, want 4096", m.Path, m.BlockSize)
		}
	}
	for i, name := range paths {
		p := filepath.Join(dir, name)
		m, ok := seen[p]
		if !ok {
			t.Errorf("missing message for %s", p)
			continue
		}
		if m.DiskType != dtypes[i] {
			t.Errorf("msg %s: DiskType = %q, want %q", p, m.DiskType, dtypes[i])
		}
	}
}

// testBlockVolumeStore is a minimal test double to avoid importing the storage package.
type testBlockVolumeStore struct {
	volumes   map[string]*BlockVol
	diskTypes map[string]string
}

// collectBlockVolumeHeartbeat mirrors BlockVolumeStore.CollectBlockVolumeHeartbeat
// using the test double, exercising the same ToBlockVolumeInfoMessage logic.
func collectBlockVolumeHeartbeat(store *testBlockVolumeStore) []BlockVolumeInfoMessage {
	msgs := make([]BlockVolumeInfoMessage, 0, len(store.volumes))
	for path, vol := range store.volumes {
		msgs = append(msgs, ToBlockVolumeInfoMessage(path, store.diskTypes[path], vol))
	}
	return msgs
}
