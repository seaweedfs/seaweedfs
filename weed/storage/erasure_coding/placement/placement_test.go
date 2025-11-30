package placement

import (
"testing"
)

// Helper function to create disk candidates for testing
func makeDisk(nodeID string, diskID uint32, dc, rack string, freeSlots int) *DiskCandidate {
	return &DiskCandidate{
		NodeID:         nodeID,
		DiskID:         diskID,
		DataCenter:     dc,
		Rack:           rack,
		VolumeCount:    0,
		MaxVolumeCount: 100,
		ShardCount:     0,
		FreeSlots:      freeSlots,
		LoadCount:      0,
	}
}

func TestSelectDestinations_SingleRack(t *testing.T) {
	// Test: 3 servers in same rack, each with 2 disks, need 6 shards
	// Expected: Should spread across all 6 disks (one per disk)
	disks := []*DiskCandidate{
		makeDisk("server1", 0, "dc1", "rack1", 10),
		makeDisk("server1", 1, "dc1", "rack1", 10),
		makeDisk("server2", 0, "dc1", "rack1", 10),
		makeDisk("server2", 1, "dc1", "rack1", 10),
		makeDisk("server3", 0, "dc1", "rack1", 10),
		makeDisk("server3", 1, "dc1", "rack1", 10),
	}

	config := PlacementConfig{
		ShardsNeeded:           6,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	result, err := SelectDestinations(disks, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.SelectedDisks) != 6 {
		t.Errorf("expected 6 selected disks, got %d", len(result.SelectedDisks))
	}

	// Verify all 3 servers are used
	if result.ServersUsed != 3 {
		t.Errorf("expected 3 servers used, got %d", result.ServersUsed)
	}

	// Verify each disk is unique
	diskSet := make(map[string]bool)
	for _, disk := range result.SelectedDisks {
		key := getDiskKey(disk)
		if diskSet[key] {
			t.Errorf("disk %s selected multiple times", key)
		}
		diskSet[key] = true
	}
}

func TestSelectDestinations_MultipleRacks(t *testing.T) {
	// Test: 2 racks with 2 servers each, each server has 2 disks
	// Need 8 shards
	// Expected: Should spread across all 8 disks
	disks := []*DiskCandidate{
		makeDisk("server1", 0, "dc1", "rack1", 10),
		makeDisk("server1", 1, "dc1", "rack1", 10),
		makeDisk("server2", 0, "dc1", "rack1", 10),
		makeDisk("server2", 1, "dc1", "rack1", 10),
		makeDisk("server3", 0, "dc1", "rack2", 10),
		makeDisk("server3", 1, "dc1", "rack2", 10),
		makeDisk("server4", 0, "dc1", "rack2", 10),
		makeDisk("server4", 1, "dc1", "rack2", 10),
	}

	config := PlacementConfig{
		ShardsNeeded:           8,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	result, err := SelectDestinations(disks, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.SelectedDisks) != 8 {
		t.Errorf("expected 8 selected disks, got %d", len(result.SelectedDisks))
	}

	// Verify all 4 servers are used
	if result.ServersUsed != 4 {
		t.Errorf("expected 4 servers used, got %d", result.ServersUsed)
	}

	// Verify both racks are used
	if result.RacksUsed != 2 {
		t.Errorf("expected 2 racks used, got %d", result.RacksUsed)
	}
}

func TestSelectDestinations_PrefersDifferentServers(t *testing.T) {
	// Test: 4 servers with 4 disks each, need 4 shards
	// Expected: Should use one disk from each server
	disks := []*DiskCandidate{
		makeDisk("server1", 0, "dc1", "rack1", 10),
		makeDisk("server1", 1, "dc1", "rack1", 10),
		makeDisk("server1", 2, "dc1", "rack1", 10),
		makeDisk("server1", 3, "dc1", "rack1", 10),
		makeDisk("server2", 0, "dc1", "rack1", 10),
		makeDisk("server2", 1, "dc1", "rack1", 10),
		makeDisk("server2", 2, "dc1", "rack1", 10),
		makeDisk("server2", 3, "dc1", "rack1", 10),
		makeDisk("server3", 0, "dc1", "rack1", 10),
		makeDisk("server3", 1, "dc1", "rack1", 10),
		makeDisk("server3", 2, "dc1", "rack1", 10),
		makeDisk("server3", 3, "dc1", "rack1", 10),
		makeDisk("server4", 0, "dc1", "rack1", 10),
		makeDisk("server4", 1, "dc1", "rack1", 10),
		makeDisk("server4", 2, "dc1", "rack1", 10),
		makeDisk("server4", 3, "dc1", "rack1", 10),
	}

	config := PlacementConfig{
		ShardsNeeded:           4,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	result, err := SelectDestinations(disks, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.SelectedDisks) != 4 {
		t.Errorf("expected 4 selected disks, got %d", len(result.SelectedDisks))
	}

	// Verify all 4 servers are used (one shard per server)
	if result.ServersUsed != 4 {
		t.Errorf("expected 4 servers used, got %d", result.ServersUsed)
	}

	// Each server should have exactly 1 shard
	for server, count := range result.ShardsPerServer {
		if count != 1 {
			t.Errorf("server %s has %d shards, expected 1", server, count)
		}
	}
}

func TestSelectDestinations_SpilloverToMultipleDisksPerServer(t *testing.T) {
	// Test: 2 servers with 4 disks each, need 6 shards
	// Expected: First pick one from each server (2 shards), then one more from each (4 shards),
	//           then fill remaining from any server (6 shards)
	disks := []*DiskCandidate{
		makeDisk("server1", 0, "dc1", "rack1", 10),
		makeDisk("server1", 1, "dc1", "rack1", 10),
		makeDisk("server1", 2, "dc1", "rack1", 10),
		makeDisk("server1", 3, "dc1", "rack1", 10),
		makeDisk("server2", 0, "dc1", "rack1", 10),
		makeDisk("server2", 1, "dc1", "rack1", 10),
		makeDisk("server2", 2, "dc1", "rack1", 10),
		makeDisk("server2", 3, "dc1", "rack1", 10),
	}

	config := PlacementConfig{
		ShardsNeeded:           6,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	result, err := SelectDestinations(disks, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.SelectedDisks) != 6 {
		t.Errorf("expected 6 selected disks, got %d", len(result.SelectedDisks))
	}

	// Both servers should be used
	if result.ServersUsed != 2 {
		t.Errorf("expected 2 servers used, got %d", result.ServersUsed)
	}

	// Each server should have exactly 3 shards (balanced)
	for server, count := range result.ShardsPerServer {
		if count != 3 {
			t.Errorf("server %s has %d shards, expected 3", server, count)
		}
	}
}

func TestSelectDestinations_MaxShardsPerServer(t *testing.T) {
	// Test: 2 servers with 4 disks each, need 6 shards, max 2 per server
	// Expected: Should only select 4 shards (2 per server limit)
	disks := []*DiskCandidate{
		makeDisk("server1", 0, "dc1", "rack1", 10),
		makeDisk("server1", 1, "dc1", "rack1", 10),
		makeDisk("server1", 2, "dc1", "rack1", 10),
		makeDisk("server1", 3, "dc1", "rack1", 10),
		makeDisk("server2", 0, "dc1", "rack1", 10),
		makeDisk("server2", 1, "dc1", "rack1", 10),
		makeDisk("server2", 2, "dc1", "rack1", 10),
		makeDisk("server2", 3, "dc1", "rack1", 10),
	}

	config := PlacementConfig{
		ShardsNeeded:           6,
		MaxShardsPerServer:     2,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	result, err := SelectDestinations(disks, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should only get 4 shards due to server limit
	if len(result.SelectedDisks) != 4 {
		t.Errorf("expected 4 selected disks (limit 2 per server), got %d", len(result.SelectedDisks))
	}

	// No server should exceed the limit
	for server, count := range result.ShardsPerServer {
		if count > 2 {
			t.Errorf("server %s has %d shards, exceeds limit of 2", server, count)
		}
	}
}

func TestSelectDestinations_14ShardsAcross7Servers(t *testing.T) {
	// Test: Real-world EC scenario - 14 shards across 7 servers with 2 disks each
	// Expected: Should spread evenly (2 shards per server)
	var disks []*DiskCandidate
	for i := 1; i <= 7; i++ {
		serverID := "server" + string(rune('0'+i))
		disks = append(disks, makeDisk(serverID, 0, "dc1", "rack1", 10))
		disks = append(disks, makeDisk(serverID, 1, "dc1", "rack1", 10))
	}

	config := PlacementConfig{
		ShardsNeeded:           14,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	result, err := SelectDestinations(disks, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.SelectedDisks) != 14 {
		t.Errorf("expected 14 selected disks, got %d", len(result.SelectedDisks))
	}

	// All 7 servers should be used
	if result.ServersUsed != 7 {
		t.Errorf("expected 7 servers used, got %d", result.ServersUsed)
	}

	// Each server should have exactly 2 shards
	for server, count := range result.ShardsPerServer {
		if count != 2 {
			t.Errorf("server %s has %d shards, expected 2", server, count)
		}
	}
}

func TestSelectDestinations_FewerServersThanShards(t *testing.T) {
	// Test: Only 3 servers but need 6 shards
	// Expected: Should distribute evenly (2 per server)
	disks := []*DiskCandidate{
		makeDisk("server1", 0, "dc1", "rack1", 10),
		makeDisk("server1", 1, "dc1", "rack1", 10),
		makeDisk("server1", 2, "dc1", "rack1", 10),
		makeDisk("server2", 0, "dc1", "rack1", 10),
		makeDisk("server2", 1, "dc1", "rack1", 10),
		makeDisk("server2", 2, "dc1", "rack1", 10),
		makeDisk("server3", 0, "dc1", "rack1", 10),
		makeDisk("server3", 1, "dc1", "rack1", 10),
		makeDisk("server3", 2, "dc1", "rack1", 10),
	}

	config := PlacementConfig{
		ShardsNeeded:           6,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	result, err := SelectDestinations(disks, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.SelectedDisks) != 6 {
		t.Errorf("expected 6 selected disks, got %d", len(result.SelectedDisks))
	}

	// All 3 servers should be used
	if result.ServersUsed != 3 {
		t.Errorf("expected 3 servers used, got %d", result.ServersUsed)
	}

	// Each server should have exactly 2 shards
	for server, count := range result.ShardsPerServer {
		if count != 2 {
			t.Errorf("server %s has %d shards, expected 2", server, count)
		}
	}
}

func TestSelectDestinations_NoSuitableDisks(t *testing.T) {
	// Test: All disks have no free slots
	disks := []*DiskCandidate{
		{NodeID: "server1", DiskID: 0, DataCenter: "dc1", Rack: "rack1", FreeSlots: 0},
		{NodeID: "server2", DiskID: 0, DataCenter: "dc1", Rack: "rack1", FreeSlots: 0},
	}

	config := PlacementConfig{
		ShardsNeeded:           4,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	_, err := SelectDestinations(disks, config)
	if err == nil {
		t.Error("expected error for no suitable disks, got nil")
	}
}

func TestSelectDestinations_EmptyInput(t *testing.T) {
	config := DefaultConfig()
	_, err := SelectDestinations([]*DiskCandidate{}, config)
	if err == nil {
		t.Error("expected error for empty input, got nil")
	}
}

func TestSelectDestinations_FiltersByLoad(t *testing.T) {
	// Test: Some disks have too high load
	disks := []*DiskCandidate{
		{NodeID: "server1", DiskID: 0, DataCenter: "dc1", Rack: "rack1", FreeSlots: 10, LoadCount: 10},
		{NodeID: "server2", DiskID: 0, DataCenter: "dc1", Rack: "rack1", FreeSlots: 10, LoadCount: 2},
		{NodeID: "server3", DiskID: 0, DataCenter: "dc1", Rack: "rack1", FreeSlots: 10, LoadCount: 1},
	}

	config := PlacementConfig{
		ShardsNeeded:           2,
		MaxTaskLoad:            5,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	result, err := SelectDestinations(disks, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should only select from server2 and server3 (server1 has too high load)
	for _, disk := range result.SelectedDisks {
		if disk.NodeID == "server1" {
			t.Errorf("disk from server1 should not be selected (load too high)")
		}
	}
}

func TestCalculateDiskScore(t *testing.T) {
	// Test that score calculation works as expected
	lowUtilDisk := &DiskCandidate{
		VolumeCount:    10,
		MaxVolumeCount: 100,
		ShardCount:     0,
		LoadCount:      0,
	}

	highUtilDisk := &DiskCandidate{
		VolumeCount:    90,
		MaxVolumeCount: 100,
		ShardCount:     5,
		LoadCount:      5,
	}

	lowScore := calculateDiskScore(lowUtilDisk)
	highScore := calculateDiskScore(highUtilDisk)

	if lowScore <= highScore {
		t.Errorf("low utilization disk should have higher score: low=%f, high=%f", lowScore, highScore)
	}
}

func TestCalculateIdealDistribution(t *testing.T) {
	tests := []struct {
		totalShards int
		numServers  int
		expectedMin int
		expectedMax int
	}{
		{14, 7, 2, 2},    // Even distribution
		{14, 4, 3, 4},    // Uneven: 14/4 = 3 remainder 2
		{6, 3, 2, 2},     // Even distribution
		{7, 3, 2, 3},     // Uneven: 7/3 = 2 remainder 1
		{10, 0, 0, 10},   // Edge case: no servers
		{0, 5, 0, 0},     // Edge case: no shards
	}

	for _, tt := range tests {
		min, max := CalculateIdealDistribution(tt.totalShards, tt.numServers)
		if min != tt.expectedMin || max != tt.expectedMax {
			t.Errorf("CalculateIdealDistribution(%d, %d) = (%d, %d), want (%d, %d)",
tt.totalShards, tt.numServers, min, max, tt.expectedMin, tt.expectedMax)
		}
	}
}

func TestVerifySpread(t *testing.T) {
	result := &PlacementResult{
		ServersUsed: 3,
		RacksUsed:   2,
	}

	// Should pass
	if err := VerifySpread(result, 3, 2); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Should fail - not enough servers
	if err := VerifySpread(result, 4, 2); err == nil {
		t.Error("expected error for insufficient servers")
	}

	// Should fail - not enough racks
	if err := VerifySpread(result, 3, 3); err == nil {
		t.Error("expected error for insufficient racks")
	}
}

func TestSelectDestinations_MultiDC(t *testing.T) {
	// Test: 2 DCs, each with 2 racks, each rack has 2 servers
	disks := []*DiskCandidate{
		// DC1, Rack1
		makeDisk("dc1-r1-s1", 0, "dc1", "rack1", 10),
		makeDisk("dc1-r1-s1", 1, "dc1", "rack1", 10),
		makeDisk("dc1-r1-s2", 0, "dc1", "rack1", 10),
		makeDisk("dc1-r1-s2", 1, "dc1", "rack1", 10),
		// DC1, Rack2
		makeDisk("dc1-r2-s1", 0, "dc1", "rack2", 10),
		makeDisk("dc1-r2-s1", 1, "dc1", "rack2", 10),
		makeDisk("dc1-r2-s2", 0, "dc1", "rack2", 10),
		makeDisk("dc1-r2-s2", 1, "dc1", "rack2", 10),
		// DC2, Rack1
		makeDisk("dc2-r1-s1", 0, "dc2", "rack1", 10),
		makeDisk("dc2-r1-s1", 1, "dc2", "rack1", 10),
		makeDisk("dc2-r1-s2", 0, "dc2", "rack1", 10),
		makeDisk("dc2-r1-s2", 1, "dc2", "rack1", 10),
		// DC2, Rack2
		makeDisk("dc2-r2-s1", 0, "dc2", "rack2", 10),
		makeDisk("dc2-r2-s1", 1, "dc2", "rack2", 10),
		makeDisk("dc2-r2-s2", 0, "dc2", "rack2", 10),
		makeDisk("dc2-r2-s2", 1, "dc2", "rack2", 10),
	}

	config := PlacementConfig{
		ShardsNeeded:           8,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	result, err := SelectDestinations(disks, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.SelectedDisks) != 8 {
		t.Errorf("expected 8 selected disks, got %d", len(result.SelectedDisks))
	}

	// Should use all 4 racks
	if result.RacksUsed != 4 {
		t.Errorf("expected 4 racks used, got %d", result.RacksUsed)
	}

	// Should use both DCs
	if result.DCsUsed != 2 {
		t.Errorf("expected 2 DCs used, got %d", result.DCsUsed)
	}
}

func TestSelectDestinations_SameRackDifferentDC(t *testing.T) {
	// Test: Same rack name in different DCs should be treated as different racks
	disks := []*DiskCandidate{
		makeDisk("dc1-s1", 0, "dc1", "rack1", 10),
		makeDisk("dc2-s1", 0, "dc2", "rack1", 10),
	}

	config := PlacementConfig{
		ShardsNeeded:           2,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	result, err := SelectDestinations(disks, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should use 2 racks (dc1:rack1 and dc2:rack1 are different)
	if result.RacksUsed != 2 {
		t.Errorf("expected 2 racks used (different DCs), got %d", result.RacksUsed)
	}
}
