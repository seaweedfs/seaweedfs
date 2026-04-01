package topology

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestRaceConditionStress simulates the original issue scenario:
// High concurrent writes causing capacity misjudgment
func TestRaceConditionStress(t *testing.T) {
	// Create a cluster similar to the issue description:
	// 3 volume servers, 200GB each, 5GB volume limit = 40 volumes max per server
	const (
		numServers          = 3
		volumeLimitMB       = 5000                                      // 5GB in MB
		storagePerServerGB  = 200                                       // 200GB per server
		maxVolumesPerServer = storagePerServerGB * 1024 / volumeLimitMB // 200*1024/5000 = 40
		concurrentRequests  = 50                                        // High concurrency like the issue
	)

	// Create test topology
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), uint64(volumeLimitMB)*1024*1024, 5, false)

	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	// Create 3 volume servers with realistic capacity
	servers := make([]*DataNode, numServers)
	for i := 0; i < numServers; i++ {
		dn := NewDataNode(fmt.Sprintf("server%d", i+1))
		rack.LinkChildNode(dn)

		// Set up disk with capacity for 40 volumes
		disk := NewDisk(types.HardDriveType.String())
		disk.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = maxVolumesPerServer
		dn.LinkChildNode(disk)

		servers[i] = dn
	}

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("000") // Single replica like the issue

	option := &VolumeGrowOption{
		Collection:       "test-bucket-large", // Same collection name as issue
		ReplicaPlacement: rp,
		DiskType:         types.HardDriveType,
	}

	// Track results
	var successfulAllocations int64
	var failedAllocations int64
	var totalVolumesCreated int64

	var wg sync.WaitGroup

	// Launch concurrent volume creation requests
	startTime := time.Now()
	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func(requestId int) {
			defer wg.Done()

			// This is the critical test: multiple threads trying to allocate simultaneously
			servers, reservation, err := vg.findEmptySlotsForOneVolume(topo, option, true)

			if err != nil {
				atomic.AddInt64(&failedAllocations, 1)
				t.Logf("Request %d failed: %v", requestId, err)
				return
			}

			// Simulate volume creation delay (like in real scenario)
			time.Sleep(time.Millisecond * 50)

			// Simulate successful volume creation
			for _, server := range servers {
				disk := server.children[NodeId(types.HardDriveType.String())].(*Disk)
				deltaDiskUsage := &DiskUsageCounts{
					volumeCount: 1,
				}
				disk.UpAdjustDiskUsageDelta(types.HardDriveType, deltaDiskUsage)
				atomic.AddInt64(&totalVolumesCreated, 1)
			}

			// Release reservations (simulates successful registration)
			reservation.releaseAllReservations()
			atomic.AddInt64(&successfulAllocations, 1)

		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// Verify results
	t.Logf("Test completed in %v", duration)
	t.Logf("Successful allocations: %d", successfulAllocations)
	t.Logf("Failed allocations: %d", failedAllocations)
	t.Logf("Total volumes created: %d", totalVolumesCreated)

	// Check capacity limits are respected
	totalCapacityUsed := int64(0)
	for i, server := range servers {
		disk := server.children[NodeId(types.HardDriveType.String())].(*Disk)
		volumeCount := disk.diskUsages.getOrCreateDisk(types.HardDriveType).volumeCount
		totalCapacityUsed += volumeCount

		t.Logf("Server %d: %d volumes (max: %d)", i+1, volumeCount, maxVolumesPerServer)

		// Critical test: No server should exceed its capacity
		if volumeCount > maxVolumesPerServer {
			t.Errorf("RACE CONDITION DETECTED: Server %d exceeded capacity: %d > %d",
				i+1, volumeCount, maxVolumesPerServer)
		}
	}

	// Verify totals make sense
	if totalVolumesCreated != totalCapacityUsed {
		t.Errorf("Volume count mismatch: created=%d, actual=%d", totalVolumesCreated, totalCapacityUsed)
	}

	// The total should never exceed the cluster capacity (120 volumes for 3 servers × 40 each)
	maxClusterCapacity := int64(numServers * maxVolumesPerServer)
	if totalCapacityUsed > maxClusterCapacity {
		t.Errorf("RACE CONDITION DETECTED: Cluster capacity exceeded: %d > %d",
			totalCapacityUsed, maxClusterCapacity)
	}

	// With reservations, we should have controlled allocation
	// Total requests = successful + failed should equal concurrentRequests
	if successfulAllocations+failedAllocations != concurrentRequests {
		t.Errorf("Request count mismatch: success=%d + failed=%d != total=%d",
			successfulAllocations, failedAllocations, concurrentRequests)
	}

	t.Logf("Race condition test passed: Capacity limits respected with %d concurrent requests",
		concurrentRequests)
}

// TestCapacityJudgmentAccuracy verifies that the capacity calculation is accurate
// under various load conditions
func TestCapacityJudgmentAccuracy(t *testing.T) {
	// Create a single server with known capacity
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 5*1024*1024*1024, 5, false)

	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	dn := NewDataNode("server1")
	rack.LinkChildNode(dn)

	// Server with capacity for exactly 10 volumes
	disk := NewDisk(types.HardDriveType.String())
	diskUsage := disk.diskUsages.getOrCreateDisk(types.HardDriveType)
	diskUsage.maxVolumeCount = 10
	dn.LinkChildNode(disk)

	// Also set max volume count on the DataNode level (gets propagated up)
	dn.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = 10

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	option := &VolumeGrowOption{
		Collection:       "test",
		ReplicaPlacement: rp,
		DiskType:         types.HardDriveType,
	}

	// Test accurate capacity reporting at each step
	for i := 0; i < 10; i++ {
		// Check available space before reservation
		availableBefore := dn.AvailableSpaceFor(option)
		availableForReservation := dn.AvailableSpaceForReservation(option)

		expectedAvailable := int64(10 - i)
		if availableBefore != expectedAvailable {
			t.Errorf("Step %d: Expected %d available, got %d", i, expectedAvailable, availableBefore)
		}

		if availableForReservation != expectedAvailable {
			t.Errorf("Step %d: Expected %d available for reservation, got %d", i, expectedAvailable, availableForReservation)
		}

		// Try to reserve and allocate
		_, reservation, err := vg.findEmptySlotsForOneVolume(topo, option, true)
		if err != nil {
			t.Fatalf("Step %d: Unexpected reservation failure: %v", i, err)
		}

		// Check that available space for reservation decreased
		availableAfterReservation := dn.AvailableSpaceForReservation(option)
		if availableAfterReservation != expectedAvailable-1 {
			t.Errorf("Step %d: Expected %d available after reservation, got %d",
				i, expectedAvailable-1, availableAfterReservation)
		}

		// Simulate successful volume creation by properly updating disk usage hierarchy
		disk := dn.children[NodeId(types.HardDriveType.String())].(*Disk)

		// Create a volume usage delta to simulate volume creation
		deltaDiskUsage := &DiskUsageCounts{
			volumeCount: 1,
		}

		// Properly propagate the usage up the hierarchy
		disk.UpAdjustDiskUsageDelta(types.HardDriveType, deltaDiskUsage)

		// Debug: Check the volume count after update
		diskUsageOnNode := dn.diskUsages.getOrCreateDisk(types.HardDriveType)
		currentVolumeCount := atomic.LoadInt64(&diskUsageOnNode.volumeCount)
		t.Logf("Step %d: Volume count after update: %d", i, currentVolumeCount)

		// Release reservation
		reservation.releaseAllReservations()

		// Verify final state
		availableAfter := dn.AvailableSpaceFor(option)
		expectedAfter := int64(10 - i - 1)
		if availableAfter != expectedAfter {
			t.Errorf("Step %d: Expected %d available after creation, got %d",
				i, expectedAfter, availableAfter)
			// More debugging
			diskUsageOnNode := dn.diskUsages.getOrCreateDisk(types.HardDriveType)
			maxVolumes := atomic.LoadInt64(&diskUsageOnNode.maxVolumeCount)
			remoteVolumes := atomic.LoadInt64(&diskUsageOnNode.remoteVolumeCount)
			actualVolumeCount := atomic.LoadInt64(&diskUsageOnNode.volumeCount)
			t.Logf("Debug Step %d: max=%d, volume=%d, remote=%d", i, maxVolumes, actualVolumeCount, remoteVolumes)
		}
	}

	// At this point, no more reservations should succeed
	_, _, err := vg.findEmptySlotsForOneVolume(topo, option, true)
	if err == nil {
		t.Error("Expected reservation to fail when at capacity")
	}

	t.Logf("Capacity judgment accuracy test passed")
}

// TestReservationSystemPerformance measures the performance impact of reservations
func TestReservationSystemPerformance(t *testing.T) {
	// Create topology
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	dn := NewDataNode("server1")
	rack.LinkChildNode(dn)

	disk := NewDisk(types.HardDriveType.String())
	disk.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = 1000
	dn.LinkChildNode(disk)

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	option := &VolumeGrowOption{
		Collection:       "test",
		ReplicaPlacement: rp,
		DiskType:         types.HardDriveType,
	}

	// Benchmark reservation operations
	const iterations = 1000

	startTime := time.Now()
	for i := 0; i < iterations; i++ {
		_, reservation, err := vg.findEmptySlotsForOneVolume(topo, option, true)
		if err != nil {
			t.Fatalf("Iteration %d failed: %v", i, err)
		}
		reservation.releaseAllReservations()

		// Simulate volume creation
		diskUsage := dn.diskUsages.getOrCreateDisk(types.HardDriveType)
		atomic.AddInt64(&diskUsage.volumeCount, 1)
	}
	duration := time.Since(startTime)

	avgDuration := duration / iterations
	t.Logf("Performance: %d reservations in %v (avg: %v per reservation)",
		iterations, duration, avgDuration)

	// Performance should be reasonable (less than 1ms per reservation on average)
	if avgDuration > time.Millisecond {
		t.Errorf("Reservation system performance concern: %v per reservation", avgDuration)
	} else {
		t.Logf("Performance test passed: %v per reservation", avgDuration)
	}
}

func TestDisk_GetEcShards_Race(t *testing.T) {
	d := NewDisk("hdd")

	// Pre-populate with one shard
	initialShard := &erasure_coding.EcVolumeInfo{
		VolumeId:   needle.VolumeId(1),
		ShardsInfo: erasure_coding.NewShardsInfo(),
	}
	initialShard.ShardsInfo.Set(erasure_coding.ShardInfo{Id: 0, Size: 100})
	d.AddOrUpdateEcShard(initialShard)

	var wg sync.WaitGroup
	wg.Add(10)

	// Goroutine 1-5: Continuously read shards
	for j := 0; j < 5; j++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				d.GetEcShards()
			}
		}()
	}

	// Goroutine 6-10: Continuously update shards
	for j := 0; j < 5; j++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				shard := &erasure_coding.EcVolumeInfo{
					VolumeId:   needle.VolumeId(i % 100),
					ShardsInfo: erasure_coding.NewShardsInfo(),
				}
				shard.ShardsInfo.Set(erasure_coding.ShardInfo{Id: erasure_coding.ShardId(i % 14), Size: 100})
				d.AddOrUpdateEcShard(shard)
			}
		}()
	}

	wg.Wait()
}
