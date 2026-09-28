package topology

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// MockGrpcDialOption simulates grpc connection for testing
type MockGrpcDialOption struct{}

func TestVolumeGrowth_ReservationBasedAllocation(t *testing.T) {
	// Create test topology with single server for predictable behavior
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	// Create data center and rack
	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	// Create single data node with limited capacity
	dn := NewDataNode("server1")
	rack.LinkChildNode(dn)

	// Set up disk with limited capacity (only 5 volumes)
	disk := NewDisk(types.HardDriveType.String())
	disk.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = 5
	dn.LinkChildNode(disk)

	// Test volume growth with reservation
	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("000") // Single copy (no replicas)

	option := &VolumeGrowOption{
		Collection:       "test",
		ReplicaPlacement: rp,
		DiskType:         types.HardDriveType,
	}

	// Try to create volumes and verify reservations work
	for i := 0; i < 5; i++ {
		servers, reservation, err := vg.findEmptySlotsForOneVolume(topo, option, true)
		if err != nil {
			t.Errorf("Failed to find slots with reservation on iteration %d: %v", i, err)
			continue
		}

		if len(servers) != 1 {
			t.Errorf("Expected 1 server for replica placement 000, got %d", len(servers))
		}

		if len(reservation.reservationIds) != 1 {
			t.Errorf("Expected 1 reservation ID, got %d", len(reservation.reservationIds))
		}

		// Verify the reservation is on our expected server
		server := servers[0]
		if server != dn {
			t.Errorf("Expected volume to be allocated on server1, got %s", server.Id())
		}

		// Check available space before and after reservation
		availableBeforeCreation := server.AvailableSpaceFor(option)
		expectedBefore := int64(5 - i)
		if availableBeforeCreation != expectedBefore {
			t.Errorf("Iteration %d: Expected %d base available space, got %d", i, expectedBefore, availableBeforeCreation)
		}

		// Simulate successful volume creation
		// Acquire lock briefly to access children map, then release before updating
		dn.RLock()
		disk := dn.children[NodeId(types.HardDriveType.String())].(*Disk)
		dn.RUnlock()

		deltaDiskUsage := &DiskUsageCounts{
			volumeCount: 1,
		}
		disk.UpAdjustDiskUsageDelta(types.HardDriveType, deltaDiskUsage)

		// Release reservation after successful creation
		reservation.releaseAllReservations()

		// Verify available space after creation
		availableAfterCreation := server.AvailableSpaceFor(option)
		expectedAfter := int64(5 - i - 1)
		if availableAfterCreation != expectedAfter {
			t.Errorf("Iteration %d: Expected %d available space after creation, got %d", i, expectedAfter, availableAfterCreation)
		}
	}

	// After 5 volumes, should have no more capacity
	_, _, err := vg.findEmptySlotsForOneVolume(topo, option, true)
	if err == nil {
		t.Error("Expected volume allocation to fail when server is at capacity")
	}
}

func TestVolumeGrowth_ConcurrentAllocationPreventsRaceCondition(t *testing.T) {
	// Create test topology with very limited capacity
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	// Single data node with capacity for only 5 volumes
	dn := NewDataNode("server1")
	rack.LinkChildNode(dn)

	disk := NewDisk(types.HardDriveType.String())
	disk.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = 5
	dn.LinkChildNode(disk)

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("000") // Single copy (no replicas)

	option := &VolumeGrowOption{
		Collection:       "test",
		ReplicaPlacement: rp,
		DiskType:         types.HardDriveType,
	}

	// Simulate concurrent volume creation attempts
	const concurrentRequests = 10
	var wg sync.WaitGroup
	var successCount, failureCount atomic.Int32
	var commitMutex sync.Mutex // Ensures atomic commit of volume creation + reservation release

	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func(requestId int) {
			defer wg.Done()

			_, reservation, err := vg.findEmptySlotsForOneVolume(topo, option, true)

			if err != nil {
				failureCount.Add(1)
				t.Logf("Request %d failed as expected: %v", requestId, err)
			} else {
				successCount.Add(1)
				t.Logf("Request %d succeeded, got reservation", requestId)

				// Simulate completion: increment volume count BEFORE releasing reservation
				if reservation != nil {
					commitMutex.Lock()

					// First, increment the volume count to reflect the created volume
					// Acquire lock briefly to access children map, then release before updating
					dn.RLock()
					disk := dn.children[NodeId(types.HardDriveType.String())].(*Disk)
					dn.RUnlock()

					deltaDiskUsage := &DiskUsageCounts{
						volumeCount: 1,
					}
					disk.UpAdjustDiskUsageDelta(types.HardDriveType, deltaDiskUsage)

					// Then release the reservation
					reservation.releaseAllReservations()

					commitMutex.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()

	// Collect results
	successes := successCount.Load()
	failures := failureCount.Load()
	total := successes + failures

	if total != concurrentRequests {
		t.Fatalf("Expected %d total attempts recorded, got %d", concurrentRequests, total)
	}

	// At most the available capacity should succeed
	const capacity = 5
	if successes > capacity {
		t.Errorf("Expected no more than %d successful reservations, got %d", capacity, successes)
	}

	// We should see at least the remaining attempts fail
	minExpectedFailures := concurrentRequests - capacity
	if failures < int32(minExpectedFailures) {
		t.Errorf("Expected at least %d failed reservations, got %d", minExpectedFailures, failures)
	}

	// Verify final state matches the number of successful allocations
	finalAvailable := dn.AvailableSpaceFor(option)
	expectedAvailable := int64(capacity - successes)
	if finalAvailable != expectedAvailable {
		t.Errorf("Expected %d available space after allocations, got %d", expectedAvailable, finalAvailable)
	}

	t.Logf("Concurrent test completed: %d successes, %d failures", successes, failures)
}

func TestVolumeGrowth_ReservationFailureRollback(t *testing.T) {
	// Create topology with multiple servers, but limited total capacity
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)

	// Create two servers with different available capacity
	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")
	rack.LinkChildNode(dn1)
	rack.LinkChildNode(dn2)

	// Server 1: 5 available slots
	disk1 := NewDisk(types.HardDriveType.String())
	disk1.diskUsages.getOrCreateDisk(types.HardDriveType).maxVolumeCount = 5
	dn1.LinkChildNode(disk1)

	// Server 2: 0 available slots (full)
	disk2 := NewDisk(types.HardDriveType.String())
	diskUsage2 := disk2.diskUsages.getOrCreateDisk(types.HardDriveType)
	diskUsage2.maxVolumeCount = 5
	diskUsage2.volumeCount = 5
	dn2.LinkChildNode(disk2)

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("010") // requires 2 replicas

	option := &VolumeGrowOption{
		Collection:       "test",
		ReplicaPlacement: rp,
		DiskType:         types.HardDriveType,
	}

	// This should fail because we can't satisfy replica requirements
	// (need 2 servers but only 1 has space)
	_, _, err := vg.findEmptySlotsForOneVolume(topo, option, true)
	if err == nil {
		t.Error("Expected reservation to fail due to insufficient replica capacity")
	}

	// Verify no reservations are left hanging
	available1 := dn1.AvailableSpaceForReservation(option)
	if available1 != 5 {
		t.Errorf("Expected server1 to have all capacity available after failed reservation, got %d", available1)
	}

	available2 := dn2.AvailableSpaceForReservation(option)
	if available2 != 0 {
		t.Errorf("Expected server2 to have no capacity available, got %d", available2)
	}
}

func TestVolumeGrowth_ReservationTimeout(t *testing.T) {
	dn := NewDataNode("server1")
	diskType := types.HardDriveType

	// Set up capacity
	diskUsage := dn.diskUsages.getOrCreateDisk(diskType)
	diskUsage.maxVolumeCount = 5

	// Create a reservation
	reservationId, success := dn.TryReserveCapacity(diskType, 2)
	if !success {
		t.Fatal("Expected successful reservation")
	}

	// Manually set the reservation time to simulate old reservation
	dn.capacityReservations.Lock()
	if reservation, exists := dn.capacityReservations.reservations[reservationId]; exists {
		reservation.createdAt = time.Now().Add(-10 * time.Minute)
	}
	dn.capacityReservations.Unlock()

	// Try another reservation - this should trigger cleanup and succeed
	_, success = dn.TryReserveCapacity(diskType, 3)
	if !success {
		t.Error("Expected reservation to succeed after cleanup of expired reservation")
	}

	// Original reservation should be cleaned up
	option := &VolumeGrowOption{DiskType: diskType}
	available := dn.AvailableSpaceForReservation(option)
	if available != 2 { // 5 - 3 = 2
		t.Errorf("Expected 2 available slots after cleanup and new reservation, got %d", available)
	}
}

func TestVolumeGrowth_ConfigurableReservationTimeout(t *testing.T) {
	origTimeout := VolumeGrowStrategy.ReservationTimeout
	defer func() {
		VolumeGrowStrategy.ReservationTimeout = origTimeout
	}()

	dn := NewDataNode("server1")
	diskType := types.HardDriveType

	// Set up capacity of 5
	diskUsage := dn.diskUsages.getOrCreateDisk(diskType)
	diskUsage.maxVolumeCount = 5

	// 1. Verify default timeout (5 minutes)
	VolumeGrowStrategy.ReservationTimeout = 5 * time.Minute
	resId1, ok := dn.TryReserveCapacity(diskType, 2)
	if !ok {
		t.Fatal("Expected reservation 1 to succeed")
	}

	// Set reservation createdAt to 4 minutes ago (not expired under 5m timeout)
	dn.capacityReservations.Lock()
	if r, exists := dn.capacityReservations.reservations[resId1]; exists {
		r.createdAt = time.Now().Add(-4 * time.Minute)
	}
	dn.capacityReservations.Unlock()

	// Available space should be 5 - 2 = 3. Trying to reserve 4 must fail.
	_, ok = dn.TryReserveCapacity(diskType, 4)
	if ok {
		t.Error("Expected reservation of 4 to fail when 2 slots are still reserved")
	}

	// Set reservation createdAt to 6 minutes ago (expired under 5m timeout)
	dn.capacityReservations.Lock()
	if r, exists := dn.capacityReservations.reservations[resId1]; exists {
		r.createdAt = time.Now().Add(-6 * time.Minute)
	}
	dn.capacityReservations.Unlock()

	// Now reserving 4 should clean up the expired reservation and succeed
	resId2, ok := dn.TryReserveCapacity(diskType, 4)
	if !ok {
		t.Fatal("Expected reservation of 4 to succeed after 6m expired reservation was cleaned up")
	}
	dn.ReleaseReservedCapacity(resId2)

	// 2. Verify custom timeout (1 minute)
	VolumeGrowStrategy.ReservationTimeout = 1 * time.Minute
	resId3, ok := dn.TryReserveCapacity(diskType, 2)
	if !ok {
		t.Fatal("Expected reservation 3 to succeed")
	}

	// Set createdAt to 45 seconds ago (not expired under 1m timeout)
	dn.capacityReservations.Lock()
	if r, exists := dn.capacityReservations.reservations[resId3]; exists {
		r.createdAt = time.Now().Add(-45 * time.Second)
	}
	dn.capacityReservations.Unlock()

	_, ok = dn.TryReserveCapacity(diskType, 4)
	if ok {
		t.Error("Expected reservation of 4 to fail when 2 slots are reserved 45s ago with 1m timeout")
	}

	// Set createdAt to 75 seconds ago (expired under 1m timeout)
	dn.capacityReservations.Lock()
	if r, exists := dn.capacityReservations.reservations[resId3]; exists {
		r.createdAt = time.Now().Add(-75 * time.Second)
	}
	dn.capacityReservations.Unlock()

	resId4, ok := dn.TryReserveCapacity(diskType, 4)
	if !ok {
		t.Fatal("Expected reservation of 4 to succeed after 75s reservation expired under 1m timeout")
	}
	dn.ReleaseReservedCapacity(resId4)

	// 3. Verify non-positive timeout fallback to 5 minutes
	VolumeGrowStrategy.ReservationTimeout = 0
	if VolumeGrowStrategy.GetReservationTimeout() != 5*time.Minute {
		t.Errorf("Expected 0 timeout to fall back to 5m, got %v", VolumeGrowStrategy.GetReservationTimeout())
	}
	VolumeGrowStrategy.ReservationTimeout = -10 * time.Second
	if VolumeGrowStrategy.GetReservationTimeout() != 5*time.Minute {
		t.Errorf("Expected negative timeout to fall back to 5m, got %v", VolumeGrowStrategy.GetReservationTimeout())
	}

	// 4. Expired reservations must not strand capacity: the selection filter
	// reads AvailableSpaceForReservation without calling TryReserveCapacity.
	VolumeGrowStrategy.ReservationTimeout = 1 * time.Minute
	resId5, ok := dn.TryReserveCapacity(diskType, 5)
	if !ok {
		t.Fatal("Expected reservation 5 to succeed")
	}
	dn.capacityReservations.Lock()
	if r, exists := dn.capacityReservations.reservations[resId5]; exists {
		r.createdAt = time.Now().Add(-2 * time.Minute)
	}
	dn.capacityReservations.Unlock()

	option := &VolumeGrowOption{DiskType: diskType}
	if available := dn.AvailableSpaceForReservation(option); available != 5 {
		t.Errorf("Expected expired reservation to free capacity in AvailableSpaceForReservation, got %d", available)
	}
}
