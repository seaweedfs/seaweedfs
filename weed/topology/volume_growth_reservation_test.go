package topology

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// MockGrpcDialOption simulates grpc connection for testing
type MockGrpcDialOption struct{}

// simulateVolumeAllocation mocks the volume allocation process
func simulateVolumeAllocation(server *DataNode, vid needle.VolumeId, option *VolumeGrowOption) error {
	// Simulate some processing time
	time.Sleep(time.Millisecond * 10)
	return nil
}

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
		// Must acquire lock before accessing children map to prevent race condition
		dn.Lock()
		disk := dn.children[NodeId(types.HardDriveType.String())].(*Disk)
		deltaDiskUsage := &DiskUsageCounts{
			volumeCount: 1,
		}
		disk.UpAdjustDiskUsageDelta(types.HardDriveType, deltaDiskUsage)
		dn.Unlock()

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
					// First, increment the volume count to reflect the created volume
					// Must acquire lock before accessing children map to prevent race condition
					dn.Lock()
					disk := dn.children[NodeId(types.HardDriveType.String())].(*Disk)
					deltaDiskUsage := &DiskUsageCounts{
						volumeCount: 1,
					}
					disk.UpAdjustDiskUsageDelta(types.HardDriveType, deltaDiskUsage)
					dn.Unlock()

					// Then release the reservation
					reservation.releaseAllReservations()
				}
			}
		}(i)
	}

	wg.Wait()

	// With reservation system, only 5 requests should succeed (capacity limit)
	// The rest should fail due to insufficient capacity
	if successCount.Load() != 5 {
		t.Errorf("Expected exactly 5 successful reservations, got %d", successCount.Load())
	}

	if failureCount.Load() != 5 {
		t.Errorf("Expected exactly 5 failed reservations, got %d", failureCount.Load())
	}

	// Verify final state
	finalAvailable := dn.AvailableSpaceFor(option)
	if finalAvailable != 0 {
		t.Errorf("Expected 0 available space after all allocations, got %d", finalAvailable)
	}

	t.Logf("Concurrent test completed: %d successes, %d failures", successCount.Load(), failureCount.Load())
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
