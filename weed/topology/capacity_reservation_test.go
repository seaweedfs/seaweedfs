package topology

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestCapacityReservations_BasicOperations(t *testing.T) {
	cr := newCapacityReservations()
	diskType := types.HardDriveType

	// Test initial state
	if count := cr.getReservedCount(diskType); count != 0 {
		t.Errorf("Expected 0 reserved count initially, got %d", count)
	}

	// Test add reservation
	reservationId := cr.addReservation(diskType, 5)
	if reservationId == "" {
		t.Error("Expected non-empty reservation ID")
	}

	if count := cr.getReservedCount(diskType); count != 5 {
		t.Errorf("Expected 5 reserved count, got %d", count)
	}

	// Test multiple reservations
	cr.addReservation(diskType, 3)
	if count := cr.getReservedCount(diskType); count != 8 {
		t.Errorf("Expected 8 reserved count after second reservation, got %d", count)
	}

	// Test remove reservation
	success := cr.removeReservation(reservationId)
	if !success {
		t.Error("Expected successful removal of existing reservation")
	}

	if count := cr.getReservedCount(diskType); count != 3 {
		t.Errorf("Expected 3 reserved count after removal, got %d", count)
	}

	// Test remove non-existent reservation
	success = cr.removeReservation("non-existent-id")
	if success {
		t.Error("Expected failure when removing non-existent reservation")
	}
}

func TestCapacityReservations_ExpiredCleaning(t *testing.T) {
	cr := newCapacityReservations()
	diskType := types.HardDriveType

	// Add reservations and manipulate their creation time
	reservationId1 := cr.addReservation(diskType, 3)
	reservationId2 := cr.addReservation(diskType, 2)

	// Make one reservation "old"
	cr.Lock()
	if reservation, exists := cr.reservations[reservationId1]; exists {
		reservation.createdAt = time.Now().Add(-10 * time.Minute) // 10 minutes ago
	}
	cr.Unlock()

	// Clean expired reservations (5 minute expiration)
	cr.cleanExpiredReservations(5 * time.Minute)

	// Only the non-expired reservation should remain
	if count := cr.getReservedCount(diskType); count != 2 {
		t.Errorf("Expected 2 reserved count after cleaning, got %d", count)
	}

	// Verify the right reservation was kept
	if !cr.removeReservation(reservationId2) {
		t.Error("Expected recent reservation to still exist")
	}

	if cr.removeReservation(reservationId1) {
		t.Error("Expected old reservation to be cleaned up")
	}
}

func TestCapacityReservations_DifferentDiskTypes(t *testing.T) {
	cr := newCapacityReservations()

	// Add reservations for different disk types
	cr.addReservation(types.HardDriveType, 5)
	cr.addReservation(types.SsdType, 3)

	// Check counts are separate
	if count := cr.getReservedCount(types.HardDriveType); count != 5 {
		t.Errorf("Expected 5 HDD reserved count, got %d", count)
	}

	if count := cr.getReservedCount(types.SsdType); count != 3 {
		t.Errorf("Expected 3 SSD reserved count, got %d", count)
	}
}

func TestNodeImpl_ReservationMethods(t *testing.T) {
	// Create a test data node
	dn := NewDataNode("test-node")
	diskType := types.HardDriveType

	// Set up some capacity
	diskUsage := dn.diskUsages.getOrCreateDisk(diskType)
	diskUsage.maxVolumeCount = 10
	diskUsage.volumeCount = 5 // 5 volumes free initially

	option := &VolumeGrowOption{DiskType: diskType}

	// Test available space calculation
	available := dn.AvailableSpaceFor(option)
	if available != 5 {
		t.Errorf("Expected 5 available slots, got %d", available)
	}

	availableForReservation := dn.AvailableSpaceForReservation(option)
	if availableForReservation != 5 {
		t.Errorf("Expected 5 available slots for reservation, got %d", availableForReservation)
	}

	// Test successful reservation
	reservationId, success := dn.TryReserveCapacity(diskType, 3)
	if !success {
		t.Error("Expected successful reservation")
	}
	if reservationId == "" {
		t.Error("Expected non-empty reservation ID")
	}

	// Available space should be reduced by reservations
	availableForReservation = dn.AvailableSpaceForReservation(option)
	if availableForReservation != 2 {
		t.Errorf("Expected 2 available slots after reservation, got %d", availableForReservation)
	}

	// Base available space should remain unchanged
	available = dn.AvailableSpaceFor(option)
	if available != 5 {
		t.Errorf("Expected base available to remain 5, got %d", available)
	}

	// Test reservation failure when insufficient capacity
	_, success = dn.TryReserveCapacity(diskType, 3)
	if success {
		t.Error("Expected reservation failure due to insufficient capacity")
	}

	// Test release reservation
	dn.ReleaseReservedCapacity(reservationId)
	availableForReservation = dn.AvailableSpaceForReservation(option)
	if availableForReservation != 5 {
		t.Errorf("Expected 5 available slots after release, got %d", availableForReservation)
	}
}

func TestNodeImpl_ConcurrentReservations(t *testing.T) {
	dn := NewDataNode("test-node")
	diskType := types.HardDriveType

	// Set up capacity
	diskUsage := dn.diskUsages.getOrCreateDisk(diskType)
	diskUsage.maxVolumeCount = 10
	diskUsage.volumeCount = 0 // 10 volumes free initially

	// Test concurrent reservations using goroutines
	var wg sync.WaitGroup
	var reservationIds sync.Map
	concurrentRequests := 10
	wg.Add(concurrentRequests)

	for i := 0; i < concurrentRequests; i++ {
		go func(i int) {
			defer wg.Done()
			if reservationId, success := dn.TryReserveCapacity(diskType, 1); success {
				reservationIds.Store(reservationId, true)
				t.Logf("goroutine %d: Successfully reserved %s", i, reservationId)
			} else {
				t.Errorf("goroutine %d: Expected successful reservation", i)
			}
		}(i)
	}

	wg.Wait()

	// Should have no more capacity
	option := &VolumeGrowOption{DiskType: diskType}
	if available := dn.AvailableSpaceForReservation(option); available != 0 {
		t.Errorf("Expected 0 available slots after all reservations, got %d", available)
		// Debug: check total reserved
		reservedCount := dn.capacityReservations.getReservedCount(diskType)
		t.Logf("Debug: Total reserved count: %d", reservedCount)
	}

	// Next reservation should fail
	_, success := dn.TryReserveCapacity(diskType, 1)
	if success {
		t.Error("Expected reservation failure when at capacity")
	}

	// Release all reservations
	reservationIds.Range(func(key, value interface{}) bool {
		dn.ReleaseReservedCapacity(key.(string))
		return true
	})

	// Should have full capacity back
	if available := dn.AvailableSpaceForReservation(option); available != 10 {
		t.Errorf("Expected 10 available slots after releasing all, got %d", available)
	}
}
