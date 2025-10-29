package consumer

import (
	"testing"
	"time"
)

func TestGroupCoordinator_StaticMembership(t *testing.T) {
	gc := NewGroupCoordinator()
	defer gc.Close()

	group := gc.GetOrCreateGroup("test-group")

	// Test static member registration
	instanceID := "static-instance-1"
	member := &GroupMember{
		ID:              "member-1",
		ClientID:        "client-1",
		ClientHost:      "localhost",
		GroupInstanceID: &instanceID,
		SessionTimeout:  30000,
		State:           MemberStatePending,
		LastHeartbeat:   time.Now(),
		JoinedAt:        time.Now(),
	}

	// Add member to group
	group.Members[member.ID] = member
	gc.RegisterStaticMember(group, member)

	// Test finding static member
	foundMember := gc.FindStaticMember(group, instanceID)
	if foundMember == nil {
		t.Error("Expected to find static member, got nil")
	}
	if foundMember.ID != member.ID {
		t.Errorf("Expected member ID %s, got %s", member.ID, foundMember.ID)
	}

	// Test IsStaticMember
	if !gc.IsStaticMember(member) {
		t.Error("Expected member to be static")
	}

	// Test dynamic member (no instance ID)
	dynamicMember := &GroupMember{
		ID:              "member-2",
		ClientID:        "client-2",
		ClientHost:      "localhost",
		GroupInstanceID: nil,
		SessionTimeout:  30000,
		State:           MemberStatePending,
		LastHeartbeat:   time.Now(),
		JoinedAt:        time.Now(),
	}

	if gc.IsStaticMember(dynamicMember) {
		t.Error("Expected member to be dynamic")
	}

	// Test unregistering static member
	gc.UnregisterStaticMember(group, instanceID)
	foundMember = gc.FindStaticMember(group, instanceID)
	if foundMember != nil {
		t.Error("Expected static member to be unregistered")
	}
}

func TestGroupCoordinator_StaticMemberReconnection(t *testing.T) {
	gc := NewGroupCoordinator()
	defer gc.Close()

	group := gc.GetOrCreateGroup("test-group")
	instanceID := "static-instance-1"

	// First connection
	member1 := &GroupMember{
		ID:              "member-1",
		ClientID:        "client-1",
		ClientHost:      "localhost",
		GroupInstanceID: &instanceID,
		SessionTimeout:  30000,
		State:           MemberStatePending,
		LastHeartbeat:   time.Now(),
		JoinedAt:        time.Now(),
	}

	group.Members[member1.ID] = member1
	gc.RegisterStaticMember(group, member1)

	// Simulate disconnection and reconnection with same instance ID
	delete(group.Members, member1.ID)

	// Reconnection with same instance ID should reuse the mapping
	member2 := &GroupMember{
		ID:              "member-2", // Different member ID
		ClientID:        "client-1",
		ClientHost:      "localhost",
		GroupInstanceID: &instanceID, // Same instance ID
		SessionTimeout:  30000,
		State:           MemberStatePending,
		LastHeartbeat:   time.Now(),
		JoinedAt:        time.Now(),
	}

	group.Members[member2.ID] = member2
	gc.RegisterStaticMember(group, member2)

	// Should find the new member with the same instance ID
	foundMember := gc.FindStaticMember(group, instanceID)
	if foundMember == nil {
		t.Error("Expected to find static member after reconnection")
	}
	if foundMember.ID != member2.ID {
		t.Errorf("Expected member ID %s, got %s", member2.ID, foundMember.ID)
	}
}

func TestGroupCoordinator_StaticMembershipEdgeCases(t *testing.T) {
	gc := NewGroupCoordinator()
	defer gc.Close()

	group := gc.GetOrCreateGroup("test-group")

	// Test empty instance ID
	member := &GroupMember{
		ID:              "member-1",
		ClientID:        "client-1",
		ClientHost:      "localhost",
		GroupInstanceID: nil,
		SessionTimeout:  30000,
		State:           MemberStatePending,
		LastHeartbeat:   time.Now(),
		JoinedAt:        time.Now(),
	}

	gc.RegisterStaticMember(group, member) // Should be no-op
	foundMember := gc.FindStaticMember(group, "")
	if foundMember != nil {
		t.Error("Expected not to find member with empty instance ID")
	}

	// Test empty string instance ID
	emptyInstanceID := ""
	member.GroupInstanceID = &emptyInstanceID
	gc.RegisterStaticMember(group, member) // Should be no-op
	foundMember = gc.FindStaticMember(group, emptyInstanceID)
	if foundMember != nil {
		t.Error("Expected not to find member with empty string instance ID")
	}

	// Test unregistering non-existent instance ID
	gc.UnregisterStaticMember(group, "non-existent") // Should be no-op
}

func TestGroupCoordinator_StaticMembershipConcurrency(t *testing.T) {
	gc := NewGroupCoordinator()
	defer gc.Close()

	group := gc.GetOrCreateGroup("test-group")
	instanceID := "static-instance-1"

	// Test concurrent access
	done := make(chan bool, 2)

	// Goroutine 1: Register static member
	go func() {
		member := &GroupMember{
			ID:              "member-1",
			ClientID:        "client-1",
			ClientHost:      "localhost",
			GroupInstanceID: &instanceID,
			SessionTimeout:  30000,
			State:           MemberStatePending,
			LastHeartbeat:   time.Now(),
			JoinedAt:        time.Now(),
		}
		group.Members[member.ID] = member
		gc.RegisterStaticMember(group, member)
		done <- true
	}()

	// Goroutine 2: Find static member
	go func() {
		time.Sleep(10 * time.Millisecond) // Small delay to ensure registration happens first
		foundMember := gc.FindStaticMember(group, instanceID)
		if foundMember == nil {
			t.Error("Expected to find static member in concurrent access")
		}
		done <- true
	}()

	// Wait for both goroutines to complete
	<-done
	<-done
}
