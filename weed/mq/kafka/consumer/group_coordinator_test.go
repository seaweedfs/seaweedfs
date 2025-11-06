package consumer

import (
	"strings"
	"testing"
	"time"
)

func TestGroupCoordinator_CreateGroup(t *testing.T) {
	gc := NewGroupCoordinator()
	defer gc.Close()

	groupID := "test-group"
	group := gc.GetOrCreateGroup(groupID)

	if group == nil {
		t.Fatal("Expected group to be created")
	}

	if group.ID != groupID {
		t.Errorf("Expected group ID %s, got %s", groupID, group.ID)
	}

	if group.State != GroupStateEmpty {
		t.Errorf("Expected initial state to be Empty, got %s", group.State)
	}

	if group.Generation != 0 {
		t.Errorf("Expected initial generation to be 0, got %d", group.Generation)
	}

	// Getting the same group should return the existing one
	group2 := gc.GetOrCreateGroup(groupID)
	if group2 != group {
		t.Error("Expected to get the same group instance")
	}
}

func TestGroupCoordinator_ValidateSessionTimeout(t *testing.T) {
	gc := NewGroupCoordinator()
	defer gc.Close()

	// Test valid timeouts
	validTimeouts := []int32{6000, 30000, 300000}
	for _, timeout := range validTimeouts {
		if !gc.ValidateSessionTimeout(timeout) {
			t.Errorf("Expected timeout %d to be valid", timeout)
		}
	}

	// Test invalid timeouts
	invalidTimeouts := []int32{1000, 5000, 400000}
	for _, timeout := range invalidTimeouts {
		if gc.ValidateSessionTimeout(timeout) {
			t.Errorf("Expected timeout %d to be invalid", timeout)
		}
	}
}

func TestGroupCoordinator_MemberManagement(t *testing.T) {
	gc := NewGroupCoordinator()
	defer gc.Close()

	group := gc.GetOrCreateGroup("test-group")

	// Add members
	member1 := &GroupMember{
		ID:             "member1",
		ClientID:       "client1",
		SessionTimeout: 30000,
		Subscription:   []string{"topic1", "topic2"},
		State:          MemberStateStable,
		LastHeartbeat:  time.Now(),
	}

	member2 := &GroupMember{
		ID:             "member2",
		ClientID:       "client2",
		SessionTimeout: 30000,
		Subscription:   []string{"topic1"},
		State:          MemberStateStable,
		LastHeartbeat:  time.Now(),
	}

	group.Mu.Lock()
	group.Members[member1.ID] = member1
	group.Members[member2.ID] = member2
	group.Mu.Unlock()

	// Update subscriptions
	group.UpdateMemberSubscription("member1", []string{"topic1", "topic3"})

	group.Mu.RLock()
	updatedMember := group.Members["member1"]
	expectedTopics := []string{"topic1", "topic3"}
	if len(updatedMember.Subscription) != len(expectedTopics) {
		t.Errorf("Expected %d subscribed topics, got %d", len(expectedTopics), len(updatedMember.Subscription))
	}

	// Check group subscribed topics
	if len(group.SubscribedTopics) != 2 { // topic1, topic3
		t.Errorf("Expected 2 group subscribed topics, got %d", len(group.SubscribedTopics))
	}
	group.Mu.RUnlock()
}

func TestGroupCoordinator_Stats(t *testing.T) {
	gc := NewGroupCoordinator()
	defer gc.Close()

	// Create multiple groups in different states
	group1 := gc.GetOrCreateGroup("group1")
	group1.Mu.Lock()
	group1.State = GroupStateStable
	group1.Members["member1"] = &GroupMember{ID: "member1"}
	group1.Members["member2"] = &GroupMember{ID: "member2"}
	group1.Mu.Unlock()

	group2 := gc.GetOrCreateGroup("group2")
	group2.Mu.Lock()
	group2.State = GroupStatePreparingRebalance
	group2.Members["member3"] = &GroupMember{ID: "member3"}
	group2.Mu.Unlock()

	stats := gc.GetGroupStats()

	totalGroups := stats["total_groups"].(int)
	if totalGroups != 2 {
		t.Errorf("Expected 2 total groups, got %d", totalGroups)
	}

	totalMembers := stats["total_members"].(int)
	if totalMembers != 3 {
		t.Errorf("Expected 3 total members, got %d", totalMembers)
	}

	stateCount := stats["group_states"].(map[string]int)
	if stateCount["Stable"] != 1 {
		t.Errorf("Expected 1 stable group, got %d", stateCount["Stable"])
	}

	if stateCount["PreparingRebalance"] != 1 {
		t.Errorf("Expected 1 preparing rebalance group, got %d", stateCount["PreparingRebalance"])
	}
}

func TestGroupCoordinator_Cleanup(t *testing.T) {
	gc := NewGroupCoordinator()
	defer gc.Close()

	// Create a group with an expired member
	group := gc.GetOrCreateGroup("test-group")

	expiredMember := &GroupMember{
		ID:             "expired-member",
		SessionTimeout: 1000,                             // 1 second
		LastHeartbeat:  time.Now().Add(-2 * time.Second), // 2 seconds ago
		State:          MemberStateStable,
	}

	activeMember := &GroupMember{
		ID:             "active-member",
		SessionTimeout: 30000,      // 30 seconds
		LastHeartbeat:  time.Now(), // just now
		State:          MemberStateStable,
	}

	group.Mu.Lock()
	group.Members[expiredMember.ID] = expiredMember
	group.Members[activeMember.ID] = activeMember
	group.Leader = expiredMember.ID // Make expired member the leader
	group.Mu.Unlock()

	// Perform cleanup
	gc.performCleanup()

	group.Mu.RLock()
	defer group.Mu.RUnlock()

	// Expired member should be removed
	if _, exists := group.Members[expiredMember.ID]; exists {
		t.Error("Expected expired member to be removed")
	}

	// Active member should remain
	if _, exists := group.Members[activeMember.ID]; !exists {
		t.Error("Expected active member to remain")
	}

	// Leader should be reset since expired member was leader
	if group.Leader == expiredMember.ID {
		t.Error("Expected leader to be reset after expired member removal")
	}
}

func TestGroupCoordinator_GenerateMemberID(t *testing.T) {
	gc := NewGroupCoordinator()
	defer gc.Close()

	// Test that same client/host combination generates consistent member ID
	id1 := gc.GenerateMemberID("client1", "host1")
	id2 := gc.GenerateMemberID("client1", "host1")

	// Same client/host should generate same ID (deterministic)
	if id1 != id2 {
		t.Errorf("Expected same member ID for same client/host: %s vs %s", id1, id2)
	}

	// Different clients should generate different IDs
	id3 := gc.GenerateMemberID("client2", "host1")
	id4 := gc.GenerateMemberID("client1", "host2")

	if id1 == id3 {
		t.Errorf("Expected different member IDs for different clients: %s vs %s", id1, id3)
	}

	if id1 == id4 {
		t.Errorf("Expected different member IDs for different hosts: %s vs %s", id1, id4)
	}

	// IDs should be properly formatted
	if len(id1) < 10 { // Should be longer than just "consumer-"
		t.Errorf("Expected member ID to be properly formatted, got: %s", id1)
	}

	// Should start with "consumer-" prefix
	if !strings.HasPrefix(id1, "consumer-") {
		t.Errorf("Expected member ID to start with 'consumer-', got: %s", id1)
	}
}
