package consumer

import (
	"testing"
	"time"
)

func TestRebalanceTimeoutManager_CheckRebalanceTimeouts(t *testing.T) {
	coordinator := NewGroupCoordinator()
	defer coordinator.Close()

	rtm := coordinator.rebalanceTimeoutManager

	// Create a group with a member that has a short rebalance timeout
	group := coordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = GroupStatePreparingRebalance

	member := &GroupMember{
		ID:               "member1",
		ClientID:         "client1",
		SessionTimeout:   30000, // 30 seconds
		RebalanceTimeout: 1000,  // 1 second (very short for testing)
		State:            MemberStatePending,
		LastHeartbeat:    time.Now(),
		JoinedAt:         time.Now().Add(-2 * time.Second), // Joined 2 seconds ago
	}
	group.Members["member1"] = member
	group.Mu.Unlock()

	// Check timeouts - member should be evicted
	rtm.CheckRebalanceTimeouts()

	group.Mu.RLock()
	if len(group.Members) != 0 {
		t.Errorf("Expected member to be evicted due to rebalance timeout, but %d members remain", len(group.Members))
	}

	if group.State != GroupStateEmpty {
		t.Errorf("Expected group state to be Empty after member eviction, got %s", group.State.String())
	}
	group.Mu.RUnlock()
}

func TestRebalanceTimeoutManager_SessionTimeoutFallback(t *testing.T) {
	coordinator := NewGroupCoordinator()
	defer coordinator.Close()

	rtm := coordinator.rebalanceTimeoutManager

	// Create a group with a member that has exceeded session timeout
	group := coordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = GroupStatePreparingRebalance

	member := &GroupMember{
		ID:               "member1",
		ClientID:         "client1",
		SessionTimeout:   1000,  // 1 second
		RebalanceTimeout: 30000, // 30 seconds
		State:            MemberStatePending,
		LastHeartbeat:    time.Now().Add(-2 * time.Second), // Last heartbeat 2 seconds ago
		JoinedAt:         time.Now(),
	}
	group.Members["member1"] = member
	group.Mu.Unlock()

	// Check timeouts - member should be evicted due to session timeout
	rtm.CheckRebalanceTimeouts()

	group.Mu.RLock()
	if len(group.Members) != 0 {
		t.Errorf("Expected member to be evicted due to session timeout, but %d members remain", len(group.Members))
	}
	group.Mu.RUnlock()
}

func TestRebalanceTimeoutManager_LeaderEviction(t *testing.T) {
	coordinator := NewGroupCoordinator()
	defer coordinator.Close()

	rtm := coordinator.rebalanceTimeoutManager

	// Create a group with leader and another member
	group := coordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = GroupStatePreparingRebalance
	group.Leader = "member1"

	// Leader with expired rebalance timeout
	leader := &GroupMember{
		ID:               "member1",
		ClientID:         "client1",
		SessionTimeout:   30000,
		RebalanceTimeout: 1000,
		State:            MemberStatePending,
		LastHeartbeat:    time.Now(),
		JoinedAt:         time.Now().Add(-2 * time.Second),
	}
	group.Members["member1"] = leader

	// Another member that's still valid
	member2 := &GroupMember{
		ID:               "member2",
		ClientID:         "client2",
		SessionTimeout:   30000,
		RebalanceTimeout: 30000,
		State:            MemberStatePending,
		LastHeartbeat:    time.Now(),
		JoinedAt:         time.Now(),
	}
	group.Members["member2"] = member2
	group.Mu.Unlock()

	// Check timeouts - leader should be evicted, new leader selected
	rtm.CheckRebalanceTimeouts()

	group.Mu.RLock()
	if len(group.Members) != 1 {
		t.Errorf("Expected 1 member to remain after leader eviction, got %d", len(group.Members))
	}

	if group.Leader != "member2" {
		t.Errorf("Expected member2 to become new leader, got %s", group.Leader)
	}

	if group.State != GroupStatePreparingRebalance {
		t.Errorf("Expected group to restart rebalancing after leader eviction, got %s", group.State.String())
	}
	group.Mu.RUnlock()
}

func TestRebalanceTimeoutManager_IsRebalanceStuck(t *testing.T) {
	coordinator := NewGroupCoordinator()
	defer coordinator.Close()

	rtm := coordinator.rebalanceTimeoutManager

	// Create a group that's been rebalancing for a while
	group := coordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = GroupStatePreparingRebalance
	group.LastActivity = time.Now().Add(-15 * time.Minute) // 15 minutes ago
	group.Mu.Unlock()

	// Check if rebalance is stuck (max 10 minutes)
	maxDuration := 10 * time.Minute
	if !rtm.IsRebalanceStuck(group, maxDuration) {
		t.Error("Expected rebalance to be detected as stuck")
	}

	// Test with a group that's not stuck
	group.Mu.Lock()
	group.LastActivity = time.Now().Add(-5 * time.Minute) // 5 minutes ago
	group.Mu.Unlock()

	if rtm.IsRebalanceStuck(group, maxDuration) {
		t.Error("Expected rebalance to not be detected as stuck")
	}

	// Test with stable group (should not be stuck)
	group.Mu.Lock()
	group.State = GroupStateStable
	group.LastActivity = time.Now().Add(-15 * time.Minute)
	group.Mu.Unlock()

	if rtm.IsRebalanceStuck(group, maxDuration) {
		t.Error("Stable group should not be detected as stuck")
	}
}

func TestRebalanceTimeoutManager_ForceCompleteRebalance(t *testing.T) {
	coordinator := NewGroupCoordinator()
	defer coordinator.Close()

	rtm := coordinator.rebalanceTimeoutManager

	// Test forcing completion from PreparingRebalance
	group := coordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = GroupStatePreparingRebalance

	member := &GroupMember{
		ID:    "member1",
		State: MemberStatePending,
	}
	group.Members["member1"] = member
	group.Mu.Unlock()

	rtm.ForceCompleteRebalance(group)

	group.Mu.RLock()
	if group.State != GroupStateCompletingRebalance {
		t.Errorf("Expected group state to be CompletingRebalance, got %s", group.State.String())
	}
	group.Mu.RUnlock()

	// Test forcing completion from CompletingRebalance
	rtm.ForceCompleteRebalance(group)

	group.Mu.RLock()
	if group.State != GroupStateStable {
		t.Errorf("Expected group state to be Stable, got %s", group.State.String())
	}

	if member.State != MemberStateStable {
		t.Errorf("Expected member state to be Stable, got %s", member.State.String())
	}
	group.Mu.RUnlock()
}

func TestRebalanceTimeoutManager_GetRebalanceStatus(t *testing.T) {
	coordinator := NewGroupCoordinator()
	defer coordinator.Close()

	rtm := coordinator.rebalanceTimeoutManager

	// Test with non-existent group
	status := rtm.GetRebalanceStatus("non-existent")
	if status != nil {
		t.Error("Expected nil status for non-existent group")
	}

	// Create a group with members
	group := coordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = GroupStatePreparingRebalance
	group.Generation = 5
	group.Leader = "member1"
	group.LastActivity = time.Now().Add(-2 * time.Minute)

	member1 := &GroupMember{
		ID:               "member1",
		State:            MemberStatePending,
		LastHeartbeat:    time.Now().Add(-30 * time.Second),
		JoinedAt:         time.Now().Add(-2 * time.Minute),
		SessionTimeout:   30000,  // 30 seconds
		RebalanceTimeout: 300000, // 5 minutes
	}
	group.Members["member1"] = member1

	member2 := &GroupMember{
		ID:               "member2",
		State:            MemberStatePending,
		LastHeartbeat:    time.Now().Add(-10 * time.Second),
		JoinedAt:         time.Now().Add(-1 * time.Minute),
		SessionTimeout:   60000,  // 1 minute
		RebalanceTimeout: 180000, // 3 minutes
	}
	group.Members["member2"] = member2
	group.Mu.Unlock()

	// Get status
	status = rtm.GetRebalanceStatus("test-group")

	if status == nil {
		t.Fatal("Expected non-nil status")
	}

	if status.GroupID != "test-group" {
		t.Errorf("Expected group ID 'test-group', got %s", status.GroupID)
	}

	if status.State != GroupStatePreparingRebalance {
		t.Errorf("Expected state PreparingRebalance, got %s", status.State.String())
	}

	if status.Generation != 5 {
		t.Errorf("Expected generation 5, got %d", status.Generation)
	}

	if status.MemberCount != 2 {
		t.Errorf("Expected 2 members, got %d", status.MemberCount)
	}

	if status.Leader != "member1" {
		t.Errorf("Expected leader 'member1', got %s", status.Leader)
	}

	if !status.IsRebalancing {
		t.Error("Expected IsRebalancing to be true")
	}

	if len(status.Members) != 2 {
		t.Errorf("Expected 2 member statuses, got %d", len(status.Members))
	}

	// Check member timeout calculations
	for _, memberStatus := range status.Members {
		if memberStatus.SessionTimeRemaining < 0 {
			t.Errorf("Session time remaining should not be negative for member %s", memberStatus.MemberID)
		}

		if memberStatus.RebalanceTimeRemaining < 0 {
			t.Errorf("Rebalance time remaining should not be negative for member %s", memberStatus.MemberID)
		}
	}
}

func TestRebalanceTimeoutManager_DefaultRebalanceTimeout(t *testing.T) {
	coordinator := NewGroupCoordinator()
	defer coordinator.Close()

	rtm := coordinator.rebalanceTimeoutManager

	// Create a group with a member that has no rebalance timeout set (0)
	group := coordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = GroupStatePreparingRebalance

	member := &GroupMember{
		ID:               "member1",
		ClientID:         "client1",
		SessionTimeout:   30000, // 30 seconds
		RebalanceTimeout: 0,     // Not set, should use default
		State:            MemberStatePending,
		LastHeartbeat:    time.Now(),
		JoinedAt:         time.Now().Add(-6 * time.Minute), // Joined 6 minutes ago
	}
	group.Members["member1"] = member
	group.Mu.Unlock()

	// Default rebalance timeout is 5 minutes (300000ms), so member should be evicted
	rtm.CheckRebalanceTimeouts()

	group.Mu.RLock()
	if len(group.Members) != 0 {
		t.Errorf("Expected member to be evicted using default rebalance timeout, but %d members remain", len(group.Members))
	}
	group.Mu.RUnlock()
}
