package consumer

import (
	"fmt"
	"testing"
	"time"
)

func TestIncrementalCooperativeAssignmentStrategy_BasicAssignment(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// Create members
	members := []*GroupMember{
		{
			ID:           "member-1",
			Subscription: []string{"topic-1"},
			Assignment:   []PartitionAssignment{}, // No existing assignment
		},
		{
			ID:           "member-2",
			Subscription: []string{"topic-1"},
			Assignment:   []PartitionAssignment{}, // No existing assignment
		},
	}

	// Topic partitions
	topicPartitions := map[string][]int32{
		"topic-1": {0, 1, 2, 3},
	}

	// First assignment (no existing assignments, should be direct)
	assignments := strategy.Assign(members, topicPartitions)

	// Verify assignments
	if len(assignments) != 2 {
		t.Errorf("Expected 2 member assignments, got %d", len(assignments))
	}

	totalPartitions := 0
	for memberID, partitions := range assignments {
		t.Logf("Member %s assigned %d partitions: %v", memberID, len(partitions), partitions)
		totalPartitions += len(partitions)
	}

	if totalPartitions != 4 {
		t.Errorf("Expected 4 total partitions assigned, got %d", totalPartitions)
	}

	// Should not be in rebalance state for initial assignment
	if strategy.IsRebalanceInProgress() {
		t.Error("Expected no rebalance in progress for initial assignment")
	}
}

func TestIncrementalCooperativeAssignmentStrategy_RebalanceWithRevocation(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// Create members with existing assignments
	members := []*GroupMember{
		{
			ID:           "member-1",
			Subscription: []string{"topic-1"},
			Assignment: []PartitionAssignment{
				{Topic: "topic-1", Partition: 0},
				{Topic: "topic-1", Partition: 1},
				{Topic: "topic-1", Partition: 2},
				{Topic: "topic-1", Partition: 3}, // This member has all partitions
			},
		},
		{
			ID:           "member-2",
			Subscription: []string{"topic-1"},
			Assignment:   []PartitionAssignment{}, // New member with no assignments
		},
	}

	topicPartitions := map[string][]int32{
		"topic-1": {0, 1, 2, 3},
	}

	// First call should start revocation phase
	assignments1 := strategy.Assign(members, topicPartitions)

	// Should be in revocation phase
	if !strategy.IsRebalanceInProgress() {
		t.Error("Expected rebalance to be in progress")
	}

	state := strategy.GetRebalanceState()
	if state.Phase != RebalancePhaseRevocation {
		t.Errorf("Expected revocation phase, got %s", state.Phase)
	}

	// Member-1 should have some partitions revoked
	member1Assignments := assignments1["member-1"]
	if len(member1Assignments) >= 4 {
		t.Errorf("Expected member-1 to have fewer than 4 partitions after revocation, got %d", len(member1Assignments))
	}

	// Member-2 should still have no assignments during revocation
	member2Assignments := assignments1["member-2"]
	if len(member2Assignments) != 0 {
		t.Errorf("Expected member-2 to have 0 partitions during revocation, got %d", len(member2Assignments))
	}

	t.Logf("Revocation phase - Member-1: %d partitions, Member-2: %d partitions",
		len(member1Assignments), len(member2Assignments))

	// Simulate time passing and second call (should move to assignment phase)
	time.Sleep(10 * time.Millisecond)

	// Force move to assignment phase by setting timeout to 0
	state.RevocationTimeout = 0

	assignments2 := strategy.Assign(members, topicPartitions)

	// Should complete rebalance
	if strategy.IsRebalanceInProgress() {
		t.Error("Expected rebalance to be completed")
	}

	// Both members should have partitions now
	member1FinalAssignments := assignments2["member-1"]
	member2FinalAssignments := assignments2["member-2"]

	if len(member1FinalAssignments) == 0 {
		t.Error("Expected member-1 to have some partitions after rebalance")
	}

	if len(member2FinalAssignments) == 0 {
		t.Error("Expected member-2 to have some partitions after rebalance")
	}

	totalFinalPartitions := len(member1FinalAssignments) + len(member2FinalAssignments)
	if totalFinalPartitions != 4 {
		t.Errorf("Expected 4 total partitions after rebalance, got %d", totalFinalPartitions)
	}

	t.Logf("Final assignment - Member-1: %d partitions, Member-2: %d partitions",
		len(member1FinalAssignments), len(member2FinalAssignments))
}

func TestIncrementalCooperativeAssignmentStrategy_NoRevocationNeeded(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// Create members with already balanced assignments
	members := []*GroupMember{
		{
			ID:           "member-1",
			Subscription: []string{"topic-1"},
			Assignment: []PartitionAssignment{
				{Topic: "topic-1", Partition: 0},
				{Topic: "topic-1", Partition: 1},
			},
		},
		{
			ID:           "member-2",
			Subscription: []string{"topic-1"},
			Assignment: []PartitionAssignment{
				{Topic: "topic-1", Partition: 2},
				{Topic: "topic-1", Partition: 3},
			},
		},
	}

	topicPartitions := map[string][]int32{
		"topic-1": {0, 1, 2, 3},
	}

	// Assignment should not trigger rebalance
	assignments := strategy.Assign(members, topicPartitions)

	// Should not be in rebalance state
	if strategy.IsRebalanceInProgress() {
		t.Error("Expected no rebalance in progress when assignments are already balanced")
	}

	// Assignments should remain the same
	member1Assignments := assignments["member-1"]
	member2Assignments := assignments["member-2"]

	if len(member1Assignments) != 2 {
		t.Errorf("Expected member-1 to keep 2 partitions, got %d", len(member1Assignments))
	}

	if len(member2Assignments) != 2 {
		t.Errorf("Expected member-2 to keep 2 partitions, got %d", len(member2Assignments))
	}
}

func TestIncrementalCooperativeAssignmentStrategy_MultipleTopics(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// Create members with mixed topic subscriptions
	members := []*GroupMember{
		{
			ID:           "member-1",
			Subscription: []string{"topic-1", "topic-2"},
			Assignment: []PartitionAssignment{
				{Topic: "topic-1", Partition: 0},
				{Topic: "topic-1", Partition: 1},
				{Topic: "topic-2", Partition: 0},
			},
		},
		{
			ID:           "member-2",
			Subscription: []string{"topic-1"},
			Assignment: []PartitionAssignment{
				{Topic: "topic-1", Partition: 2},
			},
		},
		{
			ID:           "member-3",
			Subscription: []string{"topic-2"},
			Assignment:   []PartitionAssignment{}, // New member
		},
	}

	topicPartitions := map[string][]int32{
		"topic-1": {0, 1, 2},
		"topic-2": {0, 1},
	}

	// Should trigger rebalance to distribute topic-2 partitions
	assignments := strategy.Assign(members, topicPartitions)

	// Verify all partitions are assigned
	allAssignedPartitions := make(map[string]bool)
	for _, memberAssignments := range assignments {
		for _, assignment := range memberAssignments {
			key := fmt.Sprintf("%s:%d", assignment.Topic, assignment.Partition)
			allAssignedPartitions[key] = true
		}
	}

	expectedPartitions := []string{"topic-1:0", "topic-1:1", "topic-1:2", "topic-2:0", "topic-2:1"}
	for _, expected := range expectedPartitions {
		if !allAssignedPartitions[expected] {
			t.Errorf("Expected partition %s to be assigned", expected)
		}
	}

	// Debug: Print all assigned partitions
	t.Logf("All assigned partitions: %v", allAssignedPartitions)
}

func TestIncrementalCooperativeAssignmentStrategy_ForceComplete(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// Start a rebalance - create scenario where member-1 has all partitions but member-2 joins
	members := []*GroupMember{
		{
			ID:           "member-1",
			Subscription: []string{"topic-1"},
			Assignment: []PartitionAssignment{
				{Topic: "topic-1", Partition: 0},
				{Topic: "topic-1", Partition: 1},
				{Topic: "topic-1", Partition: 2},
				{Topic: "topic-1", Partition: 3},
			},
		},
		{
			ID:           "member-2",
			Subscription: []string{"topic-1"},
			Assignment:   []PartitionAssignment{}, // New member
		},
	}

	topicPartitions := map[string][]int32{
		"topic-1": {0, 1, 2, 3},
	}

	// This should start a rebalance (member-2 needs partitions)
	strategy.Assign(members, topicPartitions)

	if !strategy.IsRebalanceInProgress() {
		t.Error("Expected rebalance to be in progress")
	}

	// Force complete the rebalance
	strategy.ForceCompleteRebalance()

	if strategy.IsRebalanceInProgress() {
		t.Error("Expected rebalance to be completed after force complete")
	}

	state := strategy.GetRebalanceState()
	if state.Phase != RebalancePhaseNone {
		t.Errorf("Expected phase to be None after force complete, got %s", state.Phase)
	}
}

func TestIncrementalCooperativeAssignmentStrategy_RevocationTimeout(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// Set a very short revocation timeout for testing
	strategy.rebalanceState.RevocationTimeout = 1 * time.Millisecond

	members := []*GroupMember{
		{
			ID:           "member-1",
			Subscription: []string{"topic-1"},
			Assignment: []PartitionAssignment{
				{Topic: "topic-1", Partition: 0},
				{Topic: "topic-1", Partition: 1},
				{Topic: "topic-1", Partition: 2},
				{Topic: "topic-1", Partition: 3},
			},
		},
		{
			ID:           "member-2",
			Subscription: []string{"topic-1"},
			Assignment:   []PartitionAssignment{},
		},
	}

	topicPartitions := map[string][]int32{
		"topic-1": {0, 1, 2, 3},
	}

	// First call starts revocation
	strategy.Assign(members, topicPartitions)

	if !strategy.IsRebalanceInProgress() {
		t.Error("Expected rebalance to be in progress")
	}

	// Wait for timeout
	time.Sleep(5 * time.Millisecond)

	// Second call should complete due to timeout
	assignments := strategy.Assign(members, topicPartitions)

	if strategy.IsRebalanceInProgress() {
		t.Error("Expected rebalance to be completed after timeout")
	}

	// Both members should have partitions
	member1Assignments := assignments["member-1"]
	member2Assignments := assignments["member-2"]

	if len(member1Assignments) == 0 {
		t.Error("Expected member-1 to have partitions after timeout")
	}

	if len(member2Assignments) == 0 {
		t.Error("Expected member-2 to have partitions after timeout")
	}
}

func TestIncrementalCooperativeAssignmentStrategy_StateTransitions(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// Initial state should be None
	state := strategy.GetRebalanceState()
	if state.Phase != RebalancePhaseNone {
		t.Errorf("Expected initial phase to be None, got %s", state.Phase)
	}

	// Create scenario that requires rebalancing
	members := []*GroupMember{
		{
			ID:           "member-1",
			Subscription: []string{"topic-1"},
			Assignment: []PartitionAssignment{
				{Topic: "topic-1", Partition: 0},
				{Topic: "topic-1", Partition: 1},
				{Topic: "topic-1", Partition: 2},
				{Topic: "topic-1", Partition: 3},
			},
		},
		{
			ID:           "member-2",
			Subscription: []string{"topic-1"},
			Assignment:   []PartitionAssignment{}, // New member
		},
	}

	topicPartitions := map[string][]int32{
		"topic-1": {0, 1, 2, 3}, // Same partitions, but need rebalancing due to new member
	}

	// First call should move to revocation phase
	strategy.Assign(members, topicPartitions)
	state = strategy.GetRebalanceState()
	if state.Phase != RebalancePhaseRevocation {
		t.Errorf("Expected phase to be Revocation, got %s", state.Phase)
	}

	// Force timeout to move to assignment phase
	state.RevocationTimeout = 0
	strategy.Assign(members, topicPartitions)

	// Should complete and return to None
	state = strategy.GetRebalanceState()
	if state.Phase != RebalancePhaseNone {
		t.Errorf("Expected phase to be None after completion, got %s", state.Phase)
	}
}
