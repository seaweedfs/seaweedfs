package consumer

import (
	"testing"
)

func TestCooperativeStickyAssignmentStrategy_Name(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()
	if strategy.Name() != ProtocolNameCooperativeSticky {
		t.Errorf("Expected strategy name '%s', got '%s'", ProtocolNameCooperativeSticky, strategy.Name())
	}
}

func TestCooperativeStickyAssignmentStrategy_InitialAssignment(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	members := []*GroupMember{
		{ID: "member1", Subscription: []string{"topic1"}, Assignment: []PartitionAssignment{}},
		{ID: "member2", Subscription: []string{"topic1"}, Assignment: []PartitionAssignment{}},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1, 2, 3},
	}

	assignments := strategy.Assign(members, topicPartitions)

	// Verify all partitions are assigned
	totalAssigned := 0
	for _, assignment := range assignments {
		totalAssigned += len(assignment)
	}

	if totalAssigned != 4 {
		t.Errorf("Expected 4 total partitions assigned, got %d", totalAssigned)
	}

	// Verify fair distribution (2 partitions each)
	for memberID, assignment := range assignments {
		if len(assignment) != 2 {
			t.Errorf("Expected member %s to get 2 partitions, got %d", memberID, len(assignment))
		}
	}

	// Verify no partition is assigned twice
	assignedPartitions := make(map[PartitionAssignment]bool)
	for _, assignment := range assignments {
		for _, pa := range assignment {
			if assignedPartitions[pa] {
				t.Errorf("Partition %v assigned multiple times", pa)
			}
			assignedPartitions[pa] = true
		}
	}
}

func TestCooperativeStickyAssignmentStrategy_StickyBehavior(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// Initial state: member1 has partitions 0,1 and member2 has partitions 2,3
	members := []*GroupMember{
		{
			ID:           "member1",
			Subscription: []string{"topic1"},
			Assignment: []PartitionAssignment{
				{Topic: "topic1", Partition: 0},
				{Topic: "topic1", Partition: 1},
			},
		},
		{
			ID:           "member2",
			Subscription: []string{"topic1"},
			Assignment: []PartitionAssignment{
				{Topic: "topic1", Partition: 2},
				{Topic: "topic1", Partition: 3},
			},
		},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1, 2, 3},
	}

	assignments := strategy.Assign(members, topicPartitions)

	// Verify sticky behavior - existing assignments should be preserved
	member1Assignment := assignments["member1"]
	member2Assignment := assignments["member2"]

	// Check that member1 still has partitions 0 and 1
	hasPartition0 := false
	hasPartition1 := false
	for _, pa := range member1Assignment {
		if pa.Topic == "topic1" && pa.Partition == 0 {
			hasPartition0 = true
		}
		if pa.Topic == "topic1" && pa.Partition == 1 {
			hasPartition1 = true
		}
	}

	if !hasPartition0 || !hasPartition1 {
		t.Errorf("Member1 should retain partitions 0 and 1, got %v", member1Assignment)
	}

	// Check that member2 still has partitions 2 and 3
	hasPartition2 := false
	hasPartition3 := false
	for _, pa := range member2Assignment {
		if pa.Topic == "topic1" && pa.Partition == 2 {
			hasPartition2 = true
		}
		if pa.Topic == "topic1" && pa.Partition == 3 {
			hasPartition3 = true
		}
	}

	if !hasPartition2 || !hasPartition3 {
		t.Errorf("Member2 should retain partitions 2 and 3, got %v", member2Assignment)
	}
}

func TestCooperativeStickyAssignmentStrategy_NewMemberJoin(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// Scenario: member1 has all partitions, member2 joins
	members := []*GroupMember{
		{
			ID:           "member1",
			Subscription: []string{"topic1"},
			Assignment: []PartitionAssignment{
				{Topic: "topic1", Partition: 0},
				{Topic: "topic1", Partition: 1},
				{Topic: "topic1", Partition: 2},
				{Topic: "topic1", Partition: 3},
			},
		},
		{
			ID:           "member2",
			Subscription: []string{"topic1"},
			Assignment:   []PartitionAssignment{}, // New member, no existing assignment
		},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1, 2, 3},
	}

	// First call: revocation phase
	assignments1 := strategy.Assign(members, topicPartitions)

	// Update members with revocation results
	members[0].Assignment = assignments1["member1"]
	members[1].Assignment = assignments1["member2"]

	// Force completion of revocation timeout
	strategy.GetRebalanceState().RevocationTimeout = 0

	// Second call: assignment phase
	assignments := strategy.Assign(members, topicPartitions)

	// Verify fair redistribution (2 partitions each)
	member1Assignment := assignments["member1"]
	member2Assignment := assignments["member2"]

	if len(member1Assignment) != 2 {
		t.Errorf("Expected member1 to have 2 partitions after rebalance, got %d", len(member1Assignment))
	}

	if len(member2Assignment) != 2 {
		t.Errorf("Expected member2 to have 2 partitions after rebalance, got %d", len(member2Assignment))
	}

	// Verify some stickiness - member1 should retain some of its original partitions
	originalPartitions := map[int32]bool{0: true, 1: true, 2: true, 3: true}
	retainedCount := 0
	for _, pa := range member1Assignment {
		if originalPartitions[pa.Partition] {
			retainedCount++
		}
	}

	if retainedCount == 0 {
		t.Error("Member1 should retain at least some of its original partitions (sticky behavior)")
	}

	t.Logf("Member1 retained %d out of 4 original partitions", retainedCount)
}

func TestCooperativeStickyAssignmentStrategy_MemberLeave(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// Scenario: member2 leaves, member1 should get its partitions
	members := []*GroupMember{
		{
			ID:           "member1",
			Subscription: []string{"topic1"},
			Assignment: []PartitionAssignment{
				{Topic: "topic1", Partition: 0},
				{Topic: "topic1", Partition: 1},
			},
		},
		// member2 has left, so it's not in the members list
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1, 2, 3}, // All partitions still need to be assigned
	}

	assignments := strategy.Assign(members, topicPartitions)

	// member1 should get all partitions
	member1Assignment := assignments["member1"]

	if len(member1Assignment) != 4 {
		t.Errorf("Expected member1 to get all 4 partitions after member2 left, got %d", len(member1Assignment))
	}

	// Verify member1 retained its original partitions (sticky behavior)
	hasPartition0 := false
	hasPartition1 := false
	for _, pa := range member1Assignment {
		if pa.Partition == 0 {
			hasPartition0 = true
		}
		if pa.Partition == 1 {
			hasPartition1 = true
		}
	}

	if !hasPartition0 || !hasPartition1 {
		t.Error("Member1 should retain its original partitions 0 and 1")
	}
}

func TestCooperativeStickyAssignmentStrategy_MultipleTopics(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	members := []*GroupMember{
		{
			ID:           "member1",
			Subscription: []string{"topic1", "topic2"},
			Assignment: []PartitionAssignment{
				{Topic: "topic1", Partition: 0},
				{Topic: "topic2", Partition: 0},
			},
		},
		{
			ID:           "member2",
			Subscription: []string{"topic1", "topic2"},
			Assignment: []PartitionAssignment{
				{Topic: "topic1", Partition: 1},
				{Topic: "topic2", Partition: 1},
			},
		},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1},
		"topic2": {0, 1},
	}

	assignments := strategy.Assign(members, topicPartitions)

	// Verify all partitions are assigned
	totalAssigned := 0
	for _, assignment := range assignments {
		totalAssigned += len(assignment)
	}

	if totalAssigned != 4 {
		t.Errorf("Expected 4 total partitions assigned across both topics, got %d", totalAssigned)
	}

	// Verify sticky behavior - each member should retain their original assignments
	member1Assignment := assignments["member1"]
	member2Assignment := assignments["member2"]

	// Check member1 retains topic1:0 and topic2:0
	hasT1P0 := false
	hasT2P0 := false
	for _, pa := range member1Assignment {
		if pa.Topic == "topic1" && pa.Partition == 0 {
			hasT1P0 = true
		}
		if pa.Topic == "topic2" && pa.Partition == 0 {
			hasT2P0 = true
		}
	}

	if !hasT1P0 || !hasT2P0 {
		t.Errorf("Member1 should retain topic1:0 and topic2:0, got %v", member1Assignment)
	}

	// Check member2 retains topic1:1 and topic2:1
	hasT1P1 := false
	hasT2P1 := false
	for _, pa := range member2Assignment {
		if pa.Topic == "topic1" && pa.Partition == 1 {
			hasT1P1 = true
		}
		if pa.Topic == "topic2" && pa.Partition == 1 {
			hasT2P1 = true
		}
	}

	if !hasT1P1 || !hasT2P1 {
		t.Errorf("Member2 should retain topic1:1 and topic2:1, got %v", member2Assignment)
	}
}

func TestCooperativeStickyAssignmentStrategy_UnevenPartitions(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// 5 partitions, 2 members - should distribute 3:2 or 2:3
	members := []*GroupMember{
		{ID: "member1", Subscription: []string{"topic1"}, Assignment: []PartitionAssignment{}},
		{ID: "member2", Subscription: []string{"topic1"}, Assignment: []PartitionAssignment{}},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1, 2, 3, 4},
	}

	assignments := strategy.Assign(members, topicPartitions)

	// Verify all partitions are assigned
	totalAssigned := 0
	for _, assignment := range assignments {
		totalAssigned += len(assignment)
	}

	if totalAssigned != 5 {
		t.Errorf("Expected 5 total partitions assigned, got %d", totalAssigned)
	}

	// Verify fair distribution
	member1Count := len(assignments["member1"])
	member2Count := len(assignments["member2"])

	// Should be 3:2 or 2:3 distribution
	if !((member1Count == 3 && member2Count == 2) || (member1Count == 2 && member2Count == 3)) {
		t.Errorf("Expected 3:2 or 2:3 distribution, got %d:%d", member1Count, member2Count)
	}
}

func TestCooperativeStickyAssignmentStrategy_PartialSubscription(t *testing.T) {
	strategy := NewIncrementalCooperativeAssignmentStrategy()

	// member1 subscribes to both topics, member2 only to topic1
	members := []*GroupMember{
		{ID: "member1", Subscription: []string{"topic1", "topic2"}, Assignment: []PartitionAssignment{}},
		{ID: "member2", Subscription: []string{"topic1"}, Assignment: []PartitionAssignment{}},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1},
		"topic2": {0, 1},
	}

	assignments := strategy.Assign(members, topicPartitions)

	// member1 should get all topic2 partitions since member2 isn't subscribed
	member1Assignment := assignments["member1"]
	member2Assignment := assignments["member2"]

	// Count topic2 partitions for each member
	member1Topic2Count := 0
	member2Topic2Count := 0

	for _, pa := range member1Assignment {
		if pa.Topic == "topic2" {
			member1Topic2Count++
		}
	}

	for _, pa := range member2Assignment {
		if pa.Topic == "topic2" {
			member2Topic2Count++
		}
	}

	if member1Topic2Count != 2 {
		t.Errorf("Expected member1 to get all 2 topic2 partitions, got %d", member1Topic2Count)
	}

	if member2Topic2Count != 0 {
		t.Errorf("Expected member2 to get 0 topic2 partitions (not subscribed), got %d", member2Topic2Count)
	}

	// Both members should get some topic1 partitions
	member1Topic1Count := 0
	member2Topic1Count := 0

	for _, pa := range member1Assignment {
		if pa.Topic == "topic1" {
			member1Topic1Count++
		}
	}

	for _, pa := range member2Assignment {
		if pa.Topic == "topic1" {
			member2Topic1Count++
		}
	}

	if member1Topic1Count+member2Topic1Count != 2 {
		t.Errorf("Expected all topic1 partitions to be assigned, got %d + %d = %d",
			member1Topic1Count, member2Topic1Count, member1Topic1Count+member2Topic1Count)
	}
}

func TestGetAssignmentStrategy_CooperativeSticky(t *testing.T) {
	strategy := GetAssignmentStrategy(ProtocolNameCooperativeSticky)
	if strategy.Name() != ProtocolNameCooperativeSticky {
		t.Errorf("Expected cooperative-sticky strategy, got %s", strategy.Name())
	}

	// Verify it's the correct type
	if _, ok := strategy.(*IncrementalCooperativeAssignmentStrategy); !ok {
		t.Errorf("Expected IncrementalCooperativeAssignmentStrategy, got %T", strategy)
	}
}
