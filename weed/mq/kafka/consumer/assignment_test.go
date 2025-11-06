package consumer

import (
	"reflect"
	"sort"
	"testing"
)

func TestRangeAssignmentStrategy(t *testing.T) {
	strategy := &RangeAssignmentStrategy{}

	if strategy.Name() != ProtocolNameRange {
		t.Errorf("Expected strategy name '%s', got '%s'", ProtocolNameRange, strategy.Name())
	}

	// Test with 2 members, 4 partitions on one topic
	members := []*GroupMember{
		{
			ID:           "member1",
			Subscription: []string{"topic1"},
		},
		{
			ID:           "member2",
			Subscription: []string{"topic1"},
		},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1, 2, 3},
	}

	assignments := strategy.Assign(members, topicPartitions)

	// Verify all members have assignments
	if len(assignments) != 2 {
		t.Fatalf("Expected assignments for 2 members, got %d", len(assignments))
	}

	// Verify total partitions assigned
	totalAssigned := 0
	for _, assignment := range assignments {
		totalAssigned += len(assignment)
	}

	if totalAssigned != 4 {
		t.Errorf("Expected 4 total partitions assigned, got %d", totalAssigned)
	}

	// Range assignment should distribute evenly: 2 partitions each
	for memberID, assignment := range assignments {
		if len(assignment) != 2 {
			t.Errorf("Expected 2 partitions for member %s, got %d", memberID, len(assignment))
		}

		// Verify all assignments are for the subscribed topic
		for _, pa := range assignment {
			if pa.Topic != "topic1" {
				t.Errorf("Expected topic 'topic1', got '%s'", pa.Topic)
			}
		}
	}
}

func TestRangeAssignmentStrategy_UnevenPartitions(t *testing.T) {
	strategy := &RangeAssignmentStrategy{}

	// Test with 3 members, 4 partitions - should distribute 2,1,1
	members := []*GroupMember{
		{ID: "member1", Subscription: []string{"topic1"}},
		{ID: "member2", Subscription: []string{"topic1"}},
		{ID: "member3", Subscription: []string{"topic1"}},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1, 2, 3},
	}

	assignments := strategy.Assign(members, topicPartitions)

	// Get assignment counts
	counts := make([]int, 0, 3)
	for _, assignment := range assignments {
		counts = append(counts, len(assignment))
	}
	sort.Ints(counts)

	// Should be distributed as [1, 1, 2] (first member gets extra partition)
	expected := []int{1, 1, 2}
	if !reflect.DeepEqual(counts, expected) {
		t.Errorf("Expected partition distribution %v, got %v", expected, counts)
	}
}

func TestRangeAssignmentStrategy_MultipleTopics(t *testing.T) {
	strategy := &RangeAssignmentStrategy{}

	members := []*GroupMember{
		{ID: "member1", Subscription: []string{"topic1", "topic2"}},
		{ID: "member2", Subscription: []string{"topic1"}},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1},
		"topic2": {0, 1},
	}

	assignments := strategy.Assign(members, topicPartitions)

	// Member1 should get assignments from both topics
	member1Assignments := assignments["member1"]
	topicsAssigned := make(map[string]int)
	for _, pa := range member1Assignments {
		topicsAssigned[pa.Topic]++
	}

	if len(topicsAssigned) != 2 {
		t.Errorf("Expected member1 to be assigned to 2 topics, got %d", len(topicsAssigned))
	}

	// Member2 should only get topic1 assignments
	member2Assignments := assignments["member2"]
	for _, pa := range member2Assignments {
		if pa.Topic != "topic1" {
			t.Errorf("Expected member2 to only get topic1, but got %s", pa.Topic)
		}
	}
}

func TestRoundRobinAssignmentStrategy(t *testing.T) {
	strategy := &RoundRobinAssignmentStrategy{}

	if strategy.Name() != ProtocolNameRoundRobin {
		t.Errorf("Expected strategy name '%s', got '%s'", ProtocolNameRoundRobin, strategy.Name())
	}

	// Test with 2 members, 4 partitions on one topic
	members := []*GroupMember{
		{ID: "member1", Subscription: []string{"topic1"}},
		{ID: "member2", Subscription: []string{"topic1"}},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1, 2, 3},
	}

	assignments := strategy.Assign(members, topicPartitions)

	// Verify all members have assignments
	if len(assignments) != 2 {
		t.Fatalf("Expected assignments for 2 members, got %d", len(assignments))
	}

	// Verify total partitions assigned
	totalAssigned := 0
	for _, assignment := range assignments {
		totalAssigned += len(assignment)
	}

	if totalAssigned != 4 {
		t.Errorf("Expected 4 total partitions assigned, got %d", totalAssigned)
	}

	// Round robin should distribute evenly: 2 partitions each
	for memberID, assignment := range assignments {
		if len(assignment) != 2 {
			t.Errorf("Expected 2 partitions for member %s, got %d", memberID, len(assignment))
		}
	}
}

func TestRoundRobinAssignmentStrategy_MultipleTopics(t *testing.T) {
	strategy := &RoundRobinAssignmentStrategy{}

	members := []*GroupMember{
		{ID: "member1", Subscription: []string{"topic1", "topic2"}},
		{ID: "member2", Subscription: []string{"topic1", "topic2"}},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1},
		"topic2": {0, 1},
	}

	assignments := strategy.Assign(members, topicPartitions)

	// Each member should get 2 partitions (round robin across topics)
	for memberID, assignment := range assignments {
		if len(assignment) != 2 {
			t.Errorf("Expected 2 partitions for member %s, got %d", memberID, len(assignment))
		}
	}

	// Verify no partition is assigned twice
	assignedPartitions := make(map[string]map[int32]bool)
	for _, assignment := range assignments {
		for _, pa := range assignment {
			if assignedPartitions[pa.Topic] == nil {
				assignedPartitions[pa.Topic] = make(map[int32]bool)
			}
			if assignedPartitions[pa.Topic][pa.Partition] {
				t.Errorf("Partition %d of topic %s assigned multiple times", pa.Partition, pa.Topic)
			}
			assignedPartitions[pa.Topic][pa.Partition] = true
		}
	}
}

func TestGetAssignmentStrategy(t *testing.T) {
	rangeStrategy := GetAssignmentStrategy(ProtocolNameRange)
	if rangeStrategy.Name() != ProtocolNameRange {
		t.Errorf("Expected range strategy, got %s", rangeStrategy.Name())
	}

	rrStrategy := GetAssignmentStrategy(ProtocolNameRoundRobin)
	if rrStrategy.Name() != ProtocolNameRoundRobin {
		t.Errorf("Expected roundrobin strategy, got %s", rrStrategy.Name())
	}

	// Unknown strategy should default to range
	defaultStrategy := GetAssignmentStrategy("unknown")
	if defaultStrategy.Name() != ProtocolNameRange {
		t.Errorf("Expected default strategy to be range, got %s", defaultStrategy.Name())
	}
}

func TestConsumerGroup_AssignPartitions(t *testing.T) {
	group := &ConsumerGroup{
		ID:       "test-group",
		Protocol: ProtocolNameRange,
		Members: map[string]*GroupMember{
			"member1": {
				ID:           "member1",
				Subscription: []string{"topic1"},
				State:        MemberStateStable,
			},
			"member2": {
				ID:           "member2",
				Subscription: []string{"topic1"},
				State:        MemberStateStable,
			},
		},
	}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1, 2, 3},
	}

	group.AssignPartitions(topicPartitions)

	// Verify assignments were created
	for memberID, member := range group.Members {
		if len(member.Assignment) == 0 {
			t.Errorf("Expected member %s to have partition assignments", memberID)
		}

		// Verify all assignments are valid
		for _, pa := range member.Assignment {
			if pa.Topic != "topic1" {
				t.Errorf("Unexpected topic assignment: %s", pa.Topic)
			}
			if pa.Partition < 0 || pa.Partition >= 4 {
				t.Errorf("Unexpected partition assignment: %d", pa.Partition)
			}
		}
	}
}

func TestConsumerGroup_GetMemberAssignments(t *testing.T) {
	group := &ConsumerGroup{
		Members: map[string]*GroupMember{
			"member1": {
				ID: "member1",
				Assignment: []PartitionAssignment{
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 1},
				},
			},
		},
	}

	assignments := group.GetMemberAssignments()

	if len(assignments) != 1 {
		t.Fatalf("Expected 1 member assignment, got %d", len(assignments))
	}

	member1Assignments := assignments["member1"]
	if len(member1Assignments) != 2 {
		t.Errorf("Expected 2 partition assignments for member1, got %d", len(member1Assignments))
	}

	// Verify assignment content
	expectedAssignments := []PartitionAssignment{
		{Topic: "topic1", Partition: 0},
		{Topic: "topic1", Partition: 1},
	}

	if !reflect.DeepEqual(member1Assignments, expectedAssignments) {
		t.Errorf("Expected assignments %v, got %v", expectedAssignments, member1Assignments)
	}
}

func TestConsumerGroup_UpdateMemberSubscription(t *testing.T) {
	group := &ConsumerGroup{
		Members: map[string]*GroupMember{
			"member1": {
				ID:           "member1",
				Subscription: []string{"topic1"},
			},
			"member2": {
				ID:           "member2",
				Subscription: []string{"topic2"},
			},
		},
		SubscribedTopics: map[string]bool{
			"topic1": true,
			"topic2": true,
		},
	}

	// Update member1's subscription
	group.UpdateMemberSubscription("member1", []string{"topic1", "topic3"})

	// Verify member subscription updated
	member1 := group.Members["member1"]
	expectedSubscription := []string{"topic1", "topic3"}
	if !reflect.DeepEqual(member1.Subscription, expectedSubscription) {
		t.Errorf("Expected subscription %v, got %v", expectedSubscription, member1.Subscription)
	}

	// Verify group subscribed topics updated
	expectedGroupTopics := []string{"topic1", "topic2", "topic3"}
	actualGroupTopics := group.GetSubscribedTopics()

	if !reflect.DeepEqual(actualGroupTopics, expectedGroupTopics) {
		t.Errorf("Expected group topics %v, got %v", expectedGroupTopics, actualGroupTopics)
	}
}

func TestAssignmentStrategy_EmptyMembers(t *testing.T) {
	rangeStrategy := &RangeAssignmentStrategy{}
	rrStrategy := &RoundRobinAssignmentStrategy{}

	topicPartitions := map[string][]int32{
		"topic1": {0, 1, 2, 3},
	}

	// Both strategies should handle empty members gracefully
	rangeAssignments := rangeStrategy.Assign([]*GroupMember{}, topicPartitions)
	rrAssignments := rrStrategy.Assign([]*GroupMember{}, topicPartitions)

	if len(rangeAssignments) != 0 {
		t.Error("Expected empty assignments for empty members list (range)")
	}

	if len(rrAssignments) != 0 {
		t.Error("Expected empty assignments for empty members list (round robin)")
	}
}
