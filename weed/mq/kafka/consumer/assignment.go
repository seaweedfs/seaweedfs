package consumer

import (
	"sort"
)

// AssignmentStrategy defines how partitions are assigned to consumers
type AssignmentStrategy interface {
	Name() string
	Assign(members []*GroupMember, topicPartitions map[string][]int32) map[string][]PartitionAssignment
}

// RangeAssignmentStrategy implements the Range assignment strategy
// Assigns partitions in ranges to consumers, similar to Kafka's range assignor
type RangeAssignmentStrategy struct{}

func (r *RangeAssignmentStrategy) Name() string {
	return "range"
}

func (r *RangeAssignmentStrategy) Assign(members []*GroupMember, topicPartitions map[string][]int32) map[string][]PartitionAssignment {
	if len(members) == 0 {
		return make(map[string][]PartitionAssignment)
	}

	assignments := make(map[string][]PartitionAssignment)
	for _, member := range members {
		assignments[member.ID] = make([]PartitionAssignment, 0)
	}

	// Sort members for consistent assignment
	sortedMembers := make([]*GroupMember, len(members))
	copy(sortedMembers, members)
	sort.Slice(sortedMembers, func(i, j int) bool {
		return sortedMembers[i].ID < sortedMembers[j].ID
	})

	// Get all subscribed topics
	subscribedTopics := make(map[string]bool)
	for _, member := range members {
		for _, topic := range member.Subscription {
			subscribedTopics[topic] = true
		}
	}

	// Assign partitions for each topic
	for topic := range subscribedTopics {
		partitions, exists := topicPartitions[topic]
		if !exists {
			continue
		}

		// Sort partitions for consistent assignment
		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i] < partitions[j]
		})

		// Find members subscribed to this topic
		topicMembers := make([]*GroupMember, 0)
		for _, member := range sortedMembers {
			for _, subscribedTopic := range member.Subscription {
				if subscribedTopic == topic {
					topicMembers = append(topicMembers, member)
					break
				}
			}
		}

		if len(topicMembers) == 0 {
			continue
		}

		// Assign partitions to members using range strategy
		numPartitions := len(partitions)
		numMembers := len(topicMembers)
		partitionsPerMember := numPartitions / numMembers
		remainingPartitions := numPartitions % numMembers

		partitionIndex := 0
		for memberIndex, member := range topicMembers {
			// Calculate how many partitions this member should get
			memberPartitions := partitionsPerMember
			if memberIndex < remainingPartitions {
				memberPartitions++
			}

			// Assign partitions to this member
			for i := 0; i < memberPartitions && partitionIndex < numPartitions; i++ {
				assignment := PartitionAssignment{
					Topic:     topic,
					Partition: partitions[partitionIndex],
				}
				assignments[member.ID] = append(assignments[member.ID], assignment)
				partitionIndex++
			}
		}
	}

	return assignments
}

// RoundRobinAssignmentStrategy implements the RoundRobin assignment strategy
// Distributes partitions evenly across all consumers in round-robin fashion
type RoundRobinAssignmentStrategy struct{}

func (rr *RoundRobinAssignmentStrategy) Name() string {
	return "roundrobin"
}

func (rr *RoundRobinAssignmentStrategy) Assign(members []*GroupMember, topicPartitions map[string][]int32) map[string][]PartitionAssignment {
	if len(members) == 0 {
		return make(map[string][]PartitionAssignment)
	}

	assignments := make(map[string][]PartitionAssignment)
	for _, member := range members {
		assignments[member.ID] = make([]PartitionAssignment, 0)
	}

	// Sort members for consistent assignment
	sortedMembers := make([]*GroupMember, len(members))
	copy(sortedMembers, members)
	sort.Slice(sortedMembers, func(i, j int) bool {
		return sortedMembers[i].ID < sortedMembers[j].ID
	})

	// Collect all partition assignments across all topics
	allAssignments := make([]PartitionAssignment, 0)

	// Get all subscribed topics
	subscribedTopics := make(map[string]bool)
	for _, member := range members {
		for _, topic := range member.Subscription {
			subscribedTopics[topic] = true
		}
	}

	// Collect all partitions from all subscribed topics
	for topic := range subscribedTopics {
		partitions, exists := topicPartitions[topic]
		if !exists {
			continue
		}

		for _, partition := range partitions {
			allAssignments = append(allAssignments, PartitionAssignment{
				Topic:     topic,
				Partition: partition,
			})
		}
	}

	// Sort assignments for consistent distribution
	sort.Slice(allAssignments, func(i, j int) bool {
		if allAssignments[i].Topic != allAssignments[j].Topic {
			return allAssignments[i].Topic < allAssignments[j].Topic
		}
		return allAssignments[i].Partition < allAssignments[j].Partition
	})

	// Distribute partitions in round-robin fashion
	memberIndex := 0
	for _, assignment := range allAssignments {
		// Find a member that is subscribed to this topic
		assigned := false
		startIndex := memberIndex

		for !assigned {
			member := sortedMembers[memberIndex]

			// Check if this member is subscribed to the topic
			subscribed := false
			for _, topic := range member.Subscription {
				if topic == assignment.Topic {
					subscribed = true
					break
				}
			}

			if subscribed {
				assignments[member.ID] = append(assignments[member.ID], assignment)
				assigned = true
			}

			memberIndex = (memberIndex + 1) % len(sortedMembers)

			// Prevent infinite loop if no member is subscribed to this topic
			if memberIndex == startIndex && !assigned {
				break
			}
		}
	}

	return assignments
}

// CooperativeStickyAssignmentStrategy implements the cooperative-sticky assignment strategy
// This strategy tries to minimize partition movement during rebalancing while ensuring fairness
type CooperativeStickyAssignmentStrategy struct{}

func (cs *CooperativeStickyAssignmentStrategy) Name() string {
	return "cooperative-sticky"
}

func (cs *CooperativeStickyAssignmentStrategy) Assign(members []*GroupMember, topicPartitions map[string][]int32) map[string][]PartitionAssignment {
	if len(members) == 0 {
		return make(map[string][]PartitionAssignment)
	}

	assignments := make(map[string][]PartitionAssignment)
	for _, member := range members {
		assignments[member.ID] = make([]PartitionAssignment, 0)
	}

	// Sort members for consistent assignment
	sortedMembers := make([]*GroupMember, len(members))
	copy(sortedMembers, members)
	sort.Slice(sortedMembers, func(i, j int) bool {
		return sortedMembers[i].ID < sortedMembers[j].ID
	})

	// Get all subscribed topics
	subscribedTopics := make(map[string]bool)
	for _, member := range members {
		for _, topic := range member.Subscription {
			subscribedTopics[topic] = true
		}
	}

	// Collect all partitions that need assignment
	allPartitions := make([]PartitionAssignment, 0)
	for topic := range subscribedTopics {
		partitions, exists := topicPartitions[topic]
		if !exists {
			continue
		}

		for _, partition := range partitions {
			allPartitions = append(allPartitions, PartitionAssignment{
				Topic:     topic,
				Partition: partition,
			})
		}
	}

	// Sort partitions for consistent assignment
	sort.Slice(allPartitions, func(i, j int) bool {
		if allPartitions[i].Topic != allPartitions[j].Topic {
			return allPartitions[i].Topic < allPartitions[j].Topic
		}
		return allPartitions[i].Partition < allPartitions[j].Partition
	})

	// Calculate target assignment counts for fairness
	totalPartitions := len(allPartitions)
	numMembers := len(sortedMembers)
	baseAssignments := totalPartitions / numMembers
	extraAssignments := totalPartitions % numMembers

	// Phase 1: Try to preserve existing assignments (sticky behavior) but respect fairness
	currentAssignments := make(map[string]map[PartitionAssignment]bool)
	for _, member := range sortedMembers {
		currentAssignments[member.ID] = make(map[PartitionAssignment]bool)
		for _, assignment := range member.Assignment {
			currentAssignments[member.ID][assignment] = true
		}
	}

	// Track which partitions are already assigned
	assignedPartitions := make(map[PartitionAssignment]bool)

	// Preserve existing assignments where possible, but respect target counts
	for i, member := range sortedMembers {
		// Calculate target count for this member
		targetCount := baseAssignments
		if i < extraAssignments {
			targetCount++
		}

		assignedCount := 0
		for assignment := range currentAssignments[member.ID] {
			// Stop if we've reached the target count for this member
			if assignedCount >= targetCount {
				break
			}

			// Check if member is still subscribed to this topic
			subscribed := false
			for _, topic := range member.Subscription {
				if topic == assignment.Topic {
					subscribed = true
					break
				}
			}

			if subscribed && !assignedPartitions[assignment] {
				assignments[member.ID] = append(assignments[member.ID], assignment)
				assignedPartitions[assignment] = true
				assignedCount++
			}
		}
	}

	// Phase 2: Assign remaining partitions using round-robin for fairness
	unassignedPartitions := make([]PartitionAssignment, 0)
	for _, partition := range allPartitions {
		if !assignedPartitions[partition] {
			unassignedPartitions = append(unassignedPartitions, partition)
		}
	}

	// Assign remaining partitions to achieve fairness
	memberIndex := 0
	for _, partition := range unassignedPartitions {
		// Find a member that needs more partitions and is subscribed to this topic
		assigned := false
		startIndex := memberIndex

		for !assigned {
			member := sortedMembers[memberIndex]

			// Check if this member is subscribed to the topic
			subscribed := false
			for _, topic := range member.Subscription {
				if topic == partition.Topic {
					subscribed = true
					break
				}
			}

			if subscribed {
				// Calculate target count for this member
				targetCount := baseAssignments
				if memberIndex < extraAssignments {
					targetCount++
				}

				// Assign if member needs more partitions
				if len(assignments[member.ID]) < targetCount {
					assignments[member.ID] = append(assignments[member.ID], partition)
					assigned = true
				}
			}

			memberIndex = (memberIndex + 1) % numMembers

			// Prevent infinite loop
			if memberIndex == startIndex && !assigned {
				// Force assign to any subscribed member
				for _, member := range sortedMembers {
					subscribed := false
					for _, topic := range member.Subscription {
						if topic == partition.Topic {
							subscribed = true
							break
						}
					}
					if subscribed {
						assignments[member.ID] = append(assignments[member.ID], partition)
						assigned = true
						break
					}
				}
				break
			}
		}
	}

	return assignments
}

// GetAssignmentStrategy returns the appropriate assignment strategy
func GetAssignmentStrategy(name string) AssignmentStrategy {
	switch name {
	case "range":
		return &RangeAssignmentStrategy{}
	case "roundrobin":
		return &RoundRobinAssignmentStrategy{}
	case "cooperative-sticky":
		return &CooperativeStickyAssignmentStrategy{}
	case "incremental-cooperative":
		return NewIncrementalCooperativeAssignmentStrategy()
	default:
		// Default to range strategy
		return &RangeAssignmentStrategy{}
	}
}

// AssignPartitions performs partition assignment for a consumer group
func (group *ConsumerGroup) AssignPartitions(topicPartitions map[string][]int32) {
	if len(group.Members) == 0 {
		return
	}

	// Convert members map to slice
	members := make([]*GroupMember, 0, len(group.Members))
	for _, member := range group.Members {
		if member.State == MemberStateStable || member.State == MemberStatePending {
			members = append(members, member)
		}
	}

	if len(members) == 0 {
		return
	}

	// Get assignment strategy
	strategy := GetAssignmentStrategy(group.Protocol)
	assignments := strategy.Assign(members, topicPartitions)

	// Apply assignments to members
	for memberID, assignment := range assignments {
		if member, exists := group.Members[memberID]; exists {
			member.Assignment = assignment
		}
	}
}

// GetMemberAssignments returns the current partition assignments for all members
func (group *ConsumerGroup) GetMemberAssignments() map[string][]PartitionAssignment {
	group.Mu.RLock()
	defer group.Mu.RUnlock()

	assignments := make(map[string][]PartitionAssignment)
	for memberID, member := range group.Members {
		assignments[memberID] = make([]PartitionAssignment, len(member.Assignment))
		copy(assignments[memberID], member.Assignment)
	}

	return assignments
}

// UpdateMemberSubscription updates a member's topic subscription
func (group *ConsumerGroup) UpdateMemberSubscription(memberID string, topics []string) {
	group.Mu.Lock()
	defer group.Mu.Unlock()

	member, exists := group.Members[memberID]
	if !exists {
		return
	}

	// Update member subscription
	member.Subscription = make([]string, len(topics))
	copy(member.Subscription, topics)

	// Update group's subscribed topics
	group.SubscribedTopics = make(map[string]bool)
	for _, m := range group.Members {
		for _, topic := range m.Subscription {
			group.SubscribedTopics[topic] = true
		}
	}
}

// GetSubscribedTopics returns all topics subscribed by the group
func (group *ConsumerGroup) GetSubscribedTopics() []string {
	group.Mu.RLock()
	defer group.Mu.RUnlock()

	topics := make([]string, 0, len(group.SubscribedTopics))
	for topic := range group.SubscribedTopics {
		topics = append(topics, topic)
	}

	sort.Strings(topics)
	return topics
}
