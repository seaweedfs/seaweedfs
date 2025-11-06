package consumer

import (
	"fmt"
	"sort"
	"time"
)

// RebalancePhase represents the phase of incremental cooperative rebalancing
type RebalancePhase int

const (
	RebalancePhaseNone RebalancePhase = iota
	RebalancePhaseRevocation
	RebalancePhaseAssignment
)

func (rp RebalancePhase) String() string {
	switch rp {
	case RebalancePhaseNone:
		return "None"
	case RebalancePhaseRevocation:
		return "Revocation"
	case RebalancePhaseAssignment:
		return "Assignment"
	default:
		return "Unknown"
	}
}

// IncrementalRebalanceState tracks the state of incremental cooperative rebalancing
type IncrementalRebalanceState struct {
	Phase                RebalancePhase
	RevocationGeneration int32                            // Generation when revocation started
	AssignmentGeneration int32                            // Generation when assignment started
	RevokedPartitions    map[string][]PartitionAssignment // Member ID -> revoked partitions
	PendingAssignments   map[string][]PartitionAssignment // Member ID -> pending assignments
	StartTime            time.Time
	RevocationTimeout    time.Duration
}

// NewIncrementalRebalanceState creates a new incremental rebalance state
func NewIncrementalRebalanceState() *IncrementalRebalanceState {
	return &IncrementalRebalanceState{
		Phase:              RebalancePhaseNone,
		RevokedPartitions:  make(map[string][]PartitionAssignment),
		PendingAssignments: make(map[string][]PartitionAssignment),
		RevocationTimeout:  30 * time.Second, // Default revocation timeout
	}
}

// IncrementalCooperativeAssignmentStrategy implements incremental cooperative rebalancing
// This strategy performs rebalancing in two phases:
// 1. Revocation phase: Members give up partitions that need to be reassigned
// 2. Assignment phase: Members receive new partitions
type IncrementalCooperativeAssignmentStrategy struct {
	rebalanceState *IncrementalRebalanceState
}

func NewIncrementalCooperativeAssignmentStrategy() *IncrementalCooperativeAssignmentStrategy {
	return &IncrementalCooperativeAssignmentStrategy{
		rebalanceState: NewIncrementalRebalanceState(),
	}
}

func (ics *IncrementalCooperativeAssignmentStrategy) Name() string {
	return ProtocolNameCooperativeSticky
}

func (ics *IncrementalCooperativeAssignmentStrategy) Assign(
	members []*GroupMember,
	topicPartitions map[string][]int32,
) map[string][]PartitionAssignment {
	if len(members) == 0 {
		return make(map[string][]PartitionAssignment)
	}

	// Check if we need to start a new rebalance
	if ics.rebalanceState.Phase == RebalancePhaseNone {
		return ics.startIncrementalRebalance(members, topicPartitions)
	}

	// Continue existing rebalance based on current phase
	switch ics.rebalanceState.Phase {
	case RebalancePhaseRevocation:
		return ics.handleRevocationPhase(members, topicPartitions)
	case RebalancePhaseAssignment:
		return ics.handleAssignmentPhase(members, topicPartitions)
	default:
		// Fallback to regular assignment
		return ics.performRegularAssignment(members, topicPartitions)
	}
}

// startIncrementalRebalance initiates a new incremental rebalance
func (ics *IncrementalCooperativeAssignmentStrategy) startIncrementalRebalance(
	members []*GroupMember,
	topicPartitions map[string][]int32,
) map[string][]PartitionAssignment {
	// Calculate ideal assignment
	idealAssignment := ics.calculateIdealAssignment(members, topicPartitions)

	// Determine which partitions need to be revoked
	partitionsToRevoke := ics.calculateRevocations(members, idealAssignment)

	if len(partitionsToRevoke) == 0 {
		// No revocations needed, proceed with regular assignment
		return idealAssignment
	}

	// Start revocation phase
	ics.rebalanceState.Phase = RebalancePhaseRevocation
	ics.rebalanceState.StartTime = time.Now()
	ics.rebalanceState.RevokedPartitions = partitionsToRevoke

	// Return current assignments minus revoked partitions
	return ics.applyRevocations(members, partitionsToRevoke)
}

// handleRevocationPhase manages the revocation phase of incremental rebalancing
func (ics *IncrementalCooperativeAssignmentStrategy) handleRevocationPhase(
	members []*GroupMember,
	topicPartitions map[string][]int32,
) map[string][]PartitionAssignment {
	// Check if revocation timeout has passed
	if time.Since(ics.rebalanceState.StartTime) > ics.rebalanceState.RevocationTimeout {
		// Force move to assignment phase
		ics.rebalanceState.Phase = RebalancePhaseAssignment
		return ics.handleAssignmentPhase(members, topicPartitions)
	}

	// Continue with revoked assignments (members should stop consuming revoked partitions)
	return ics.getCurrentAssignmentsWithRevocations(members)
}

// handleAssignmentPhase manages the assignment phase of incremental rebalancing
func (ics *IncrementalCooperativeAssignmentStrategy) handleAssignmentPhase(
	members []*GroupMember,
	topicPartitions map[string][]int32,
) map[string][]PartitionAssignment {
	// Calculate final assignment including previously revoked partitions
	finalAssignment := ics.calculateIdealAssignment(members, topicPartitions)

	// Complete the rebalance
	ics.rebalanceState.Phase = RebalancePhaseNone
	ics.rebalanceState.RevokedPartitions = make(map[string][]PartitionAssignment)
	ics.rebalanceState.PendingAssignments = make(map[string][]PartitionAssignment)

	return finalAssignment
}

// calculateIdealAssignment computes the ideal partition assignment
func (ics *IncrementalCooperativeAssignmentStrategy) calculateIdealAssignment(
	members []*GroupMember,
	topicPartitions map[string][]int32,
) map[string][]PartitionAssignment {
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

	// Distribute partitions based on subscriptions
	if len(allPartitions) > 0 && len(sortedMembers) > 0 {
		// Group partitions by topic
		partitionsByTopic := make(map[string][]PartitionAssignment)
		for _, partition := range allPartitions {
			partitionsByTopic[partition.Topic] = append(partitionsByTopic[partition.Topic], partition)
		}

		// Assign partitions topic by topic
		for topic, topicPartitions := range partitionsByTopic {
			// Find members subscribed to this topic
			subscribedMembers := make([]*GroupMember, 0)
			for _, member := range sortedMembers {
				for _, subscribedTopic := range member.Subscription {
					if subscribedTopic == topic {
						subscribedMembers = append(subscribedMembers, member)
						break
					}
				}
			}

			if len(subscribedMembers) == 0 {
				continue // No members subscribed to this topic
			}

			// Distribute topic partitions among subscribed members
			partitionsPerMember := len(topicPartitions) / len(subscribedMembers)
			extraPartitions := len(topicPartitions) % len(subscribedMembers)

			partitionIndex := 0
			for i, member := range subscribedMembers {
				// Calculate how many partitions this member should get for this topic
				numPartitions := partitionsPerMember
				if i < extraPartitions {
					numPartitions++
				}

				// Assign partitions to this member
				for j := 0; j < numPartitions && partitionIndex < len(topicPartitions); j++ {
					assignments[member.ID] = append(assignments[member.ID], topicPartitions[partitionIndex])
					partitionIndex++
				}
			}
		}
	}

	return assignments
}

// calculateRevocations determines which partitions need to be revoked for rebalancing
func (ics *IncrementalCooperativeAssignmentStrategy) calculateRevocations(
	members []*GroupMember,
	idealAssignment map[string][]PartitionAssignment,
) map[string][]PartitionAssignment {
	revocations := make(map[string][]PartitionAssignment)

	for _, member := range members {
		currentAssignment := member.Assignment
		memberIdealAssignment := idealAssignment[member.ID]

		// Find partitions that are currently assigned but not in ideal assignment
		currentMap := make(map[string]bool)
		for _, assignment := range currentAssignment {
			key := fmt.Sprintf("%s:%d", assignment.Topic, assignment.Partition)
			currentMap[key] = true
		}

		idealMap := make(map[string]bool)
		for _, assignment := range memberIdealAssignment {
			key := fmt.Sprintf("%s:%d", assignment.Topic, assignment.Partition)
			idealMap[key] = true
		}

		// Identify partitions to revoke
		var toRevoke []PartitionAssignment
		for _, assignment := range currentAssignment {
			key := fmt.Sprintf("%s:%d", assignment.Topic, assignment.Partition)
			if !idealMap[key] {
				toRevoke = append(toRevoke, assignment)
			}
		}

		if len(toRevoke) > 0 {
			revocations[member.ID] = toRevoke
		}
	}

	return revocations
}

// applyRevocations returns current assignments with specified partitions revoked
func (ics *IncrementalCooperativeAssignmentStrategy) applyRevocations(
	members []*GroupMember,
	revocations map[string][]PartitionAssignment,
) map[string][]PartitionAssignment {
	assignments := make(map[string][]PartitionAssignment)

	for _, member := range members {
		assignments[member.ID] = make([]PartitionAssignment, 0)

		// Get revoked partitions for this member
		revokedPartitions := make(map[string]bool)
		if revoked, exists := revocations[member.ID]; exists {
			for _, partition := range revoked {
				key := fmt.Sprintf("%s:%d", partition.Topic, partition.Partition)
				revokedPartitions[key] = true
			}
		}

		// Add current assignments except revoked ones
		for _, assignment := range member.Assignment {
			key := fmt.Sprintf("%s:%d", assignment.Topic, assignment.Partition)
			if !revokedPartitions[key] {
				assignments[member.ID] = append(assignments[member.ID], assignment)
			}
		}
	}

	return assignments
}

// getCurrentAssignmentsWithRevocations returns current assignments with revocations applied
func (ics *IncrementalCooperativeAssignmentStrategy) getCurrentAssignmentsWithRevocations(
	members []*GroupMember,
) map[string][]PartitionAssignment {
	return ics.applyRevocations(members, ics.rebalanceState.RevokedPartitions)
}

// performRegularAssignment performs a regular (non-incremental) assignment as fallback
func (ics *IncrementalCooperativeAssignmentStrategy) performRegularAssignment(
	members []*GroupMember,
	topicPartitions map[string][]int32,
) map[string][]PartitionAssignment {
	// Reset rebalance state
	ics.rebalanceState = NewIncrementalRebalanceState()

	// Use ideal assignment calculation (non-incremental cooperative assignment)
	return ics.calculateIdealAssignment(members, topicPartitions)
}

// GetRebalanceState returns the current rebalance state (for monitoring/debugging)
func (ics *IncrementalCooperativeAssignmentStrategy) GetRebalanceState() *IncrementalRebalanceState {
	return ics.rebalanceState
}

// IsRebalanceInProgress returns true if an incremental rebalance is currently in progress
func (ics *IncrementalCooperativeAssignmentStrategy) IsRebalanceInProgress() bool {
	return ics.rebalanceState.Phase != RebalancePhaseNone
}

// ForceCompleteRebalance forces completion of the current rebalance (for timeout scenarios)
func (ics *IncrementalCooperativeAssignmentStrategy) ForceCompleteRebalance() {
	ics.rebalanceState.Phase = RebalancePhaseNone
	ics.rebalanceState.RevokedPartitions = make(map[string][]PartitionAssignment)
	ics.rebalanceState.PendingAssignments = make(map[string][]PartitionAssignment)
}
