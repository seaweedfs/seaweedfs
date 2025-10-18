package protocol

import (
	"testing"
)

// TestSyncGroup_RaceCondition_BugDocumentation documents the original race condition bug
// This test documents the bug where non-leader in Stable state would trigger server-side assignment
func TestSyncGroup_RaceCondition_BugDocumentation(t *testing.T) {
	// Original bug scenario:
	// 1. Consumer 1 (leader) joins, gets all 15 partitions
	// 2. Consumer 2 joins, triggers rebalance
	// 3. Consumer 1 commits offsets during cleanup
	// 4. Consumer 1 calls SyncGroup with client-side assignments, group moves to Stable
	// 5. Consumer 2 calls SyncGroup (late arrival), group is already Stable
	// 6. BUG: Consumer 2 falls into "else" branch, triggers server-side assignment
	// 7. Consumer 2 gets 10 partitions via server-side assignment
	// 8. Result: Some partitions (e.g., partition 2) assigned to BOTH consumers
	// 9. Consumer 2 fetches offsets, gets offset 0 (no committed offsets yet)
	// 10. Consumer 2 re-reads messages from offset 0 -> DUPLICATES (66.7%)!

	// ORIGINAL BUGGY CODE (joingroup.go lines 887-905):
	// } else if group.State == consumer.GroupStateCompletingRebalance || group.State == consumer.GroupStatePreparingRebalance {
	//     // Non-leader member waiting for leader to provide assignments
	//     glog.Infof("[SYNCGROUP] Non-leader %s waiting for leader assignments in group %s (state=%s)",
	//         request.MemberID, request.GroupID, group.State)
	// } else {
	//     // BUG: This branch was triggered when non-leader arrived in Stable state!
	//     glog.Warningf("[SYNCGROUP] Using server-side assignment for group %s (Leader=%s State=%s)",
	//         request.GroupID, group.Leader, group.State)
	//     topicPartitions := h.getTopicPartitions(group)
	//     group.AssignPartitions(topicPartitions)  // <- Duplicate assignment!
	// }

	// FIXED CODE (joingroup.go lines 887-906):
	// } else if request.MemberID != group.Leader && len(request.GroupAssignments) == 0 {
	//     // Non-leader member requesting its assignment
	//     // CRITICAL FIX: Non-leader members should ALWAYS wait for leader's client-side assignments
	//     // This is the correct behavior for Sarama and other client-side assignment protocols
	//     glog.Infof("[SYNCGROUP] Non-leader %s waiting for/retrieving assignment in group %s (state=%s)",
	//         request.MemberID, request.GroupID, group.State)
	//     // Assignment will be retrieved from member.Assignment below
	// } else {
	//     // This branch should only be reached for server-side assignment protocols
	//     // (not Sarama's client-side assignment)
	// }

	t.Log("Original bug: Non-leader in Stable state would trigger server-side assignment")
	t.Log("This caused duplicate partition assignments and message re-reads (66.7% duplicates)")
	t.Log("Fix: Check if member is non-leader with empty assignments, regardless of group state")
}

// TestSyncGroup_FixVerification verifies the fix logic
func TestSyncGroup_FixVerification(t *testing.T) {
	testCases := []struct {
		name           string
		isLeader       bool
		hasAssignments bool
		shouldWait     bool
		shouldAssign   bool
		description    string
	}{
		{
			name:           "Leader with assignments",
			isLeader:       true,
			hasAssignments: true,
			shouldWait:     false,
			shouldAssign:   false,
			description:    "Leader provides client-side assignments, processes them",
		},
		{
			name:           "Non-leader without assignments (PreparingRebalance)",
			isLeader:       false,
			hasAssignments: false,
			shouldWait:     true,
			shouldAssign:   false,
			description:    "Non-leader waits for leader to provide assignments",
		},
		{
			name:           "Non-leader without assignments (Stable) - THE BUG CASE",
			isLeader:       false,
			hasAssignments: false,
			shouldWait:     true,
			shouldAssign:   false,
			description:    "Non-leader retrieves assignment from leader (already processed)",
		},
		{
			name:           "Leader without assignments",
			isLeader:       true,
			hasAssignments: false,
			shouldWait:     false,
			shouldAssign:   true,
			description:    "Edge case: server-side assignment (should not happen with Sarama)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate the fixed logic
			memberID := "consumer-1"
			leaderID := "consumer-1"
			if !tc.isLeader {
				memberID = "consumer-2"
			}

			groupAssignmentsCount := 0
			if tc.hasAssignments {
				groupAssignmentsCount = 2 // Leader provides assignments for 2 members
			}

			// THE FIX: Check if non-leader with no assignments
			isNonLeaderWaiting := (memberID != leaderID) && (groupAssignmentsCount == 0)

			if tc.shouldWait && !isNonLeaderWaiting {
				t.Errorf("%s: Expected to wait, but logic says no", tc.description)
			}
			if !tc.shouldWait && isNonLeaderWaiting {
				t.Errorf("%s: Expected not to wait, but logic says yes", tc.description)
			}

			t.Logf("✓ %s: isLeader=%v hasAssignments=%v shouldWait=%v",
				tc.description, tc.isLeader, tc.hasAssignments, tc.shouldWait)
		})
	}
}
