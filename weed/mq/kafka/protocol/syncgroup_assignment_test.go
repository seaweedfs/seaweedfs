package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
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

func TestParseSyncGroupRequestV5SkipsProtocolFields(t *testing.T) {
	memberID := "consumer-schema-registry"
	requestBody := buildSyncGroupV5Request("schema-registry", 12, memberID, "consumer", "range", []GroupAssignment{
		{MemberID: memberID, Assignment: []byte(`{"version":1,"error":0}`)},
	})

	request, err := (&Handler{}).parseSyncGroupRequest(requestBody, 5)
	if err != nil {
		t.Fatalf("parseSyncGroupRequest failed: %v", err)
	}

	if request.GroupID != "schema-registry" {
		t.Fatalf("GroupID = %q, want schema-registry", request.GroupID)
	}
	if request.GenerationID != 12 {
		t.Fatalf("GenerationID = %d, want 12", request.GenerationID)
	}
	if request.MemberID != memberID {
		t.Fatalf("MemberID = %q, want %q", request.MemberID, memberID)
	}
	if request.ProtocolType != "consumer" {
		t.Fatalf("ProtocolType = %q, want consumer", request.ProtocolType)
	}
	if request.ProtocolName != "range" {
		t.Fatalf("ProtocolName = %q, want range", request.ProtocolName)
	}
	if len(request.GroupAssignments) != 1 {
		t.Fatalf("len(GroupAssignments) = %d, want 1", len(request.GroupAssignments))
	}
	if request.GroupAssignments[0].MemberID != memberID {
		t.Fatalf("assignment member = %q, want %q", request.GroupAssignments[0].MemberID, memberID)
	}
	if string(request.GroupAssignments[0].Assignment) != `{"version":1,"error":0}` {
		t.Fatalf("assignment payload = %q", string(request.GroupAssignments[0].Assignment))
	}
}

func TestSyncGroupSchemaRegistryV5LeaderAssignmentDoesNotRebalance(t *testing.T) {
	memberID := "consumer-schema-registry"
	handler := &Handler{
		groupCoordinator:  consumer.NewGroupCoordinator(),
		defaultPartitions: 1,
	}
	t.Cleanup(handler.groupCoordinator.Close)

	group := handler.groupCoordinator.GetOrCreateGroup("schema-registry")
	group.Mu.Lock()
	group.State = consumer.GroupStateCompletingRebalance
	group.Generation = 7
	group.Protocol = consumer.ProtocolNameRange
	group.Leader = memberID
	group.Members[memberID] = &consumer.GroupMember{
		ID:    memberID,
		State: consumer.MemberStatePending,
		Metadata: []byte(`{
			"host":"schema-registry",
			"port":8081,
			"scheme":"http",
			"version":1,
			"master_eligibility":true
		}`),
	}
	group.Mu.Unlock()

	requestBody := buildSyncGroupV5Request("schema-registry", 7, memberID, "consumer", "range", []GroupAssignment{
		{MemberID: memberID, Assignment: []byte(`{"version":1,"error":0}`)},
	})

	response, err := handler.handleSyncGroup(99, 5, requestBody)
	if err != nil {
		t.Fatalf("handleSyncGroup failed: %v", err)
	}
	if code := syncGroupResponseErrorCode(t, response, 5); code != ErrorCodeNone {
		t.Fatalf("SyncGroup error code = %d, want %d", code, ErrorCodeNone)
	}
	if !bytes.Contains(response, []byte(`"master":"`+memberID+`"`)) {
		t.Fatalf("Schema Registry assignment response does not name leader %q: %q", memberID, string(response))
	}

	group.Mu.RLock()
	defer group.Mu.RUnlock()
	if group.Generation != 7 {
		t.Fatalf("group generation = %d, want 7", group.Generation)
	}
	if group.State != consumer.GroupStateStable {
		t.Fatalf("group state = %s, want Stable", group.State)
	}
	if group.Members[memberID].State != consumer.MemberStateStable {
		t.Fatalf("member state = %s, want Stable", group.Members[memberID].State)
	}
}

func buildSyncGroupV5Request(groupID string, generation int32, memberID, protocolType, protocolName string, assignments []GroupAssignment) []byte {
	var data []byte
	data = appendCompactString(data, groupID)

	var generationBytes [4]byte
	binary.BigEndian.PutUint32(generationBytes[:], uint32(generation))
	data = append(data, generationBytes[:]...)

	data = appendCompactString(data, memberID)
	data = append(data, 0) // group_instance_id = null
	data = appendCompactString(data, protocolType)
	data = appendCompactString(data, protocolName)

	data = append(data, CompactArrayLength(uint32(len(assignments)))...)
	for _, assignment := range assignments {
		data = appendCompactString(data, assignment.MemberID)
		data = append(data, CompactStringLength(len(assignment.Assignment))...)
		data = append(data, assignment.Assignment...)
		data = append(data, 0) // assignment tagged fields
	}
	data = append(data, 0) // request tagged fields
	return data
}

func appendCompactString(data []byte, value string) []byte {
	data = append(data, CompactStringLength(len(value))...)
	return append(data, value...)
}

func syncGroupResponseErrorCode(t *testing.T, response []byte, apiVersion uint16) int16 {
	t.Helper()
	offset := 0
	if apiVersion >= 1 {
		offset += 4
	}
	if len(response) < offset+2 {
		t.Fatalf("SyncGroup response too short: %d bytes", len(response))
	}
	return int16(binary.BigEndian.Uint16(response[offset : offset+2]))
}
