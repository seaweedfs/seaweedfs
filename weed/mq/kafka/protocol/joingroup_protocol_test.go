package protocol

import (
	"encoding/binary"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
)

// build minimal JoinGroup body helper is in static_membership_test.go, but we reimplement a tiny helper here for clarity
func buildJoinGroupBody(groupID, memberID, instanceID, protocolType string, protocols []GroupProtocol, sessionMs, rebalanceMs int32) []byte {
	b := make([]byte, 0, 256)
	// group id
	tmp2 := make([]byte, 2)
	binary.BigEndian.PutUint16(tmp2, uint16(len(groupID)))
	b = append(b, tmp2...)
	b = append(b, []byte(groupID)...)
	// session
	tmp4 := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp4, uint32(sessionMs))
	b = append(b, tmp4...)
	// rebalance
	binary.BigEndian.PutUint32(tmp4, uint32(rebalanceMs))
	b = append(b, tmp4...)
	// member id
	binary.BigEndian.PutUint16(tmp2, uint16(len(memberID)))
	b = append(b, tmp2...)
	b = append(b, []byte(memberID)...)
	// instance id (nullable)
	if instanceID == "" {
		binary.BigEndian.PutUint16(tmp2, 0xFFFF)
		b = append(b, tmp2...)
	} else {
		binary.BigEndian.PutUint16(tmp2, uint16(len(instanceID)))
		b = append(b, tmp2...)
		b = append(b, []byte(instanceID)...)
	}
	// protocol type
	binary.BigEndian.PutUint16(tmp2, uint16(len(protocolType)))
	b = append(b, tmp2...)
	b = append(b, []byte(protocolType)...)
	// protocols array
	tmp4 = make([]byte, 4)
	binary.BigEndian.PutUint32(tmp4, uint32(len(protocols)))
	b = append(b, tmp4...)
	for _, p := range protocols {
		binary.BigEndian.PutUint16(tmp2, uint16(len(p.Name)))
		b = append(b, tmp2...)
		b = append(b, []byte(p.Name)...)
		tmp4 = make([]byte, 4)
		binary.BigEndian.PutUint32(tmp4, uint32(len(p.Metadata)))
		b = append(b, tmp4...)
		b = append(b, p.Metadata...)
	}
	return b
}

func TestJoinGroup_ProtocolEnforcement(t *testing.T) {
	h := NewSimpleTestHandler()

	groupID := "grp-proto"

	// First join chooses sticky (preferred) when client offers sticky and roundrobin
	req1 := buildJoinGroupBody(groupID, "", "", "consumer", []GroupProtocol{
		{Name: "roundrobin", Metadata: []byte{}},
		{Name: "sticky", Metadata: []byte{}},
	}, 30000, 300000)

	resp1, err := h.handleJoinGroup(1, 5, req1)
	if err != nil {
		t.Fatalf("handleJoinGroup failed: %v", err)
	}
	if len(resp1) < 10 {
		t.Fatalf("response too short")
	}
	if ec := binary.BigEndian.Uint16(resp1[4:6]); ec != 0 {
		t.Fatalf("expected error code 0, got %d", ec)
	}

	// Verify group protocol selected
	g := h.groupCoordinator.GetGroup(groupID)
	if g == nil {
		t.Fatalf("group not found")
	}
	if g.Protocol != "sticky" {
		t.Fatalf("expected group protocol 'sticky', got %q", g.Protocol)
	}

	// Second join: client that does NOT support sticky should be rejected with InconsistentGroupProtocol
	req2 := buildJoinGroupBody(groupID, "", "", "consumer", []GroupProtocol{
		{Name: "range", Metadata: []byte{}},
		{Name: "roundrobin", Metadata: []byte{}},
	}, 30000, 300000)

	resp2, err := h.handleJoinGroup(2, 5, req2)
	if err != nil {
		t.Fatalf("handleJoinGroup failed: %v", err)
	}
	if len(resp2) < 10 {
		t.Fatalf("response too short")
	}
	if ec := binary.BigEndian.Uint16(resp2[4:6]); ec != uint16(ErrorCodeInconsistentGroupProtocol) {
		t.Fatalf("expected InconsistentGroupProtocol, got %d", ec)
	}

	// Third join: client that supports sticky should succeed
	req3 := buildJoinGroupBody(groupID, "", "", "consumer", []GroupProtocol{
		{Name: "sticky", Metadata: []byte{}},
	}, 30000, 300000)

	resp3, err := h.handleJoinGroup(3, 5, req3)
	if err != nil {
		t.Fatalf("handleJoinGroup failed: %v", err)
	}
	if len(resp3) < 10 {
		t.Fatalf("response too short")
	}
	if ec := binary.BigEndian.Uint16(resp3[4:6]); ec != 0 {
		t.Fatalf("expected success, got error code %d", ec)
	}
}

func TestParseMemberAssignment_RoundTrip(t *testing.T) {
	h := NewSimpleTestHandler()

	// Test round-trip: serialize -> parse -> should match original
	original := []consumer.PartitionAssignment{
		{Topic: "topic1", Partition: 0},
		{Topic: "topic1", Partition: 1},
		{Topic: "topic2", Partition: 0},
	}

	// Serialize
	serialized := h.serializeMemberAssignment(original)
	if len(serialized) == 0 {
		t.Fatal("serialized assignment is empty")
	}

	// Parse back
	parsed, err := h.parseMemberAssignment(serialized)
	if err != nil {
		t.Fatalf("parseMemberAssignment failed: %v", err)
	}

	// Compare
	if len(parsed) != len(original) {
		t.Fatalf("expected %d assignments, got %d", len(original), len(parsed))
	}

	for i, orig := range original {
		if parsed[i].Topic != orig.Topic || parsed[i].Partition != orig.Partition {
			t.Errorf("assignment %d: expected %+v, got %+v", i, orig, parsed[i])
		}
	}
}

func TestParseMemberAssignment_Empty(t *testing.T) {
	h := NewSimpleTestHandler()

	// Test empty assignment
	parsed, err := h.parseMemberAssignment([]byte{})
	if err != nil {
		t.Fatalf("parseMemberAssignment failed on empty: %v", err)
	}
	if len(parsed) != 0 {
		t.Errorf("expected empty assignment, got %d items", len(parsed))
	}

	// Test minimal valid assignment (version + 0 topics)
	minimal := []byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 0} // version=1, topics=0, userdata=0
	parsed, err = h.parseMemberAssignment(minimal)
	if err != nil {
		t.Fatalf("parseMemberAssignment failed on minimal: %v", err)
	}
	if len(parsed) != 0 {
		t.Errorf("expected empty assignment, got %d items", len(parsed))
	}
}

func buildSyncGroupBody(groupID, memberID, instanceID string, generationID int32, assignments []GroupAssignment) []byte {
	b := make([]byte, 0, 256)

	// group id
	tmp2 := make([]byte, 2)
	binary.BigEndian.PutUint16(tmp2, uint16(len(groupID)))
	b = append(b, tmp2...)
	b = append(b, []byte(groupID)...)

	// generation id
	tmp4 := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp4, uint32(generationID))
	b = append(b, tmp4...)

	// member id
	binary.BigEndian.PutUint16(tmp2, uint16(len(memberID)))
	b = append(b, tmp2...)
	b = append(b, []byte(memberID)...)

	// instance id (nullable)
	if instanceID == "" {
		binary.BigEndian.PutUint16(tmp2, 0xFFFF)
		b = append(b, tmp2...)
	} else {
		binary.BigEndian.PutUint16(tmp2, uint16(len(instanceID)))
		b = append(b, tmp2...)
		b = append(b, []byte(instanceID)...)
	}

	// assignments array
	binary.BigEndian.PutUint32(tmp4, uint32(len(assignments)))
	b = append(b, tmp4...)

	for _, assign := range assignments {
		// member id
		binary.BigEndian.PutUint16(tmp2, uint16(len(assign.MemberID)))
		b = append(b, tmp2...)
		b = append(b, []byte(assign.MemberID)...)

		// assignment bytes
		binary.BigEndian.PutUint32(tmp4, uint32(len(assign.Assignment)))
		b = append(b, tmp4...)
		b = append(b, assign.Assignment...)
	}

	return b
}

func TestParseSyncGroupRequest_Assignments(t *testing.T) {
	h := NewSimpleTestHandler()

	// Create some assignment data
	assignment1 := h.serializeMemberAssignment([]consumer.PartitionAssignment{
		{Topic: "topic1", Partition: 0},
	})
	assignment2 := h.serializeMemberAssignment([]consumer.PartitionAssignment{
		{Topic: "topic1", Partition: 1},
		{Topic: "topic2", Partition: 0},
	})

	assignments := []GroupAssignment{
		{MemberID: "member1", Assignment: assignment1},
		{MemberID: "member2", Assignment: assignment2},
	}

	body := buildSyncGroupBody("test-group", "leader", "", 1, assignments)

	req, err := h.parseSyncGroupRequest(body, 3)
	if err != nil {
		t.Fatalf("parseSyncGroupRequest failed: %v", err)
	}

	if req.GroupID != "test-group" {
		t.Errorf("expected GroupID 'test-group', got %q", req.GroupID)
	}
	if req.GenerationID != 1 {
		t.Errorf("expected GenerationID 1, got %d", req.GenerationID)
	}
	if req.MemberID != "leader" {
		t.Errorf("expected MemberID 'leader', got %q", req.MemberID)
	}

	if len(req.GroupAssignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(req.GroupAssignments))
	}

	// Verify first assignment
	if req.GroupAssignments[0].MemberID != "member1" {
		t.Errorf("expected first assignment MemberID 'member1', got %q", req.GroupAssignments[0].MemberID)
	}
	parsed1, err := h.parseMemberAssignment(req.GroupAssignments[0].Assignment)
	if err != nil {
		t.Fatalf("failed to parse first assignment: %v", err)
	}
	if len(parsed1) != 1 || parsed1[0].Topic != "topic1" || parsed1[0].Partition != 0 {
		t.Errorf("unexpected first assignment: %+v", parsed1)
	}

	// Verify second assignment
	if req.GroupAssignments[1].MemberID != "member2" {
		t.Errorf("expected second assignment MemberID 'member2', got %q", req.GroupAssignments[1].MemberID)
	}
	parsed2, err := h.parseMemberAssignment(req.GroupAssignments[1].Assignment)
	if err != nil {
		t.Fatalf("failed to parse second assignment: %v", err)
	}
	if len(parsed2) != 2 {
		t.Errorf("expected 2 partitions in second assignment, got %d", len(parsed2))
	}
}

func TestProcessGroupAssignments_LeaderAssignments(t *testing.T) {
	h := NewSimpleTestHandler()

	// Create a group with members
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Members = map[string]*consumer.GroupMember{
		"member1": {ID: "member1", ClientID: "client1"},
		"member2": {ID: "member2", ClientID: "client2"},
		"member3": {ID: "member3", ClientID: "client3"},
	}

	// Create assignments
	assignment1 := h.serializeMemberAssignment([]consumer.PartitionAssignment{
		{Topic: "topic1", Partition: 0},
		{Topic: "topic2", Partition: 0},
	})
	assignment2 := h.serializeMemberAssignment([]consumer.PartitionAssignment{
		{Topic: "topic1", Partition: 1},
	})

	assignments := []GroupAssignment{
		{MemberID: "member1", Assignment: assignment1},
		{MemberID: "member2", Assignment: assignment2},
		// member3 gets no assignment (empty)
	}

	// Process assignments
	err := h.processGroupAssignments(group, assignments)
	if err != nil {
		t.Fatalf("processGroupAssignments failed: %v", err)
	}

	// Verify member1 assignment
	member1 := group.Members["member1"]
	if len(member1.Assignment) != 2 {
		t.Errorf("expected member1 to have 2 assignments, got %d", len(member1.Assignment))
	} else {
		if member1.Assignment[0].Topic != "topic1" || member1.Assignment[0].Partition != 0 {
			t.Errorf("unexpected member1 assignment[0]: %+v", member1.Assignment[0])
		}
		if member1.Assignment[1].Topic != "topic2" || member1.Assignment[1].Partition != 0 {
			t.Errorf("unexpected member1 assignment[1]: %+v", member1.Assignment[1])
		}
	}

	// Verify member2 assignment
	member2 := group.Members["member2"]
	if len(member2.Assignment) != 1 {
		t.Errorf("expected member2 to have 1 assignment, got %d", len(member2.Assignment))
	} else {
		if member2.Assignment[0].Topic != "topic1" || member2.Assignment[0].Partition != 1 {
			t.Errorf("unexpected member2 assignment: %+v", member2.Assignment[0])
		}
	}

	// Verify member3 has no assignment
	member3 := group.Members["member3"]
	if len(member3.Assignment) != 0 {
		t.Errorf("expected member3 to have no assignments, got %d", len(member3.Assignment))
	}
}

func TestHandleSyncGroup_LeaderAssignments(t *testing.T) {
	h := NewSimpleTestHandler()

	// Setup: create group with members via JoinGroup
	groupID := "sync-test-group"

	// Member 1 joins (becomes leader)
	req1 := buildJoinGroupBody(groupID, "", "", "consumer", []GroupProtocol{
		{Name: "range", Metadata: []byte{}},
	}, 30000, 300000)

	resp1, err := h.handleJoinGroup(1, 5, req1)
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}
	if binary.BigEndian.Uint16(resp1[4:6]) != 0 {
		t.Fatal("JoinGroup should succeed")
	}

	// Member 2 joins
	req2 := buildJoinGroupBody(groupID, "", "", "consumer", []GroupProtocol{
		{Name: "range", Metadata: []byte{}},
	}, 30000, 300000)

	resp2, err := h.handleJoinGroup(2, 5, req2)
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}
	if binary.BigEndian.Uint16(resp2[4:6]) != 0 {
		t.Fatal("JoinGroup should succeed")
	}

	group := h.groupCoordinator.GetGroup(groupID)
	if group == nil {
		t.Fatal("group not found")
	}
	if len(group.Members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(group.Members))
	}

	// Get member IDs
	var member1ID, member2ID string
	for memberID := range group.Members {
		if member1ID == "" {
			member1ID = memberID
		} else {
			member2ID = memberID
		}
	}

	// Leader (member1) sends SyncGroup with assignments
	assignment1 := h.serializeMemberAssignment([]consumer.PartitionAssignment{
		{Topic: "topic1", Partition: 0},
	})
	assignment2 := h.serializeMemberAssignment([]consumer.PartitionAssignment{
		{Topic: "topic1", Partition: 1},
	})

	assignments := []GroupAssignment{
		{MemberID: member1ID, Assignment: assignment1},
		{MemberID: member2ID, Assignment: assignment2},
	}

	syncBody := buildSyncGroupBody(groupID, group.Leader, "", group.Generation, assignments)
	syncResp, err := h.handleSyncGroup(3, 3, syncBody)
	if err != nil {
		t.Fatalf("handleSyncGroup failed: %v", err)
	}

	// Check SyncGroup response
	if len(syncResp) < 10 {
		t.Fatal("SyncGroup response too short")
	}
	if binary.BigEndian.Uint16(syncResp[8:10]) != 0 { // error code after correlation+throttle
		t.Fatal("SyncGroup should succeed")
	}

	// Verify assignments were applied
	if len(group.Members[member1ID].Assignment) != 1 {
		t.Errorf("member1 should have 1 assignment, got %d", len(group.Members[member1ID].Assignment))
	}
	if len(group.Members[member2ID].Assignment) != 1 {
		t.Errorf("member2 should have 1 assignment, got %d", len(group.Members[member2ID].Assignment))
	}

	// Verify group is stable
	if group.State != consumer.GroupStateStable {
		t.Errorf("group should be stable, got %s", group.State.String())
	}

	// Non-leader member sends SyncGroup (no assignments)
	nonLeaderID := member2ID
	if group.Leader == member2ID {
		nonLeaderID = member1ID
	}

	syncBody2 := buildSyncGroupBody(groupID, nonLeaderID, "", group.Generation, []GroupAssignment{})
	syncResp2, err := h.handleSyncGroup(4, 3, syncBody2)
	if err != nil {
		t.Fatalf("handleSyncGroup (non-leader) failed: %v", err)
	}

	if len(syncResp2) < 10 {
		t.Fatal("SyncGroup response too short")
	}
	if binary.BigEndian.Uint16(syncResp2[8:10]) != 0 {
		t.Fatal("SyncGroup (non-leader) should succeed")
	}
}
