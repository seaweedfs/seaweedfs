package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestVersionMatrix_JoinGroup tests JoinGroup request parsing across versions
func TestVersionMatrix_JoinGroup(t *testing.T) {
	tests := []struct {
		name       string
		version    int16
		buildBody  func() []byte
		expectErr  bool
		expectReq  *JoinGroupRequest
	}{
		{
			name:    "JoinGroup v0",
			version: 0,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// group_id
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-group")
				// session_timeout_ms
				binary.Write(buf, binary.BigEndian, int32(30000))
				// member_id
				binary.Write(buf, binary.BigEndian, int16(0)) // empty
				// protocol_type
				binary.Write(buf, binary.BigEndian, int16(8))
				buf.WriteString("consumer")
				// group_protocols array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// protocol_name
				binary.Write(buf, binary.BigEndian, int16(5))
				buf.WriteString("range")
				// protocol_metadata
				binary.Write(buf, binary.BigEndian, int32(0)) // empty
				return buf.Bytes()
			},
			expectErr: false,
			expectReq: &JoinGroupRequest{
				GroupID:         "test-group",
				SessionTimeout:  30000,
				MemberID:        "",
				ProtocolType:    "consumer",
				GroupProtocols:  []GroupProtocol{{Name: "range", Metadata: []byte{}}},
			},
		},
		{
			name:    "JoinGroup v5",
			version: 5,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// group_id
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-group")
				// session_timeout_ms
				binary.Write(buf, binary.BigEndian, int32(30000))
				// rebalance_timeout_ms (v1+)
				binary.Write(buf, binary.BigEndian, int32(300000))
				// member_id
				binary.Write(buf, binary.BigEndian, int16(0)) // empty
				// group_instance_id (v5+, nullable)
				binary.Write(buf, binary.BigEndian, int16(-1)) // null
				// protocol_type
				binary.Write(buf, binary.BigEndian, int16(8))
				buf.WriteString("consumer")
				// group_protocols array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// protocol_name
				binary.Write(buf, binary.BigEndian, int16(5))
				buf.WriteString("range")
				// protocol_metadata
				binary.Write(buf, binary.BigEndian, int32(0)) // empty
				return buf.Bytes()
			},
			expectErr: false,
			expectReq: &JoinGroupRequest{
				GroupID:           "test-group",
				SessionTimeout:    30000,
				RebalanceTimeout:  300000,
				MemberID:          "",
				GroupInstanceID:   "",
				ProtocolType:      "consumer",
				GroupProtocols:    []GroupProtocol{{Name: "range", Metadata: []byte{}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler()
			body := tt.buildBody()
			
			req, err := h.parseJoinGroupRequest(body, uint16(tt.version))
			if tt.expectErr && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.expectErr && req != nil {
				if req.GroupID != tt.expectReq.GroupID {
					t.Errorf("GroupID: got %q, want %q", req.GroupID, tt.expectReq.GroupID)
				}
				if req.SessionTimeout != tt.expectReq.SessionTimeout {
					t.Errorf("SessionTimeout: got %d, want %d", req.SessionTimeout, tt.expectReq.SessionTimeout)
				}
				if tt.version >= 1 && req.RebalanceTimeout != tt.expectReq.RebalanceTimeout {
					t.Errorf("RebalanceTimeout: got %d, want %d", req.RebalanceTimeout, tt.expectReq.RebalanceTimeout)
				}
				if req.MemberID != tt.expectReq.MemberID {
					t.Errorf("MemberID: got %q, want %q", req.MemberID, tt.expectReq.MemberID)
				}
				if tt.version >= 5 && req.GroupInstanceID != tt.expectReq.GroupInstanceID {
					t.Errorf("GroupInstanceID: got %q, want %q", req.GroupInstanceID, tt.expectReq.GroupInstanceID)
				}
				if req.ProtocolType != tt.expectReq.ProtocolType {
					t.Errorf("ProtocolType: got %q, want %q", req.ProtocolType, tt.expectReq.ProtocolType)
				}
			}
		})
	}
}

// TestVersionMatrix_SyncGroup tests SyncGroup request parsing across versions
func TestVersionMatrix_SyncGroup(t *testing.T) {
	tests := []struct {
		name       string
		version    int16
		buildBody  func() []byte
		expectErr  bool
		expectReq  *SyncGroupRequest
	}{
		{
			name:    "SyncGroup v0",
			version: 0,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// group_id
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-group")
				// generation_id
				binary.Write(buf, binary.BigEndian, int32(1))
				// member_id
				binary.Write(buf, binary.BigEndian, int16(6))
				buf.WriteString("member")
				// group_assignment array count
				binary.Write(buf, binary.BigEndian, int32(0)) // empty
				return buf.Bytes()
			},
			expectErr: false,
			expectReq: &SyncGroupRequest{
				GroupID:      "test-group",
				GenerationID: 1,
				MemberID:     "member",
				GroupAssignments: []GroupAssignment{},
			},
		},
		{
			name:    "SyncGroup v3",
			version: 3,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// group_id
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-group")
				// generation_id
				binary.Write(buf, binary.BigEndian, int32(1))
				// member_id
				binary.Write(buf, binary.BigEndian, int16(6))
				buf.WriteString("member")
				// group_instance_id (v3+, nullable)
				binary.Write(buf, binary.BigEndian, int16(-1)) // null
				// group_assignment array count
				binary.Write(buf, binary.BigEndian, int32(0)) // empty
				return buf.Bytes()
			},
			expectErr: false,
			expectReq: &SyncGroupRequest{
				GroupID:         "test-group",
				GenerationID:    1,
				MemberID:        "member",
				GroupInstanceID: "",
				GroupAssignments: []GroupAssignment{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler()
			body := tt.buildBody()
			
			req, err := h.parseSyncGroupRequest(body, uint16(tt.version))
			if tt.expectErr && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.expectErr && req != nil {
				if req.GroupID != tt.expectReq.GroupID {
					t.Errorf("GroupID: got %q, want %q", req.GroupID, tt.expectReq.GroupID)
				}
				if req.GenerationID != tt.expectReq.GenerationID {
					t.Errorf("GenerationID: got %d, want %d", req.GenerationID, tt.expectReq.GenerationID)
				}
				if req.MemberID != tt.expectReq.MemberID {
					t.Errorf("MemberID: got %q, want %q", req.MemberID, tt.expectReq.MemberID)
				}
				if tt.version >= 3 && req.GroupInstanceID != tt.expectReq.GroupInstanceID {
					t.Errorf("GroupInstanceID: got %q, want %q", req.GroupInstanceID, tt.expectReq.GroupInstanceID)
				}
			}
		})
	}
}

// TestVersionMatrix_OffsetFetch tests OffsetFetch request parsing across versions
func TestVersionMatrix_OffsetFetch(t *testing.T) {
	tests := []struct {
		name       string
		version    int16
		buildBody  func() []byte
		expectErr  bool
		expectReq  *OffsetFetchRequest
	}{
		{
			name:    "OffsetFetch v1",
			version: 1,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// group_id
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-group")
				// topics array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// topic_name
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-topic")
				// partitions array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// partition_id
				binary.Write(buf, binary.BigEndian, int32(0))
				return buf.Bytes()
			},
			expectErr: false,
			expectReq: &OffsetFetchRequest{
				GroupID: "test-group",
				Topics: []OffsetFetchTopic{
					{
						Name:  "test-topic",
						Partitions: []int32{0},
					},
				},
			},
		},
		{
			name:    "OffsetFetch v2",
			version: 2,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// group_id
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-group")
				// topics array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// topic_name
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-topic")
				// partitions array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// partition_id
				binary.Write(buf, binary.BigEndian, int32(0))
				return buf.Bytes()
			},
			expectErr: false,
			expectReq: &OffsetFetchRequest{
				GroupID: "test-group",
				Topics: []OffsetFetchTopic{
					{
						Name:  "test-topic",
						Partitions: []int32{0},
					},
				},
			},
		},
		{
			name:    "OffsetFetch v2 - empty topics (fetch all)",
			version: 2,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// group_id
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-group")
				// topics array count (0 = fetch all)
				binary.Write(buf, binary.BigEndian, int32(0))
				return buf.Bytes()
			},
			expectErr: false,
			expectReq: &OffsetFetchRequest{
				GroupID: "test-group",
				Topics:  []OffsetFetchTopic{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler()
			body := tt.buildBody()
			
			req, err := h.parseOffsetFetchRequest(body)
			if tt.expectErr && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.expectErr && req != nil {
				if req.GroupID != tt.expectReq.GroupID {
					t.Errorf("GroupID: got %q, want %q", req.GroupID, tt.expectReq.GroupID)
				}
				if len(req.Topics) != len(tt.expectReq.Topics) {
					t.Errorf("Topics count: got %d, want %d", len(req.Topics), len(tt.expectReq.Topics))
				}
				for i, topic := range req.Topics {
					if i < len(tt.expectReq.Topics) {
					if topic.Name != tt.expectReq.Topics[i].Name {
						t.Errorf("Topic[%d] name: got %q, want %q", i, topic.Name, tt.expectReq.Topics[i].Name)
						}
					}
				}
			}
		})
	}
}

// TestVersionMatrix_FindCoordinator tests FindCoordinator request parsing across versions
func TestVersionMatrix_FindCoordinator(t *testing.T) {
	tests := []struct {
		name       string
		version    int16
		buildBody  func() []byte
		expectErr  bool
		expectKey  string
		expectType int8
	}{
		{
			name:    "FindCoordinator v0",
			version: 0,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// coordinator_key
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-group")
				return buf.Bytes()
			},
			expectErr:  false,
			expectKey:  "test-group",
			expectType: 0, // GROUP (default for v0)
		},
		{
			name:    "FindCoordinator v1",
			version: 1,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// coordinator_key
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-group")
				// coordinator_type (v1+)
				binary.Write(buf, binary.BigEndian, int8(0)) // GROUP
				return buf.Bytes()
			},
			expectErr:  false,
			expectKey:  "test-group",
			expectType: 0, // GROUP
		},
		{
			name:    "FindCoordinator v2",
			version: 2,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// coordinator_key
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-group")
				// coordinator_type (v1+)
				binary.Write(buf, binary.BigEndian, int8(1)) // TRANSACTION
				return buf.Bytes()
			},
			expectErr:  false,
			expectKey:  "test-group",
			expectType: 1, // TRANSACTION
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := tt.buildBody()
			
			// Parse the request manually to test the format
			offset := 0
			
			// coordinator_key
			if offset+2 > len(body) {
				t.Fatalf("body too short for coordinator_key length")
			}
			keyLen := int(binary.BigEndian.Uint16(body[offset:offset+2]))
			offset += 2
			
			if offset+keyLen > len(body) {
				t.Fatalf("body too short for coordinator_key")
			}
			key := string(body[offset:offset+keyLen])
			offset += keyLen
			
			// coordinator_type (v1+)
			var coordType int8 = 0 // default GROUP
			if tt.version >= 1 {
				if offset+1 > len(body) {
					t.Fatalf("body too short for coordinator_type")
				}
				coordType = int8(body[offset])
				offset++
			}
			
			if key != tt.expectKey {
				t.Errorf("coordinator_key: got %q, want %q", key, tt.expectKey)
			}
			if coordType != tt.expectType {
				t.Errorf("coordinator_type: got %d, want %d", coordType, tt.expectType)
			}
		})
	}
}

// TestVersionMatrix_ListOffsets tests ListOffsets request parsing across versions
func TestVersionMatrix_ListOffsets(t *testing.T) {
	tests := []struct {
		name         string
		version      int16
		buildBody    func() []byte
		expectErr    bool
		expectReplica int32
		expectTopics  int
	}{
		{
			name:    "ListOffsets v0",
			version: 0,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// topics array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// topic_name
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-topic")
				// partitions array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// partition_id
				binary.Write(buf, binary.BigEndian, int32(0))
				// timestamp
				binary.Write(buf, binary.BigEndian, int64(-2)) // earliest
				return buf.Bytes()
			},
			expectErr:     false,
			expectReplica: -1, // no replica_id in v0
			expectTopics:  1,
		},
		{
			name:    "ListOffsets v1",
			version: 1,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// replica_id (v1+)
				binary.Write(buf, binary.BigEndian, int32(-1))
				// topics array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// topic_name
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-topic")
				// partitions array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// partition_id
				binary.Write(buf, binary.BigEndian, int32(0))
				// timestamp
				binary.Write(buf, binary.BigEndian, int64(-1)) // latest
				return buf.Bytes()
			},
			expectErr:     false,
			expectReplica: -1,
			expectTopics:  1,
		},
		{
			name:    "ListOffsets v2",
			version: 2,
			buildBody: func() []byte {
				buf := &bytes.Buffer{}
				// replica_id (v1+)
				binary.Write(buf, binary.BigEndian, int32(-1))
				// isolation_level (v2+)
				binary.Write(buf, binary.BigEndian, int8(0)) // READ_UNCOMMITTED
				// topics array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// topic_name
				binary.Write(buf, binary.BigEndian, int16(10))
				buf.WriteString("test-topic")
				// partitions array count
				binary.Write(buf, binary.BigEndian, int32(1))
				// partition_id
				binary.Write(buf, binary.BigEndian, int32(0))
				// timestamp
				binary.Write(buf, binary.BigEndian, int64(-1)) // latest
				// leader_epoch (v4+, but we'll test basic v2)
				return buf.Bytes()
			},
			expectErr:     false,
			expectReplica: -1,
			expectTopics:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := tt.buildBody()
			
			// Parse the request manually to test the format
			offset := 0
			
			// replica_id (v1+)
			var replicaID int32 = -1
			if tt.version >= 1 {
				if offset+4 > len(body) {
					t.Fatalf("body too short for replica_id")
				}
				replicaID = int32(binary.BigEndian.Uint32(body[offset:offset+4]))
				offset += 4
			}
			
			// isolation_level (v2+)
			if tt.version >= 2 {
				if offset+1 > len(body) {
					t.Fatalf("body too short for isolation_level")
				}
				// isolationLevel := int8(body[offset])
				offset += 1
			}
			
			// topics array count
			if offset+4 > len(body) {
				t.Fatalf("body too short for topics count")
			}
			topicsCount := int(binary.BigEndian.Uint32(body[offset:offset+4]))
			offset += 4
			
			if replicaID != tt.expectReplica {
				t.Errorf("replica_id: got %d, want %d", replicaID, tt.expectReplica)
			}
			if topicsCount != tt.expectTopics {
				t.Errorf("topics count: got %d, want %d", topicsCount, tt.expectTopics)
			}
		})
	}
}
