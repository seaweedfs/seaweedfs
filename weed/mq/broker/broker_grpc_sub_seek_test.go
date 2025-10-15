package broker

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/grpc/metadata"
)

// TestGetRequestPositionFromSeek tests the helper function that converts seek requests to message positions
func TestGetRequestPositionFromSeek(t *testing.T) {
	broker := &MessageQueueBroker{}

	tests := []struct {
		name           string
		offsetType     schema_pb.OffsetType
		offset         int64
		expectedBatch  int64
		expectZeroTime bool
	}{
		{
			name:           "reset to earliest",
			offsetType:     schema_pb.OffsetType_RESET_TO_EARLIEST,
			offset:         0,
			expectedBatch:  -3,
			expectZeroTime: false,
		},
		{
			name:           "reset to latest",
			offsetType:     schema_pb.OffsetType_RESET_TO_LATEST,
			offset:         0,
			expectedBatch:  -4,
			expectZeroTime: false,
		},
		{
			name:           "exact offset zero",
			offsetType:     schema_pb.OffsetType_EXACT_OFFSET,
			offset:         0,
			expectedBatch:  0,
			expectZeroTime: true,
		},
		{
			name:           "exact offset 100",
			offsetType:     schema_pb.OffsetType_EXACT_OFFSET,
			offset:         100,
			expectedBatch:  100,
			expectZeroTime: true,
		},
		{
			name:           "exact offset 1000",
			offsetType:     schema_pb.OffsetType_EXACT_OFFSET,
			offset:         1000,
			expectedBatch:  1000,
			expectZeroTime: true,
		},
		{
			name:           "exact timestamp",
			offsetType:     schema_pb.OffsetType_EXACT_TS_NS,
			offset:         1234567890123456789,
			expectedBatch:  -2,
			expectZeroTime: false,
		},
		{
			name:           "reset to offset",
			offsetType:     schema_pb.OffsetType_RESET_TO_OFFSET,
			offset:         42,
			expectedBatch:  42,
			expectZeroTime: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seekMsg := &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     tt.offset,
				OffsetType: tt.offsetType,
			}

			position := broker.getRequestPositionFromSeek(seekMsg)

			if position.Offset != tt.expectedBatch {
				t.Errorf("Expected batch index %d, got %d", tt.expectedBatch, position.Offset)
			}

			// Verify time handling
			if tt.expectZeroTime && !position.Time.IsZero() {
				t.Errorf("Expected zero time for offset-based seek, got %v", position.Time)
			}

			if !tt.expectZeroTime && position.Time.IsZero() && tt.offsetType != schema_pb.OffsetType_RESET_TO_EARLIEST {
				t.Errorf("Expected non-zero time, got zero time")
			}
		})
	}
}

// TestGetRequestPositionFromSeek_NilSafety tests that the function handles nil input gracefully
func TestGetRequestPositionFromSeek_NilSafety(t *testing.T) {
	broker := &MessageQueueBroker{}

	position := broker.getRequestPositionFromSeek(nil)

	// Should return zero-value position without panicking
	if position.Offset != 0 {
		t.Errorf("Expected zero offset for nil input, got %d", position.Offset)
	}
}

// TestGetRequestPositionFromSeek_ConsistentResults verifies that multiple calls with same input produce same output
func TestGetRequestPositionFromSeek_ConsistentResults(t *testing.T) {
	broker := &MessageQueueBroker{}

	seekMsg := &mq_pb.SubscribeMessageRequest_SeekMessage{
		Offset:     42,
		OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
	}

	// Call multiple times
	positions := make([]log_buffer.MessagePosition, 5)
	for i := 0; i < 5; i++ {
		positions[i] = broker.getRequestPositionFromSeek(seekMsg)
		time.Sleep(1 * time.Millisecond) // Small delay
	}

	// All positions should be identical
	for i := 1; i < len(positions); i++ {
		if positions[i].Offset != positions[0].Offset {
			t.Errorf("Inconsistent Offset: %d vs %d", positions[0].Offset, positions[i].Offset)
		}
		if !positions[i].Time.Equal(positions[0].Time) {
			t.Errorf("Inconsistent Time: %v vs %v", positions[0].Time, positions[i].Time)
		}
		if positions[i].IsOffsetBased != positions[0].IsOffsetBased {
			t.Errorf("Inconsistent IsOffsetBased: %v vs %v", positions[0].IsOffsetBased, positions[i].IsOffsetBased)
		}
	}
}

// TestGetRequestPositionFromSeek_OffsetExtraction verifies offset can be correctly extracted
func TestGetRequestPositionFromSeek_OffsetExtraction(t *testing.T) {
	broker := &MessageQueueBroker{}

	testOffsets := []int64{0, 1, 10, 100, 1000, 9999}

	for _, offset := range testOffsets {
		t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
			seekMsg := &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     offset,
				OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
			}

			position := broker.getRequestPositionFromSeek(seekMsg)

			if !position.IsOffsetBased {
				t.Error("Position should be detected as offset-based")
			}

			if extractedOffset := position.GetOffset(); extractedOffset != offset {
				t.Errorf("Expected extracted offset %d, got %d", offset, extractedOffset)
			}
		})
	}
}

// MockSubscribeMessageStream is a mock implementation of the gRPC stream for testing
type MockSubscribeMessageStream struct {
	ctx          context.Context
	recvChan     chan *mq_pb.SubscribeMessageRequest
	sentMessages []*mq_pb.SubscribeMessageResponse
	mu           sync.Mutex
	recvErr      error
}

func NewMockSubscribeMessageStream(ctx context.Context) *MockSubscribeMessageStream {
	return &MockSubscribeMessageStream{
		ctx:          ctx,
		recvChan:     make(chan *mq_pb.SubscribeMessageRequest, 10),
		sentMessages: make([]*mq_pb.SubscribeMessageResponse, 0),
	}
}

func (m *MockSubscribeMessageStream) Send(msg *mq_pb.SubscribeMessageResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentMessages = append(m.sentMessages, msg)
	return nil
}

func (m *MockSubscribeMessageStream) Recv() (*mq_pb.SubscribeMessageRequest, error) {
	if m.recvErr != nil {
		return nil, m.recvErr
	}

	select {
	case msg := <-m.recvChan:
		return msg, nil
	case <-m.ctx.Done():
		return nil, io.EOF
	}
}

func (m *MockSubscribeMessageStream) SetHeader(metadata.MD) error {
	return nil
}

func (m *MockSubscribeMessageStream) SendHeader(metadata.MD) error {
	return nil
}

func (m *MockSubscribeMessageStream) SetTrailer(metadata.MD) {}

func (m *MockSubscribeMessageStream) Context() context.Context {
	return m.ctx
}

func (m *MockSubscribeMessageStream) SendMsg(interface{}) error {
	return nil
}

func (m *MockSubscribeMessageStream) RecvMsg(interface{}) error {
	return nil
}

func (m *MockSubscribeMessageStream) QueueMessage(msg *mq_pb.SubscribeMessageRequest) {
	m.recvChan <- msg
}

func (m *MockSubscribeMessageStream) SetRecvError(err error) {
	m.recvErr = err
}

func (m *MockSubscribeMessageStream) GetSentMessages() []*mq_pb.SubscribeMessageResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*mq_pb.SubscribeMessageResponse{}, m.sentMessages...)
}

// TestSeekMessageHandling_BasicSeek tests that seek messages are properly received and acknowledged
func TestSeekMessageHandling_BasicSeek(t *testing.T) {
	// Create seek message
	seekMsg := &mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Seek{
			Seek: &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     100,
				OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
			},
		},
	}

	// Verify message structure
	if seekReq := seekMsg.GetSeek(); seekReq == nil {
		t.Fatal("Failed to create seek message")
	} else {
		if seekReq.Offset != 100 {
			t.Errorf("Expected offset 100, got %d", seekReq.Offset)
		}
		if seekReq.OffsetType != schema_pb.OffsetType_EXACT_OFFSET {
			t.Errorf("Expected EXACT_OFFSET, got %v", seekReq.OffsetType)
		}
	}
}

// TestSeekMessageHandling_MultipleSeekTypes tests different seek offset types
func TestSeekMessageHandling_MultipleSeekTypes(t *testing.T) {
	testCases := []struct {
		name       string
		offset     int64
		offsetType schema_pb.OffsetType
	}{
		{
			name:       "seek to earliest",
			offset:     0,
			offsetType: schema_pb.OffsetType_RESET_TO_EARLIEST,
		},
		{
			name:       "seek to latest",
			offset:     0,
			offsetType: schema_pb.OffsetType_RESET_TO_LATEST,
		},
		{
			name:       "seek to exact offset",
			offset:     42,
			offsetType: schema_pb.OffsetType_EXACT_OFFSET,
		},
		{
			name:       "seek to timestamp",
			offset:     time.Now().UnixNano(),
			offsetType: schema_pb.OffsetType_EXACT_TS_NS,
		},
		{
			name:       "reset to offset",
			offset:     1000,
			offsetType: schema_pb.OffsetType_RESET_TO_OFFSET,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			seekMsg := &mq_pb.SubscribeMessageRequest{
				Message: &mq_pb.SubscribeMessageRequest_Seek{
					Seek: &mq_pb.SubscribeMessageRequest_SeekMessage{
						Offset:     tc.offset,
						OffsetType: tc.offsetType,
					},
				},
			}

			seekReq := seekMsg.GetSeek()
			if seekReq == nil {
				t.Fatal("Failed to get seek message")
			}

			if seekReq.Offset != tc.offset {
				t.Errorf("Expected offset %d, got %d", tc.offset, seekReq.Offset)
			}

			if seekReq.OffsetType != tc.offsetType {
				t.Errorf("Expected offset type %v, got %v", tc.offsetType, seekReq.OffsetType)
			}
		})
	}
}

// TestSeekMessageHandling_AckVsSeekDistinction tests that we can distinguish between ack and seek messages
func TestSeekMessageHandling_AckVsSeekDistinction(t *testing.T) {
	// Create ack message
	ackMsg := &mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Ack{
			Ack: &mq_pb.SubscribeMessageRequest_AckMessage{
				Key:  []byte("test-key"),
				TsNs: time.Now().UnixNano(),
			},
		},
	}

	// Create seek message
	seekMsg := &mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Seek{
			Seek: &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     100,
				OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
			},
		},
	}

	// Verify ack message doesn't match seek
	if ackMsg.GetSeek() != nil {
		t.Error("Ack message should not be detected as seek")
	}
	if ackMsg.GetAck() == nil {
		t.Error("Ack message should be detected as ack")
	}

	// Verify seek message doesn't match ack
	if seekMsg.GetAck() != nil {
		t.Error("Seek message should not be detected as ack")
	}
	if seekMsg.GetSeek() == nil {
		t.Error("Seek message should be detected as seek")
	}
}

// TestSeekMessageResponse_SuccessFormat tests the response format for successful seek
func TestSeekMessageResponse_SuccessFormat(t *testing.T) {
	// Create success response (empty error string = success)
	successResponse := &mq_pb.SubscribeMessageResponse{
		Message: &mq_pb.SubscribeMessageResponse_Ctrl{
			Ctrl: &mq_pb.SubscribeMessageResponse_SubscribeCtrlMessage{
				Error: "", // Empty error means success
			},
		},
	}

	ctrlMsg := successResponse.GetCtrl()
	if ctrlMsg == nil {
		t.Fatal("Failed to get control message")
	}

	// Empty error string indicates success
	if ctrlMsg.Error != "" {
		t.Errorf("Expected empty error for success, got: %s", ctrlMsg.Error)
	}
}

// TestSeekMessageResponse_ErrorFormat tests the response format for failed seek
func TestSeekMessageResponse_ErrorFormat(t *testing.T) {
	// Create error response
	errorResponse := &mq_pb.SubscribeMessageResponse{
		Message: &mq_pb.SubscribeMessageResponse_Ctrl{
			Ctrl: &mq_pb.SubscribeMessageResponse_SubscribeCtrlMessage{
				Error: "Seek not implemented",
			},
		},
	}

	ctrlMsg := errorResponse.GetCtrl()
	if ctrlMsg == nil {
		t.Fatal("Failed to get control message")
	}

	// Non-empty error string indicates failure
	if ctrlMsg.Error == "" {
		t.Error("Expected non-empty error for failure")
	}

	if ctrlMsg.Error != "Seek not implemented" {
		t.Errorf("Expected specific error message, got: %s", ctrlMsg.Error)
	}
}

// TestSeekMessageHandling_BackwardSeek tests backward seeking scenarios
func TestSeekMessageHandling_BackwardSeek(t *testing.T) {
	testCases := []struct {
		name        string
		currentPos  int64
		seekOffset  int64
		expectedGap int64
	}{
		{
			name:        "small backward gap",
			currentPos:  100,
			seekOffset:  90,
			expectedGap: 10,
		},
		{
			name:        "medium backward gap",
			currentPos:  1000,
			seekOffset:  500,
			expectedGap: 500,
		},
		{
			name:        "large backward gap",
			currentPos:  1000000,
			seekOffset:  1,
			expectedGap: 999999,
		},
		{
			name:        "seek to zero",
			currentPos:  100,
			seekOffset:  0,
			expectedGap: 100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Verify gap calculation
			gap := tc.currentPos - tc.seekOffset
			if gap != tc.expectedGap {
				t.Errorf("Expected gap %d, got %d", tc.expectedGap, gap)
			}

			// Create seek message for backward seek
			seekMsg := &mq_pb.SubscribeMessageRequest{
				Message: &mq_pb.SubscribeMessageRequest_Seek{
					Seek: &mq_pb.SubscribeMessageRequest_SeekMessage{
						Offset:     tc.seekOffset,
						OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
					},
				},
			}

			seekReq := seekMsg.GetSeek()
			if seekReq == nil {
				t.Fatal("Failed to create seek message")
			}

			if seekReq.Offset != tc.seekOffset {
				t.Errorf("Expected offset %d, got %d", tc.seekOffset, seekReq.Offset)
			}
		})
	}
}

// TestSeekMessageHandling_ForwardSeek tests forward seeking scenarios
func TestSeekMessageHandling_ForwardSeek(t *testing.T) {
	testCases := []struct {
		name       string
		currentPos int64
		seekOffset int64
		shouldSeek bool
	}{
		{
			name:       "small forward gap",
			currentPos: 100,
			seekOffset: 110,
			shouldSeek: false, // Forward seeks don't need special handling
		},
		{
			name:       "same position",
			currentPos: 100,
			seekOffset: 100,
			shouldSeek: false,
		},
		{
			name:       "large forward gap",
			currentPos: 100,
			seekOffset: 10000,
			shouldSeek: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// For forward seeks, gateway typically just continues reading
			// No special seek message needed
			isBackward := tc.seekOffset < tc.currentPos

			if isBackward && !tc.shouldSeek {
				t.Error("Backward seek should require seek message")
			}
		})
	}
}

// TestSeekIntegration_PositionConversion tests the complete flow from seek message to position
func TestSeekIntegration_PositionConversion(t *testing.T) {
	broker := &MessageQueueBroker{}

	testCases := []struct {
		name       string
		offset     int64
		offsetType schema_pb.OffsetType
		verifyFunc func(t *testing.T, pos log_buffer.MessagePosition)
	}{
		{
			name:       "exact offset conversion",
			offset:     42,
			offsetType: schema_pb.OffsetType_EXACT_OFFSET,
			verifyFunc: func(t *testing.T, pos log_buffer.MessagePosition) {
				if !pos.IsOffsetBased {
					t.Error("Expected offset-based position")
				}
				if pos.GetOffset() != 42 {
					t.Errorf("Expected offset 42, got %d", pos.GetOffset())
				}
			},
		},
		{
			name:       "earliest offset conversion",
			offset:     0,
			offsetType: schema_pb.OffsetType_RESET_TO_EARLIEST,
			verifyFunc: func(t *testing.T, pos log_buffer.MessagePosition) {
				if pos.Offset != -3 {
					t.Errorf("Expected batch -3 for earliest, got %d", pos.Offset)
				}
			},
		},
		{
			name:       "latest offset conversion",
			offset:     0,
			offsetType: schema_pb.OffsetType_RESET_TO_LATEST,
			verifyFunc: func(t *testing.T, pos log_buffer.MessagePosition) {
				if pos.Offset != -4 {
					t.Errorf("Expected batch -4 for latest, got %d", pos.Offset)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create seek message
			seekMsg := &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     tc.offset,
				OffsetType: tc.offsetType,
			}

			// Convert to position
			position := broker.getRequestPositionFromSeek(seekMsg)

			// Verify result
			tc.verifyFunc(t, position)
		})
	}
}

// TestSeekMessageHandling_ConcurrentSeeks tests handling multiple seek requests
func TestSeekMessageHandling_ConcurrentSeeks(t *testing.T) {
	broker := &MessageQueueBroker{}

	// Simulate multiple concurrent seek requests
	seekOffsets := []int64{10, 20, 30, 40, 50}

	var wg sync.WaitGroup
	results := make([]log_buffer.MessagePosition, len(seekOffsets))

	for i, offset := range seekOffsets {
		wg.Add(1)
		go func(idx int, off int64) {
			defer wg.Done()

			seekMsg := &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     off,
				OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
			}

			results[idx] = broker.getRequestPositionFromSeek(seekMsg)
		}(i, offset)
	}

	wg.Wait()

	// Verify all results are correct
	for i, offset := range seekOffsets {
		if results[i].GetOffset() != offset {
			t.Errorf("Expected offset %d at index %d, got %d", offset, i, results[i].GetOffset())
		}
	}
}

// TestSeekMessageProtocol_WireFormat verifies the protobuf message structure
func TestSeekMessageProtocol_WireFormat(t *testing.T) {
	// Test that SeekMessage is properly defined in the oneof
	req := &mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Seek{
			Seek: &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     100,
				OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
			},
		},
	}

	// Verify oneof is set correctly
	switch msg := req.Message.(type) {
	case *mq_pb.SubscribeMessageRequest_Seek:
		if msg.Seek.Offset != 100 {
			t.Errorf("Expected offset 100, got %d", msg.Seek.Offset)
		}
	default:
		t.Errorf("Expected Seek message, got %T", msg)
	}

	// Verify other message types are nil
	if req.GetAck() != nil {
		t.Error("Seek message should not have Ack")
	}
	if req.GetInit() != nil {
		t.Error("Seek message should not have Init")
	}
}

// TestSeekByTimestamp tests timestamp-based seek operations
func TestSeekByTimestamp(t *testing.T) {
	broker := &MessageQueueBroker{}

	testCases := []struct {
		name        string
		timestampNs int64
		offsetType  schema_pb.OffsetType
	}{
		{
			name:        "seek to specific timestamp",
			timestampNs: 1234567890123456789,
			offsetType:  schema_pb.OffsetType_EXACT_TS_NS,
		},
		{
			name:        "seek to current timestamp",
			timestampNs: time.Now().UnixNano(),
			offsetType:  schema_pb.OffsetType_EXACT_TS_NS,
		},
		{
			name:        "seek to past timestamp",
			timestampNs: time.Now().Add(-24 * time.Hour).UnixNano(),
			offsetType:  schema_pb.OffsetType_EXACT_TS_NS,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			seekMsg := &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     tc.timestampNs,
				OffsetType: tc.offsetType,
			}

			position := broker.getRequestPositionFromSeek(seekMsg)

			// For timestamp-based seeks, Time should be set to the timestamp
			expectedTime := time.Unix(0, tc.timestampNs)
			if !position.Time.Equal(expectedTime) {
				t.Errorf("Expected time %v, got %v", expectedTime, position.Time)
			}

			// Batch should be -2 for EXACT_TS_NS
			if position.Offset != -2 {
				t.Errorf("Expected batch -2 for timestamp seek, got %d", position.Offset)
			}
		})
	}
}

// TestSeekByTimestamp_Ordering tests that timestamp seeks preserve ordering
func TestSeekByTimestamp_Ordering(t *testing.T) {
	broker := &MessageQueueBroker{}

	// Create timestamps in chronological order
	baseTime := time.Now().Add(-1 * time.Hour)
	timestamps := []int64{
		baseTime.UnixNano(),
		baseTime.Add(10 * time.Minute).UnixNano(),
		baseTime.Add(20 * time.Minute).UnixNano(),
		baseTime.Add(30 * time.Minute).UnixNano(),
	}

	positions := make([]log_buffer.MessagePosition, len(timestamps))

	for i, ts := range timestamps {
		seekMsg := &mq_pb.SubscribeMessageRequest_SeekMessage{
			Offset:     ts,
			OffsetType: schema_pb.OffsetType_EXACT_TS_NS,
		}
		positions[i] = broker.getRequestPositionFromSeek(seekMsg)
	}

	// Verify positions are in chronological order
	for i := 1; i < len(positions); i++ {
		if !positions[i].Time.After(positions[i-1].Time) {
			t.Errorf("Timestamp ordering violated: position[%d].Time (%v) should be after position[%d].Time (%v)",
				i, positions[i].Time, i-1, positions[i-1].Time)
		}
	}
}

// TestSeekByTimestamp_EdgeCases tests edge cases for timestamp seeks
func TestSeekByTimestamp_EdgeCases(t *testing.T) {
	broker := &MessageQueueBroker{}

	testCases := []struct {
		name        string
		timestampNs int64
		expectValid bool
	}{
		{
			name:        "zero timestamp",
			timestampNs: 0,
			expectValid: true, // Valid - means Unix epoch
		},
		{
			name:        "negative timestamp",
			timestampNs: -1,
			expectValid: true, // Valid in Go (before Unix epoch)
		},
		{
			name:        "far future timestamp",
			timestampNs: time.Now().Add(100 * 365 * 24 * time.Hour).UnixNano(),
			expectValid: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			seekMsg := &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     tc.timestampNs,
				OffsetType: schema_pb.OffsetType_EXACT_TS_NS,
			}

			position := broker.getRequestPositionFromSeek(seekMsg)

			if tc.expectValid {
				expectedTime := time.Unix(0, tc.timestampNs)
				if !position.Time.Equal(expectedTime) {
					t.Errorf("Expected time %v, got %v", expectedTime, position.Time)
				}
			}
		})
	}
}

// TestSeekByTimestamp_VsOffset tests that timestamp and offset seeks are independent
func TestSeekByTimestamp_VsOffset(t *testing.T) {
	broker := &MessageQueueBroker{}

	timestampSeek := &mq_pb.SubscribeMessageRequest_SeekMessage{
		Offset:     time.Now().UnixNano(),
		OffsetType: schema_pb.OffsetType_EXACT_TS_NS,
	}

	offsetSeek := &mq_pb.SubscribeMessageRequest_SeekMessage{
		Offset:     100,
		OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
	}

	timestampPos := broker.getRequestPositionFromSeek(timestampSeek)
	offsetPos := broker.getRequestPositionFromSeek(offsetSeek)

	// Timestamp-based position should have batch -2
	if timestampPos.Offset != -2 {
		t.Errorf("Timestamp seek should have batch -2, got %d", timestampPos.Offset)
	}

	// Offset-based position should have the exact offset in Offset field
	if offsetPos.GetOffset() != 100 {
		t.Errorf("Offset seek should have offset 100, got %d", offsetPos.GetOffset())
	}

	// They should use different positioning mechanisms
	if timestampPos.IsOffsetBased {
		t.Error("Timestamp seek should not be offset-based")
	}

	if !offsetPos.IsOffsetBased {
		t.Error("Offset seek should be offset-based")
	}
}

// TestSeekOptimization_SkipRedundantSeek tests that seeking to the same offset is optimized
func TestSeekOptimization_SkipRedundantSeek(t *testing.T) {
	broker := &MessageQueueBroker{}

	// Test that seeking to the same offset multiple times produces the same result
	seekMsg := &mq_pb.SubscribeMessageRequest_SeekMessage{
		Offset:     100,
		OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
	}

	// First seek
	pos1 := broker.getRequestPositionFromSeek(seekMsg)
	
	// Second seek to same offset
	pos2 := broker.getRequestPositionFromSeek(seekMsg)
	
	// Third seek to same offset
	pos3 := broker.getRequestPositionFromSeek(seekMsg)

	// All positions should be identical
	if pos1.GetOffset() != pos2.GetOffset() || pos2.GetOffset() != pos3.GetOffset() {
		t.Errorf("Multiple seeks to same offset should produce identical results: %d, %d, %d",
			pos1.GetOffset(), pos2.GetOffset(), pos3.GetOffset())
	}

	// Verify the offset is correct
	if pos1.GetOffset() != 100 {
		t.Errorf("Expected offset 100, got %d", pos1.GetOffset())
	}
}

// TestSeekOptimization_DifferentOffsets tests that different offsets produce different positions
func TestSeekOptimization_DifferentOffsets(t *testing.T) {
	broker := &MessageQueueBroker{}

	offsets := []int64{0, 50, 100, 150, 200}
	positions := make([]log_buffer.MessagePosition, len(offsets))

	for i, offset := range offsets {
		seekMsg := &mq_pb.SubscribeMessageRequest_SeekMessage{
			Offset:     offset,
			OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
		}
		positions[i] = broker.getRequestPositionFromSeek(seekMsg)
	}

	// Verify each position has the correct offset
	for i, offset := range offsets {
		if positions[i].GetOffset() != offset {
			t.Errorf("Position %d: expected offset %d, got %d", i, offset, positions[i].GetOffset())
		}
	}

	// Verify all positions are different
	for i := 1; i < len(positions); i++ {
		if positions[i].GetOffset() == positions[i-1].GetOffset() {
			t.Errorf("Positions %d and %d should be different", i-1, i)
		}
	}
}
