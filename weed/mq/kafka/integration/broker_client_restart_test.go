package integration

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/metadata"
)

// MockSubscribeStream implements mq_pb.SeaweedMessaging_SubscribeMessageClient for testing
type MockSubscribeStream struct {
	sendCalls []interface{}
	closed    bool
}

func (m *MockSubscribeStream) Send(req *mq_pb.SubscribeMessageRequest) error {
	m.sendCalls = append(m.sendCalls, req)
	return nil
}

func (m *MockSubscribeStream) Recv() (*mq_pb.SubscribeMessageResponse, error) {
	return nil, nil
}

func (m *MockSubscribeStream) CloseSend() error {
	m.closed = true
	return nil
}

func (m *MockSubscribeStream) Header() (metadata.MD, error) { return nil, nil }
func (m *MockSubscribeStream) Trailer() metadata.MD         { return nil }
func (m *MockSubscribeStream) Context() context.Context     { return context.Background() }
func (m *MockSubscribeStream) SendMsg(m2 interface{}) error { return nil }
func (m *MockSubscribeStream) RecvMsg(m2 interface{}) error { return nil }

// TestNeedsRestart tests the NeedsRestart logic
func TestNeedsRestart(t *testing.T) {
	bc := &BrokerClient{}

	tests := []struct {
		name            string
		session         *BrokerSubscriberSession
		requestedOffset int64
		want            bool
		reason          string
	}{
		{
			name: "Stream is nil - needs restart",
			session: &BrokerSubscriberSession{
				Topic:       "test-topic",
				Partition:   0,
				StartOffset: 100,
				Stream:      nil,
			},
			requestedOffset: 100,
			want:            true,
			reason:          "Stream is nil",
		},
		{
			name: "Offset in cache - no restart needed",
			session: &BrokerSubscriberSession{
				Topic:       "test-topic",
				Partition:   0,
				StartOffset: 100,
				Stream:      &MockSubscribeStream{},
				Ctx:         context.Background(),
				consumedRecords: []*SeaweedRecord{
					{Offset: 95},
					{Offset: 96},
					{Offset: 97},
					{Offset: 98},
					{Offset: 99},
				},
			},
			requestedOffset: 97,
			want:            false,
			reason:          "Offset 97 is in cache [95-99]",
		},
		{
			name: "Offset before current - needs restart",
			session: &BrokerSubscriberSession{
				Topic:       "test-topic",
				Partition:   0,
				StartOffset: 100,
				Stream:      &MockSubscribeStream{},
				Ctx:         context.Background(),
			},
			requestedOffset: 50,
			want:            true,
			reason:          "Requested offset 50 < current 100",
		},
		{
			name: "Large gap ahead - needs restart",
			session: &BrokerSubscriberSession{
				Topic:       "test-topic",
				Partition:   0,
				StartOffset: 100,
				Stream:      &MockSubscribeStream{},
				Ctx:         context.Background(),
			},
			requestedOffset: 2000,
			want:            true,
			reason:          "Gap of 1900 is > 1000",
		},
		{
			name: "Small gap ahead - no restart needed",
			session: &BrokerSubscriberSession{
				Topic:       "test-topic",
				Partition:   0,
				StartOffset: 100,
				Stream:      &MockSubscribeStream{},
				Ctx:         context.Background(),
			},
			requestedOffset: 150,
			want:            false,
			reason:          "Gap of 50 is < 1000",
		},
		{
			name: "Exact match - no restart needed",
			session: &BrokerSubscriberSession{
				Topic:       "test-topic",
				Partition:   0,
				StartOffset: 100,
				Stream:      &MockSubscribeStream{},
				Ctx:         context.Background(),
			},
			requestedOffset: 100,
			want:            false,
			reason:          "Exact match with current offset",
		},
		{
			name: "Context is nil - needs restart",
			session: &BrokerSubscriberSession{
				Topic:       "test-topic",
				Partition:   0,
				StartOffset: 100,
				Stream:      &MockSubscribeStream{},
				Ctx:         nil,
			},
			requestedOffset: 100,
			want:            true,
			reason:          "Context is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bc.NeedsRestart(tt.session, tt.requestedOffset)
			if got != tt.want {
				t.Errorf("NeedsRestart() = %v, want %v (reason: %s)", got, tt.want, tt.reason)
			}
		})
	}
}

// TestNeedsRestart_CacheLogic tests cache-based restart decisions
func TestNeedsRestart_CacheLogic(t *testing.T) {
	bc := &BrokerClient{}

	// Create session with cache containing offsets 100-109
	session := &BrokerSubscriberSession{
		Topic:       "test-topic",
		Partition:   0,
		StartOffset: 110,
		Stream:      &MockSubscribeStream{},
		Ctx:         context.Background(),
		consumedRecords: []*SeaweedRecord{
			{Offset: 100}, {Offset: 101}, {Offset: 102}, {Offset: 103}, {Offset: 104},
			{Offset: 105}, {Offset: 106}, {Offset: 107}, {Offset: 108}, {Offset: 109},
		},
	}

	testCases := []struct {
		offset int64
		want   bool
		desc   string
	}{
		{100, false, "First offset in cache"},
		{105, false, "Middle offset in cache"},
		{109, false, "Last offset in cache"},
		{99, true, "Before cache start"},
		{110, false, "Current position"},
		{111, false, "One ahead"},
		{1200, true, "Large gap > 1000"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got := bc.NeedsRestart(session, tc.offset)
			if got != tc.want {
				t.Errorf("NeedsRestart(offset=%d) = %v, want %v (%s)", tc.offset, got, tc.want, tc.desc)
			}
		})
	}
}

// TestNeedsRestart_EmptyCache tests behavior with empty cache
func TestNeedsRestart_EmptyCache(t *testing.T) {
	bc := &BrokerClient{}

	session := &BrokerSubscriberSession{
		Topic:           "test-topic",
		Partition:       0,
		StartOffset:     100,
		Stream:          &MockSubscribeStream{},
		Ctx:             context.Background(),
		consumedRecords: nil, // Empty cache
	}

	tests := []struct {
		offset int64
		want   bool
		desc   string
	}{
		{50, true, "Before current"},
		{100, false, "At current"},
		{150, false, "Small gap ahead"},
		{1200, true, "Large gap ahead"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			got := bc.NeedsRestart(session, tt.offset)
			if got != tt.want {
				t.Errorf("NeedsRestart(offset=%d) = %v, want %v (%s)", tt.offset, got, tt.want, tt.desc)
			}
		})
	}
}

// TestNeedsRestart_ThreadSafety tests concurrent access
func TestNeedsRestart_ThreadSafety(t *testing.T) {
	bc := &BrokerClient{}

	session := &BrokerSubscriberSession{
		Topic:       "test-topic",
		Partition:   0,
		StartOffset: 100,
		Stream:      &MockSubscribeStream{},
		Ctx:         context.Background(),
	}

	// Run many concurrent checks
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(offset int64) {
			bc.NeedsRestart(session, offset)
			done <- true
		}(int64(i))
	}

	// Wait for all to complete
	for i := 0; i < 100; i++ {
		<-done
	}

	// Test passes if no panic/race condition
}

// TestRestartSubscriber_StateManagement tests session state management
func TestRestartSubscriber_StateManagement(t *testing.T) {
	oldStream := &MockSubscribeStream{}
	oldCtx, oldCancel := context.WithCancel(context.Background())

	session := &BrokerSubscriberSession{
		Topic:       "test-topic",
		Partition:   0,
		StartOffset: 100,
		Stream:      oldStream,
		Ctx:         oldCtx,
		Cancel:      oldCancel,
		consumedRecords: []*SeaweedRecord{
			{Offset: 100, Key: []byte("key100"), Value: []byte("value100")},
			{Offset: 101, Key: []byte("key101"), Value: []byte("value101")},
			{Offset: 102, Key: []byte("key102"), Value: []byte("value102")},
		},
		nextOffsetToRead: 103,
	}

	// Verify initial state
	if len(session.consumedRecords) != 3 {
		t.Errorf("Initial cache size = %d, want 3", len(session.consumedRecords))
	}
	if session.nextOffsetToRead != 103 {
		t.Errorf("Initial nextOffsetToRead = %d, want 103", session.nextOffsetToRead)
	}
	if session.StartOffset != 100 {
		t.Errorf("Initial StartOffset = %d, want 100", session.StartOffset)
	}

	// Note: Full RestartSubscriber testing requires gRPC mocking
	// These tests verify the core state management and NeedsRestart logic
}

// BenchmarkNeedsRestart_CacheHit benchmarks cache hit performance
func BenchmarkNeedsRestart_CacheHit(b *testing.B) {
	bc := &BrokerClient{}

	session := &BrokerSubscriberSession{
		Topic:           "test-topic",
		Partition:       0,
		StartOffset:     1000,
		Stream:          &MockSubscribeStream{},
		Ctx:             context.Background(),
		consumedRecords: make([]*SeaweedRecord, 100),
	}

	for i := 0; i < 100; i++ {
		session.consumedRecords[i] = &SeaweedRecord{Offset: int64(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bc.NeedsRestart(session, 50) // Hit cache
	}
}

// BenchmarkNeedsRestart_CacheMiss benchmarks cache miss performance
func BenchmarkNeedsRestart_CacheMiss(b *testing.B) {
	bc := &BrokerClient{}

	session := &BrokerSubscriberSession{
		Topic:           "test-topic",
		Partition:       0,
		StartOffset:     1000,
		Stream:          &MockSubscribeStream{},
		Ctx:             context.Background(),
		consumedRecords: make([]*SeaweedRecord, 100),
	}

	for i := 0; i < 100; i++ {
		session.consumedRecords[i] = &SeaweedRecord{Offset: int64(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bc.NeedsRestart(session, 500) // Miss cache (within gap threshold)
	}
}
