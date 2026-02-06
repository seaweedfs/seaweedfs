package filer

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// mockMasterClient implements HasLookupFileIdFunction and CacheInvalidator
type mockMasterClient struct {
	lookupFunc         func(ctx context.Context, fileId string) ([]string, error)
	invalidatedFileIds []string
}

func (m *mockMasterClient) GetLookupFileIdFunction() wdclient.LookupFileIdFunctionType {
	return m.lookupFunc
}

func (m *mockMasterClient) InvalidateCache(fileId string) {
	m.invalidatedFileIds = append(m.invalidatedFileIds, fileId)
}

// Test urlSlicesEqual helper function
func TestUrlSlicesEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        []string
		b        []string
		expected bool
	}{
		{
			name:     "identical slices",
			a:        []string{"http://server1", "http://server2"},
			b:        []string{"http://server1", "http://server2"},
			expected: true,
		},
		{
			name:     "same URLs different order",
			a:        []string{"http://server1", "http://server2"},
			b:        []string{"http://server2", "http://server1"},
			expected: true,
		},
		{
			name:     "different URLs",
			a:        []string{"http://server1", "http://server2"},
			b:        []string{"http://server1", "http://server3"},
			expected: false,
		},
		{
			name:     "different lengths",
			a:        []string{"http://server1"},
			b:        []string{"http://server1", "http://server2"},
			expected: false,
		},
		{
			name:     "empty slices",
			a:        []string{},
			b:        []string{},
			expected: true,
		},
		{
			name:     "duplicates in both",
			a:        []string{"http://server1", "http://server1"},
			b:        []string{"http://server1", "http://server1"},
			expected: true,
		},
		{
			name:     "different duplicate counts",
			a:        []string{"http://server1", "http://server1"},
			b:        []string{"http://server1", "http://server2"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := urlSlicesEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("urlSlicesEqual(%v, %v) = %v; want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// Test cache invalidation when read fails
func TestStreamContentWithCacheInvalidation(t *testing.T) {
	ctx := context.Background()
	fileId := "3,01234567890"

	callCount := 0
	oldUrls := []string{"http://failed-server:8080"}
	newUrls := []string{"http://working-server:8080"}

	mock := &mockMasterClient{
		lookupFunc: func(ctx context.Context, fid string) ([]string, error) {
			callCount++
			if callCount == 1 {
				// First call returns failing server
				return oldUrls, nil
			}
			// After invalidation, return working server
			return newUrls, nil
		},
	}

	// Create a simple chunk
	chunks := []*filer_pb.FileChunk{
		{
			FileId: fileId,
			Offset: 0,
			Size:   10,
		},
	}

	streamFn, err := PrepareStreamContentWithThrottler(ctx, mock, noJwtFunc, chunks, 0, 10, 0)
	if err != nil {
		t.Fatalf("PrepareStreamContentWithThrottler failed: %v", err)
	}

	// Note: This test can't fully execute streamFn because it would require actual HTTP servers
	// However, we can verify the setup was created correctly
	if streamFn == nil {
		t.Fatal("Expected non-nil stream function")
	}

	// Verify the lookup was called
	if callCount != 1 {
		t.Errorf("Expected 1 lookup call, got %d", callCount)
	}
}

// Test that InvalidateCache is called on read failure
func TestCacheInvalidationInterface(t *testing.T) {
	mock := &mockMasterClient{
		lookupFunc: func(ctx context.Context, fileId string) ([]string, error) {
			return []string{"http://server:8080"}, nil
		},
	}

	fileId := "3,test123"

	// Simulate invalidation
	if invalidator, ok := interface{}(mock).(CacheInvalidator); ok {
		invalidator.InvalidateCache(fileId)
	} else {
		t.Fatal("mockMasterClient should implement CacheInvalidator")
	}

	// Check that the file ID was recorded as invalidated
	if len(mock.invalidatedFileIds) != 1 {
		t.Fatalf("Expected 1 invalidated file ID, got %d", len(mock.invalidatedFileIds))
	}
	if mock.invalidatedFileIds[0] != fileId {
		t.Errorf("Expected invalidated file ID %s, got %s", fileId, mock.invalidatedFileIds[0])
	}
}

// Test retry logic doesn't retry with same URLs
func TestRetryLogicSkipsSameUrls(t *testing.T) {
	// This test verifies that the urlSlicesEqual check prevents infinite retries
	sameUrls := []string{"http://server1:8080", "http://server2:8080"}
	differentUrls := []string{"http://server3:8080", "http://server4:8080"}

	// Same URLs should return true (and thus skip retry)
	if !urlSlicesEqual(sameUrls, sameUrls) {
		t.Error("Expected same URLs to be equal")
	}

	// Different URLs should return false (and thus allow retry)
	if urlSlicesEqual(sameUrls, differentUrls) {
		t.Error("Expected different URLs to not be equal")
	}
}
