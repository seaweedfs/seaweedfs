package s3api

import (
	"context"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A definitive NotFound (or an aborted request) is an answer, not a transient
// failure. Walking the full backoff ladder before the pre-versioning fallback
// turned every missing-key GetObjectRetention into a 12.7s stall in the field.
func TestLookupVersionsEntryTerminalErrorsSkipTheLadder(t *testing.T) {
	for _, terminal := range []error{
		filer_pb.ErrNotFound,
		fmt.Errorf("wrapped: %w", filer_pb.ErrNotFound),
		status.Error(codes.NotFound, "gone"),
		context.Canceled,
		status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
	} {
		calls := 0
		entry, err := lookupVersionsEntryWithRetry(func() (*filer_pb.Entry, error) {
			calls++
			return nil, terminal
		}, 8)
		assert.Nil(t, entry)
		assert.Equal(t, terminal, err)
		assert.Equal(t, 1, calls, "%v is definitive, no retry", terminal)
	}
}

// Transient filer errors keep the original retry-with-backoff behavior.
func TestLookupVersionsEntryTransientErrorsStillRetry(t *testing.T) {
	want := &filer_pb.Entry{Name: "obj" + ".versions"}
	calls := 0
	entry, err := lookupVersionsEntryWithRetry(func() (*filer_pb.Entry, error) {
		calls++
		if calls < 3 {
			return nil, status.Error(codes.Unavailable, "transport is closing")
		}
		return want, nil
	}, 8)
	assert.NoError(t, err)
	assert.Same(t, want, entry)
	assert.Equal(t, 3, calls)
}

// Exhausting the attempts surfaces the last transient error to the caller.
func TestLookupVersionsEntryTransientExhaustionReturnsLastErr(t *testing.T) {
	calls := 0
	entry, err := lookupVersionsEntryWithRetry(func() (*filer_pb.Entry, error) {
		calls++
		return nil, status.Error(codes.Unavailable, "still down")
	}, 2)
	assert.Nil(t, entry)
	assert.Error(t, err)
	assert.Equal(t, 2, calls)
}
