package s3api

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestShouldWriteStreamingErrorResponse(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "context canceled",
			err:      context.Canceled,
			expected: false,
		},
		{
			name:     "wrapped context canceled",
			err:      &StreamError{Err: context.Canceled},
			expected: false,
		},
		{
			name:     "grpc canceled",
			err:      status.Error(codes.Canceled, "client connection is closing"),
			expected: false,
		},
		{
			name:     "wrapped grpc canceled",
			err:      &StreamError{Err: status.Error(codes.Canceled, "client connection is closing")},
			expected: false,
		},
		{
			name:     "deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: true,
		},
		{
			name:     "wrapped deadline exceeded",
			err:      &StreamError{Err: context.DeadlineExceeded},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldWriteStreamingErrorResponse(tt.err); got != tt.expected {
				t.Fatalf("shouldWriteStreamingErrorResponse(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}
