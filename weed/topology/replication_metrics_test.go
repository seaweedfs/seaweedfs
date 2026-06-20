package topology

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/stats"
)

func TestClassifyReplicationError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"deadline exceeded", context.DeadlineExceeded, stats.FailureTimeout},
		{"context cancelled", context.Canceled, stats.FailureContextCancelled},
		{"connection refused", errors.New("connection refused"), stats.FailureConnectionRefused},
		{"generic error", errors.New("internal server error"), stats.FailureServerError},
		{"nil error", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyReplicationError(tt.err)
			if got != tt.want {
				t.Errorf("classifyReplicationError() = %q, want %q", got, tt.want)
			}
		})
	}
}
