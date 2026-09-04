package sts

import (
	"testing"
	"time"
)

func TestCalculateSessionDuration(t *testing.T) {
	svc := NewSTSService()
	if err := svc.Initialize(&STSConfig{
		TokenDuration:    FlexibleDuration{time.Hour},
		MaxSessionLength: FlexibleDuration{12 * time.Hour},
		Issuer:           "test-issuer",
		SigningKey:       []byte("test-signing-key-at-least-32-bytes-long"),
	}); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	seconds := func(v int64) *int64 { return &v }

	tests := []struct {
		name            string
		durationSeconds *int64
		want            time.Duration
	}{
		{"default from config", nil, time.Hour},
		{"explicit request", seconds(1800), 30 * time.Minute},
		{"capped at MaxSessionLength", seconds(86400), 12 * time.Hour},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := svc.calculateSessionDuration(tc.durationSeconds); got != tc.want {
				t.Errorf("calculateSessionDuration(%v) = %v, want %v", tc.durationSeconds, got, tc.want)
			}
		})
	}
}
