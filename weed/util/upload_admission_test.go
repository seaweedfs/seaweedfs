package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUploadAdmits(t *testing.T) {
	cases := []struct {
		name                  string
		inFlight, size, limit int64
		want                  bool
	}{
		{"unlimited", 1 << 40, 1 << 30, 0, true},
		{"fits exactly", 6, 4, 10, true},
		{"would overshoot", 8, 4, 10, false},
		{"oversized alone", 0, 100, 10, true},
		{"oversized behind others", 1, 100, 10, false},
		{"zero size at limit", 10, 0, 10, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, UploadAdmits(c.inFlight, c.size, c.limit))
		})
	}
}

func TestCountedUploadSize(t *testing.T) {
	require.Equal(t, int64(0), CountedUploadSize(0, 8<<20))
	require.Equal(t, int64(123), CountedUploadSize(123, 8<<20))
	require.Equal(t, int64(8<<20), CountedUploadSize(-1, 8<<20))
}
