package util

import (
	"testing"
)

func TestTTLReadWrite(t *testing.T) {
	var tests = []struct {
		n        int // input
		expected int // expected result
	}{
		{0, -1},
		{1, 0},
		{1 << 4, 0},
		{1 << 6, 1},
		{1 << 8, 2},
		{1 << 10, 3},
		{1 << 12, 4},
		{1 << 14, 5},
		{1 << 16, 6},
		{1 << 18, 7},
		{1<<4 + 1, 1},
		{1<<6 + 1, 2},
		{1<<8 + 1, 3},
		{1<<10 + 1, 4},
		{1<<12 + 1, 5},
		{1<<14 + 1, 6},
		{1<<16 + 1, 7},
		{1<<18 + 1, 8},
		{1<<28 - 1, 12},
		{1 << 28, 12},
		{1<<28 + 2134, -1},
		{1080, 4},
	}
	for _, tt := range tests {
		actual := findChunkPoolIndex(tt.n)
		if actual != tt.expected {
			t.Errorf("findChunkPoolIndex(%d): expected %d, actual %d", tt.n, tt.expected, actual)
		}
	}
}
