package balancer

import "testing"

func TestDiskTooFullAfter(t *testing.T) {
	const gb = uint64(1) << 30
	tests := []struct {
		name                  string
		total, free, incoming uint64
		pct                   int
		want                  bool
	}{
		{"disabled at 0", 1000 * gb, 40 * gb, 0, 0, false},
		{"disabled at 100", 1000 * gb, 40 * gb, 0, 100, false},
		{"not reported (total 0)", 0, 0, 0, 90, false},
		{"below mark", 1000 * gb, 300 * gb, 0, 90, false},                           // 70% used
		{"at mark", 1000 * gb, 100 * gb, 0, 90, true},                               // exactly 90% used
		{"above mark", 1000 * gb, 40 * gb, 0, 90, true},                             // 96% used
		{"below but crosses with incoming", 1000 * gb, 120 * gb, 30 * gb, 90, true}, // 88% -> 91%
		{"below and stays below with incoming", 1000 * gb, 300 * gb, 30 * gb, 90, false},
		{"free exceeds total (defensive)", 1000 * gb, 2000 * gb, 0, 90, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := DiskTooFullAfter(tc.total, tc.free, tc.incoming, tc.pct); got != tc.want {
				t.Errorf("DiskTooFullAfter(%d,%d,%d,%d) = %v, want %v",
					tc.total, tc.free, tc.incoming, tc.pct, got, tc.want)
			}
		})
	}
}
