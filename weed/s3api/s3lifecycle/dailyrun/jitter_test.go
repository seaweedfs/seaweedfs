package dailyrun

import (
	"testing"
	"time"
)

func TestJitterBounds(t *testing.T) {
	cases := []time.Duration{
		200 * time.Millisecond,
		1 * time.Second,
		5 * time.Second,
	}

	for _, d := range cases {
		for i := 0; i < 100; i++ {
			j := jitter(d)
			if j < d/2 {
				t.Errorf("jitter(%v) = %v, below lower bound %v", d, j, d/2)
			}
			if j >= d {
				t.Errorf("jitter(%v) = %v, at or above upper bound %v", d, j, d)
			}
		}
	}
}

func TestJitterZeroAndNegative(t *testing.T) {
	if j := jitter(0); j != 0 {
		t.Errorf("jitter(0) = %v, want 0", j)
	}
	if j := jitter(-1 * time.Second); j != 0 {
		t.Errorf("jitter(-1s) = %v, want 0", j)
	}
}

func TestJitterTinyDuration(t *testing.T) {
	// When d < 2, half == 0 and rand.Int63n(0) panics.
	// We should return d unmodified in that case.
	j := jitter(1 * time.Nanosecond)
	if j != 1*time.Nanosecond {
		t.Errorf("jitter(1ns) = %v, want 1ns", j)
	}
}
