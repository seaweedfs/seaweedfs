package app

import "testing"

func TestGroupDigits(t *testing.T) {
	for n, want := range map[float64]string{0: "0", 999: "999", 1000: "1,000", 12400: "12,400", 2140312: "2,140,312", 8899.6: "8,900"} {
		if got := groupDigits(n); got != want {
			t.Errorf("groupDigits(%v) = %q, want %q", n, got, want)
		}
	}
}
