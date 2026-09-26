package integration

import "testing"

func intPtr(v int64) *int64 { return &v }

func TestCapDurationByRole(t *testing.T) {
	cases := []struct {
		name       string
		requested  *int64
		roleMax    int64
		defaultSec int64
		want       int64
	}{
		{"no cap, no request -> default", nil, 0, 3600, 3600},
		{"no cap, with request", intPtr(7200), 0, 3600, 7200},
		{"cap below default, no request -> cap", nil, 1800, 3600, 1800},
		{"cap above default, no request -> default", nil, 43200, 3600, 3600},
		{"request below cap -> request", intPtr(1800), 3600, 900, 1800},
		{"request equal cap -> request", intPtr(3600), 3600, 900, 3600},
		{"request above cap -> cap", intPtr(43200), 3600, 900, 3600},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := capDurationByRole(tc.requested, tc.roleMax, tc.defaultSec)
			if got == nil || *got != tc.want {
				t.Fatalf("got=%v want=%d", got, tc.want)
			}
		})
	}
}
