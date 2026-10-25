package integration

import "testing"

func intPtr(v int64) *int64 { return &v }

func TestCapDurationByRole(t *testing.T) {
	cases := []struct {
		name          string
		requested     *int64
		roleMax       int64
		defaultSec    int64
		serviceMaxSec int64
		want          *int64
	}{
		{"no cap, no request -> nil keeps service default", nil, 0, 3600, 43200, nil},
		{"no cap, with request", intPtr(7200), 0, 3600, 43200, intPtr(7200)},
		{"cap below default, no request -> cap", nil, 1800, 3600, 43200, intPtr(1800)},
		{"cap above default, no request -> nil", nil, 43200, 3600, 43200, nil},
		{"request below cap -> request", intPtr(1800), 3600, 900, 43200, intPtr(1800)},
		{"request equal cap -> request", intPtr(3600), 3600, 900, 43200, intPtr(3600)},
		{"request above cap -> cap", intPtr(43200), 3600, 900, 43200, intPtr(3600)},
		{"default above service max -> service cap materialized", nil, 0, 7200, 3600, intPtr(3600)},
		{"role bound above service max -> service cap still applies", nil, 40000, 43200, 3600, intPtr(3600)},
		{"role bound below service floor -> tightest issuable", nil, 500, 3600, 43200, intPtr(900)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := capDurationByRole(tc.requested, tc.roleMax, tc.defaultSec, tc.serviceMaxSec)
			switch {
			case got == nil && tc.want == nil:
				return
			case got == nil || tc.want == nil:
				t.Fatalf("nilness mismatch: got=%v want=%v", got, tc.want)
			case *got != *tc.want:
				t.Fatalf("got=%d want=%d", *got, *tc.want)
			}
		})
	}
}
