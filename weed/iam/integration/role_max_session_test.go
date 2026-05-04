package integration

import "testing"

func intPtr(v int64) *int64 { return &v }

func TestCapDurationByRole(t *testing.T) {
	cases := []struct {
		name      string
		requested *int64
		roleMax   int64
		want      *int64
	}{
		{"no cap, no request", nil, 0, nil},
		{"no cap, with request", intPtr(7200), 0, intPtr(7200)},
		{"cap only, no request -> use cap", nil, 3600, intPtr(3600)},
		{"request below cap -> request", intPtr(1800), 3600, intPtr(1800)},
		{"request equal cap -> request", intPtr(3600), 3600, intPtr(3600)},
		{"request above cap -> cap", intPtr(43200), 3600, intPtr(3600)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := capDurationByRole(tc.requested, tc.roleMax)
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
