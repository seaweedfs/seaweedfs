package s3api

import "testing"

func TestComputePackedPolicySize(t *testing.T) {
	cases := []struct {
		name      string
		policyLen int
		empty     bool
		want      int64
	}{
		{"empty -> nil", 0, true, 0},
		{"tiny policy -> 0%", 10, false, 0},
		{"half budget -> 50%", sessionPolicyBudgetBytes / 2, false, 50},
		{"full budget -> 100%", sessionPolicyBudgetBytes, false, 100},
		{"oversized -> capped at 100", sessionPolicyBudgetBytes * 3, false, 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy := repeat('a', tc.policyLen)
			got := computePackedPolicySize(policy)
			switch {
			case tc.empty:
				if got != nil {
					t.Fatalf("expected nil for empty input, got %d", *got)
				}
			case got == nil:
				t.Fatalf("expected non-nil result, got nil")
			case *got != tc.want:
				t.Fatalf("got=%d want=%d", *got, tc.want)
			}
		})
	}
}
