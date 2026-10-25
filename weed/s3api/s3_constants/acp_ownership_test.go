package s3_constants

import "testing"

func TestEffectiveOwnership(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "unset", input: "", want: OwnershipBucketOwnerEnforced},
		{name: "invalid", input: "Bogus", want: OwnershipBucketOwnerEnforced},
		{name: "object writer", input: OwnershipObjectWriter, want: OwnershipObjectWriter},
		{name: "bucket owner preferred", input: OwnershipBucketOwnerPreferred, want: OwnershipBucketOwnerPreferred},
		{name: "bucket owner enforced", input: OwnershipBucketOwnerEnforced, want: OwnershipBucketOwnerEnforced},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := EffectiveOwnership(tc.input); got != tc.want {
				t.Fatalf("EffectiveOwnership(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
