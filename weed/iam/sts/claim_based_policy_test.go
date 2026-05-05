package sts

import "testing"

func TestIsClaimBasedPolicyRoleArn(t *testing.T) {
	cases := []struct {
		name string
		arn  string
		want bool
	}{
		{"empty matches", "", true},
		{"sentinel matches", ClaimBasedPolicyRoleArn, true},
		{"concrete role does not", "arn:aws:iam::123:role/admin", false},
		{"random ARN does not", "arn:aws:sts::123:assumed-role/X/Y", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsClaimBasedPolicyRoleArn(tc.arn); got != tc.want {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
		})
	}
}
