package credential

import "testing"

func TestValidateServiceAccountId(t *testing.T) {
	cases := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{"simple", "sa:alice:abcdef0123456789", false},
		{"uppercase parent", "sa:ALICE:abcdef0123456789", false},
		{"hyphenated uuid suffix", "sa:alice:123e4567-e89b-12d3-a456-426614174000", false},
		{"with hyphen", "sa:test-user:abcdef0123456789", false},
		{"with underscore", "sa:test_user:abcdef0123456789", false},

		// AWS IAM usernames accept `+=,.@-` in addition to alphanumerics
		// and underscore. Service account IDs must accept them too, or
		// callers with realistic usernames hit a validation error at the
		// persistence layer. See
		// https://docs.aws.amazon.com/IAM/latest/APIReference/API_User.html
		{"email-style", "sa:alice@example.com:abcdef0123456789", false},
		{"with dot", "sa:first.last:abcdef0123456789", false},
		{"with plus", "sa:user+tag:abcdef0123456789", false},
		{"with equals", "sa:user=prod:abcdef0123456789", false},
		{"with comma", "sa:a,b:abcdef0123456789", false},

		{"empty", "", true},
		{"missing prefix", "alice:abcdef0123456789", true},
		{"wrong prefix", "svc:alice:abcdef0123456789", true},
		{"uppercase uuid", "sa:alice:ABCDEF0123456789", true},
		{"missing uuid", "sa:alice:", true},
		{"missing user", "sa::abcdef0123456789", true},
		// Colon is not in the AWS IAM username set, and would break
		// the colon-separated layout.
		{"colon in user", "sa:a:b:abcdef0123456789", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateServiceAccountId(tc.id)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for %q, got nil", tc.id)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.id, err)
			}
		})
	}
}
