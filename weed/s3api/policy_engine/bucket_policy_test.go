package policy_engine

import "testing"

func TestResourceMatchesBucket(t *testing.T) {
	tests := []struct {
		name     string
		resource string
		bucket   string
		want     bool
	}{
		{"bare bucket name", "my-bucket", "my-bucket", true},
		{"bare wildcard", "my-bucket/*", "my-bucket", true},
		{"bare object key", "my-bucket/path/to/key", "my-bucket", true},
		{"arn bucket", "arn:aws:s3:::my-bucket", "my-bucket", true},
		{"arn wildcard", "arn:aws:s3:::my-bucket/*", "my-bucket", true},
		{"arn object key", "arn:aws:s3:::my-bucket/path/to/key", "my-bucket", true},
		{"wrong bucket", "other-bucket", "my-bucket", false},
		{"wrong bucket arn", "arn:aws:s3:::other-bucket/*", "my-bucket", false},
		{"prefix collision", "my-bucket2", "my-bucket", false},
		{"prefix collision arn", "arn:aws:s3:::my-bucket2/*", "my-bucket", false},
		{"empty resource", "", "my-bucket", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResourceMatchesBucket(tt.resource, tt.bucket); got != tt.want {
				t.Errorf("ResourceMatchesBucket(%q, %q) = %v, want %v", tt.resource, tt.bucket, got, tt.want)
			}
		})
	}
}

func simpleAwsPrincipal() *PolicyPrincipal {
	return NewPolicyPrincipalPtr("*")
}

func TestValidateBucketPolicy(t *testing.T) {
	bucket := "my-bucket"

	validStatement := func() PolicyStatement {
		return PolicyStatement{
			Effect:    PolicyEffectAllow,
			Principal: simpleAwsPrincipal(),
			Action:    NewStringOrStringSlice("s3:GetObject"),
			Resource:  NewStringOrStringSlicePtr("arn:aws:s3:::" + bucket + "/*"),
		}
	}

	t.Run("valid policy", func(t *testing.T) {
		doc := &PolicyDocument{Version: PolicyVersion2012_10_17, Statement: []PolicyStatement{validStatement()}}
		if err := ValidateBucketPolicy(doc, bucket); err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("bad version", func(t *testing.T) {
		// Version and non-empty-statement rules live in ValidatePolicy,
		// which callers run first.
		doc := &PolicyDocument{Version: "2008-10-17", Statement: []PolicyStatement{validStatement()}}
		if err := ValidatePolicy(doc); err == nil {
			t.Error("expected error for bad version")
		}
	})

	t.Run("zero statements", func(t *testing.T) {
		doc := &PolicyDocument{Version: PolicyVersion2012_10_17, Statement: []PolicyStatement{}}
		if err := ValidatePolicy(doc); err == nil {
			t.Error("expected error for zero statements")
		}
	})

	t.Run("missing principal", func(t *testing.T) {
		stmt := validStatement()
		stmt.Principal = nil
		doc := &PolicyDocument{Version: PolicyVersion2012_10_17, Statement: []PolicyStatement{stmt}}
		if err := ValidateBucketPolicy(doc, bucket); err == nil {
			t.Error("expected error for missing principal")
		}
	})

	t.Run("foreign resource", func(t *testing.T) {
		stmt := validStatement()
		stmt.Resource = NewStringOrStringSlicePtr("arn:aws:s3:::other-bucket/*")
		doc := &PolicyDocument{Version: PolicyVersion2012_10_17, Statement: []PolicyStatement{stmt}}
		if err := ValidateBucketPolicy(doc, bucket); err == nil {
			t.Error("expected error for foreign resource")
		}
	})

	t.Run("foreign not-resource", func(t *testing.T) {
		stmt := validStatement()
		stmt.Resource = nil
		stmt.NotResource = NewStringOrStringSlicePtr("arn:aws:s3:::other-bucket/*")
		doc := &PolicyDocument{Version: PolicyVersion2012_10_17, Statement: []PolicyStatement{stmt}}
		if err := ValidateBucketPolicy(doc, bucket); err == nil {
			t.Error("expected error for foreign NotResource")
		}
	})

	t.Run("non-s3 action", func(t *testing.T) {
		stmt := validStatement()
		stmt.Action = NewStringOrStringSlice("iam:CreateUser")
		doc := &PolicyDocument{Version: PolicyVersion2012_10_17, Statement: []PolicyStatement{stmt}}
		if err := ValidateBucketPolicy(doc, bucket); err == nil {
			t.Error("expected error for non-s3 action")
		}
	})
}
