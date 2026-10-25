package policy_engine

import (
	"strings"
	"testing"
)

// BucketsAllowedForAction has to read a grant at least as loosely as the
// authorizer applies it, since a bucket it misses disappears from ListBuckets.
func TestBucketsAllowedForAction(t *testing.T) {
	engine := NewPolicyEngine()
	policies := map[string]string{
		"put": `{"Version":"2012-10-17","Statement":[
			{"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::b1/*"]}]}`,
		"mixed-case": `{"Version":"2012-10-17","Statement":[
			{"Effect":"Allow","Action":["S3:LISTBUCKET"],"Resource":["arn:aws:s3:::b2"]}]}`,
		"any-bucket": `{"Version":"2012-10-17","Statement":[
			{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::*"]}]}`,
	}
	for name, document := range policies {
		if err := engine.SetBucketPolicy(name, document); err != nil {
			t.Fatalf("load policy %s: %v", name, err)
		}
	}

	tests := []struct {
		name         string
		policy       string
		action       string
		wantBuckets  string
		wantComplete bool
	}{
		// Multipart uploads ride on s3:PutObject, in whatever case they are asked for.
		{name: "multipart inherits put", policy: "put", action: "s3:UploadPart", wantBuckets: "b1", wantComplete: true},
		{name: "multipart inherits put in any case", policy: "put", action: "S3:UPLOADPART", wantBuckets: "b1", wantComplete: true},
		{name: "unrelated action names nothing", policy: "put", action: "s3:ListBucket", wantComplete: true},
		{name: "mixed case grant is read", policy: "mixed-case", action: "s3:ListBucket", wantBuckets: "b2", wantComplete: true},
		{name: "wildcard bucket is incomplete", policy: "any-bucket", action: "s3:ListBucket"},
		{name: "unknown policy is incomplete", policy: "absent", action: "s3:ListBucket"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buckets, complete := engine.BucketsAllowedForAction(tt.policy, tt.action)
			if complete != tt.wantComplete {
				t.Fatalf("complete = %v, want %v", complete, tt.wantComplete)
			}
			if got := strings.Join(buckets, ","); got != tt.wantBuckets {
				t.Errorf("buckets = %q, want %q", got, tt.wantBuckets)
			}
		})
	}
}
