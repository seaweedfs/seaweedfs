package s3api

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestEscapeOwnerName(t *testing.T) {
	tests := []struct {
		owner string
		want  string
	}{
		{"alice", "alice"},
		{"arn:aws:iam::123:user/bob", "arn:aws:iam::123:user%2Fbob"},
		{".", "%2E"},
		{"..", "%2E%2E"},
		{"../../../etc", "..%2F..%2F..%2Fetc"},
		{"%2E", "%252E"},
	}
	for _, tt := range tests {
		got := escapeOwnerName(tt.owner)
		if got != tt.want {
			t.Errorf("escapeOwnerName(%q) = %q, want %q", tt.owner, got, tt.want)
		}
		// No result may resolve to a different directory level.
		if strings.ContainsRune(got, '/') || got == "." || got == ".." {
			t.Errorf("escapeOwnerName(%q) = %q is not a safe single path segment", tt.owner, got)
		}
	}
}

func TestBucketEntryOwner(t *testing.T) {
	if owner := bucketEntryOwner(nil); owner != "" {
		t.Errorf("nil entry owner = %q", owner)
	}
	if owner := bucketEntryOwner(&filer_pb.Entry{Name: "b"}); owner != "" {
		t.Errorf("ownerless entry owner = %q", owner)
	}
	entry := &filer_pb.Entry{
		Name:     "b",
		Extended: map[string][]byte{s3_constants.AmzIdentityId: []byte("alice")},
	}
	if owner := bucketEntryOwner(entry); owner != "alice" {
		t.Errorf("owner = %q, want alice", owner)
	}
}
