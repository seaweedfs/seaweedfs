package shell

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	s3_constants "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// The heal clears the pointer only while the directory still names the file
// the audit found missing; a version written since then has moved it.
func TestHealStrandedPointerIsConditional(t *testing.T) {
	fake := &conditionalUpdateFiler{}
	entry := &filer_pb.Entry{Name: "obj.versions", IsDirectory: true, Extended: map[string][]byte{
		s3_constants.ExtLatestVersionIdKey:       []byte("v1"),
		s3_constants.ExtLatestVersionFileNameKey: []byte("v_v1"),
		"keep":                                   []byte("x"),
	}}
	if err := healStrandedPointer(context.Background(), fake, "/buckets/b", entry); err != nil {
		t.Fatalf("heal: %v", err)
	}

	clauses := fake.req.GetCondition().GetClauses()
	if len(clauses) != 1 || clauses[0].Kind != filer_pb.WriteCondition_IF_ENTRY_EQUAL {
		t.Fatalf("heal must be conditioned on the audited entry, got %v", fake.req.GetCondition())
	}
	if string(clauses[0].ExpectedEntry.Extended[s3_constants.ExtLatestVersionIdKey]) != "v1" {
		t.Fatalf("expected entry must hold the pointer the audit saw, got %v", clauses[0].ExpectedEntry.Extended)
	}
	written := fake.req.Entry.Extended
	_, idKept := written[s3_constants.ExtLatestVersionIdKey]
	_, nameKept := written[s3_constants.ExtLatestVersionFileNameKey]
	if idKept || nameKept || string(written["keep"]) != "x" {
		t.Fatalf("written entry must clear only the pointer, got %v", written)
	}
}
