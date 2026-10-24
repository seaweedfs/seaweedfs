package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func bucketEntryWithPolicy(name, policy string) *filer_pb.Entry {
	entry := &filer_pb.Entry{Name: name}
	if policy != "" {
		entry.Extended = map[string][]byte{BUCKET_POLICY_METADATA_KEY: []byte(policy)}
	}
	return entry
}

func TestBucketPolicyMirrorOps(t *testing.T) {
	policyA := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`
	policyB := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny"}]}`

	tests := []struct {
		name         string
		oldEntry     *filer_pb.Entry
		newEntry     *filer_pb.Entry
		removeName   string
		updateName   string
		updatePolicy string
	}{
		{
			name:     "create without policy",
			newEntry: bucketEntryWithPolicy("a", ""),
		},
		{
			name:         "policy set",
			oldEntry:     bucketEntryWithPolicy("a", ""),
			newEntry:     bucketEntryWithPolicy("a", policyA),
			updateName:   "a",
			updatePolicy: policyA,
		},
		{
			name:         "policy replaced",
			oldEntry:     bucketEntryWithPolicy("a", policyA),
			newEntry:     bucketEntryWithPolicy("a", policyB),
			updateName:   "a",
			updatePolicy: policyB,
		},
		{
			name:     "policy unchanged",
			oldEntry: bucketEntryWithPolicy("a", policyA),
			newEntry: bucketEntryWithPolicy("a", policyA),
		},
		{
			name:       "policy removed",
			oldEntry:   bucketEntryWithPolicy("a", policyA),
			newEntry:   bucketEntryWithPolicy("a", ""),
			removeName: "a",
		},
		{
			name:       "bucket deleted",
			oldEntry:   bucketEntryWithPolicy("a", policyA),
			removeName: "a",
		},
		{
			name:     "bucket deleted without policy",
			oldEntry: bucketEntryWithPolicy("a", ""),
		},
		{
			// The rename case: same policy bytes on both sides must still
			// move the mirror to the new name.
			name:         "renamed with unchanged policy",
			oldEntry:     bucketEntryWithPolicy("a", policyA),
			newEntry:     bucketEntryWithPolicy("b", policyA),
			removeName:   "a",
			updateName:   "b",
			updatePolicy: policyA,
		},
		{
			name:         "renamed with changed policy",
			oldEntry:     bucketEntryWithPolicy("a", policyA),
			newEntry:     bucketEntryWithPolicy("b", policyB),
			removeName:   "a",
			updateName:   "b",
			updatePolicy: policyB,
		},
		{
			name:       "renamed and policy dropped",
			oldEntry:   bucketEntryWithPolicy("a", policyA),
			newEntry:   bucketEntryWithPolicy("b", ""),
			removeName: "a",
		},
		{
			name:         "renamed without prior policy",
			oldEntry:     bucketEntryWithPolicy("a", ""),
			newEntry:     bucketEntryWithPolicy("b", policyA),
			updateName:   "b",
			updatePolicy: policyA,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			removeName, updateName, updatePolicy := bucketPolicyMirrorOps(tc.oldEntry, tc.newEntry)
			if removeName != tc.removeName {
				t.Errorf("removeName = %q, want %q", removeName, tc.removeName)
			}
			if updateName != tc.updateName {
				t.Errorf("updateName = %q, want %q", updateName, tc.updateName)
			}
			if string(updatePolicy) != tc.updatePolicy {
				t.Errorf("updatePolicy = %q, want %q", updatePolicy, tc.updatePolicy)
			}
		})
	}
}
