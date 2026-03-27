package s3api

import (
	"encoding/xml"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestValidateDeleteIfMatch(t *testing.T) {
	s3a := NewS3ApiServerForTest()
	existingEntry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.ExtETagKey: []byte("\"abc123\""),
		},
	}
	deleteMarkerEntry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.ExtDeleteMarkerKey: []byte("true"),
		},
	}

	testCases := []struct {
		name        string
		entry       *filer_pb.Entry
		ifMatch     string
		missingCode s3err.ErrorCode
		expected    s3err.ErrorCode
	}{
		{
			name:        "matching etag succeeds",
			entry:       existingEntry,
			ifMatch:     "\"abc123\"",
			missingCode: s3err.ErrPreconditionFailed,
			expected:    s3err.ErrNone,
		},
		{
			name:        "wildcard succeeds for existing entry",
			entry:       existingEntry,
			ifMatch:     "*",
			missingCode: s3err.ErrPreconditionFailed,
			expected:    s3err.ErrNone,
		},
		{
			name:        "mismatched etag fails",
			entry:       existingEntry,
			ifMatch:     "\"other\"",
			missingCode: s3err.ErrPreconditionFailed,
			expected:    s3err.ErrPreconditionFailed,
		},
		{
			name:        "missing current object fails single delete",
			entry:       nil,
			ifMatch:     "*",
			missingCode: s3err.ErrPreconditionFailed,
			expected:    s3err.ErrPreconditionFailed,
		},
		{
			name:        "missing current object returns no such key for batch delete",
			entry:       nil,
			ifMatch:     "*",
			missingCode: s3err.ErrNoSuchKey,
			expected:    s3err.ErrNoSuchKey,
		},
		{
			name:        "current delete marker behaves like missing object",
			entry:       normalizeConditionalTargetEntry(deleteMarkerEntry),
			ifMatch:     "*",
			missingCode: s3err.ErrPreconditionFailed,
			expected:    s3err.ErrPreconditionFailed,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if errCode := s3a.validateDeleteIfMatch(tc.entry, tc.ifMatch, tc.missingCode); errCode != tc.expected {
				t.Fatalf("validateDeleteIfMatch() = %v, want %v", errCode, tc.expected)
			}
		})
	}
}

func TestDeleteObjectsRequestUnmarshalConditionalETags(t *testing.T) {
	var req DeleteObjectsRequest
	body := []byte(`
<Delete>
  <Quiet>true</Quiet>
  <Object>
    <Key>first.txt</Key>
    <ETag>*</ETag>
  </Object>
  <Object>
    <Key>second.txt</Key>
    <VersionId>3HL4kqCxf3vjVBH40Nrjfkd</VersionId>
    <ETag>"abc123"</ETag>
  </Object>
</Delete>`)

	if err := xml.Unmarshal(body, &req); err != nil {
		t.Fatalf("xml.Unmarshal() error = %v", err)
	}
	if !req.Quiet {
		t.Fatalf("expected Quiet=true")
	}
	if len(req.Objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(req.Objects))
	}
	if req.Objects[0].ETag != "*" {
		t.Fatalf("expected first object ETag to be '*', got %q", req.Objects[0].ETag)
	}
	if req.Objects[1].ETag != "\"abc123\"" {
		t.Fatalf("expected second object ETag to preserve quotes, got %q", req.Objects[1].ETag)
	}
	if req.Objects[1].VersionId != "3HL4kqCxf3vjVBH40Nrjfkd" {
		t.Fatalf("expected second object VersionId to unmarshal, got %q", req.Objects[1].VersionId)
	}
}
