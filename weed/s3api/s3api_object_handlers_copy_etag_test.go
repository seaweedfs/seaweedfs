package s3api

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestCopyEntryETagPrefersStoredExtendedETag(t *testing.T) {
	storedETag := "11111111111111111111111111111111-2"
	entry := newCopyETagTestEntry(t, storedETag, "22222222222222222222222222222222")

	if got := copyEntryETag(entry); got != storedETag {
		t.Fatalf("copyEntryETag() = %q, want stored extended ETag %q", got, storedETag)
	}
}

func TestCopyEntryETagFallsBackToFilerETag(t *testing.T) {
	computedETag := "33333333333333333333333333333333"
	entry := newCopyETagTestEntry(t, "", computedETag)

	if got := strings.Trim(copyEntryETag(entry), `"`); got != computedETag {
		t.Fatalf("copyEntryETag() = %q, want fallback filer ETag %q", got, computedETag)
	}
}

func TestValidateConditionalCopyHeadersUsesStoredExtendedETag(t *testing.T) {
	storedETag := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-2"
	entry := newCopyETagTestEntry(t, storedETag, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	s3a := &S3ApiServer{}

	matchReq := httptest.NewRequest("PUT", "/dst", nil)
	matchReq.Header.Set(s3_constants.AmzCopySourceIfMatch, `"`+storedETag+`"`)
	if got := s3a.validateConditionalCopyHeaders(matchReq, entry); got != s3err.ErrNone {
		t.Fatalf("validateConditionalCopyHeaders(If-Match stored ETag) = %v, want %v", got, s3err.ErrNone)
	}

	noneMatchReq := httptest.NewRequest("PUT", "/dst", nil)
	noneMatchReq.Header.Set(s3_constants.AmzCopySourceIfNoneMatch, storedETag)
	if got := s3a.validateConditionalCopyHeaders(noneMatchReq, entry); got != s3err.ErrPreconditionFailed {
		t.Fatalf("validateConditionalCopyHeaders(If-None-Match stored ETag) = %v, want %v", got, s3err.ErrPreconditionFailed)
	}
}

func newCopyETagTestEntry(t *testing.T, storedETag, computedETag string) *filer_pb.Entry {
	t.Helper()

	entry := &filer_pb.Entry{
		Name: "object",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 5,
			Md5:      mustDecodeHexETagForTest(t, computedETag),
		},
	}
	if storedETag != "" {
		entry.Extended = map[string][]byte{
			s3_constants.ExtETagKey: []byte(storedETag),
		}
	}
	return entry
}
