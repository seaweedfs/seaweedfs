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

// A part copy has no way to report a short part, so an unsatisfiable
// x-amz-copy-source-range has to be rejected rather than clamped like a GET —
// the re-encrypting path would otherwise pad the part out with zeros.
func TestParseRangeHeaderRejectsUnsatisfiableRange(t *testing.T) {
	const fileSize = 2048

	testCases := []struct {
		name       string
		header     string
		wantStart  int64
		wantEnd    int64
		wantReject bool
	}{
		{name: "whole source", header: "bytes=0-2047", wantEnd: 2047},
		{name: "leading slice", header: "bytes=0-1023", wantEnd: 1023},
		{name: "trailing slice", header: "bytes=1024-2047", wantStart: 1024, wantEnd: 2047},
		{name: "single byte", header: "bytes=7-7", wantStart: 7, wantEnd: 7},
		{name: "no bytes prefix", header: "0-1023", wantEnd: 1023},
		{name: "end past the source", header: "bytes=0-2048", wantReject: true},
		{name: "start past the source", header: "bytes=4096-8191", wantReject: true},
		{name: "start at the source size", header: "bytes=2048-2048", wantReject: true},
		{name: "reversed", header: "bytes=1023-0", wantReject: true},
		{name: "negative start", header: "bytes=-1-1023", wantReject: true},
		{name: "malformed", header: "bytes=abc", wantReject: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start, end, err := parseRangeHeader(tc.header, fileSize)
			if tc.wantReject {
				if err == nil {
					t.Fatalf("parseRangeHeader(%q) = %d, %d, want an error", tc.header, start, end)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRangeHeader(%q): %v", tc.header, err)
			}
			if start != tc.wantStart || end != tc.wantEnd {
				t.Errorf("parseRangeHeader(%q) = %d, %d, want %d, %d", tc.header, start, end, tc.wantStart, tc.wantEnd)
			}
		})
	}
}
