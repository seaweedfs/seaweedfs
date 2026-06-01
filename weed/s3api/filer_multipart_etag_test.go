// Tests multipart upload ETag behavior when both stored S3 ETags and
// MD5-derived ETags are present. The tests verify that SeaweedFS uses
// the stored ETag for part validation and multipart ETag calculation,
// while still falling back to the MD5-derived value when necessary.
package s3api

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestGetEtagFromEntryPrefersStoredExtendedETag(t *testing.T) {
	storedETag := "11111111111111111111111111111111"
	computedMD5 := mustDecodeHexETagForTest(t, "22222222222222222222222222222222")

	entry := &filer_pb.Entry{
		Name: "0001.part",
		Attributes: &filer_pb.FuseAttributes{
			Md5: computedMD5,
		},
		Extended: map[string][]byte{
			s3_constants.ExtETagKey: []byte(storedETag),
		},
	}

	if got, want := getEtagFromEntry(entry), `"`+storedETag+`"`; got != want {
		t.Fatalf("getEtagFromEntry() = %q, want stored extended ETag %q", got, want)
	}

	match, invalid, normalizedPartETag, normalizedEntryETag := validateCompletePartETag(storedETag, entry)
	if invalid || !match {
		t.Fatalf("validateCompletePartETag() = match:%v invalid:%v part:%q entry:%q, want stored ETag match", match, invalid, normalizedPartETag, normalizedEntryETag)
	}

	computedETag := hex.EncodeToString(computedMD5)
	match, invalid, normalizedPartETag, normalizedEntryETag = validateCompletePartETag(computedETag, entry)
	if invalid || match {
		t.Fatalf("validateCompletePartETag() = match:%v invalid:%v part:%q entry:%q, want computed filer ETag mismatch", match, invalid, normalizedPartETag, normalizedEntryETag)
	}
}

func TestGetEtagFromEntryFallbacksToFilerETag(t *testing.T) {
	computedETag := "33333333333333333333333333333333"
	entry := &filer_pb.Entry{
		Name: "0001.part",
		Attributes: &filer_pb.FuseAttributes{
			Md5: mustDecodeHexETagForTest(t, computedETag),
		},
		Extended: map[string][]byte{
			s3_constants.ExtETagKey: []byte(""),
		},
	}

	if got, want := getEtagFromEntry(entry), `"`+computedETag+`"`; got != want {
		t.Fatalf("getEtagFromEntry() = %q, want fallback filer ETag %q", got, want)
	}
}

func TestCalculateMultipartETagUsesStoredPartETags(t *testing.T) {
	storedPart1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	storedPart2 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	computedPart1 := "cccccccccccccccccccccccccccccccc"
	computedPart2 := "dddddddddddddddddddddddddddddddd"

	partEntries := map[int][]*filer_pb.Entry{
		1: {newMultipartETagTestEntry(t, "0001.part", storedPart1, computedPart1)},
		2: {newMultipartETagTestEntry(t, "0002.part", storedPart2, computedPart2)},
	}

	got := calculateMultipartETag(partEntries, []int{1, 2})
	want := expectedMultipartETagForTest(t, storedPart1, storedPart2)
	if got != want {
		t.Fatalf("calculateMultipartETag() = %q, want %q", got, want)
	}

	legacyComputedETag := expectedMultipartETagForTest(t, computedPart1, computedPart2)
	if got == legacyComputedETag {
		t.Fatalf("calculateMultipartETag() = %q, still used filer.ETag-derived part ETags", got)
	}
}

func newMultipartETagTestEntry(t *testing.T, name, storedETag, computedETag string) *filer_pb.Entry {
	t.Helper()
	return &filer_pb.Entry{
		Name: name,
		Attributes: &filer_pb.FuseAttributes{
			Md5: mustDecodeHexETagForTest(t, computedETag),
		},
		Extended: map[string][]byte{
			s3_constants.ExtETagKey: []byte(storedETag),
		},
	}
}

func expectedMultipartETagForTest(t *testing.T, partETags ...string) string {
	t.Helper()
	var combined []byte
	for _, partETag := range partETags {
		partETag = strings.Trim(partETag, `"`)
		if before, _, found := strings.Cut(partETag, "-"); found {
			partETag = before
		}
		combined = append(combined, mustDecodeHexETagForTest(t, partETag)...)
	}
	return fmt.Sprintf("%x-%d", md5.Sum(combined), len(partETags))
}

func mustDecodeHexETagForTest(t *testing.T, etag string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(etag)
	if err != nil {
		t.Fatalf("decode hex ETag %q: %v", etag, err)
	}
	return decoded
}