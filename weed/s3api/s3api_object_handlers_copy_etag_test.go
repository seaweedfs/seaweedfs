package s3api

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
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

func TestQuoteETag(t *testing.T) {
	testCases := []struct {
		etag string
		want string
	}{
		{etag: "b1946ac92492d2347c6235b4d2611184", want: `"b1946ac92492d2347c6235b4d2611184"`},
		{etag: `"b1946ac92492d2347c6235b4d2611184"`, want: `"b1946ac92492d2347c6235b4d2611184"`},
		{etag: "", want: ""},
	}
	for _, tc := range testCases {
		if got := quoteETag(tc.etag); got != tc.want {
			t.Errorf("quoteETag(%q) = %q, want %q", tc.etag, got, tc.want)
		}
	}
}

func TestBuildCopyPartResultQuotesETag(t *testing.T) {
	result := buildCopyPartResult("b1946ac92492d2347c6235b4d2611184", time.Now(), SSEResponseMetadata{})
	if result.ETag != `"b1946ac92492d2347c6235b4d2611184"` {
		t.Fatalf("buildCopyPartResult().ETag = %q, want quoted", result.ETag)
	}
	if encoded := string(s3err.EncodeXMLResponse(result)); !strings.Contains(encoded, `<ETag>&#34;b1946ac92492d2347c6235b4d2611184&#34;</ETag>`) {
		t.Fatalf("response %q does not contain a quoted ETag", encoded)
	}
}

func TestCopyObjectResultXMLQuotesETag(t *testing.T) {
	encoded := string(s3err.EncodeXMLResponse(CopyObjectResult{
		ETag:         quoteETag("b1946ac92492d2347c6235b4d2611184"),
		LastModified: time.Now(),
	}))
	if !strings.Contains(encoded, `<ETag>&#34;b1946ac92492d2347c6235b4d2611184&#34;</ETag>`) {
		t.Fatalf("response %q does not contain a quoted ETag", encoded)
	}
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

// A part copied with x-amz-copy-source-range must report the MD5 of the copied
// bytes: per destination chunk for a partial-chunk copy, and the whole-part MD5
// the part entry's Attributes.Md5 carries for CopyPartResult / ListParts /
// complete's composite ETag (#11855).
func TestCopyChunksForRangeCopiedETags(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{volume: volume, entries: map[string]*filer_pb.Entry{}}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))
	util_http.InitGlobalHttpClient()

	const chunkSize = 4 << 20
	payload := make([]byte, 2*chunkSize)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}
	b64md5 := func(data []byte) string {
		sum := md5.Sum(data)
		return base64.StdEncoding.EncodeToString(sum[:])
	}
	fid0 := fmt.Sprintf("3,%016x%08x", 101, 202)
	fid1 := fmt.Sprintf("3,%016x%08x", 103, 204)
	volume.stored[fid0] = payload[:chunkSize]
	volume.stored[fid1] = payload[chunkSize:]
	entry := &filer_pb.Entry{
		Name: "src",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: uint64(len(payload)),
		},
		Chunks: []*filer_pb.FileChunk{
			{FileId: fid0, Offset: 0, Size: chunkSize, ETag: b64md5(payload[:chunkSize])},
			{FileId: fid1, Offset: chunkSize, Size: chunkSize, ETag: b64md5(payload[chunkSize:])},
		},
	}

	// A range spanning the first chunk fully plus 1 MiB of the second.
	multiEnd := int64(chunkSize + 1<<20 - 1)
	dstChunks, partMd5, err := s3a.copyChunksForRange(entry, 0, multiEnd, "/dst")
	if err != nil {
		t.Fatalf("copyChunksForRange: %v", err)
	}
	if len(dstChunks) != 2 {
		t.Fatalf("copyChunksForRange returned %d chunks, want 2", len(dstChunks))
	}
	// A whole-chunk copy forwards the stored bytes, so the source ETag stays valid.
	if got, want := dstChunks[0].ETag, entry.Chunks[0].ETag; got != want {
		t.Errorf("whole-chunk copy ETag = %q, want source chunk ETag %q", got, want)
	}
	// A partial copy stores only the range bytes; its ETag must be their MD5.
	if got, want := dstChunks[1].ETag, b64md5(payload[chunkSize:multiEnd+1]); got != want {
		t.Errorf("partial-chunk copy ETag = %q, want copied-bytes MD5 %q", got, want)
	}
	// The copied bytes landed on the destination volume (inside the multipart framing).
	if stored := volume.stored[dstChunks[1].GetFileIdString()]; !bytes.Contains(stored, payload[chunkSize:multiEnd+1]) {
		t.Error("destination volume does not hold the copied range bytes")
	}
	// The whole-part MD5 hashes every copied byte once, in range order.
	wantPartMd5 := md5.Sum(payload[:multiEnd+1])
	if !bytes.Equal(partMd5, wantPartMd5[:]) {
		t.Errorf("part Md5 = %x, want %x", partMd5, wantPartMd5)
	}
	partEntry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Md5: partMd5},
		Chunks:     dstChunks,
	}
	if got, want := copyEntryETag(partEntry), fmt.Sprintf("%x", wantPartMd5); got != want {
		t.Errorf("copyEntryETag = %q, want %q", got, want)
	}

	// A range inside one chunk: the part's ETag is the MD5 of those bytes.
	innerChunks, innerMd5, err := s3a.copyChunksForRange(entry, 100, 199, "/dst")
	if err != nil {
		t.Fatalf("copyChunksForRange inner range: %v", err)
	}
	if len(innerChunks) != 1 {
		t.Fatalf("inner range returned %d chunks, want 1", len(innerChunks))
	}
	if got, want := innerChunks[0].ETag, b64md5(payload[100:200]); got != want {
		t.Errorf("inner-range chunk ETag = %q, want copied-bytes MD5 %q", got, want)
	}
	wantInnerMd5 := md5.Sum(payload[100:200])
	if !bytes.Equal(innerMd5, wantInnerMd5[:]) {
		t.Errorf("inner-range part Md5 = %x, want %x", innerMd5, wantInnerMd5)
	}
}

// A range over a sparse source zero-fills the gaps, so the part MD5 has to
// hash the logical range — not just the one chunk that overlaps it.
func TestCopyChunksForRangeSparseETags(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{volume: volume, entries: map[string]*filer_pb.Entry{}}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))
	util_http.InitGlobalHttpClient()

	fid := fmt.Sprintf("3,%016x%08x", 105, 206)
	volume.stored[fid] = []byte("abc")
	sum := md5.Sum([]byte("abc"))
	entry := &filer_pb.Entry{
		Name: "sparse",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 6,
		},
		Chunks: []*filer_pb.FileChunk{
			{FileId: fid, Offset: 3, Size: 3, ETag: base64.StdEncoding.EncodeToString(sum[:])},
		},
	}

	// bytes 0-5 read as "\x00\x00\x00abc"; the part MD5 must cover the zeros too.
	dstChunks, partMd5, err := s3a.copyChunksForRange(entry, 0, 5, "/dst")
	if err != nil {
		t.Fatalf("copyChunksForRange sparse: %v", err)
	}
	if len(dstChunks) != 1 || dstChunks[0].Offset != 3 {
		t.Fatalf("sparse copy chunks = %+v, want one chunk at offset 3", dstChunks)
	}
	wantMd5 := md5.Sum([]byte("\x00\x00\x00abc"))
	if !bytes.Equal(partMd5, wantMd5[:]) {
		t.Errorf("sparse part Md5 = %x, want %x (zero-filled range)", partMd5, wantMd5)
	}

	// A range over a pure hole copies nothing but still reads as zeros.
	holeChunks, holeMd5, err := s3a.copyChunksForRange(entry, 0, 2, "/dst")
	if err != nil {
		t.Fatalf("copyChunksForRange hole: %v", err)
	}
	if len(holeChunks) != 0 {
		t.Fatalf("hole copy returned %d chunks, want 0", len(holeChunks))
	}
	wantHoleMd5 := md5.Sum([]byte{0, 0, 0})
	if !bytes.Equal(holeMd5, wantHoleMd5[:]) {
		t.Errorf("hole part Md5 = %x, want %x (all zeros)", holeMd5, wantHoleMd5)
	}
}

// Destination chunks must resolve overlaps the way the source did: reads
// pick winners by ModifiedTsNs, so the copied chunks keep the source's
// precedence even though they were uploaded concurrently.
func TestCopyChunksForRangePreservesSourcePrecedence(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{volume: volume, entries: map[string]*filer_pb.Entry{}}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))
	util_http.InitGlobalHttpClient()

	etagOf := func(b []byte) string {
		sum := md5.Sum(b)
		return base64.StdEncoding.EncodeToString(sum[:])
	}
	fidA := fmt.Sprintf("3,%016x%08x", 107, 208)
	fidB := fmt.Sprintf("3,%016x%08x", 109, 210)
	volume.stored[fidA] = []byte("aaaa")
	volume.stored[fidB] = []byte("bbbb")

	// A covers [0,4), B covers [2,6); B's range is copied into a chunk that
	// overlaps A's copy on [2,4), and the source says A wins there.
	entry := &filer_pb.Entry{
		Name:       "overlap",
		Attributes: &filer_pb.FuseAttributes{FileSize: 6},
		Chunks: []*filer_pb.FileChunk{
			{FileId: fidA, Offset: 0, Size: 4, ModifiedTsNs: 20, ETag: etagOf([]byte("aaaa"))},
			{FileId: fidB, Offset: 2, Size: 4, ModifiedTsNs: 10, ETag: etagOf([]byte("bbbb"))},
		},
	}
	dstChunks, partMd5, err := s3a.copyChunksForRange(entry, 0, 5, "/dst")
	if err != nil {
		t.Fatalf("copyChunksForRange overlap: %v", err)
	}
	if len(dstChunks) != 2 {
		t.Fatalf("overlap copy returned %d chunks, want 2", len(dstChunks))
	}
	if dstChunks[0].ModifiedTsNs <= dstChunks[1].ModifiedTsNs {
		t.Errorf("destination precedence inverted: chunk from ts=20 got %d, chunk from ts=10 got %d",
			dstChunks[0].ModifiedTsNs, dstChunks[1].ModifiedTsNs)
	}
	// The source resolves to "aaaa"+"bb"; the part MD5 must match that view.
	wantMd5 := md5.Sum([]byte("aaaabb"))
	if !bytes.Equal(partMd5, wantMd5[:]) {
		t.Errorf("overlap part Md5 = %x, want %x", partMd5, wantMd5)
	}
}
