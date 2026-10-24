package s3api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	part1Size = 5 * 1024 * 1024
	part2Size = 3 * 1024 * 1024
)

func TestRequestedPartNumber(t *testing.T) {
	for query, want := range map[string]int{
		"":                 0,
		"partNumber=1":     1,
		"PartNumber=2":     2,
		"partNumber=0":     0,
		"partNumber=-1":    0,
		"partNumber=abc":   0,
		"versionId=v1":     0,
		"partNumber=2&x=1": 2,
	} {
		r := httptest.NewRequest(http.MethodHead, "/bucket/object?"+query, nil)
		assert.Equal(t, want, requestedPartNumber(r), "query %q", query)
	}
}

// twoPartEntry models a 2-part multipart object of part1Size + part2Size bytes.
func twoPartEntry(t *testing.T, withBoundaries bool) *filer_pb.Entry {
	t.Helper()
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{FileSize: part1Size + part2Size},
		Chunks: []*filer_pb.FileChunk{
			{FileId: "1,a", Offset: 0, Size: part1Size},
			{FileId: "1,b", Offset: part1Size, Size: part2Size},
		},
		Extended: map[string][]byte{
			s3_constants.SeaweedFSMultipartPartsCount: []byte("2"),
		},
	}
	if withBoundaries {
		boundaries, err := json.Marshal([]PartBoundaryInfo{
			{PartNumber: 1, StartChunk: 0, EndChunk: 1, StartOffset: 0, EndOffset: part1Size},
			{PartNumber: 2, StartChunk: 1, EndChunk: 2, StartOffset: part1Size, EndOffset: part1Size + part2Size},
		})
		require.NoError(t, err)
		entry.Extended[s3_constants.SeaweedFSMultipartPartBoundaries] = boundaries
	}
	return entry
}

// partRequest is a HEAD of partNumber, optionally narrowed by a client Range.
func partRequest(rangeHeader string) *http.Request {
	r := httptest.NewRequest(http.MethodHead, "/bucket/object", nil)
	if rangeHeader != "" {
		r.Header.Set("Range", rangeHeader)
	}
	return r
}

func TestPartByteRange(t *testing.T) {
	s3a := &S3ApiServer{}

	for _, withBoundaries := range []bool{true, false} {
		entry := twoPartEntry(t, withBoundaries)

		w := httptest.NewRecorder()
		start, end, errCode := s3a.partByteRange(w, partRequest(""), entry, 1)
		assert.Equal(t, s3err.ErrNone, errCode)
		assert.Equal(t, int64(0), start)
		assert.Equal(t, int64(part1Size-1), end)
		assert.Equal(t, "2", w.Header().Get(s3_constants.AmzMpPartsCount))

		w = httptest.NewRecorder()
		start, end, errCode = s3a.partByteRange(w, partRequest(""), entry, 2)
		assert.Equal(t, s3err.ErrNone, errCode)
		assert.Equal(t, int64(part1Size), start)
		assert.Equal(t, int64(part1Size+part2Size-1), end)

		// A client Range applies within the selected part
		w = httptest.NewRecorder()
		start, end, errCode = s3a.partByteRange(w, partRequest("bytes=0-1023"), entry, 2)
		assert.Equal(t, s3err.ErrNone, errCode)
		assert.Equal(t, int64(part1Size), start)
		assert.Equal(t, int64(part1Size+1023), end)

		w = httptest.NewRecorder()
		_, _, errCode = s3a.partByteRange(w, partRequest(""), entry, 3)
		assert.Equal(t, s3err.ErrInvalidPartNumber, errCode, "part beyond the object must not resolve")
	}

	// AWS answers an unsatisfiable partNumber with 416, not 400.
	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, s3err.GetAPIError(s3err.ErrInvalidPartNumber).HTTPStatusCode)
}

// Completion accepts ascending, not consecutive, part numbers.
func TestPartByteRangeSparsePartNumbers(t *testing.T) {
	s3a := &S3ApiServer{}
	entry := twoPartEntry(t, true)
	boundaries, err := json.Marshal([]PartBoundaryInfo{
		{PartNumber: 1, StartChunk: 0, EndChunk: 1, StartOffset: 0, EndOffset: part1Size},
		{PartNumber: 3, StartChunk: 1, EndChunk: 2, StartOffset: part1Size, EndOffset: part1Size + part2Size},
	})
	require.NoError(t, err)
	entry.Extended[s3_constants.SeaweedFSMultipartPartBoundaries] = boundaries

	start, end, errCode := s3a.partByteRange(httptest.NewRecorder(), partRequest(""), entry, 3)
	assert.Equal(t, s3err.ErrNone, errCode, "the uploaded part 3 must resolve")
	assert.Equal(t, int64(part1Size), start)
	assert.Equal(t, int64(part1Size+part2Size-1), end)

	_, _, errCode = s3a.partByteRange(httptest.NewRecorder(), partRequest(""), entry, 2)
	assert.Equal(t, s3err.ErrInvalidPartNumber, errCode, "part 2 was never uploaded")
}

// The stored checksum covers the whole object, so a part response must not carry it.
func TestSetResponseHeadersSkipsChecksumForPart(t *testing.T) {
	s3a := &S3ApiServer{}
	entry := twoPartEntry(t, true)
	entry.Extended[s3_constants.ExtChecksumAlgorithm] = []byte("x-amz-checksum-crc32")
	entry.Extended[s3_constants.ExtChecksumValue] = []byte("AAAAAA==")

	for query, want := range map[string]string{
		"":             "AAAAAA==",
		"partNumber=1": "",
	} {
		r := httptest.NewRequest(http.MethodHead, "/bucket/object?"+query, nil)
		r.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
		w := httptest.NewRecorder()
		s3a.setResponseHeaders(w, r, entry, part1Size)
		assert.Equal(t, want, w.Header().Get("x-amz-checksum-crc32"), "query %q", query)
	}
}
