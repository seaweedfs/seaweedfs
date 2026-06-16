package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestApplyDestChecksumHeaderToCopyRequest(t *testing.T) {
	entry := &filer_pb.Entry{Extended: map[string][]byte{
		s3_constants.ExtChecksumAlgorithm: []byte(s3_constants.AmzChecksumCRC64NVME),
	}}
	request := httptest.NewRequest(http.MethodPut, "http://example.com/bucket/object", nil)

	applyDestChecksumHeaderToCopyRequest(request, entry)

	algorithm, headerName, errCode := detectRequestedChecksumAlgorithm(request)
	if errCode != s3err.ErrNone {
		t.Fatalf("detect checksum returned %v", errCode)
	}
	if algorithm != ChecksumAlgorithmCRC64NVMe {
		t.Fatalf("algorithm = %v, want %v", algorithm, ChecksumAlgorithmCRC64NVMe)
	}
	if headerName != s3_constants.AmzChecksumCRC64NVME {
		t.Fatalf("header = %q, want %q", headerName, s3_constants.AmzChecksumCRC64NVME)
	}

	for _, noChecksum := range []*filer_pb.Entry{
		nil,
		{},
		{Extended: map[string][]byte{s3_constants.ExtChecksumAlgorithm: []byte("unknown")}},
	} {
		req := httptest.NewRequest(http.MethodPut, "http://example.com/bucket/object", nil)
		applyDestChecksumHeaderToCopyRequest(req, noChecksum)
		if got := req.Header.Get(s3_constants.AmzChecksumAlgorithm); got != "" {
			t.Fatalf("expected no checksum header, got %q", got)
		}
	}
}

func TestUploadEntryHasChecksum(t *testing.T) {
	entry := &filer_pb.Entry{Extended: map[string][]byte{
		s3_constants.ExtChecksumAlgorithm: []byte(s3_constants.AmzChecksumCRC64NVME),
	}}
	if !uploadEntryHasChecksum(entry) {
		t.Fatal("checksum-enabled upload was not detected")
	}

	entry.Extended[s3_constants.ExtChecksumAlgorithm] = []byte("unknown")
	if uploadEntryHasChecksum(entry) {
		t.Fatal("unknown checksum algorithm was accepted")
	}

	if uploadEntryHasChecksum(nil) || uploadEntryHasChecksum(&filer_pb.Entry{}) {
		t.Fatal("nil entry or nil Extended map reported a checksum")
	}
}

func TestBuildCopyPartResult(t *testing.T) {
	modified := time.Unix(123, 0).UTC()
	tests := []struct {
		name     string
		header   string
		element  string
		expected CopyPartResult
	}{
		{
			name:    "CRC32",
			header:  s3_constants.AmzChecksumCRC32,
			element: "<ChecksumCRC32>value</ChecksumCRC32>",
			expected: CopyPartResult{
				ETag: "etag", LastModified: modified, ChecksumCRC32: "value",
			},
		},
		{
			name:    "CRC32C",
			header:  s3_constants.AmzChecksumCRC32C,
			element: "<ChecksumCRC32C>value</ChecksumCRC32C>",
			expected: CopyPartResult{
				ETag: "etag", LastModified: modified, ChecksumCRC32C: "value",
			},
		},
		{
			name:    "CRC64NVME",
			header:  s3_constants.AmzChecksumCRC64NVME,
			element: "<ChecksumCRC64NVME>value</ChecksumCRC64NVME>",
			expected: CopyPartResult{
				ETag: "etag", LastModified: modified, ChecksumCRC64NVME: "value",
			},
		},
		{
			name:    "SHA1",
			header:  s3_constants.AmzChecksumSHA1,
			element: "<ChecksumSHA1>value</ChecksumSHA1>",
			expected: CopyPartResult{
				ETag: "etag", LastModified: modified, ChecksumSHA1: "value",
			},
		},
		{
			name:    "SHA256",
			header:  s3_constants.AmzChecksumSHA256,
			element: "<ChecksumSHA256>value</ChecksumSHA256>",
			expected: CopyPartResult{
				ETag: "etag", LastModified: modified, ChecksumSHA256: "value",
			},
		},
		{
			name:     "Unknown",
			header:   "x-amz-checksum-unknown",
			element:  "",
			expected: CopyPartResult{ETag: "etag", LastModified: modified},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := buildCopyPartResult("etag", modified, SSEResponseMetadata{
				ChecksumHeaderName: test.header,
				ChecksumValue:      "value",
			})
			if result != test.expected {
				t.Fatalf("result = %#v, want %#v", result, test.expected)
			}
			if encoded := string(s3err.EncodeXMLResponse(result)); test.element != "" && !strings.Contains(encoded, test.element) {
				t.Fatalf("response %q does not contain %q", encoded, test.element)
			}
		})
	}
}
