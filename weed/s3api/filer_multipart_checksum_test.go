package s3api

import (
	"bytes"
	"encoding/base64"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/minio/crc64nvme"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func makeCRC64NVMEPartEntry(data []byte) *filer_pb.Entry {
	sum := crc64nvme.New()
	sum.Write(data)
	return &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(data))},
		Extended: map[string][]byte{
			s3_constants.ExtChecksumAlgorithm: []byte(s3_constants.AmzChecksumCRC64NVME),
			s3_constants.ExtChecksumValue:     []byte(base64.StdEncoding.EncodeToString(sum.Sum(nil))),
		},
	}
}

func makeCRC32PartEntry(data []byte) *filer_pb.Entry {
	sum := crc32.NewIEEE()
	sum.Write(data)
	return &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(data))},
		Extended: map[string][]byte{
			s3_constants.ExtChecksumAlgorithm: []byte(s3_constants.AmzChecksumCRC32),
			s3_constants.ExtChecksumValue:     []byte(base64.StdEncoding.EncodeToString(sum.Sum(nil))),
		},
	}
}

// Tests the FULL_OBJECT checksum of a multipart upload equals the CRC64NVME of the concatenated
// part data, with no composite "-N" suffix.
func TestComputeFullObjectChecksumCRC64NVME(t *testing.T) {
	parts := [][]byte{
		bytes.Repeat([]byte("a"), 5*1024*1024),
		bytes.Repeat([]byte("b"), 5*1024*1024),
		[]byte("tail part, smaller than the rest"),
	}

	partEntries := map[int][]*filer_pb.Entry{}
	var whole []byte
	completed := []int{}
	for i, data := range parts {
		partNumber := i + 1
		partEntries[partNumber] = []*filer_pb.Entry{makeCRC64NVMEPartEntry(data)}
		completed = append(completed, partNumber)
		whole = append(whole, data...)
	}

	got, err := computeFullObjectChecksum(s3_constants.AmzChecksumCRC64NVME, partEntries, completed)
	if err != nil {
		t.Fatalf("computeFullObjectChecksum: %v", err)
	}

	wholeSum := crc64nvme.New()
	wholeSum.Write(whole)
	expected := base64.StdEncoding.EncodeToString(wholeSum.Sum(nil))

	if got != expected {
		t.Fatalf("full object checksum = %q, want %q", got, expected)
	}
	if bytes.ContainsRune([]byte(got), '-') {
		t.Fatalf("full object checksum must not carry a -N suffix: %q", got)
	}
}

// Tests the FULL_OBJECT checksum of a multipart upload equals the CRC32 of the concatenated
// part data, with no composite "-N" suffix.
func TestComputeFullObjectChecksumCRC32(t *testing.T) {
	parts := [][]byte{
		bytes.Repeat([]byte("a"), 5*1024*1024),
		bytes.Repeat([]byte("b"), 5*1024*1024),
		[]byte("tail part, smaller than the rest"),
	}

	partEntries := map[int][]*filer_pb.Entry{}
	var whole []byte
	completed := []int{}
	for i, data := range parts {
		partNumber := i + 1
		partEntries[partNumber] = []*filer_pb.Entry{makeCRC32PartEntry(data)}
		completed = append(completed, partNumber)
		whole = append(whole, data...)
	}

	got, err := computeFullObjectChecksum(s3_constants.AmzChecksumCRC32, partEntries, completed)
	if err != nil {
		t.Fatalf("computeFullObjectChecksum: %v", err)
	}

	wholeSum := crc32.NewIEEE()
	wholeSum.Write(whole)
	expected := base64.StdEncoding.EncodeToString(wholeSum.Sum(nil))

	if got != expected {
		t.Fatalf("full object checksum = %q, want %q", got, expected)
	}
	if bytes.ContainsRune([]byte(got), '-') {
		t.Fatalf("full object checksum must not carry a -N suffix: %q", got)
	}
}

func TestResolveMultipartChecksumType(t *testing.T) {
	cases := []struct {
		name      string
		algo      ChecksumAlgorithm
		requested string
		want      string
		wantErr   bool
	}{
		{"crc64 default full", ChecksumAlgorithmCRC64NVMe, "", s3_constants.ChecksumTypeFullObject, false},
		{"crc64 explicit full", ChecksumAlgorithmCRC64NVMe, s3_constants.ChecksumTypeFullObject, s3_constants.ChecksumTypeFullObject, false},
		{"crc64 composite rejected", ChecksumAlgorithmCRC64NVMe, s3_constants.ChecksumTypeComposite, "", true},
		{"crc32 default composite", ChecksumAlgorithmCRC32, "", s3_constants.ChecksumTypeComposite, false},
		{"crc32 explicit composite", ChecksumAlgorithmCRC32, s3_constants.ChecksumTypeComposite, s3_constants.ChecksumTypeComposite, false},
		{"crc32 explicit full", ChecksumAlgorithmCRC32, s3_constants.ChecksumTypeFullObject, s3_constants.ChecksumTypeFullObject, false},
		{"crc32c explicit full", ChecksumAlgorithmCRC32C, s3_constants.ChecksumTypeFullObject, s3_constants.ChecksumTypeFullObject, false},
		{"sha256 default composite", ChecksumAlgorithmSHA256, "", s3_constants.ChecksumTypeComposite, false},
		{"sha256 full rejected", ChecksumAlgorithmSHA256, s3_constants.ChecksumTypeFullObject, "", true},
		{"invalid checksum type", ChecksumAlgorithmCRC64NVMe, "bogus", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveMultipartChecksumType(tc.algo, tc.requested)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// CompleteMultipartUpload returns the object checksum in the XML body, where the
// AWS SDKs read it from — not as an HTTP response header.
func TestCompleteMultipartUploadResultChecksumXML(t *testing.T) {
	result := &CompleteMultipartUploadResult{
		ETag:         aws.String("\"etag-1\""),
		ChecksumType: s3_constants.ChecksumTypeComposite,
	}
	result.SetChecksum(s3_constants.AmzChecksumCRC32C, "fx4FQw==-1")

	encoded := string(s3err.EncodeXMLResponse(result))
	for _, want := range []string{
		"<ChecksumCRC32C>fx4FQw==-1</ChecksumCRC32C>",
		"<ChecksumType>COMPOSITE</ChecksumType>",
	} {
		if !strings.Contains(encoded, want) {
			t.Fatalf("response %q does not contain %q", encoded, want)
		}
	}
	if strings.Contains(encoded, "<ChecksumCRC32>") {
		t.Fatalf("response %q carries an unrequested algorithm", encoded)
	}
}

// A retried CompleteMultipartUpload rebuilds the response from the committed
// entry, so it must repeat the checksum the first attempt returned.
func TestCompleteMultipartResultChecksumFromEntry(t *testing.T) {
	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String("bucket"), Key: aws.String("key")}
	entry := &filer_pb.Entry{Extended: map[string][]byte{
		s3_constants.ExtChecksumAlgorithm: []byte(s3_constants.AmzChecksumCRC32C),
		s3_constants.ExtChecksumValue:     []byte("fx4FQw==-1"),
		s3_constants.ExtChecksumType:      []byte(s3_constants.ChecksumTypeComposite),
	}}

	result := completeMultipartResult(httptest.NewRequest(http.MethodPost, "/bucket/key", nil), input, "\"etag-1\"", entry)
	if result.ChecksumCRC32C != "fx4FQw==-1" {
		t.Fatalf("ChecksumCRC32C = %q, want %q", result.ChecksumCRC32C, "fx4FQw==-1")
	}
	if result.ChecksumType != s3_constants.ChecksumTypeComposite {
		t.Fatalf("ChecksumType = %q, want %q", result.ChecksumType, s3_constants.ChecksumTypeComposite)
	}
}

// CreateMultipartUpload echoes the algorithm and type it recorded, so a client
// can tell which checksum the upload will be completed with.
func TestCreateMultipartUploadResultChecksumHeaders(t *testing.T) {
	result := &InitiateMultipartUploadResult{
		ChecksumAlgorithm: "CRC32C",
		ChecksumType:      s3_constants.ChecksumTypeComposite,
	}
	if encoded := string(s3err.EncodeXMLResponse(result)); strings.Contains(encoded, "Checksum") {
		t.Fatalf("response %q carries checksum members in the XML body", encoded)
	}
}

func TestApplyMultipartChecksumHeaderToRequest(t *testing.T) {
	uploadEntry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.ExtChecksumAlgorithm: []byte(s3_constants.AmzChecksumSHA256),
		},
	}

	// 1. Inherit checksum algorithm when request has no checksum header
	r1 := httptest.NewRequest(http.MethodPut, "/bucket/key?partNumber=1&uploadId=123", nil)
	applyMultipartChecksumHeaderToRequest(r1, uploadEntry)
	if got := r1.Header.Get(s3_constants.AmzChecksumAlgorithm); got != "SHA256" {
		t.Fatalf("expected inherited algorithm SHA256, got %q", got)
	}

	// 2. Do not override when request already provides explicit matching checksum header (SHA256)
	r2 := httptest.NewRequest(http.MethodPut, "/bucket/key?partNumber=1&uploadId=123", nil)
	r2.Header.Set(s3_constants.AmzChecksumSHA256, "explicit-sha256-value")
	applyMultipartChecksumHeaderToRequest(r2, uploadEntry)
	if got := r2.Header.Get(s3_constants.AmzChecksumAlgorithm); got != "" {
		t.Fatalf("expected empty AmzChecksumAlgorithm when explicit matching checksum present, got %q", got)
	}
	if got := r2.Header.Get(s3_constants.AmzChecksumSHA256); got != "explicit-sha256-value" {
		t.Fatalf("expected explicit checksum header preserved, got %q", got)
	}

	// 3. Do not override when request provides explicit CONFLICTING checksum headers
	conflictingHeaders := []struct {
		header string
		value  string
	}{
		{s3_constants.AmzChecksumCRC32, "explicit-crc32"},
		{s3_constants.AmzChecksumCRC32C, "explicit-crc32c"},
		{s3_constants.AmzChecksumCRC64NVME, "explicit-crc64nvme"},
		{s3_constants.AmzChecksumSHA1, "explicit-sha1"},
	}
	for _, ch := range conflictingHeaders {
		r := httptest.NewRequest(http.MethodPut, "/bucket/key?partNumber=1&uploadId=123", nil)
		r.Header.Set(ch.header, ch.value)
		applyMultipartChecksumHeaderToRequest(r, uploadEntry)
		if got := r.Header.Get(s3_constants.AmzChecksumAlgorithm); got != "" {
			t.Fatalf("expected empty AmzChecksumAlgorithm when explicit %s present, got %q", ch.header, got)
		}
		if got := r.Header.Get(ch.header); got != ch.value {
			t.Fatalf("expected explicit header %s preserved, got %q", ch.header, got)
		}
	}

	// 4. Do not override when request provides explicit AmzChecksumAlgorithm
	r4 := httptest.NewRequest(http.MethodPut, "/bucket/key?partNumber=1&uploadId=123", nil)
	r4.Header.Set(s3_constants.AmzChecksumAlgorithm, "CRC32")
	applyMultipartChecksumHeaderToRequest(r4, uploadEntry)
	if got := r4.Header.Get(s3_constants.AmzChecksumAlgorithm); got != "CRC32" {
		t.Fatalf("expected explicit algorithm CRC32, got %q", got)
	}

	// 5. Do not override when request provides AmzSdkChecksumAlgorithm
	r5 := httptest.NewRequest(http.MethodPut, "/bucket/key?partNumber=1&uploadId=123", nil)
	r5.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, "CRC32C")
	applyMultipartChecksumHeaderToRequest(r5, uploadEntry)
	if got := r5.Header.Get(s3_constants.AmzChecksumAlgorithm); got != "" {
		t.Fatalf("expected empty AmzChecksumAlgorithm when AmzSdkChecksumAlgorithm present, got %q", got)
	}
	if got := r5.Header.Get(s3_constants.AmzSdkChecksumAlgorithm); got != "CRC32C" {
		t.Fatalf("expected explicit sdk algorithm CRC32C, got %q", got)
	}

	// 6. Do not override when request provides AmzTrailer
	r6 := httptest.NewRequest(http.MethodPut, "/bucket/key?partNumber=1&uploadId=123", nil)
	r6.Header.Set(s3_constants.AmzTrailer, "x-amz-checksum-crc32")
	applyMultipartChecksumHeaderToRequest(r6, uploadEntry)
	if got := r6.Header.Get(s3_constants.AmzChecksumAlgorithm); got != "" {
		t.Fatalf("expected empty AmzChecksumAlgorithm when checksum trailer present, got %q", got)
	}
}

func TestSetResponseHeadersChecksumModeVariations(t *testing.T) {
	s3a := &S3ApiServer{}
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{FileSize: 100},
		Extended: map[string][]byte{
			s3_constants.ExtChecksumAlgorithm: []byte(s3_constants.AmzChecksumSHA256),
			s3_constants.ExtChecksumValue:     []byte("abcdefg=="),
			s3_constants.ExtChecksumType:      []byte(s3_constants.ChecksumTypeComposite),
		},
	}

	cases := []struct {
		name         string
		headerMode   string
		queryMode    string
		wantChecksum string
		wantType     string
	}{
		{"exact uppercase header", "ENABLED", "", "abcdefg==", s3_constants.ChecksumTypeComposite},
		{"lowercase header", "enabled", "", "abcdefg==", s3_constants.ChecksumTypeComposite},
		{"mixed case header", "Enabled", "", "abcdefg==", s3_constants.ChecksumTypeComposite},
		{"query parameter uppercase", "", "ENABLED", "abcdefg==", s3_constants.ChecksumTypeComposite},
		{"query parameter lowercase", "", "enabled", "abcdefg==", s3_constants.ChecksumTypeComposite},
		{"disabled header", "DISABLED", "", "", ""},
		{"no mode", "", "", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			urlStr := "/bucket/key"
			if tc.queryMode != "" {
				urlStr += "?x-amz-checksum-mode=" + tc.queryMode
			}
			r := httptest.NewRequest(http.MethodHead, urlStr, nil)
			if tc.headerMode != "" {
				r.Header.Set(s3_constants.AmzChecksumMode, tc.headerMode)
			}
			w := httptest.NewRecorder()
			s3a.setResponseHeaders(w, r, entry, 100)

			gotChecksum := w.Header().Get(s3_constants.AmzChecksumSHA256)
			gotType := w.Header().Get(s3_constants.AmzChecksumType)
			if gotChecksum != tc.wantChecksum {
				t.Fatalf("checksum header: got %q, want %q", gotChecksum, tc.wantChecksum)
			}
			if gotType != tc.wantType {
				t.Fatalf("checksum type header: got %q, want %q", gotType, tc.wantType)
			}
		})
	}
}
