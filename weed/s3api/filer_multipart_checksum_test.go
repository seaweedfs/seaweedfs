package s3api

import (
	"bytes"
	"encoding/base64"
	"hash/crc32"
	"testing"

	"github.com/minio/crc64nvme"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
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
