package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

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
}
