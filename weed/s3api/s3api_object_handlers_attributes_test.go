package s3api

import (
	"encoding/xml"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestObjectAttributesChecksum verifies that GetObjectAttributes reports the
// checksum that PutObject or CompleteMultipartUpload stored with the object
func TestObjectAttributesChecksum(t *testing.T) {
	testCases := []struct {
		name     string
		extended map[string][]byte
		want     *ObjectAttributesChecksum
	}{
		{
			name: "PutObject with SHA256",
			extended: map[string][]byte{
				s3_constants.ExtChecksumAlgorithm: []byte(s3_constants.AmzChecksumSHA256),
				s3_constants.ExtChecksumValue:     []byte("arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0="),
			},
			want: &ObjectAttributesChecksum{
				ChecksumResult: ChecksumResult{ChecksumSHA256: "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0="},
			},
		},
		{
			name: "multipart upload with a composite CRC32C",
			extended: map[string][]byte{
				s3_constants.ExtChecksumAlgorithm: []byte(s3_constants.AmzChecksumCRC32C),
				s3_constants.ExtChecksumValue:     []byte("x3Y2bw==-3"),
				s3_constants.ExtChecksumType:      []byte("COMPOSITE"),
			},
			want: &ObjectAttributesChecksum{
				ChecksumResult: ChecksumResult{ChecksumCRC32C: "x3Y2bw==-3"},
				ChecksumType:   "COMPOSITE",
			},
		},
		{
			name: "multipart upload with a full object CRC64NVME",
			extended: map[string][]byte{
				s3_constants.ExtChecksumAlgorithm: []byte(s3_constants.AmzChecksumCRC64NVME),
				s3_constants.ExtChecksumValue:     []byte("AAAAAAAAAAA="),
				s3_constants.ExtChecksumType:      []byte("FULL_OBJECT"),
			},
			want: &ObjectAttributesChecksum{
				ChecksumResult: ChecksumResult{ChecksumCRC64NVME: "AAAAAAAAAAA="},
				ChecksumType:   "FULL_OBJECT",
			},
		},
		{
			name:     "no checksum",
			extended: map[string][]byte{s3_constants.ExtETagKey: []byte("d41d8cd98f00b204e9800998ecf8427e")},
		},
		{
			name: "unknown algorithm",
			extended: map[string][]byte{
				s3_constants.ExtChecksumAlgorithm: []byte("X-Amz-Checksum-Md5"),
				s3_constants.ExtChecksumValue:     []byte("1B2M2Y8AsgTpgAmY7PhCfg=="),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, objectAttributesChecksum(&filer_pb.Entry{Extended: tc.extended}))
		})
	}
	assert.Nil(t, objectAttributesChecksum(&filer_pb.Entry{}))
}

// TestGetObjectAttributesChecksumXML verifies the Checksum element's layout
func TestGetObjectAttributesChecksumXML(t *testing.T) {
	resp := GetObjectAttributesResponse{
		Checksum: &ObjectAttributesChecksum{
			ChecksumResult: ChecksumResult{ChecksumCRC32: "NhCmhg=="},
			ChecksumType:   "FULL_OBJECT",
		},
	}
	out, err := xml.Marshal(resp)
	require.NoError(t, err)
	assert.Equal(t, "<GetObjectAttributesResponse><Checksum><ChecksumCRC32>NhCmhg==</ChecksumCRC32>"+
		"<ChecksumType>FULL_OBJECT</ChecksumType></Checksum></GetObjectAttributesResponse>", string(out))
}
