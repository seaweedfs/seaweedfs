package s3api

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
)

func TestInitiateMultipartUploadResult(t *testing.T) {

	expected := `<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>example-bucket</Bucket><Key>example-object</Key><UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId></InitiateMultipartUploadResult>`
	response := &InitiateMultipartUploadResult{
		CreateMultipartUploadOutput: s3.CreateMultipartUploadOutput{
			Bucket:   aws.String("example-bucket"),
			Key:      aws.String("example-object"),
			UploadId: aws.String("VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"),
		},
	}

	encoded := string(s3err.EncodeXMLResponse(response))
	if encoded != expected {
		t.Errorf("unexpected output: %s\nexpecting:%s", encoded, expected)
	}

}

func TestListPartsResult(t *testing.T) {

	expected := `<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><ETag>&#34;12345678&#34;</ETag><LastModified>1970-01-01T00:00:00Z</LastModified><PartNumber>1</PartNumber><Size>123</Size></Part></ListPartsResult>`
	response := &ListPartsResult{
		Part: []*s3.Part{
			{
				PartNumber:   aws.Int64(int64(1)),
				LastModified: aws.Time(time.Unix(0, 0).UTC()),
				Size:         aws.Int64(int64(123)),
				ETag:         aws.String("\"12345678\""),
			},
		},
	}

	encoded := string(s3err.EncodeXMLResponse(response))
	if encoded != expected {
		t.Errorf("unexpected output: %s\nexpecting:%s", encoded, expected)
	}

}

func Test_parsePartNumber(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		partNum  int
	}{
		{
			"first",
			"0001_uuid.part",
			1,
		},
		{
			"second",
			"0002.part",
			2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partNumber, _ := parsePartNumber(tt.fileName)
			assert.Equalf(t, tt.partNum, partNumber, "parsePartNumber(%v)", tt.fileName)
		})
	}
}

func TestGetEntryNameAndDir(t *testing.T) {
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}

	tests := []struct {
		name           string
		bucket         string
		key            string
		expectedName   string
		expectedDirEnd string // We check the suffix since dir includes BucketsPath
	}{
		{
			name:           "simple file at root",
			bucket:         "test-bucket",
			key:            "/file.txt",
			expectedName:   "file.txt",
			expectedDirEnd: "/buckets/test-bucket",
		},
		{
			name:           "file in subdirectory",
			bucket:         "test-bucket",
			key:            "/folder/file.txt",
			expectedName:   "file.txt",
			expectedDirEnd: "/buckets/test-bucket/folder",
		},
		{
			name:           "file in nested subdirectory",
			bucket:         "test-bucket",
			key:            "/folder/subfolder/file.txt",
			expectedName:   "file.txt",
			expectedDirEnd: "/buckets/test-bucket/folder/subfolder",
		},
		{
			name:           "key without leading slash",
			bucket:         "test-bucket",
			key:            "folder/file.txt",
			expectedName:   "file.txt",
			expectedDirEnd: "/buckets/test-bucket/folder",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &s3.CompleteMultipartUploadInput{
				Bucket: aws.String(tt.bucket),
				Key:    aws.String(tt.key),
			}
			entryName, dirName := s3a.getEntryNameAndDir(input)
			assert.Equal(t, tt.expectedName, entryName, "entry name mismatch")
			assert.Equal(t, tt.expectedDirEnd, dirName, "directory mismatch")
		})
	}
}

func TestValidateCompletePartETag(t *testing.T) {
	t.Run("matches_composite_etag_from_extended", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				s3_constants.ExtETagKey: []byte("ea58527f14c6ae0dd53089966e44941b-2"),
			},
			Attributes: &filer_pb.FuseAttributes{},
		}
		match, invalid, part, stored := validateCompletePartETag(`"ea58527f14c6ae0dd53089966e44941b-2"`, entry)
		assert.True(t, match)
		assert.False(t, invalid)
		assert.Equal(t, "ea58527f14c6ae0dd53089966e44941b-2", part)
		assert.Equal(t, "ea58527f14c6ae0dd53089966e44941b-2", stored)
	})

	t.Run("matches_md5_from_attributes", func(t *testing.T) {
		md5Bytes, err := hex.DecodeString("324b2665939fde5b8678d3a8b5c46970")
		assert.NoError(t, err)
		entry := &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{
				Md5: md5Bytes,
			},
		}
		match, invalid, part, stored := validateCompletePartETag("324b2665939fde5b8678d3a8b5c46970", entry)
		assert.True(t, match)
		assert.False(t, invalid)
		assert.Equal(t, "324b2665939fde5b8678d3a8b5c46970", part)
		assert.Equal(t, "324b2665939fde5b8678d3a8b5c46970", stored)
	})

	t.Run("detects_mismatch", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				s3_constants.ExtETagKey: []byte("67fdd2e302502ff9f9b606bc036e6892-2"),
			},
			Attributes: &filer_pb.FuseAttributes{},
		}
		match, invalid, _, _ := validateCompletePartETag("686f7d71bacdcd539dd4e17a0d7f1e5f-2", entry)
		assert.False(t, match)
		assert.False(t, invalid)
	})

	t.Run("flags_empty_client_etag_as_invalid", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				s3_constants.ExtETagKey: []byte("67fdd2e302502ff9f9b606bc036e6892-2"),
			},
			Attributes: &filer_pb.FuseAttributes{},
		}
		match, invalid, _, _ := validateCompletePartETag(`""`, entry)
		assert.False(t, match)
		assert.True(t, invalid)
	})
}
