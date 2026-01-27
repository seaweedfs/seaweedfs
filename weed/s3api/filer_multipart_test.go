package s3api

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
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
