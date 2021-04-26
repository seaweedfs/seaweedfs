package s3api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"testing"
	"time"
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

	encoded := string(encodeResponse(response))
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

	encoded := string(encodeResponse(response))
	if encoded != expected {
		t.Errorf("unexpected output: %s\nexpecting:%s", encoded, expected)
	}

}
