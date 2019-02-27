package s3api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"testing"
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
