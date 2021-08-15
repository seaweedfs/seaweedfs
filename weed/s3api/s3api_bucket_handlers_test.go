package s3api

import (
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestListBucketsHandler(t *testing.T) {

	expected := `<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><DisplayName></DisplayName><ID></ID></Owner><Buckets><Bucket><CreationDate>2011-04-09T12:34:49Z</CreationDate><Name>test1</Name></Bucket><Bucket><CreationDate>2011-02-09T12:34:49Z</CreationDate><Name>test2</Name></Bucket></Buckets></ListAllMyBucketsResult>`
	var response ListAllMyBucketsResult

	var buckets []*s3.Bucket
	buckets = append(buckets, &s3.Bucket{
		Name:         aws.String("test1"),
		CreationDate: aws.Time(time.Date(2011, 4, 9, 12, 34, 49, 0, time.UTC)),
	})
	buckets = append(buckets, &s3.Bucket{
		Name:         aws.String("test2"),
		CreationDate: aws.Time(time.Date(2011, 2, 9, 12, 34, 49, 0, time.UTC)),
	})

	response = ListAllMyBucketsResult{
		Owner: &s3.Owner{
			ID:          aws.String(""),
			DisplayName: aws.String(""),
		},
		Buckets: buckets,
	}

	encoded := string(s3err.EncodeXMLResponse(response))
	if encoded != expected {
		t.Errorf("unexpected output: %s\nexpecting:%s", encoded, expected)
	}
}
