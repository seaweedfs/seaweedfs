package S3Sink

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func (s3sink *S3Sink) deleteObject(key string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s3sink.bucket),
		Key:    aws.String(key),
	}

	result, err := s3sink.conn.DeleteObject(input)

	if err == nil {
		glog.V(2).Infof("[%s] delete %s: %v", s3sink.bucket, key, result)
	} else {
		glog.Errorf("[%s] delete %s: %v", s3sink.bucket, key, err)
	}

	return err

}
