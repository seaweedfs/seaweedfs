package S3Sink

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
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

func (s3sink *S3Sink) createMultipartUpload(key string, entry *filer_pb.Entry) (uploadId string, err error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(s3sink.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(entry.Attributes.Mime),
	}
	if s3sink.acl != "" {
		input.ACL = aws.String(s3sink.acl)
	}

	result, err := s3sink.conn.CreateMultipartUpload(input)

	if err == nil {
		glog.V(2).Infof("[%s] createMultipartUpload %s: %v", s3sink.bucket, key, result)
	} else {
		glog.Errorf("[%s] createMultipartUpload %s: %v", s3sink.bucket, key, err)
		return "", err
	}

	return *result.UploadId, err
}

func (s3sink *S3Sink) abortMultipartUpload(key, uploadId string) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s3sink.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
	}

	result, err := s3sink.conn.AbortMultipartUpload(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchUpload:
				glog.Errorf("[%s] abortMultipartUpload %s: %v %v", s3sink.bucket, key, s3.ErrCodeNoSuchUpload, aerr.Error())
			default:
				glog.Errorf("[%s] abortMultipartUpload %s: %v", s3sink.bucket, key, aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			glog.Errorf("[%s] abortMultipartUpload %s: %v", s3sink.bucket, key, aerr.Error())
		}
		return err
	}

	glog.V(0).Infof("[%s] abortMultipartUpload %s: %v", s3sink.bucket, key, result)

	return nil
}

// To complete multipart upload
func (s3sink *S3Sink) completeMultipartUpload(ctx context.Context, key, uploadId string, parts []*s3.CompletedPart) error {
	input := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s3sink.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
	}

	result, err := s3sink.conn.CompleteMultipartUpload(input)
	if err == nil {
		glog.V(2).Infof("[%s] completeMultipartUpload %s: %v", s3sink.bucket, key, result)
	} else {
		glog.Errorf("[%s] completeMultipartUpload %s: %v", s3sink.bucket, key, err)
		return fmt.Errorf("[%s] completeMultipartUpload %s: %v", s3sink.bucket, key, err)
	}

	return nil
}

// To upload a part
func (s3sink *S3Sink) uploadPart(key, uploadId string, partId int, chunk *filer.ChunkView) (*s3.CompletedPart, error) {
	var readSeeker io.ReadSeeker

	readSeeker, err := s3sink.buildReadSeeker(chunk)
	if err != nil {
		glog.Errorf("[%s] uploadPart %s %d read: %v", s3sink.bucket, key, partId, err)
		return nil, fmt.Errorf("[%s] uploadPart %s %d read: %v", s3sink.bucket, key, partId, err)
	}

	input := &s3.UploadPartInput{
		Body:       readSeeker,
		Bucket:     aws.String(s3sink.bucket),
		Key:        aws.String(key),
		PartNumber: aws.Int64(int64(partId)),
		UploadId:   aws.String(uploadId),
	}

	result, err := s3sink.conn.UploadPart(input)
	if err == nil {
		glog.V(2).Infof("[%s] uploadPart %s %d upload: %v", s3sink.bucket, key, partId, result)
	} else {
		glog.Errorf("[%s] uploadPart %s %d upload: %v", s3sink.bucket, key, partId, err)
	}

	part := &s3.CompletedPart{
		ETag:       result.ETag,
		PartNumber: aws.Int64(int64(partId)),
	}

	return part, err
}

// To upload a part by copying byte range from an existing object as data source
func (s3sink *S3Sink) uploadPartCopy(key, uploadId string, partId int64, copySource string, sourceStart, sourceStop int) error {
	input := &s3.UploadPartCopyInput{
		Bucket:          aws.String(s3sink.bucket),
		CopySource:      aws.String(fmt.Sprintf("/%s/%s", s3sink.bucket, copySource)),
		CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", sourceStart, sourceStop)),
		Key:             aws.String(key),
		PartNumber:      aws.Int64(partId),
		UploadId:        aws.String(uploadId),
	}

	result, err := s3sink.conn.UploadPartCopy(input)
	if err == nil {
		glog.V(0).Infof("[%s] uploadPartCopy %s %d: %v", s3sink.bucket, key, partId, result)
	} else {
		glog.Errorf("[%s] uploadPartCopy %s %d: %v", s3sink.bucket, key, partId, err)
	}

	return err
}

func (s3sink *S3Sink) buildReadSeeker(chunk *filer.ChunkView) (io.ReadSeeker, error) {
	fileUrls, err := s3sink.filerSource.LookupFileId(chunk.FileId)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, chunk.Size)
	for _, fileUrl := range fileUrls {
		_, err = util.ReadUrl(fileUrl, chunk.CipherKey, chunk.IsGzipped, false, chunk.Offset, int(chunk.Size), buf)
		if err != nil {
			glog.V(1).Infof("read from %s: %v", fileUrl, err)
		} else {
			break
		}
	}
	return bytes.NewReader(buf), nil
}
