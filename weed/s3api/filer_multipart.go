package s3api

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/satori/go.uuid"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (s3a *S3ApiServer) createMultipartUpload(input *s3.CreateMultipartUploadInput) (output *s3.CreateMultipartUploadOutput, code ErrorCode) {
	uploadId, _ := uuid.NewV4()
	uploadIdString := uploadId.String()

	if err := s3a.mkdir(s3a.genUploadsFolder(*input.Bucket), uploadIdString, func(entry *filer_pb.Entry) {
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		entry.Extended["key"] = []byte(*input.Key)
	}); err != nil {
		glog.Errorf("NewMultipartUpload error: %v", err)
		return nil, ErrInternalError
	}

	output = &s3.CreateMultipartUploadOutput{
		Bucket:   input.Bucket,
		Key:      input.Key,
		UploadId: aws.String(uploadIdString),
	}

	return
}

func (s3a *S3ApiServer) completeMultipartUpload(input *s3.CompleteMultipartUploadInput) (output *s3.CompleteMultipartUploadOutput, code ErrorCode) {
	return
}

func (s3a *S3ApiServer) abortMultipartUpload(input *s3.AbortMultipartUploadInput) (output *s3.AbortMultipartUploadOutput, code ErrorCode) {
	return
}

func (s3a *S3ApiServer) listMultipartUploads(input *s3.ListMultipartUploadsInput) (output *s3.ListMultipartUploadsOutput, code ErrorCode) {
	entries, err := s3a.list(s3a.genUploadsFolder(*input.Bucket))
	if err != nil {
		glog.Errorf("listMultipartUploads %s error: %v", *input.Bucket, err)
		return nil, ErrNoSuchUpload
	}
	output = &s3.ListMultipartUploadsOutput{
		Bucket:       input.Bucket,
		Delimiter:    input.Delimiter,
		EncodingType: input.EncodingType,
		KeyMarker:    input.KeyMarker,
		MaxUploads:   input.MaxUploads,
		Prefix:       input.Prefix,
	}
	for _, entry := range entries {
		if entry.Extended != nil {
			key := entry.Extended["key"]
			output.Uploads = append(output.Uploads, &s3.MultipartUpload{
				Key:      aws.String(string(key)),
				UploadId: aws.String(entry.Name),
			})
		}
	}
	return
}

func (s3a *S3ApiServer) listObjectParts(input *s3.ListPartsInput) (output *s3.ListPartsOutput, code ErrorCode) {
	return
}
