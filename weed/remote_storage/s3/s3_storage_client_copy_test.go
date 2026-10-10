package s3

import (
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/stretchr/testify/require"
)

// copyObjectMock serves HeadObject for a source of size bytes and records the
// copy calls.
type copyObjectMock struct {
	s3iface.S3API
	size        int64
	missing     bool
	partErr     error
	copyInputs  []*awss3.CopyObjectInput
	partInputs  []*awss3.UploadPartCopyInput
	createInput *awss3.CreateMultipartUploadInput
	tagInputs   []*awss3.GetObjectTaggingInput
	completed   *awss3.CompleteMultipartUploadInput
	aborted     bool
}

func (m *copyObjectMock) HeadObject(input *awss3.HeadObjectInput) (*awss3.HeadObjectOutput, error) {
	if m.missing {
		return nil, awserr.NewRequestFailure(awserr.New("NotFound", "not found", nil), http.StatusNotFound, "")
	}
	return &awss3.HeadObjectOutput{
		ContentLength:           aws.Int64(m.size),
		ContentType:             aws.String("image/jpeg"),
		Expires:                 aws.String("Wed, 21 Oct 2026 07:28:00 GMT"),
		WebsiteRedirectLocation: aws.String("/elsewhere"),
		ServerSideEncryption:    aws.String("aws:kms"),
		SSEKMSKeyId:             aws.String("key-1"),
		ETag:                    aws.String(`"etag-` + aws.StringValue(input.Key) + `"`),
	}, nil
}

func (m *copyObjectMock) GetObjectTagging(input *awss3.GetObjectTaggingInput) (*awss3.GetObjectTaggingOutput, error) {
	m.tagInputs = append(m.tagInputs, input)
	return &awss3.GetObjectTaggingOutput{TagSet: []*awss3.Tag{
		{Key: aws.String("album"), Value: aws.String("2026 & co")},
		{Key: aws.String("a+b"), Value: aws.String("x=y")},
	}}, nil
}

func (m *copyObjectMock) CopyObject(input *awss3.CopyObjectInput) (*awss3.CopyObjectOutput, error) {
	m.copyInputs = append(m.copyInputs, input)
	return &awss3.CopyObjectOutput{}, nil
}

func (m *copyObjectMock) CreateMultipartUpload(input *awss3.CreateMultipartUploadInput) (*awss3.CreateMultipartUploadOutput, error) {
	m.createInput = input
	return &awss3.CreateMultipartUploadOutput{UploadId: aws.String("upload-1")}, nil
}

func (m *copyObjectMock) UploadPartCopy(input *awss3.UploadPartCopyInput) (*awss3.UploadPartCopyOutput, error) {
	m.partInputs = append(m.partInputs, input)
	if m.partErr != nil && len(m.partInputs) == 2 {
		return nil, m.partErr
	}
	return &awss3.UploadPartCopyOutput{CopyPartResult: &awss3.CopyPartResult{ETag: aws.String(`"part"`)}}, nil
}

func (m *copyObjectMock) CompleteMultipartUpload(input *awss3.CompleteMultipartUploadInput) (*awss3.CompleteMultipartUploadOutput, error) {
	m.completed = input
	return &awss3.CompleteMultipartUploadOutput{}, nil
}

func (m *copyObjectMock) AbortMultipartUpload(*awss3.AbortMultipartUploadInput) (*awss3.AbortMultipartUploadOutput, error) {
	m.aborted = true
	return &awss3.AbortMultipartUploadOutput{}, nil
}

func newCopyTestClient(mock *copyObjectMock) *s3RemoteStorageClient {
	return &s3RemoteStorageClient{conf: &remote_pb.RemoteConf{Name: "test"}, conn: mock}
}

var (
	copySrc = &remote_pb.RemoteStorageLocation{Name: "test", Bucket: "bucket", Path: "/src/a b.jpg"}
	copyDst = &remote_pb.RemoteStorageLocation{Name: "test", Bucket: "bucket", Path: "/dst/a b.jpg"}
)

func TestS3ClientImplementsObjectCopier(t *testing.T) {
	var _ remote_storage.RemoteStorageObjectCopier = &s3RemoteStorageClient{}
}

func TestS3CopyFileUsesCopyObjectUpToTheLimit(t *testing.T) {
	mock := &copyObjectMock{size: s3CopyObjectSizeLimit}

	remoteEntry, err := newCopyTestClient(mock).CopyFile(copySrc, copyDst)
	require.NoError(t, err)

	require.Len(t, mock.copyInputs, 1)
	require.Equal(t, "bucket/src/a%20b.jpg", aws.StringValue(mock.copyInputs[0].CopySource))
	require.Equal(t, "dst/a b.jpg", aws.StringValue(mock.copyInputs[0].Key))
	require.Equal(t, "aws:kms", aws.StringValue(mock.copyInputs[0].ServerSideEncryption))
	require.Equal(t, "key-1", aws.StringValue(mock.copyInputs[0].SSEKMSKeyId))
	require.Nil(t, mock.createInput)
	// the RemoteEntry describes the destination
	require.Equal(t, `"etag-dst/a b.jpg"`, remoteEntry.RemoteETag)
	require.Equal(t, int64(s3CopyObjectSizeLimit), remoteEntry.RemoteSize)
}

func TestS3CopyFileCopiesLargeObjectsInParts(t *testing.T) {
	size := int64(s3CopyObjectSizeLimit) + 1
	mock := &copyObjectMock{size: size}

	_, err := newCopyTestClient(mock).CopyFile(copySrc, copyDst)
	require.NoError(t, err)

	require.Empty(t, mock.copyInputs)
	require.NotNil(t, mock.createInput)
	require.Equal(t, "image/jpeg", aws.StringValue(mock.createInput.ContentType))
	require.Equal(t, "/elsewhere", aws.StringValue(mock.createInput.WebsiteRedirectLocation))
	require.Equal(t, "aws:kms", aws.StringValue(mock.createInput.ServerSideEncryption))
	require.Equal(t, "key-1", aws.StringValue(mock.createInput.SSEKMSKeyId))
	require.Equal(t, int64(1792567680), aws.TimeValue(mock.createInput.Expires).Unix())
	// tags only where the remote supports tagging
	require.Empty(t, mock.tagInputs)
	require.Nil(t, mock.createInput.Tagging)
	require.Len(t, mock.partInputs, 11)
	require.Equal(t, "bytes=0-536870911", aws.StringValue(mock.partInputs[0].CopySourceRange))
	require.Equal(t, "bytes=5368709120-5368709120", aws.StringValue(mock.partInputs[10].CopySourceRange))
	for _, part := range mock.partInputs {
		require.Equal(t, `"etag-src/a b.jpg"`, aws.StringValue(part.CopySourceIfMatch))
	}
	require.Len(t, mock.completed.MultipartUpload.Parts, 11)
	require.Equal(t, int64(11), aws.Int64Value(mock.completed.MultipartUpload.Parts[10].PartNumber))
	require.False(t, mock.aborted)
}

func TestS3CopyFileCarriesTagsIntoAMultipartCopy(t *testing.T) {
	mock := &copyObjectMock{size: s3CopyObjectSizeLimit + 1}
	client := newCopyTestClient(mock)
	client.conf.S3SupportTagging = true

	_, err := client.CopyFile(copySrc, copyDst)
	require.NoError(t, err)

	require.Len(t, mock.tagInputs, 1)
	require.Equal(t, "src/a b.jpg", aws.StringValue(mock.tagInputs[0].Key))
	require.Equal(t, "album=2026%20%26%20co&a%2Bb=x%3Dy", aws.StringValue(mock.createInput.Tagging))
}

func TestS3CopyFileAbortsAFailedMultipartCopy(t *testing.T) {
	mock := &copyObjectMock{size: 2 * s3CopyObjectSizeLimit, partErr: errors.New("part failed")}

	_, err := newCopyTestClient(mock).CopyFile(copySrc, copyDst)
	require.ErrorContains(t, err, "part failed")
	require.True(t, mock.aborted)
	require.Nil(t, mock.completed)
}

func TestS3CopyFileMissingSource(t *testing.T) {
	mock := &copyObjectMock{missing: true}

	_, err := newCopyTestClient(mock).CopyFile(copySrc, copyDst)
	require.ErrorIs(t, err, remote_storage.ErrRemoteObjectNotFound)
	require.Empty(t, mock.copyInputs)
}
