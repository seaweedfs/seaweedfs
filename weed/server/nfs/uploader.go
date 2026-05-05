package nfs

import (
	"io"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type chunkUploader interface {
	UploadWithRetry(
		filerClient filer_pb.FilerClient,
		assignRequest *filer_pb.AssignVolumeRequest,
		uploadOption *operation.UploadOption,
		genFileUrlFn func(host, fileId string) string,
		reader io.Reader,
	) (fileId string, uploadResult *operation.UploadResult, err error, data []byte)
}

type operationChunkUploader struct {
	uploader *operation.Uploader
}

func (u operationChunkUploader) UploadWithRetry(
	filerClient filer_pb.FilerClient,
	assignRequest *filer_pb.AssignVolumeRequest,
	uploadOption *operation.UploadOption,
	genFileUrlFn func(host, fileId string) string,
	reader io.Reader,
) (string, *operation.UploadResult, error, []byte) {
	return u.uploader.UploadWithRetry(filerClient, assignRequest, uploadOption, genFileUrlFn, reader)
}

func newChunkUploader() (chunkUploader, error) {
	uploader, err := operation.NewUploader()
	if err != nil {
		return nil, err
	}
	return operationChunkUploader{uploader: uploader}, nil
}
