package filer

import (
	"io"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type recordingUploader struct {
	option *operation.UploadOption
}

func (u *recordingUploader) UploadWithRetry(_ filer_pb.FilerClient, _ *filer_pb.AssignVolumeRequest, option *operation.UploadOption, reader io.Reader) (string, *operation.UploadResult, error, []byte) {
	u.option = option
	data, _ := io.ReadAll(reader)
	return "1,01", &operation.UploadResult{Size: uint32(len(data))}, nil, data
}

type nopFilerClient struct{ filer_pb.FilerClient }

// Without the chunk md5 the file ETag is the md5 of empty digests, so an
// overwrite keeps the same ETag.
func TestSaveGatewayDataAsChunkAsksForMd5(t *testing.T) {
	uploader := &recordingUploader{}
	_, err := SaveGatewayDataAsChunk(GatewayChunkUploadRequest{
		FilerClient: nopFilerClient{},
		Uploader:    uploader,
		Reader:      strings.NewReader("hello"),
		FullPath:    "/buckets/b/x.txt",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !uploader.option.WantMd5 {
		t.Fatal("WantMd5 is false; the chunk gets no ETag")
	}
}
