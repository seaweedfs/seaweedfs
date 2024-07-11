package filersink

import (
	"fmt"
	"github.com/schollz/progressbar/v3"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func (fs *FilerSink) replicateChunks(sourceChunks []*filer_pb.FileChunk, path string) (replicatedChunks []*filer_pb.FileChunk, err error) {
	if len(sourceChunks) == 0 {
		return
	}

	// a simple progress bar. Not ideal. Fix me.
	var bar *progressbar.ProgressBar
	if len(sourceChunks) > 1 {
		name := filepath.Base(path)
		bar = progressbar.NewOptions64(int64(len(sourceChunks)),
			progressbar.OptionClearOnFinish(),
			progressbar.OptionOnCompletion(func() {
				fmt.Fprint(os.Stderr, "\n")
			}),
			progressbar.OptionFullWidth(),
			progressbar.OptionSetDescription(name),
		)
	}

	replicatedChunks = make([]*filer_pb.FileChunk, len(sourceChunks))

	var wg sync.WaitGroup
	for chunkIndex, sourceChunk := range sourceChunks {
		wg.Add(1)
		index, source := chunkIndex, sourceChunk
		fs.executor.Execute(func() {
			defer wg.Done()
			util.Retry("replicate chunks", func() error {
				replicatedChunk, e := fs.replicateOneChunk(source, path)
				if e != nil {
					err = e
					return e
				}
				replicatedChunks[index] = replicatedChunk
				if bar != nil {
					bar.Add(1)
				}
				err = nil
				return nil
			})
		})
	}
	wg.Wait()

	return
}

func (fs *FilerSink) replicateOneChunk(sourceChunk *filer_pb.FileChunk, path string) (*filer_pb.FileChunk, error) {

	fileId, err := fs.fetchAndWrite(sourceChunk, path)
	if err != nil {
		return nil, fmt.Errorf("copy %s: %v", sourceChunk.GetFileIdString(), err)
	}

	return &filer_pb.FileChunk{
		FileId:       fileId,
		Offset:       sourceChunk.Offset,
		Size:         sourceChunk.Size,
		ModifiedTsNs: sourceChunk.ModifiedTsNs,
		ETag:         sourceChunk.ETag,
		SourceFileId: sourceChunk.GetFileIdString(),
		CipherKey:    sourceChunk.CipherKey,
		IsCompressed: sourceChunk.IsCompressed,
	}, nil
}

func (fs *FilerSink) fetchAndWrite(sourceChunk *filer_pb.FileChunk, path string) (fileId string, err error) {

	filename, header, resp, err := fs.filerSource.ReadPart(sourceChunk.GetFileIdString())
	if err != nil {
		return "", fmt.Errorf("read part %s: %v", sourceChunk.GetFileIdString(), err)
	}
	defer util_http.CloseResponse(resp)

	uploader, err := operation.NewUploader()
	if err != nil {
		glog.V(0).Infof("upload source data %v: %v", sourceChunk.GetFileIdString(), err)
		return "", fmt.Errorf("upload data: %v", err)
	}

	fileId, uploadResult, err, _ := uploader.UploadWithRetry(
		fs,
		&filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: fs.replication,
			Collection:  fs.collection,
			TtlSec:      fs.ttlSec,
			DataCenter:  fs.dataCenter,
			DiskType:    fs.diskType,
			Path:        path,
		},
		&operation.UploadOption{
			Filename:          filename,
			Cipher:            false,
			IsInputCompressed: "gzip" == header.Get("Content-Encoding"),
			MimeType:          header.Get("Content-Type"),
			PairMap:           nil,
		},
		func(host, fileId string) string {
			fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
			if fs.writeChunkByFiler {
				fileUrl = fmt.Sprintf("http://%s/?proxyChunkId=%s", fs.address, fileId)
			}
			glog.V(4).Infof("replicating %s to %s header:%+v", filename, fileUrl, header)
			return fileUrl
		},
		resp.Body,
	)

	if err != nil {
		glog.V(0).Infof("upload source data %v: %v", sourceChunk.GetFileIdString(), err)
		return "", fmt.Errorf("upload data: %v", err)
	}
	if uploadResult.Error != "" {
		glog.V(0).Infof("upload failure %v: %v", filename, err)
		return "", fmt.Errorf("upload result: %v", uploadResult.Error)
	}

	return
}

var _ = filer_pb.FilerClient(&FilerSink{})

func (fs *FilerSink) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithGrpcClient(streamingMode, fs.signature, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, fs.grpcAddress, false, fs.grpcDialOption)

}

func (fs *FilerSink) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (fs *FilerSink) GetDataCenter() string {
	return fs.dataCenter
}
