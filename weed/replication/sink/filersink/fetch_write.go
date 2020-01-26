package filersink

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"strings"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerSink) replicateChunks(ctx context.Context, sourceChunks []*filer_pb.FileChunk) (replicatedChunks []*filer_pb.FileChunk, err error) {
	if len(sourceChunks) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, sourceChunk := range sourceChunks {
		wg.Add(1)
		go func(chunk *filer_pb.FileChunk) {
			defer wg.Done()
			replicatedChunk, e := fs.replicateOneChunk(ctx, chunk)
			if e != nil {
				err = e
			}
			replicatedChunks = append(replicatedChunks, replicatedChunk)
		}(sourceChunk)
	}
	wg.Wait()

	return
}

func (fs *FilerSink) replicateOneChunk(ctx context.Context, sourceChunk *filer_pb.FileChunk) (*filer_pb.FileChunk, error) {

	fileId, err := fs.fetchAndWrite(ctx, sourceChunk)
	if err != nil {
		return nil, fmt.Errorf("copy %s: %v", sourceChunk.GetFileIdString(), err)
	}

	return &filer_pb.FileChunk{
		FileId:       fileId,
		Offset:       sourceChunk.Offset,
		Size:         sourceChunk.Size,
		Mtime:        sourceChunk.Mtime,
		ETag:         sourceChunk.ETag,
		SourceFileId: sourceChunk.GetFileIdString(),
	}, nil
}

func (fs *FilerSink) fetchAndWrite(ctx context.Context, sourceChunk *filer_pb.FileChunk) (fileId string, err error) {

	filename, header, readCloser, err := fs.filerSource.ReadPart(ctx, sourceChunk.GetFileIdString())
	if err != nil {
		return "", fmt.Errorf("read part %s: %v", sourceChunk.GetFileIdString(), err)
	}
	defer readCloser.Close()

	var host string
	var auth security.EncodedJwt

	if err := fs.withFilerClient(ctx, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: fs.replication,
			Collection:  fs.collection,
			TtlSec:      fs.ttlSec,
			DataCenter:  fs.dataCenter,
		}

		resp, err := client.AssignVolume(ctx, request)
		if err != nil {
			glog.V(0).Infof("assign volume failure %v: %v", request, err)
			return err
		}

		fileId, host, auth = resp.FileId, resp.Url, security.EncodedJwt(resp.Auth)

		return nil
	}); err != nil {
		return "", fmt.Errorf("filerGrpcAddress assign volume: %v", err)
	}

	fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)

	glog.V(4).Infof("replicating %s to %s header:%+v", filename, fileUrl, header)

	uploadResult, err := operation.Upload(fileUrl, filename, readCloser,
		"gzip" == header.Get("Content-Encoding"), header.Get("Content-Type"), nil, auth)
	if err != nil {
		glog.V(0).Infof("upload data %v to %s: %v", filename, fileUrl, err)
		return "", fmt.Errorf("upload data: %v", err)
	}
	if uploadResult.Error != "" {
		glog.V(0).Infof("upload failure %v to %s: %v", filename, fileUrl, err)
		return "", fmt.Errorf("upload result: %v", uploadResult.Error)
	}

	return
}

func (fs *FilerSink) withFilerClient(ctx context.Context, fn func(context.Context, filer_pb.SeaweedFilerClient) error) error {

	return util.WithCachedGrpcClient(ctx, func(ctx context.Context, grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(ctx, client)
	}, fs.grpcAddress, fs.grpcDialOption)

}

func volumeId(fileId string) string {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex]
	}
	return fileId
}
