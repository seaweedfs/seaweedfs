package filesys

import (
	"context"
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
)

func (wfs *WFS) saveDataAsChunk(dir string) filer2.SaveDataAsChunkFunctionType {

	return func(reader io.Reader, filename string, offset int64) (chunk *filer_pb.FileChunk, collection, replication string, err error) {
		var fileId, host string
		var auth security.EncodedJwt

		if err := wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

			request := &filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: wfs.option.Replication,
				Collection:  wfs.option.Collection,
				TtlSec:      wfs.option.TtlSec,
				DataCenter:  wfs.option.DataCenter,
				ParentPath:  dir,
			}

			resp, err := client.AssignVolume(context.Background(), request)
			if err != nil {
				glog.V(0).Infof("assign volume failure %v: %v", request, err)
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("assign volume failure %v: %v", request, resp.Error)
			}

			fileId, host, auth = resp.FileId, resp.Url, security.EncodedJwt(resp.Auth)
			host = wfs.AdjustedUrl(host)
			collection, replication = resp.Collection, resp.Replication

			return nil
		}); err != nil {
			return nil, "", "", fmt.Errorf("filerGrpcAddress assign volume: %v", err)
		}

		fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
		uploadResult, err, data := operation.Upload(fileUrl, filename, wfs.option.Cipher, reader, false, "", nil, auth)
		if err != nil {
			glog.V(0).Infof("upload data %v to %s: %v", filename, fileUrl, err)
			return nil, "", "", fmt.Errorf("upload data: %v", err)
		}
		if uploadResult.Error != "" {
			glog.V(0).Infof("upload failure %v to %s: %v", filename, fileUrl, err)
			return nil, "", "", fmt.Errorf("upload result: %v", uploadResult.Error)
		}

		wfs.chunkCache.SetChunk(fileId, data)

		chunk = uploadResult.ToPbFileChunk(fileId, offset)
		return chunk, "", "", nil
	}
}
