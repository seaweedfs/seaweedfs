package filesys

import (
	"context"
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (wfs *WFS) saveDataAsChunk(fullPath util.FullPath, writeOnly bool) filer.SaveDataAsChunkFunctionType {

	return func(reader io.Reader, filename string, offset int64) (chunk *filer_pb.FileChunk, collection, replication string, err error) {
		var fileId, host string
		var auth security.EncodedJwt

		if err := wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
			return util.Retry("assignVolume", func() error {
				request := &filer_pb.AssignVolumeRequest{
					Count:       1,
					Replication: wfs.option.Replication,
					Collection:  wfs.option.Collection,
					TtlSec:      wfs.option.TtlSec,
					DiskType:    string(wfs.option.DiskType),
					DataCenter:  wfs.option.DataCenter,
					Path:        string(fullPath),
				}

				resp, err := client.AssignVolume(context.Background(), request)
				if err != nil {
					glog.V(0).Infof("assign volume failure %v: %v", request, err)
					return err
				}
				if resp.Error != "" {
					return fmt.Errorf("assign volume failure %v: %v", request, resp.Error)
				}

				fileId, auth = resp.FileId, security.EncodedJwt(resp.Auth)
				loc := &filer_pb.Location{
					Url:       resp.Url,
					PublicUrl: resp.PublicUrl,
				}
				host = wfs.AdjustedUrl(loc)
				collection, replication = resp.Collection, resp.Replication

				return nil
			})
		}); err != nil {
			return nil, "", "", fmt.Errorf("filerGrpcAddress assign volume: %v", err)
		}

		fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
		if wfs.option.VolumeServerAccess == "filerProxy" {
			fileUrl = fmt.Sprintf("http://%s/?proxyChunkId=%s", wfs.option.FilerAddress, fileId)
		}
		uploadResult, err, data := operation.Upload(fileUrl, filename, wfs.option.Cipher, reader, false, "", nil, auth)
		if err != nil {
			glog.V(0).Infof("upload data %v to %s: %v", filename, fileUrl, err)
			return nil, "", "", fmt.Errorf("upload data: %v", err)
		}
		if uploadResult.Error != "" {
			glog.V(0).Infof("upload failure %v to %s: %v", filename, fileUrl, err)
			return nil, "", "", fmt.Errorf("upload result: %v", uploadResult.Error)
		}

		if !writeOnly {
			wfs.chunkCache.SetChunk(fileId, data)
		}

		chunk = uploadResult.ToPbFileChunk(fileId, offset)
		return chunk, collection, replication, nil
	}
}
