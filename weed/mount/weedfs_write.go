package mount

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

func (wfs *WFS) saveDataAsChunk(fullPath util.FullPath) filer.SaveDataAsChunkFunctionType {

	return func(reader io.Reader, filename string, offset int64) (chunk *filer_pb.FileChunk, collection, replication string, err error) {
		var fileId, host string
		var auth security.EncodedJwt

		if err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
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
				loc := resp.Location
				host = wfs.AdjustedUrl(loc)
				collection, replication = resp.Collection, resp.Replication

				return nil
			})
		}); err != nil {
			return nil, "", "", fmt.Errorf("filerGrpcAddress assign volume: %v", err)
		}

		fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
		if wfs.option.VolumeServerAccess == "filerProxy" {
			fileUrl = fmt.Sprintf("http://%s/?proxyChunkId=%s", wfs.getCurrentFiler(), fileId)
		}
		uploadOption := &operation.UploadOption{
			UploadUrl:         fileUrl,
			Filename:          filename,
			Cipher:            wfs.option.Cipher,
			IsInputCompressed: false,
			MimeType:          "",
			PairMap:           nil,
			Jwt:               auth,
		}
		uploadResult, err, data := operation.Upload(reader, uploadOption)
		if err != nil {
			glog.V(0).Infof("upload data %v to %s: %v", filename, fileUrl, err)
			return nil, "", "", fmt.Errorf("upload data: %v", err)
		}
		if uploadResult.Error != "" {
			glog.V(0).Infof("upload failure %v to %s: %v", filename, fileUrl, err)
			return nil, "", "", fmt.Errorf("upload result: %v", uploadResult.Error)
		}

		if offset == 0 {
			wfs.chunkCache.SetChunk(fileId, data)
		}

		chunk = uploadResult.ToPbFileChunk(fileId, offset)
		return chunk, collection, replication, nil
	}
}
