package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"sync"
	"time"
)

func (vs *VolumeServer) FetchAndWriteNeedle(ctx context.Context, req *volume_server_pb.FetchAndWriteNeedleRequest) (resp *volume_server_pb.FetchAndWriteNeedleResponse, err error) {
	resp = &volume_server_pb.FetchAndWriteNeedleResponse{}
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	remoteConf := req.RemoteConf

	client, getClientErr := remote_storage.GetRemoteStorage(remoteConf)
	if getClientErr != nil {
		return nil, fmt.Errorf("get remote client: %v", getClientErr)
	}

	remoteStorageLocation := req.RemoteLocation

	data, ReadRemoteErr := client.ReadFile(remoteStorageLocation, req.Offset, req.Size)
	if ReadRemoteErr != nil {
		return nil, fmt.Errorf("read from remote %+v: %v", remoteStorageLocation, ReadRemoteErr)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := new(needle.Needle)
		n.Id = types.NeedleId(req.NeedleId)
		n.Cookie = types.Cookie(req.Cookie)
		n.Data, n.DataSize = data, uint32(len(data))
		// copied from *Needle.prepareWriteBuffer()
		n.Size = 4 + types.Size(n.DataSize) + 1
		n.Checksum = needle.NewCRC(n.Data)
		n.LastModified = uint64(time.Now().Unix())
		n.SetHasLastModifiedDate()
		if _, localWriteErr := vs.store.WriteVolumeNeedle(v.Id, n, true, false); localWriteErr != nil {
			if err == nil {
				err = fmt.Errorf("local write needle %d size %d: %v", req.NeedleId, req.Size, localWriteErr)
			}
		} else {
			resp.ETag = n.Etag()
		}
	}()
	if len(req.Replicas) > 0 {
		fileId := needle.NewFileId(v.Id, req.NeedleId, req.Cookie)
		for _, replica := range req.Replicas {
			wg.Add(1)
			go func(targetVolumeServer string) {
				defer wg.Done()
				uploadOption := &operation.UploadOption{
					UploadUrl:         fmt.Sprintf("http://%s/%s?type=replicate", targetVolumeServer, fileId.String()),
					Filename:          "",
					Cipher:            false,
					IsInputCompressed: false,
					MimeType:          "",
					PairMap:           nil,
					Jwt:               security.EncodedJwt(req.Auth),
				}

				uploader, uploaderErr := operation.NewUploader()
				if uploaderErr != nil && err == nil {
					err = fmt.Errorf("remote write needle %d size %d: %v", req.NeedleId, req.Size, uploaderErr)
					return
				}

				if _, replicaWriteErr := uploader.UploadData(data, uploadOption); replicaWriteErr != nil && err == nil {
					err = fmt.Errorf("remote write needle %d size %d: %v", req.NeedleId, req.Size, replicaWriteErr)
				}
			}(replica.Url)
		}
	}

	wg.Wait()

	return resp, err
}
