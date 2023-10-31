package weed_server

import (
	"context"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) BatchDelete(ctx context.Context, req *volume_server_pb.BatchDeleteRequest) (*volume_server_pb.BatchDeleteResponse, error) {

	resp := &volume_server_pb.BatchDeleteResponse{}

	now := uint64(time.Now().Unix())

	for _, fid := range req.FileIds {
		vid, id_cookie, err := operation.ParseFileId(fid)
		if err != nil {
			resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusBadRequest,
				Error:  err.Error()})
			continue
		}

		n := new(needle.Needle)
		volumeId, _ := needle.NewVolumeId(vid)
		ecVolume, isEcVolume := vs.store.FindEcVolume(volumeId)
		if req.SkipCookieCheck {
			n.Id, _, err = needle.ParseNeedleIdCookie(id_cookie)
			if err != nil {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusBadRequest,
					Error:  err.Error()})
				continue
			}
		} else {
			n.ParsePath(id_cookie)
			cookie := n.Cookie
			if !isEcVolume {
				if _, err := vs.store.ReadVolumeNeedle(volumeId, n, nil, nil); err != nil {
					resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
						FileId: fid,
						Status: http.StatusNotFound,
						Error:  err.Error(),
					})
					continue
				}
			} else {
				if _, err := vs.store.ReadEcShardNeedle(volumeId, n, nil); err != nil {
					resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
						FileId: fid,
						Status: http.StatusNotFound,
						Error:  err.Error(),
					})
					continue
				}
			}
			if n.Cookie != cookie {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusBadRequest,
					Error:  "File Random Cookie does not match.",
				})
				break
			}
		}

		if n.IsChunkedManifest() {
			resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusNotAcceptable,
				Error:  "ChunkManifest: not allowed in batch delete mode.",
			})
			continue
		}

		n.LastModified = now
		if !isEcVolume {
			if size, err := vs.store.DeleteVolumeNeedle(volumeId, n); err != nil {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusInternalServerError,
					Error:  err.Error()},
				)
			} else if size == 0 {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusNotModified},
				)
			} else {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusAccepted,
					Size:   uint32(size)},
				)
			}
		} else {
			if size, err := vs.store.DeleteEcShardNeedle(ecVolume, n, n.Cookie); err != nil {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusInternalServerError,
					Error:  err.Error()},
				)
			} else {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusAccepted,
					Size:   uint32(size)},
				)
			}
		}
	}

	return resp, nil

}
