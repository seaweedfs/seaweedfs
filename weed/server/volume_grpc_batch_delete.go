package weed_server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func (vs *VolumeServer) BatchDelete(ctx context.Context, req *volume_server_pb.BatchDeleteRequest) (*volume_server_pb.BatchDeleteResponse, error) {
	resp := &volume_server_pb.BatchDeleteResponse{}

	if err := vs.checkGrpcAdminAuth(ctx); err != nil {
		return resp, err
	}

	if err := vs.CheckMaintenanceMode(); err != nil {
		return resp, err
	}

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
		var fidCookie types.Cookie
		if req.SkipCookieCheck {
			n.Id, fidCookie, err = needle.ParseNeedleIdCookie(id_cookie)
			if err != nil {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusBadRequest,
					Error:  err.Error()})
				continue
			}
		} else {
			if err := n.ParsePath(id_cookie); err != nil {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusBadRequest,
					Error:  err.Error()})
				continue
			}
		}

		ecVolume, isEcVolume := vs.store.FindEcVolume(volumeId)
		if !req.SkipCookieCheck {
			cookie := n.Cookie
			if !isEcVolume {
				if _, err := vs.store.ReadVolumeNeedle(volumeId, n, deleteReadOption(), nil); err != nil {
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
				continue
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
			if req.SkipCookieCheck {
				// the delete cancels in-flight writes carrying this cookie
				n.Cookie = fidCookie
			}
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
			size, err := vs.store.DeleteEcShardNeedle(ecVolume, n, n.Cookie)
			if errors.Is(err, storage.ErrorDeleted) {
				// Already gone, which is what the caller asked for. The
				// non-EC branch above reports that as StatusNotModified.
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusNotModified},
				)
			} else if err != nil {
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
