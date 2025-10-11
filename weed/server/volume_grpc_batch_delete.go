package weed_server

import (
	"context"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
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
		var cookie types.Cookie
		glog.Errorf("🔥 BATCH DELETE: fid=%s, volumeId=%d, isEcVolume=%t, SkipCookieCheck=%t", fid, volumeId, isEcVolume, req.SkipCookieCheck)
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
			cookie = n.Cookie
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
			if req.SkipCookieCheck {
				// When skipping cookie check, use the direct gRPC deletion path that bypasses cookie validation
				glog.Errorf("🎯 SKIP COOKIE DELETE: volume %d, needle %d, using direct DeleteNeedleFromEcx", ecVolume.VolumeId, n.Id)
				err = ecVolume.DeleteNeedleFromEcx(n.Id)
				var size int64 = 0
				if err == nil {
					// Return a reasonable size for success status
					size = int64(n.Size)
				}
				if err != nil {
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
			} else {
				// Cookie check enabled, use the cookie validation path
				if size, err := vs.store.DeleteEcShardNeedle(ecVolume, n, cookie); err != nil {
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
	}

	return resp, nil

}
