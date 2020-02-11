package weed_server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/topology"
)

func (vs *VolumeServer) fastPostHandler(ctx *fasthttp.RequestCtx) {

	stats.VolumeServerRequestCounter.WithLabelValues("post").Inc()
	start := time.Now()
	defer func() {
		stats.VolumeServerRequestHistogram.WithLabelValues("post").Observe(time.Since(start).Seconds())
	}()

	requestPath := string(ctx.Path())

	vid, fid, _, _, _ := parseURLPath(requestPath)
	volumeId, ve := needle.NewVolumeId(vid)
	if ve != nil {
		glog.V(0).Infoln("NewVolumeId error:", ve)
		writeJsonError(ctx, http.StatusBadRequest, ve)
		return
	}

	if !vs.maybeCheckJwtAuthorization(ctx, vid, fid, true) {
		writeJsonError(ctx, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	needle, originalSize, ne := needle.CreateNeedleFromRequest(ctx, vs.FixJpgOrientation, vs.fileSizeLimitBytes)
	if ne != nil {
		writeJsonError(ctx, http.StatusBadRequest, ne)
		return
	}

	ret := operation.UploadResult{}
	_, isUnchanged, writeError := topology.OldReplicatedWrite(vs.GetMaster(), vs.store, volumeId, needle, r)

	// http 304 status code does not allow body
	if writeError == nil && isUnchanged {
		ctx.SetStatusCode(http.StatusNotModified)
		return
	}

	httpStatus := http.StatusCreated
	if writeError != nil {
		httpStatus = http.StatusInternalServerError
		ret.Error = writeError.Error()
	}
	if needle.HasName() {
		ret.Name = string(needle.Name)
	}
	ret.Size = uint32(originalSize)
	ret.ETag = needle.Etag()
	fastSetEtag(ctx, ret.ETag)
	writeJsonQuiet(ctx, httpStatus, ret)
}

func (vs *VolumeServer) DeleteHandler(ctx *fasthttp.RequestCtx) {

	stats.VolumeServerRequestCounter.WithLabelValues("delete").Inc()
	start := time.Now()
	defer func() {
		stats.VolumeServerRequestHistogram.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	}()

	requestPath := string(ctx.Path())
	n := new(needle.Needle)
	vid, fid, _, _, _ := parseURLPath(requestPath)
	volumeId, _ := needle.NewVolumeId(vid)
	n.ParsePath(fid)

	if !vs.maybeCheckJwtAuthorization(ctx, vid, fid, true) {
		writeJsonError(ctx, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	// glog.V(2).Infof("volume %s deleting %s", vid, n)

	cookie := n.Cookie

	ecVolume, hasEcVolume := vs.store.FindEcVolume(volumeId)

	if hasEcVolume {
		count, err := vs.store.DeleteEcShardNeedle(context.Background(), ecVolume, n, cookie)
		writeDeleteResult(err, count, ctx)
		return
	}

	_, ok := vs.store.ReadVolumeNeedle(volumeId, n)
	if ok != nil {
		m := make(map[string]uint32)
		m["size"] = 0
		writeJsonQuiet(ctx, http.StatusNotFound, m)
		return
	}

	if n.Cookie != cookie {
		glog.V(0).Infof("delete %s with unmaching cookie from %s agent %s", requestPath, ctx.RemoteAddr(), ctx.UserAgent())
		writeJsonError(ctx, http.StatusBadRequest, errors.New("File Random Cookie does not match."))
		return
	}

	count := int64(n.Size)

	if n.IsChunkedManifest() {
		chunkManifest, e := operation.LoadChunkManifest(n.Data, n.IsGzipped())
		if e != nil {
			writeJsonError(ctx, http.StatusInternalServerError, fmt.Errorf("Load chunks manifest error: %v", e))
			return
		}
		// make sure all chunks had deleted before delete manifest
		if e := chunkManifest.DeleteChunks(vs.GetMaster(), vs.grpcDialOption); e != nil {
			writeJsonError(ctx, http.StatusInternalServerError, fmt.Errorf("Delete chunks error: %v", e))
			return
		}
		count = chunkManifest.Size
	}

	n.LastModified = uint64(time.Now().Unix())
	tsValue := ctx.FormValue("ts")
	if tsValue != nil {
		modifiedTime, err := strconv.ParseInt(string(tsValue), 10, 64)
		if err == nil {
			n.LastModified = uint64(modifiedTime)
		}
	}

	_, err := topology.ReplicatedDelete(vs.GetMaster(), vs.store, volumeId, n, ctx)

	writeDeleteResult(err, count, ctx)

}

func writeDeleteResult(err error, count int64, ctx *fasthttp.RequestCtx) {
	if err == nil {
		m := make(map[string]int64)
		m["size"] = count
		writeJsonQuiet(ctx, http.StatusAccepted, m)
	} else {
		writeJsonQuiet(ctx, http.StatusInternalServerError, fmt.Errorf("Deletion Failed: %v", err))
	}
}
