package weed_server

import (
	"strings"

	"github.com/valyala/fasthttp"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/stats"
)

func (vs *VolumeServer) HandleFastHTTP(ctx *fasthttp.RequestCtx) {

	switch string(ctx.Method()) {
	case "GET", "HEAD":
		vs.fastGetOrHeadHandler(ctx)
	case "DELETE":
		stats.DeleteRequest()
		vs.guard.WhiteList(vs.DeleteHandler)(ctx)
	case "PUT", "POST":
		stats.WriteRequest()
		vs.guard.WhiteList(vs.fastPostHandler)(ctx)
	}

}

func (vs *VolumeServer) publicReadOnlyHandler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Method()) {
	case "GET":
		stats.ReadRequest()
		vs.fastGetOrHeadHandler(ctx)
	case "HEAD":
		stats.ReadRequest()
		vs.fastGetOrHeadHandler(ctx)
	}
}

func (vs *VolumeServer) maybeCheckJwtAuthorization(ctx *fasthttp.RequestCtx, vid, fid string, isWrite bool) bool {

	var signingKey security.SigningKey

	if isWrite {
		if len(vs.guard.SigningKey) == 0 {
			return true
		} else {
			signingKey = vs.guard.SigningKey
		}
	} else {
		if len(vs.guard.ReadSigningKey) == 0 {
			return true
		} else {
			signingKey = vs.guard.ReadSigningKey
		}
	}

	tokenStr := security.GetJwt(ctx)
	if tokenStr == "" {
		glog.V(1).Infof("missing jwt from %s", ctx.RemoteAddr())
		return false
	}

	token, err := security.DecodeJwt(signingKey, tokenStr)
	if err != nil {
		glog.V(1).Infof("jwt verification error from %s: %v", ctx.RemoteAddr(), err)
		return false
	}
	if !token.Valid {
		glog.V(1).Infof("jwt invalid from %s: %v", ctx.RemoteAddr(), tokenStr)
		return false
	}

	if sc, ok := token.Claims.(*security.SeaweedFileIdClaims); ok {
		if sepIndex := strings.LastIndex(fid, "_"); sepIndex > 0 {
			fid = fid[:sepIndex]
		}
		return sc.Fid == vid+","+fid
	}
	glog.V(1).Infof("unexpected jwt from %s: %v", ctx.RemoteAddr(), tokenStr)
	return false
}
