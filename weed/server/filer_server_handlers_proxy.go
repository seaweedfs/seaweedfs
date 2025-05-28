package weed_server

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"io"
	"math/rand/v2"
	"net/http"
)

func (fs *FilerServer) maybeAddVolumeJwtAuthorization(r *http.Request, fileId string, isWrite bool) {
	encodedJwt := fs.maybeGetVolumeJwtAuthorizationToken(fileId, isWrite)

	if encodedJwt == "" {
		return
	}

	r.Header.Set("Authorization", "BEARER "+string(encodedJwt))
}

func (fs *FilerServer) maybeGetVolumeJwtAuthorizationToken(fileId string, isWrite bool) string {
	var encodedJwt security.EncodedJwt
	if isWrite {
		encodedJwt = security.GenJwtForVolumeServer(fs.volumeGuard.SigningKey, fs.volumeGuard.ExpiresAfterSec, fileId)
	} else {
		encodedJwt = security.GenJwtForVolumeServer(fs.volumeGuard.ReadSigningKey, fs.volumeGuard.ReadExpiresAfterSec, fileId)
	}
	return string(encodedJwt)
}

func (fs *FilerServer) proxyToVolumeServer(w http.ResponseWriter, r *http.Request, fileId string) {
	ctx := r.Context()
	urlStrings, err := fs.filer.MasterClient.GetLookupFileIdFunction()(ctx, fileId)
	if err != nil {
		glog.Errorf("locate %s: %v", fileId, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if len(urlStrings) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	proxyReq, err := http.NewRequest(r.Method, urlStrings[rand.IntN(len(urlStrings))], r.Body)
	if err != nil {
		glog.Errorf("NewRequest %s: %v", urlStrings[0], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	proxyReq.Header.Set("Host", r.Host)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	util.ReqWithRequestId(proxyReq, ctx)

	for header, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}

	proxyResponse, postErr := util_http.GetGlobalHttpClient().Do(proxyReq)

	if postErr != nil {
		glog.Errorf("post to filer: %v", postErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer util_http.CloseResponse(proxyResponse)

	for k, v := range proxyResponse.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(proxyResponse.StatusCode)

	buf := mem.Allocate(128 * 1024)
	defer mem.Free(buf)
	io.CopyBuffer(w, proxyResponse.Body, buf)

}
