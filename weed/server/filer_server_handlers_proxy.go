package weed_server

import (
	"context"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"

	"io"
	"math/rand/v2"
	"net/http"
)

// proxyReadConcurrencyPerVolumeServer limits how many concurrent proxy read
// requests the filer will issue to any single volume server. Without this,
// replication bursts can open hundreds of connections to one volume server,
// causing it to drop connections with "unexpected EOF".
const proxyReadConcurrencyPerVolumeServer = 16

var (
	proxySemaphores   sync.Map // host -> chan struct{}
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

func acquireProxySemaphore(ctx context.Context, host string) error {
	v, _ := proxySemaphores.LoadOrStore(host, make(chan struct{}, proxyReadConcurrencyPerVolumeServer))
	sem := v.(chan struct{})
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func releaseProxySemaphore(host string) {
	v, ok := proxySemaphores.Load(host)
	if !ok {
		return
	}
	select {
	case <-v.(chan struct{}):
	default:
		glog.Warningf("proxy semaphore for %s was already empty on release", host)
	}
}

func (fs *FilerServer) proxyToVolumeServer(w http.ResponseWriter, r *http.Request, fileId string) {
	ctx := r.Context()
	urlStrings, err := fs.filer.MasterClient.GetLookupFileIdFunction()(ctx, fileId)
	if err != nil {
		glog.ErrorfCtx(ctx, "locate %s: %v", fileId, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if len(urlStrings) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	proxyReq, err := http.NewRequest(r.Method, urlStrings[rand.IntN(len(urlStrings))], r.Body)
	if err != nil {
		glog.ErrorfCtx(ctx, "NewRequest %s: %v", urlStrings[0], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Limit concurrent requests per volume server to prevent overload
	volumeHost := proxyReq.URL.Host
	if err := acquireProxySemaphore(ctx, volumeHost); err != nil {
		glog.V(0).InfofCtx(ctx, "proxy to %s cancelled while waiting: %v", volumeHost, err)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	defer releaseProxySemaphore(volumeHost)

	proxyReq.Header.Set("Host", r.Host)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	request_id.InjectToRequest(ctx, proxyReq)

	for header, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}

	proxyResponse, postErr := util_http.GetGlobalHttpClient().Do(proxyReq)

	if postErr != nil {
		glog.ErrorfCtx(ctx, "post to filer: %v", postErr)
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
	if _, copyErr := io.CopyBuffer(w, proxyResponse.Body, buf); copyErr != nil {
		glog.V(0).InfofCtx(ctx, "proxy copy %s: %v", fileId, copyErr)
	}

}
