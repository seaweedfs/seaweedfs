package weed_server

import (
	"context"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"

	"io"
	"math/rand/v2"
	"net/http"
	"strings"
)

// proxyReadConcurrencyPerVolumeServer limits how many concurrent proxy read
// requests the filer will issue to any single volume server. Without this,
// replication bursts can open hundreds of connections to one volume server,
// causing it to drop connections with "unexpected EOF".
const proxyReadConcurrencyPerVolumeServer = 16

var (
	proxySemaphores sync.Map // host -> chan struct{}
)

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

// isProxyReadMethod reports whether a proxied request only reads. Everything
// else is treated as a write for both credential and concurrency purposes.
func isProxyReadMethod(method string) bool {
	return method == http.MethodGet || method == http.MethodHead
}

// baseFileId strips the trailing _N delta suffix that batch assigns append to a
// fid, and only that: the suffix must be a non-empty run of digits, otherwise
// the fid is returned whole for the caller to reject. The volume server compares
// a JWT's fid claim against the stripped form (see
// VolumeServer.maybeCheckJwtAuthorization), so anything minting or parsing a fid
// on this side has to agree with it.
//
// That server strips at the last "_" unconditionally, which is safe there
// because its fid already came out of a path the mux parsed and so cannot hold
// a "/". Here the value is raw query input, and an unguarded strip would reduce
// "3,01637037d6_1/../../status" to a valid fid and wave the traversal through.
func baseFileId(fileId string) string {
	sepIndex := strings.LastIndex(fileId, "_")
	if sepIndex <= 0 {
		return fileId
	}
	delta := fileId[sepIndex+1:]
	if delta == "" {
		return fileId
	}
	for _, c := range delta {
		if c < '0' || c > '9' {
			return fileId
		}
	}
	return fileId[:sepIndex]
}

// validateProxyChunkId rejects a proxyChunkId that is not a well-formed fid.
// LookupFileId only requires a single comma, and the value is pasted into the
// volume server URL path, so "3,x/../../status" resolves to a volume the caller
// never named -- the volume server's mux cleans the dot segments and redirects
// to /status, which the filer follows and relays.
func validateProxyChunkId(fileId string) error {
	_, err := needle.ParseFileIdFromString(baseFileId(fileId))
	return err
}

func (fs *FilerServer) proxyToVolumeServer(w http.ResponseWriter, r *http.Request, fileId string) {
	ctx := r.Context()

	if err := validateProxyChunkId(fileId); err != nil {
		glog.V(1).InfofCtx(ctx, "reject proxyChunkId %q: %v", fileId, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

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

	fs.proxyToVolumeServerURL(w, r, fileId, urlStrings[rand.IntN(len(urlStrings))])
}

// proxyToVolumeServerURL forwards the request to one already-resolved volume
// server URL.
func (fs *FilerServer) proxyToVolumeServerURL(w http.ResponseWriter, r *http.Request, fileId, targetURL string) {
	ctx := r.Context()

	// targetURL from LookupFileId already contains the fileId in the path
	// (e.g. http://server:8080/6,08136bdce4). Forward the caller's query params
	// (e.g. readDeleted=true from weed mount) but drop the internal proxyChunkId.
	query := r.URL.Query()
	query.Del("proxyChunkId")
	if encoded := query.Encode(); encoded != "" {
		targetURL += "?" + encoded
	}

	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		glog.ErrorfCtx(ctx, "NewRequest %s: %v", targetURL, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Limit concurrent reads per volume server to prevent overload. Writes are
	// deliberately exempt: a proxied write carries the caller's AssignVolume
	// token, which expires 10s after the assign by default, so queueing one here
	// can push it past expiry and turn it into a 401 the uploader does not
	// re-assign on.
	if isProxyReadMethod(r.Method) {
		volumeHost := proxyReq.URL.Host
		if err := acquireProxySemaphore(ctx, volumeHost); err != nil {
			glog.V(0).InfofCtx(ctx, "proxy to %s cancelled while waiting: %v", volumeHost, err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		defer releaseProxySemaphore(volumeHost)
	}

	proxyReq.Header.Set("Host", r.Host)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	request_id.InjectToRequest(ctx, proxyReq)

	for header, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}

	// Decide the volume credential explicitly rather than letting the copied
	// header stand, because the two directions need opposite handling.
	//
	// Reads: the volume server may require a read JWT even though the proxy
	// endpoint doesn't, so mint one. When there is nothing to mint, drop the
	// caller's Authorization instead of relaying it -- on this path it is a
	// filer credential, and a volume server has no business seeing one.
	//
	// Writes: never mint. This branch runs ahead of the filer's JWT gate, so a
	// token minted here would be signed for an unauthenticated caller. A
	// legitimate writer already carries its own volume JWT from AssignVolume,
	// so that one is forwarded untouched.
	if isProxyReadMethod(r.Method) {
		if jwt := fs.maybeGetVolumeReadJwtAuthorizationToken(fileId); jwt != "" {
			proxyReq.Header.Set("Authorization", security.BearerPrefix+jwt)
		} else {
			proxyReq.Header.Del("Authorization")
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
