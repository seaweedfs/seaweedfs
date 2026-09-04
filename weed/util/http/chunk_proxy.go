package http

import (
	"fmt"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ProxyChunkIdParam is the query parameter a filer reads to serve one chunk
// from the volume server holding it, for callers that cannot reach volume
// servers directly.
const ProxyChunkIdParam = "proxyChunkId"

var (
	filerSigningKey            security.SigningKey
	filerSigningKeyExpires     int
	filerReadSigningKey        security.SigningKey
	filerReadSigningKeyExpires int
	loadFilerJwtConfigOnce     sync.Once
)

func loadFilerJwtConfig() {
	v := util.GetViper()
	filerSigningKey = security.SigningKey(v.GetString("jwt.filer_signing.key"))
	filerSigningKeyExpires = v.GetInt("jwt.filer_signing.expires_after_seconds")
	if filerSigningKeyExpires == 0 {
		filerSigningKeyExpires = 10
	}
	filerReadSigningKey = security.SigningKey(v.GetString("jwt.filer_signing.read.key"))
	filerReadSigningKeyExpires = v.GetInt("jwt.filer_signing.read.expires_after_seconds")
	if filerReadSigningKeyExpires == 0 {
		filerReadSigningKeyExpires = 60
	}
}

// JwtForFilerServer generates a JWT for the filer's HTTP API if jwt.filer_signing
// is configured for that access level, the way filer.JwtForVolumeServer does for
// volume servers. Empty when the key is unset, which is also when the filer
// serves the request unsigned.
func JwtForFilerServer(isWrite bool) string {
	loadFilerJwtConfigOnce.Do(loadFilerJwtConfig)
	if isWrite {
		return string(security.GenJwtForFilerServer(filerSigningKey, filerSigningKeyExpires))
	}
	return string(security.GenJwtForFilerServer(filerReadSigningKey, filerReadSigningKeyExpires))
}

// ProxyChunkUrl builds the request that reads or writes one chunk through a
// filer instead of the volume server holding it.
func ProxyChunkUrl(filerAddress string, fileId string) string {
	return fmt.Sprintf("http://%s/?%s=%s", filerAddress, ProxyChunkIdParam, fileId)
}

// IsProxyChunkUrl reports whether a chunk URL addresses a filer's chunk proxy
// rather than a volume server. Such a request is authorized by the filer, which
// attaches the volume credential itself, so the token it carries is a filer one.
func IsProxyChunkUrl(urlString string) bool {
	return strings.Contains(urlString, ProxyChunkIdParam+"=")
}
