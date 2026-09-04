package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A chunk read through a filer is a request to the filer, so it carries a filer
// token; a read straight from the volume server holding the needle carries a
// volume token scoped to that file id. Sending the wrong one is not a
// degradation but a 401, so the two must not be confused.
func TestChunkReadJwt(t *testing.T) {
	const (
		volumeReadKey = "volume-read-key"
		filerReadKey  = "filer-read-key"
		fileId        = "3,01637037d6"
	)
	util.GetViper().Set("jwt.filer_signing.read.key", filerReadKey)
	loadJwtConfigOnce.Do(func() {})
	previousKey, previousExpires := jwtSigningReadKey, jwtSigningReadKeyExpires
	t.Cleanup(func() { jwtSigningReadKey, jwtSigningReadKeyExpires = previousKey, previousExpires })
	jwtSigningReadKey, jwtSigningReadKeyExpires = security.SigningKey(volumeReadKey), 60

	t.Run("through a filer", func(t *testing.T) {
		token := security.EncodedJwt(ChunkReadJwt([]string{"http://filer:8888/?proxyChunkId=" + fileId}, fileId))
		if _, err := security.DecodeJwt(security.SigningKey(filerReadKey), token, &security.SeaweedFilerClaims{}); err != nil {
			t.Fatalf("token does not validate against the filer read key: %v", err)
		}
	})

	t.Run("straight to a volume server", func(t *testing.T) {
		token := security.EncodedJwt(ChunkReadJwt([]string{"http://volume:8080/" + fileId}, fileId))
		claims := &security.SeaweedFileIdClaims{}
		if _, err := security.DecodeJwt(security.SigningKey(volumeReadKey), token, claims); err != nil {
			t.Fatalf("token does not validate against the volume read key: %v", err)
		}
		if claims.Fid != fileId {
			t.Fatalf("token authorizes file %q, want %q", claims.Fid, fileId)
		}
	})
}
