package http

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestIsProxyChunkUrl(t *testing.T) {
	for _, tc := range []struct {
		urlString string
		want      bool
	}{
		{ProxyChunkUrl("filer:8888", "3,01637037d6"), true},
		{"http://filer:8888/?proxyChunkId=3,01637037d6&readDeleted=true", true},
		{"http://volume:8080/3,01637037d6", false},
		{"http://volume:8080/3,01637037d6?readDeleted=true", false},
		{"", false},
	} {
		if got := IsProxyChunkUrl(tc.urlString); got != tc.want {
			t.Errorf("IsProxyChunkUrl(%q) = %v, want %v", tc.urlString, got, tc.want)
		}
	}
}

// The credential a proxied chunk carries is a filer one, signed with the key
// for the access level the caller is about to use.
func TestJwtForFilerServer(t *testing.T) {
	const (
		writeKey = "filer-write-key"
		readKey  = "filer-read-key"
	)
	v := util.GetViper()
	v.Set("jwt.filer_signing.key", writeKey)
	v.Set("jwt.filer_signing.read.key", readKey)

	for _, tc := range []struct {
		name     string
		isWrite  bool
		signedBy string
		otherKey string
	}{
		{"read", false, readKey, writeKey},
		{"write", true, writeKey, readKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := security.EncodedJwt(JwtForFilerServer(tc.isWrite))
			claims := &security.SeaweedFilerClaims{}
			if _, err := security.DecodeJwt(security.SigningKey(tc.signedBy), token, claims); err != nil {
				t.Fatalf("token does not validate against the %s key: %v", tc.name, err)
			}
			if claims.ExpiresAt == nil {
				t.Fatal("token never expires")
			}
			if _, err := security.DecodeJwt(security.SigningKey(tc.otherKey), token, &security.SeaweedFilerClaims{}); err == nil {
				t.Fatal("token also validates against the other access level's key")
			}
		})
	}
}

func TestJwtForFilerServerWithoutKeys(t *testing.T) {
	loadFilerJwtConfigOnce.Do(func() {})
	write, read := filerSigningKey, filerReadSigningKey
	t.Cleanup(func() { filerSigningKey, filerReadSigningKey = write, read })
	filerSigningKey, filerReadSigningKey = nil, nil

	for _, isWrite := range []bool{false, true} {
		if token := JwtForFilerServer(isWrite); token != "" {
			t.Errorf("unconfigured signing key still produced %q", token)
		}
	}
}
