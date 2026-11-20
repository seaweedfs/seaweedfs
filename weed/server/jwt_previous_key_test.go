package weed_server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/security"
)

func TestJwtRotationAcceptsPreviousKey(t *testing.T) {
	bearer := func(method, target string, token security.EncodedJwt) *http.Request {
		r := httptest.NewRequest(method, target, nil)
		r.Header.Set("Authorization", "Bearer "+string(token))
		return r
	}

	fs := &FilerServer{filerGuard: security.NewGuard(nil, "filer-new", 10, "filer-read-new", 60, "filer-old", "filer-read-old")}
	vs := &VolumeServer{guard: security.NewGuard(nil, "volume-new", 10, "volume-read-new", 60, "volume-old", "volume-read-old")}
	check := func(want bool) {
		t.Helper()
		for _, tc := range []struct {
			name    string
			key     string
			isWrite bool
		}{
			{"filer write", "filer-old", true},
			{"filer read", "filer-read-old", false},
		} {
			r := bearer(http.MethodGet, "/dir/file", security.GenJwtForFilerServer(security.SigningKey(tc.key), 60))
			if got := fs.maybeCheckJwtAuthorization(r, tc.isWrite); got != want {
				t.Errorf("%s signed by %s: authorized=%v, want %v", tc.name, tc.key, got, want)
			}
		}
		for _, tc := range []struct {
			name    string
			key     string
			isWrite bool
		}{
			{"volume write", "volume-old", true},
			{"volume read", "volume-read-old", false},
		} {
			r := bearer(http.MethodGet, "/3,01637037d6", security.GenJwtForVolumeServer(security.SigningKey(tc.key), 60, "3,01637037d6"))
			if got := vs.maybeCheckJwtAuthorization(r, "3", "01637037d6", tc.isWrite); got != want {
				t.Errorf("%s signed by %s: authorized=%v, want %v", tc.name, tc.key, got, want)
			}
		}
	}
	check(true)

	other := bearer(http.MethodGet, "/dir/file", security.GenJwtForFilerServer(security.SigningKey("unrelated"), 60))
	if fs.maybeCheckJwtAuthorization(other, true) {
		t.Fatal("a token signed by neither key must be refused")
	}
	current := bearer(http.MethodGet, "/3,01637037d6", security.GenJwtForVolumeServer(security.SigningKey("volume-new"), 60, "3,01637037d6"))
	if !vs.maybeCheckJwtAuthorization(current, "3", "01637037d6", true) {
		t.Fatal("a token signed by the current key must be accepted")
	}

	fs.filerGuard.UpdateSigningKeys("filer-new", 10, "filer-read-new", 60, "", "")
	vs.guard.UpdateSigningKeys("volume-new", 10, "volume-read-new", 60, "", "")
	check(false)
}
