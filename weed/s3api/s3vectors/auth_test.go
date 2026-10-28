package s3vectors

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

type fakeAuthenticator struct {
	identity *s3api.Identity
}

func (f *fakeAuthenticator) AuthenticateRequest(r *http.Request) (string, interface{}, s3err.ErrorCode) {
	return f.identity.Name, f.identity, s3err.ErrNone
}

func (f *fakeAuthenticator) DefaultAllow() bool { return true }

// The authenticated identity object must ride the request context — only the
// name survives otherwise, and downstream authorization loses actions,
// account, and claims.
func TestAuthKeepsIdentityObject(t *testing.T) {
	want := &s3api.Identity{Name: "alice", Actions: []s3api.Action{"Admin"}}
	s := &Server{authenticator: &fakeAuthenticator{identity: want}}
	var got interface{}
	h := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		got = s3_constants.GetIdentityFromContext(r)
	})
	h(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	if got != interface{}(want) {
		t.Fatalf("identity object missing from context: %v", got)
	}
}
