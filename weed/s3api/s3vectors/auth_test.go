package s3vectors

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

type fakeAuthenticator struct {
	identity *s3api.Identity
	errCode  s3err.ErrorCode
	calls    int
}

func (f *fakeAuthenticator) AuthenticateRequest(r *http.Request) (string, interface{}, s3err.ErrorCode) {
	f.calls++
	if f.errCode != s3err.ErrNone {
		return "", nil, f.errCode
	}
	return f.identity.Name, f.identity, s3err.ErrNone
}

func (f *fakeAuthenticator) DefaultAllow() bool { return true }

func TestAuthKeepsIdentityObject(t *testing.T) {
	want := &s3api.Identity{Name: "alice", Actions: []s3api.Action{"Admin"}}
	s := &Server{authenticator: &fakeAuthenticator{identity: want}}
	var got interface{}
	h := s.Auth(func(w http.ResponseWriter, r *http.Request) {
		got = s3_constants.GetIdentityFromContext(r)
		if name := s3_constants.GetIdentityNameFromContext(r); name != want.Name {
			t.Errorf("identity name = %q, want %q", name, want.Name)
		}
	})
	h(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	if got != interface{}(want) {
		t.Fatalf("identity object missing from context: %v", got)
	}
}

func TestAuthAnswersOversizedBodyWithValidationException(t *testing.T) {
	alice := &s3api.Identity{Name: "alice", Actions: []s3api.Action{"Admin"}}
	for _, tc := range []struct {
		name          string
		contentLength int64
		errCode       s3err.ErrorCode
		wantStatus    int
		wantCode      string
		wantAuthCalls int
	}{
		{"declared length over the limit", maxRequestBodyBytes + 1, s3err.ErrNone, http.StatusBadRequest, "ValidationException", 0},
		{"streamed body over the limit", -1, s3err.ErrEntityTooLarge, http.StatusBadRequest, "ValidationException", 1},
		{"bad signature", 10, s3err.ErrSignatureDoesNotMatch, http.StatusForbidden, "AccessDenied", 1},
		{"declared length at the limit", maxRequestBodyBytes, s3err.ErrNone, http.StatusOK, "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auth := &fakeAuthenticator{identity: alice, errCode: tc.errCode}
			s := &Server{authenticator: auth}
			req := httptest.NewRequest(http.MethodPost, "/PutVectors", strings.NewReader("{}"))
			req.ContentLength = tc.contentLength
			rec := httptest.NewRecorder()
			s.Auth(func(w http.ResponseWriter, r *http.Request) {})(rec, req)

			if rec.Code != tc.wantStatus || auth.calls != tc.wantAuthCalls {
				t.Fatalf("status %d after %d auth calls, want %d after %d: %s", rec.Code, auth.calls, tc.wantStatus, tc.wantAuthCalls, rec.Body)
			}
			if tc.wantCode != "" && !strings.Contains(rec.Body.String(), `"code":"`+tc.wantCode+`"`) {
				t.Fatalf("body %s, want code %s", rec.Body, tc.wantCode)
			}
			if tc.wantCode == "ValidationException" && !strings.Contains(rec.Body.String(), "request body exceeds 32 MiB") {
				t.Fatalf("body %s does not name the limit", rec.Body)
			}
		})
	}
}

func TestReadJSONRefusesBodyOverTheLimit(t *testing.T) {
	var req PutVectorsRequest
	over := httptest.NewRequest(http.MethodPost, "/PutVectors", io.MultiReader(
		strings.NewReader(`{"indexName":"`), strings.NewReader(strings.Repeat("a", maxRequestBodyBytes)), strings.NewReader(`"}`)))
	if err := readJSON(over, &req); !errors.Is(err, errRequestBodyTooLarge) {
		t.Fatalf("readJSON over the limit = %v, want %v", err, errRequestBodyTooLarge)
	}
	within := httptest.NewRequest(http.MethodPost, "/PutVectors", strings.NewReader(`{"indexName":"idx"}`))
	if err := readJSON(within, &req); err != nil || req.IndexName != "idx" {
		t.Fatalf("readJSON within the limit = %v, indexName %q", err, req.IndexName)
	}
}
