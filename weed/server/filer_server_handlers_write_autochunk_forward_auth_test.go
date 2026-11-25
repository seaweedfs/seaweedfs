package weed_server

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HTTP append and offset forwarding must keep compatibility-mode requests
// tokenless while require-mode carries the request's bearer to the owner.
func TestSaveMetaDataHTTPForwardAuthFollowsRequireMode(t *testing.T) {
	for _, tc := range []struct {
		name         string
		requireToken bool
		write        string
		auth         string
		wantError    codes.Code
	}{
		{name: "compatibility/append expired bearer", write: "append", auth: "expired", wantError: codes.OK},
		{name: "compatibility/append unrelated bearer", write: "append", auth: "unrelated", wantError: codes.OK},
		{name: "compatibility/append AT cookie", write: "append", auth: "cookie", wantError: codes.OK},
		{name: "compatibility/offset expired bearer", write: "offset", auth: "expired", wantError: codes.OK},
		{name: "require/append valid bearer", requireToken: true, write: "append", auth: "valid", wantError: codes.OK},
		{name: "require/append expired bearer", requireToken: true, write: "append", auth: "expired", wantError: codes.Unauthenticated},
		{name: "require/append unrelated bearer", requireToken: true, write: "append", auth: "unrelated", wantError: codes.Unauthenticated},
		{name: "require/append valid AT cookie", requireToken: true, write: "append", auth: "cookie", wantError: codes.OK},
		{name: "require/offset valid bearer", requireToken: true, write: "offset", auth: "valid", wantError: codes.OK},
		{name: "require/offset expired bearer", requireToken: true, write: "offset", auth: "expired", wantError: codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, key := newHTTPForwardAuthFilers(t, tc.requireToken)
			path := c.keyOwnedBy(t, c.addrB, c.addrB)
			seedHTTPForwardEntry(t, c, path, tc.write)
			requestURL := string(path)
			if tc.write == "append" {
				requestURL += "?op=append"
			}
			r := httptest.NewRequest(http.MethodPut, requestURL, nil)
			setHTTPForwardAuth(t, r, key, tc.auth)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var saveErr error
			if tc.write == "append" {
				_, saveErr, _ = c.fsA.saveMetaData(ctx, r, path.Name(), "", &operation.StorageOption{}, 4, nil, nil, 3, []byte("new"))
			} else {
				_, saveErr, _ = c.fsA.saveMetaData(ctx, r, path.Name(), "", &operation.StorageOption{}, 4, nil, []*filer_pb.FileChunk{{FileId: "3,new", Offset: 3, Size: 3}}, 6, nil)
			}
			if got := status.Code(saveErr); got != tc.wantError {
				t.Fatalf("saveMetaData error code = %v, want %v (error=%v)", got, tc.wantError, saveErr)
			}

			stored, err := c.store.FindEntry(ctx, path)
			if err != nil {
				t.Fatalf("find entry: %v", err)
			}
			if tc.write == "append" {
				wantContent := ""
				if tc.wantError == codes.OK {
					wantContent = "new"
				}
				if got := string(stored.Content); got != wantContent {
					t.Fatalf("stored content = %q, want %q", got, wantContent)
				}
			} else {
				wantChunks := 1
				wantSize := uint64(3)
				if tc.wantError == codes.OK {
					wantChunks = 2
					wantSize = 6
				}
				if got := len(stored.GetChunks()); got != wantChunks {
					t.Fatalf("stored chunk count = %d, want %d", got, wantChunks)
				}
				if got := stored.FileSize; got != wantSize {
					t.Fatalf("stored file size = %d, want %d", got, wantSize)
				}
			}
		})
	}
}

func newHTTPForwardAuthFilers(t *testing.T, requireToken bool) (*twoFilers, security.SigningKey) {
	t.Helper()
	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatalf("generate filer signing key: %v", err)
	}
	key := security.SigningKey(keyBytes)
	c := newTwoFilers(t, func(fs *FilerServer) {
		fs.option.SaveToFilerLimit = 64
		fs.filerGuard = security.NewGuard(nil, string(key), 10, "", 0, "", "")
		fs.filer.PeerContext = fs.peerContext
		fs.grpcScopeSettings.Store(&grpcScopeSettings{requireToken: requireToken})
		fs.grpcScope = newFilerGrpcScope(scopeConfig{
			signingKey:       fs.filerGuard.SigningKey,
			verificationKeys: func() (security.SigningKey, security.SigningKey) { return fs.filerGuard.VerificationKeys(true) },
			requireToken:     func() bool { return requireToken },
		}, fs)
	})
	sharedWriteRing(c)
	return c, key
}

func seedHTTPForwardEntry(t *testing.T, c *twoFilers, path util.FullPath, write string) {
	t.Helper()
	if write == "append" {
		c.seed(t, path)
		return
	}
	entry := newFileEntry(string(path), 7)
	entry.FileSize = 3
	entry.Chunks = []*filer_pb.FileChunk{{FileId: "3,old", Offset: 0, Size: 3}}
	if err := c.store.InsertEntry(context.Background(), entry); err != nil {
		t.Fatalf("seed offset entry: %v", err)
	}
}

func setHTTPForwardAuth(t *testing.T, r *http.Request, key security.SigningKey, kind string) {
	t.Helper()
	switch kind {
	case "valid":
		r.Header.Set("Authorization", "Bearer "+peerAdminToken(key))
	case "expired":
		claims := jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute))}
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(key))
		if err != nil {
			t.Fatalf("sign expired test token: %v", err)
		}
		r.Header.Set("Authorization", "Bearer "+token)
	case "unrelated":
		r.Header.Set("Authorization", "Bearer "+peerAdminToken(security.SigningKey("unrelated-key")))
	case "cookie":
		r.AddCookie(&http.Cookie{Name: "AT", Value: peerAdminToken(key)})
	default:
		t.Fatalf("unknown HTTP auth kind %q", kind)
	}
}
