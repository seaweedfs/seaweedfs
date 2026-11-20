package weed_server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestAdminGrpcChecksFollowFilerGuardReload(t *testing.T) {
	cm, err := credential.NewCredentialManager(credential.StoreTypeMemory, nil, "")
	if err != nil {
		t.Fatalf("credential manager: %v", err)
	}
	guard := security.NewGuard(nil, "admin-new", 10, "", 60, "admin-old", "")
	iam := NewIamGrpcServer(cm, guard)
	fs := &FilerServer{filerGuard: guard}
	v := util.GetViper()
	v.Set("jwt.filer_signing.subscribe.key", "subscribe-key")
	t.Cleanup(func() { v.Set("jwt.filer_signing.subscribe.key", "") })

	oldAdmin := ctxWithBearer(string(security.GenJwtForFilerAdmin(security.SigningKey("admin-old"), 60)))
	if _, err := iam.ListUsers(oldAdmin, &iam_pb.ListUsersRequest{}); err != nil {
		t.Fatalf("IAM must accept an admin token signed with the previous key: %v", err)
	}
	if scope, err := fs.metadataSubscriptionScope(oldAdmin, security.MethodSubscribeMetadata); err != nil || scope != nil {
		t.Fatalf("metadata subscription with the previous admin key: scope=%v err=%v", scope, err)
	}

	guard.UpdateSigningKeys("admin-new", 10, "", 60, "", "")
	if _, err := iam.ListUsers(oldAdmin, &iam_pb.ListUsersRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("IAM must refuse the removed previous key: %v", err)
	}
	if _, err := fs.metadataSubscriptionScope(oldAdmin, security.MethodSubscribeMetadata); err == nil {
		t.Fatal("metadata subscription must refuse the removed previous key")
	}
}
