package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/require"
)

func TestObjectLockRoutesUseDedicatedActions(t *testing.T) {
	bindings := handlerActionBindings(t)
	for _, tc := range []struct{ handler, want string }{
		{"GetObjectRetentionHandler", "ACTION_GET_OBJECT_RETENTION"},
		{"PutObjectRetentionHandler", "ACTION_PUT_OBJECT_RETENTION"},
		{"GetObjectLegalHoldHandler", "ACTION_GET_OBJECT_LEGAL_HOLD"},
		{"PutObjectLegalHoldHandler", "ACTION_PUT_OBJECT_LEGAL_HOLD"},
		{"GetObjectLockConfigurationHandler", "ACTION_GET_BUCKET_OBJECT_LOCK_CONFIG"},
		{"PutObjectLockConfigurationHandler", "ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG"},
	} {
		if got := bindings[tc.handler]; got != tc.want {
			t.Errorf("%s is gated on %s, want %s", tc.handler, got, tc.want)
		}
	}
}

func TestGovernanceBypassUsesDedicatedAuthorization(t *testing.T) {
	for _, tc := range []struct {
		name    string
		action  Action
		allowed bool
	}{
		{"dedicated permission", Action(s3_constants.ACTION_BYPASS_GOVERNANCE_RETENTION + ":test-bucket/*"), true},
		{"coarse write", Action(s3_constants.ACTION_WRITE + ":test-bucket/*"), false},
		{"admin", Action(s3_constants.ACTION_ADMIN), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			iam := newTestIAM()
			iam.identities[0].Actions = []Action{tc.action}
			iam.identities[0].Account = &Account{Id: "test-account"}
			s3a := &S3ApiServer{iam: iam}
			req := httptest.NewRequest(http.MethodDelete, "http://localhost:8333/test-bucket/test-object", nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": "test-bucket", "object": "test-object"})
			require.NoError(t, signRawHTTPRequest(context.Background(), req,
				"AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1"))

			if got := s3a.checkGovernanceBypassPermission(req, "test-bucket", "/test-object"); got != tc.allowed {
				t.Errorf("checkGovernanceBypassPermission() = %v, want %v", got, tc.allowed)
			}
		})
	}
}

func TestGovernanceBypassUsesBodyObjectKey(t *testing.T) {
	iam := newTestIAM()
	iam.identities[0].Actions = []Action{
		Action(s3_constants.ACTION_BYPASS_GOVERNANCE_RETENTION + ":test-bucket/allowed/*"),
	}
	iam.identities[0].Account = &Account{Id: "test-account"}
	s3a := &S3ApiServer{iam: iam}

	req := httptest.NewRequest(http.MethodPost, "http://localhost:8333/test-bucket?delete", nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": "test-bucket"})
	req = req.WithContext(s3_constants.SetIdentityInContext(req.Context(), iam.identities[0]))

	if !s3a.checkGovernanceBypassPermission(req, "test-bucket", "allowed/object") {
		t.Fatal("object-scoped bypass grant did not authorize a DeleteObjects body key")
	}
	if s3a.checkGovernanceBypassPermission(req, "test-bucket", "denied/object") {
		t.Fatal("object-scoped bypass grant authorized a key outside its prefix")
	}
}

func TestGovernanceBypassNormalizesBodyObjectKey(t *testing.T) {
	iam := newTestIAM()
	iam.identities[0].Account = &Account{Id: "test-account"}
	s3a := &S3ApiServer{iam: iam}

	req := httptest.NewRequest(http.MethodPost, "http://localhost:8333/test-bucket?delete", nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": "test-bucket"})
	req = req.WithContext(s3_constants.SetIdentityInContext(req.Context(), iam.identities[0]))

	iam.identities[0].Actions = []Action{
		Action(s3_constants.ACTION_BYPASS_GOVERNANCE_RETENTION + ":test-bucket/allowed/o?ject"),
	}
	if !s3a.checkGovernanceBypassPermission(req, "test-bucket", "//allowed//object") {
		t.Fatal("canonical object grant did not authorize the equivalent noncanonical body key")
	}

	iam.identities[0].Actions = []Action{
		Action(s3_constants.ACTION_BYPASS_GOVERNANCE_RETENTION + ":test-bucket/allowed//o?ject"),
	}
	if s3a.checkGovernanceBypassPermission(req, "test-bucket", "//allowed//object") {
		t.Fatal("noncanonical alias grant authorized the canonical mutation target")
	}
}

func TestCoarseReadWriteDoNotGrantObjectLockActions(t *testing.T) {
	identity := &Identity{
		Name: "object-reader-writer",
		Actions: []Action{
			Action(s3_constants.ACTION_READ + ":test-bucket"),
			Action(s3_constants.ACTION_WRITE + ":test-bucket"),
		},
	}

	for _, action := range []string{
		s3_constants.ACTION_GET_OBJECT_RETENTION,
		s3_constants.ACTION_PUT_OBJECT_RETENTION,
		s3_constants.ACTION_GET_OBJECT_LEGAL_HOLD,
		s3_constants.ACTION_PUT_OBJECT_LEGAL_HOLD,
		s3_constants.ACTION_GET_BUCKET_OBJECT_LOCK_CONFIG,
		s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG,
		s3_constants.ACTION_BYPASS_GOVERNANCE_RETENTION,
	} {
		if identity.CanDo(Action(action), "test-bucket", "some/key") {
			t.Errorf("coarse Read/Write unexpectedly granted %s", action)
		}
	}
}

func TestObjectLockActionsRoundTrip(t *testing.T) {
	for _, tc := range []struct{ policyAction, identityAction string }{
		{"GetObjectRetention", s3_constants.ACTION_GET_OBJECT_RETENTION},
		{"PutObjectRetention", s3_constants.ACTION_PUT_OBJECT_RETENTION},
		{"GetObjectLegalHold", s3_constants.ACTION_GET_OBJECT_LEGAL_HOLD},
		{"PutObjectLegalHold", s3_constants.ACTION_PUT_OBJECT_LEGAL_HOLD},
		{"GetBucketObjectLockConfiguration", s3_constants.ACTION_GET_BUCKET_OBJECT_LOCK_CONFIG},
		{"PutBucketObjectLockConfiguration", s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG},
		{"BypassGovernanceRetention", s3_constants.ACTION_BYPASS_GOVERNANCE_RETENTION},
	} {
		t.Run(tc.policyAction, func(t *testing.T) {
			for _, action := range []string{tc.policyAction, "s3:" + tc.policyAction} {
				if got := iamlib.MapToStatementAction(action); got != tc.identityAction {
					t.Errorf("MapToStatementAction(%q) = %q, want %q", action, got, tc.identityAction)
				}
			}
			if got := iamlib.MapToIdentitiesAction(tc.identityAction); got != tc.policyAction {
				t.Errorf("MapToIdentitiesAction(%q) = %q, want %q", tc.identityAction, got, tc.policyAction)
			}
		})
	}
}
