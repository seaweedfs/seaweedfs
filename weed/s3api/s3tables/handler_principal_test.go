package s3tables

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

func TestGetAccountIDRejectsHeaderAdmin(t *testing.T) {
	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(s3_constants.AmzAccountId, s3_constants.AccountAdminId)

	assert.NotEqual(t, s3_constants.AccountAdminId, h.getAccountID(req),
		"a client-supplied account header must not resolve to the admin principal")
}

func TestGetAccountIDHeaderBranchResolvesCaller(t *testing.T) {
	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(s3_constants.AmzAccountId, "alice")

	assert.Equal(t, "alice", h.getAccountID(req))
}

func TestCheckPermissionDenyPolicyBindsNonAdmin(t *testing.T) {
	denyAll := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:DeleteTableBucket"}]}`
	assert.False(t, CheckPermissionWithContext("s3tables:DeleteTableBucket", "mallory", "owner123", denyAll, "arn:aws:s3tables:us-east-1:000000000000:bucket/victim",
		&PolicyContext{DefaultAllow: true}))
}
