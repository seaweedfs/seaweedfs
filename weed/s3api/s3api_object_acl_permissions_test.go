package s3api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/require"
)

// objectACLUpdateFiler records the UpdateEntry request the handler issues so
// tests can assert that denied requests never write object metadata.
type objectACLUpdateFiler struct {
	fakeLookupFiler
	mu     sync.Mutex
	update *filer_pb.UpdateEntryRequest
}

func (f *objectACLUpdateFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.update = req
	return &filer_pb.UpdateEntryResponse{}, nil
}

// TestPutObjectAclPermissions exercises PutObjectAcl authorization end to end
// with signed requests: legacy scoped actions, IAM and bucket policies, canned
// and XML ACLs, retired owners, and objects without stored owner metadata.
func TestPutObjectAclPermissions(t *testing.T) {
	const bucket, object, owner = "acl-bucket", "allowed/image.png", "object-owner"
	const bucketOwner = "bucket-owner"
	xmlACL := func(ownerID, granteeID string) string {
		return fmt.Sprintf(`<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>%s</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>%s</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>`, ownerID, granteeID)
	}
	tests := []struct {
		name          string
		account       string
		action        Action
		acl           string
		policy        string
		retiredOwner  bool
		ownerMetadata string
		body          string
		status        int
	}{
		{name: "bucket-wide write-acp grant", account: owner, action: "WriteAcp:acl-bucket", acl: "private", status: http.StatusOK},
		{name: "prefix write-acp grant", account: owner, action: "WriteAcp:acl-bucket/allowed/*", acl: "public-read", status: http.StatusOK},
		{name: "grant outside prefix denied", account: owner, action: "WriteAcp:acl-bucket/other/*", acl: "private", status: http.StatusForbidden},
		{name: "grant on other bucket denied", account: owner, action: "WriteAcp:other-bucket", acl: "private", status: http.StatusForbidden},
		{name: "read-only grant denied", account: owner, action: "Read:acl-bucket", acl: "private", status: http.StatusForbidden},
		{name: "non-owner denied", account: "other-owner", action: "WriteAcp:acl-bucket", acl: "private", status: http.StatusForbidden},
		{name: "admin sets private", account: AccountAdmin.Id, action: "Admin", acl: "private", status: http.StatusOK},
		{name: "admin sets public-read", account: AccountAdmin.Id, action: "Admin", acl: "public-read", status: http.StatusOK},
		{name: "iam policy allow", account: owner, policy: "iam-allow", acl: "private", status: http.StatusOK},
		{name: "bucket policy allow", account: owner, policy: "bucket-allow", acl: "private", status: http.StatusOK},
		{name: "iam policy explicit deny", account: owner, action: "WriteAcp:acl-bucket", policy: "iam-deny", acl: "private", status: http.StatusForbidden},
		{name: "bucket policy explicit deny", account: owner, action: "WriteAcp:acl-bucket", policy: "bucket-deny", acl: "private", status: http.StatusForbidden},
		{name: "retired owner private", account: AccountAdmin.Id, action: "Admin", acl: "private", retiredOwner: true, status: http.StatusOK},
		{name: "retired owner public-read", account: AccountAdmin.Id, action: "Admin", acl: "public-read", retiredOwner: true, status: http.StatusOK},
		{name: "bucket-owner-read with distinct bucket owner", account: AccountAdmin.Id, action: "Admin", acl: "bucket-owner-read", status: http.StatusOK},
		{name: "bucket-owner-full-control with distinct bucket owner", account: AccountAdmin.Id, action: "Admin", acl: "bucket-owner-full-control", status: http.StatusOK},
		{name: "xml acl preserves retired owner", account: AccountAdmin.Id, action: "Admin", body: xmlACL(owner, owner), retiredOwner: true, status: http.StatusOK},
		{name: "xml acl unknown grantee denied", account: AccountAdmin.Id, action: "Admin", body: xmlACL(owner, "unknown-grantee"), retiredOwner: true, status: http.StatusBadRequest},
		{name: "xml acl owner change denied", account: AccountAdmin.Id, action: "Admin", body: xmlACL("other-owner", owner), status: http.StatusForbidden},
		{name: "nil extended metadata denied", account: owner, action: "WriteAcp:acl-bucket", acl: "private", ownerMetadata: "nil", status: http.StatusForbidden},
		{name: "absent owner denied", account: owner, action: "WriteAcp:acl-bucket", acl: "private", ownerMetadata: "absent", status: http.StatusForbidden},
		{name: "empty owner denied", account: owner, action: "WriteAcp:acl-bucket", acl: "private", ownerMetadata: "empty", status: http.StatusForbidden},
		{name: "ownerless iam policy denied", account: owner, policy: "iam-allow", acl: "private", ownerMetadata: "absent", status: http.StatusForbidden},
		{name: "ownerless bucket policy denied", account: owner, policy: "bucket-allow", acl: "private", ownerMetadata: "absent", status: http.StatusForbidden},
		{name: "admin takes ownerless object private", account: AccountAdmin.Id, action: "Admin", acl: "private", ownerMetadata: "absent", status: http.StatusOK},
		{name: "admin takes ownerless object public-read", account: AccountAdmin.Id, action: "Admin", acl: "public-read", ownerMetadata: "nil", status: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filer := &objectACLUpdateFiler{fakeLookupFiler: fakeLookupFiler{entry: &filer_pb.Entry{
				Name: "image.png", Extended: map[string][]byte{s3_constants.ExtAmzOwnerKey: []byte(owner)},
			}}}
			// Model the ways an object written outside the S3 path (or by an
			// older version) can lack owner metadata; the requester must not
			// be treated as the owner in any of them.
			switch tt.ownerMetadata {
			case "nil":
				filer.entry.Extended = nil
			case "absent":
				delete(filer.entry.Extended, s3_constants.ExtAmzOwnerKey)
			case "empty":
				filer.entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte{}
			}
			s3a := newHeadBucketTestServer(t, filer)
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set(bucket, &BucketConfig{Name: bucket, Ownership: s3_constants.OwnershipObjectWriter, Owner: bucketOwner})
			s3a.iam = NewIdentityAccessManagementWithStore(s3a.option, nil, "memory")
			t.Cleanup(s3a.iam.Shutdown)
			s3a.iam.isAuthEnabled = true
			account := &Account{Id: tt.account, DisplayName: tt.account}
			identity := &Identity{
				Name: "acl-test-user", Account: account, Actions: []Action{tt.action}, IsStatic: true,
				Credentials: []*Credential{{AccessKey: routingTestAccessKey, SecretKey: routingTestSecretKey}},
			}
			if tt.action == "" {
				identity.Actions = nil
			}
			s3a.iam.accessKeyIdent[routingTestAccessKey] = identity
			s3a.iam.nameToIdentity[identity.Name] = identity
			if !tt.retiredOwner {
				s3a.iam.accounts[owner] = &Account{Id: owner, DisplayName: owner}
			}
			s3a.iam.accounts[bucketOwner] = &Account{Id: bucketOwner, DisplayName: bucketOwner}
			s3a.iam.accounts[account.Id] = account
			if tt.policy != "" {
				effect := "Allow"
				if strings.HasSuffix(tt.policy, "deny") {
					effect = "Deny"
				}
				statement := fmt.Sprintf(`{"Effect":%q,"Action":"s3:PutObjectAcl","Resource":"arn:aws:s3:::acl-bucket/allowed/*"}`, effect)
				if strings.HasPrefix(tt.policy, "iam") {
					require.NoError(t, s3a.iam.PutPolicy("acl-policy", `{"Version":"2012-10-17","Statement":[`+statement+`]}`))
					identity.PolicyNames = []string{"acl-policy"}
				} else {
					statement = strings.Replace(statement, `{"Effect":`, `{"Principal":"*","Effect":`, 1)
					s3a.iam.policyEngine = NewBucketPolicyEngine()
					require.NoError(t, s3a.iam.policyEngine.engine.SetBucketPolicy(bucket, `{"Version":"2012-10-17","Statement":[`+statement+`]}`))
				}
			}

			req := httptest.NewRequest(http.MethodPut, "http://s3/"+bucket+"/"+object+"?acl", strings.NewReader(tt.body))
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
			req.Header.Set(s3_constants.AmzCannedAcl, tt.acl)
			signRoutingTestRequest(t, req, tt.body, "s3")
			// The signing helper replaces the body; restore the bodyless shape
			// a real HTTP request has so it is not mistaken for an empty XML ACL.
			if tt.body == "" {
				req.Body = http.NoBody
			}
			rr := httptest.NewRecorder()
			s3a.iam.Auth(s3a.PutObjectAclHandler, s3_constants.ACTION_WRITE_ACP)(rr, req)
			require.Equal(t, tt.status, rr.Code, rr.Body.String())

			filer.mu.Lock()
			update := filer.update
			filer.mu.Unlock()
			if tt.status != http.StatusOK {
				require.Nil(t, update, "denied requests must not write metadata")
				return
			}
			require.NotNil(t, update)
			require.Equal(t, "/buckets/acl-bucket/allowed", update.Directory)
			wantOwner := owner
			if tt.ownerMetadata != "" {
				// Admins keep the existing fallback that establishes them as
				// owner of objects without stored owner metadata.
				wantOwner = tt.account
			}
			require.Equal(t, wantOwner, string(update.Entry.Extended[s3_constants.ExtAmzOwnerKey]))
			grants := GetAcpGrants(update.Entry.Extended)
			wantGrants := 1
			if tt.acl == "public-read" || strings.HasPrefix(tt.acl, "bucket-owner-") {
				wantGrants = 2
			}
			require.Len(t, grants, wantGrants)
			require.Equal(t, wantOwner, aws.StringValue(grants[0].Grantee.ID), "full-control grant must belong to the object owner")
			require.Equal(t, s3_constants.PermissionFullControl, aws.StringValue(grants[0].Permission))
			if tt.acl == "public-read" {
				require.Equal(t, s3_constants.GranteeGroupAllUsers, aws.StringValue(grants[1].Grantee.URI))
				require.Equal(t, s3_constants.PermissionRead, aws.StringValue(grants[1].Permission))
			}
			if strings.HasPrefix(tt.acl, "bucket-owner-") {
				require.Equal(t, bucketOwner, aws.StringValue(grants[1].Grantee.ID))
				permission := s3_constants.PermissionRead
				if tt.acl == "bucket-owner-full-control" {
					permission = s3_constants.PermissionFullControl
				}
				require.Equal(t, permission, aws.StringValue(grants[1].Permission))
			}
		})
	}
}
