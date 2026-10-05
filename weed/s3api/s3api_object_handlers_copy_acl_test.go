package s3api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// copyACLFiler records both ordinary creates and the metadata-only self-copy
// transaction, so tests inspect the actual entry sent to the filer.
type copyACLFiler struct {
	*ambiguousPutFiler
	transactions []*filer_pb.ObjectTransactionRequest
}

// These fixtures have no prior destination versions to list during a null write.
func (f *copyACLFiler) ListEntries(_ *filer_pb.ListEntriesRequest, _ filer_pb.SeaweedFiler_ListEntriesServer) error {
	return nil
}

func (f *copyACLFiler) ObjectTransaction(_ context.Context, req *filer_pb.ObjectTransactionRequest) (*filer_pb.ObjectTransactionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transactions = append(f.transactions, proto.Clone(req).(*filer_pb.ObjectTransactionRequest))
	for _, mutation := range req.Mutations {
		if mutation.Type != filer_pb.ObjectMutation_PATCH_EXTENDED {
			return &filer_pb.ObjectTransactionResponse{Error: "unexpected mutation"}, nil
		}
		entry := f.entries[mutation.Directory+"/"+mutation.Name]
		if entry == nil {
			return &filer_pb.ObjectTransactionResponse{Error: "missing entry"}, nil
		}
		for _, key := range mutation.DeleteExtended {
			delete(entry.Extended, key)
		}
		for key, value := range mutation.SetExtended {
			entry.Extended[key] = append([]byte(nil), value...)
		}
	}
	return &filer_pb.ObjectTransactionResponse{}, nil
}

// TestCopyObjectDestinationACL uses authenticated CopyObject requests to verify
// that source grants never become destination grants, including self-copies.
func TestCopyObjectDestinationACL(t *testing.T) {
	const bucket, writer, bucketOwner, sourceOwner = "copy-acl", "copy-writer", "bucket-owner", "source-owner"
	const object, sourceObject = "destination", "source"
	const versionID = "672a75d526ef29c79fe6b5680cad4a0d"
	tests := []struct {
		name, acl, grantHeader, grant, ownership, versioning, denyAction, errorCode      string
		self, routed, replace, presigned, v2, noRead, noWrite, noWriteACP, sourceVersion bool
		query                                                                            url.Values
		sourceMode                                                                       uint32
		mime                                                                             string
		wantPatch                                                                        bool
	}{
		{name: "default private"},
		{name: "replace metadata also defaults private", replace: true},
		{name: "explicit private", acl: "private"},
		{name: "explicit public read", acl: "public-read"},
		{name: "signed query public read", acl: "public-read", presigned: true},
		{name: "custom read", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`},
		{name: "custom full control", grantHeader: s3_constants.AmzAclFullControl, grant: `id="bucket-owner"`},
		{name: "custom read acp", grantHeader: s3_constants.AmzAclReadAcp, grant: `id="bucket-owner"`},
		{name: "custom write acp", grantHeader: s3_constants.AmzAclWriteAcp, grant: `id="bucket-owner"`, presigned: true},
		{name: "self copy bootstrap", self: true, replace: true},
		{name: "self copy routed private", self: true, replace: true, routed: true, wantPatch: true},
		{name: "self copy routed explicit public", self: true, replace: true, routed: true, acl: "public-read", sourceMode: 0644, wantPatch: true},
		{name: "self copy routed custom", self: true, replace: true, routed: true, grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, wantPatch: true},
		{name: "self copy changes file mode", self: true, replace: true, routed: true, sourceMode: 0644},
		{name: "self copy changes mime", self: true, replace: true, routed: true, mime: "image/png"},
		{name: "versioned destination", versioning: s3_constants.VersioningEnabled},
		{name: "versioned public destination", versioning: s3_constants.VersioningEnabled, acl: "public-read"},
		{name: "suspended destination", versioning: s3_constants.VersioningSuspended},
		{name: "specific source version", sourceVersion: true},
		{name: "versioned self copy", versioning: s3_constants.VersioningEnabled, sourceVersion: true, self: true},
		{name: "preferred bucket owner", ownership: s3_constants.OwnershipBucketOwnerPreferred, acl: "bucket-owner-full-control"},
		{name: "enforced bucket owner", ownership: s3_constants.OwnershipBucketOwnerEnforced},
		{name: "enforced accepts full control", ownership: s3_constants.OwnershipBucketOwnerEnforced, acl: "bucket-owner-full-control"},
		{name: "enforced rejects public", ownership: s3_constants.OwnershipBucketOwnerEnforced, acl: "public-read", errorCode: "AccessControlListNotSupported"},
		{name: "default needs no write acp", noWriteACP: true},
		{name: "public requires write acp", acl: "public-read", noWriteACP: true, errorCode: "AccessDenied"},
		{name: "custom requires write acp", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, noWriteACP: true, errorCode: "AccessDenied"},
		{name: "requires source read", noRead: true, errorCode: "AccessDenied"},
		{name: "requires destination write", noWrite: true, errorCode: "AccessDenied"},
		{name: "query put deny", acl: "public-read", presigned: true, denyAction: "s3:PutObject", errorCode: "AccessDenied"},
		{name: "query acl deny", acl: "public-read", presigned: true, denyAction: "s3:PutObjectAcl", errorCode: "AccessDenied"},
		{name: "query custom put deny", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, presigned: true, denyAction: "s3:PutObject", errorCode: "AccessDenied"},
		{name: "query custom acl deny", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, presigned: true, denyAction: "s3:PutObjectAcl", errorCode: "AccessDenied"},
		{name: "invalid canned", acl: "invalid", errorCode: "InvalidRequest"},
		{name: "conflicting canned and grants", acl: "private", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, errorCode: "InvalidRequest"},
		{name: "duplicate signed query", query: url.Values{"x-amz-acl": {"private", "public-read"}}, presigned: true, errorCode: "InvalidRequest"},
		{name: "v2 unsigned query ignored", v2: true, query: url.Values{"x-amz-acl": {"public-read"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filer := &copyACLFiler{ambiguousPutFiler: &ambiguousPutFiler{apply: true, entries: map[string]*filer_pb.Entry{}}}
			address := startFakeFiler(t, filer)
			s3a := newPutTestServer(t, address)
			s3a.iam = NewIdentityAccessManagementWithStore(s3a.option, nil, "memory")
			t.Cleanup(s3a.iam.Shutdown)
			s3a.iam.isAuthEnabled = true
			account := &Account{Id: writer, DisplayName: writer}
			identity := &Identity{Name: "copy-acl-test", Account: account, IsStatic: true,
				Credentials: []*Credential{{AccessKey: routingTestAccessKey, SecretKey: routingTestSecretKey}}}
			for action, enabled := range map[Action]bool{"Read:" + bucket: !tt.noRead, "Write:" + bucket: !tt.noWrite, "WriteAcp:" + bucket: !tt.noWriteACP} {
				if enabled {
					identity.Actions = append(identity.Actions, action)
				}
			}
			s3a.iam.accessKeyIdent[routingTestAccessKey] = identity
			s3a.iam.nameToIdentity[identity.Name] = identity
			s3a.iam.accounts[writer] = account
			s3a.iam.accounts[bucketOwner] = &Account{Id: bucketOwner, DisplayName: bucketOwner}
			ownership := tt.ownership
			if ownership == "" {
				ownership = s3_constants.OwnershipObjectWriter
			}
			bucketEntry := &filer_pb.Entry{Name: bucket, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}, Extended: map[string][]byte{
				s3_constants.ExtAmzOwnerKey: []byte(bucketOwner), s3_constants.ExtOwnershipKey: []byte(ownership),
			}}
			filer.entries["/buckets/"+bucket] = bucketEntry
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set(bucket, &BucketConfig{Name: bucket, Owner: bucketOwner, Ownership: ownership, Versioning: tt.versioning})
			s3a.bucketRegistry = NewBucketRegistry(s3a)
			s3a.bucketRegistry.LoadBucketMetadata(bucketEntry)
			mode := tt.sourceMode
			if mode == 0 {
				mode = defaultFileMode
			}
			source := &filer_pb.Entry{Name: sourceObject, Attributes: &filer_pb.FuseAttributes{FileMode: mode, FileSize: 4, Mime: "text/plain"}, Content: []byte("data"),
				Extended: map[string][]byte{"X-Amz-Meta-Source": []byte("kept"), "internal-other": []byte("retained")}}
			require.Equal(t, s3err.ErrNone, AssembleEntryWithAcp(source, sourceOwner, []*s3.Grant{
				{Grantee: &s3.Grantee{Type: aws.String(s3_constants.GrantTypeCanonicalUser), ID: aws.String(sourceOwner)}, Permission: aws.String(s3_constants.PermissionFullControl)},
				{Grantee: &s3.Grantee{Type: aws.String(s3_constants.GrantTypeGroup), URI: aws.String(s3_constants.GranteeGroupAllUsers)}, Permission: aws.String(s3_constants.PermissionRead)},
			}))
			key := object
			if tt.self {
				key = sourceObject
			}
			sourcePath := "/buckets/" + bucket + "/" + sourceObject
			copySource := "/" + bucket + "/" + sourceObject
			if tt.sourceVersion {
				source.Name = s3a.getVersionFileName(versionID)
				source.Extended[s3_constants.ExtVersionIdKey] = []byte(versionID)
				sourcePath += s3_constants.VersionsFolder + "/" + source.Name
				copySource += "?versionId=" + versionID
			}
			filer.entries[sourcePath] = source
			originalSource := proto.Clone(source).(*filer_pb.Entry)
			if tt.versioning != "" || tt.sourceVersion {
				filer.entries["/buckets/"+bucket+"/"+key+s3_constants.VersionsFolder] = &filer_pb.Entry{Name: key + s3_constants.VersionsFolder, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
			}
			if tt.routed {
				s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, address)
				s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{address}, 1)
			}
			if tt.denyAction != "" {
				s3a.policyEngine = NewBucketPolicyEngine()
				s3a.iam.policyEngine = s3a.policyEngine
				conditionKey, conditionValue := "s3:x-amz-acl", "public-read"
				if tt.grantHeader != "" {
					conditionKey, conditionValue = "s3:"+strings.ToLower(tt.grantHeader), tt.grant
				}
				policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Principal":"*","Effect":"Deny","Action":%q,"Resource":"arn:aws:s3:::copy-acl/*","Condition":{"StringEquals":{%q:%q}}}]}`, tt.denyAction, conditionKey, conditionValue)
				require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy(bucket, policy))
			}
			req := httptest.NewRequest(http.MethodPut, "http://s3/"+bucket+"/"+key, nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": key})
			req.Header.Set("X-Amz-Copy-Source", copySource)
			// Forged internal headers must not influence the new owner or grants.
			req.Header.Set(s3_constants.ExtAmzOwnerKey, "forged-owner")
			req.Header.Set(s3_constants.ExtAmzAclKey, string(source.Extended[s3_constants.ExtAmzAclKey]))
			if tt.replace {
				req.Header.Set(s3_constants.AmzUserMetaDirective, "REPLACE")
				req.Header.Set("X-Amz-Meta-Destination", "new")
				req.Header.Set("Content-Type", "text/plain")
			}
			if tt.mime != "" {
				req.Header.Set("Content-Type", tt.mime)
			}
			if tt.acl != "" {
				req.Header.Set(s3_constants.AmzCannedAcl, tt.acl)
			}
			if tt.grantHeader != "" {
				req.Header.Set(tt.grantHeader, tt.grant)
			}
			req.URL.RawQuery = tt.query.Encode()
			if tt.presigned {
				query := req.URL.Query()
				for _, header := range []string{s3_constants.AmzCannedAcl, tt.grantHeader} {
					if value := req.Header.Get(header); value != "" {
						query.Set(header, value)
						req.Header.Del(header)
					}
				}
				req.URL.RawQuery = query.Encode()
				signer := v4.NewSigner(credentials.NewStaticCredentials(routingTestAccessKey, routingTestSecretKey, ""))
				_, err := signer.Presign(req, nil, "s3", "us-east-1", time.Minute, time.Now())
				require.NoError(t, err)
			} else if tt.v2 {
				req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
				req.Header.Set("Authorization", signatureV2(&Credential{AccessKey: routingTestAccessKey, SecretKey: routingTestSecretKey}, req.Method, req.URL.EscapedPath(), req.URL.RawQuery, req.Header))
			} else {
				signRoutingTestRequest(t, req, "", "s3")
			}
			rr := httptest.NewRecorder()
			s3a.iam.Auth(s3a.CopyObjectHandler, s3_constants.ACTION_WRITE)(rr, req)
			filer.mu.Lock()
			defer filer.mu.Unlock()
			if tt.errorCode != "" {
				require.Contains(t, rr.Body.String(), "<Code>"+tt.errorCode+"</Code>")
				require.NotEqual(t, http.StatusOK, rr.Code)
				require.Empty(t, filer.transactions, "rejected copies must not issue mutations")
				require.Nil(t, filer.entries["/buckets/"+bucket+"/"+object])
				require.True(t, proto.Equal(originalSource, filer.entries[sourcePath]), "rejected copies must preserve the source")
				return
			}
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			storedPath := "/buckets/" + bucket + "/" + key
			if tt.versioning == s3_constants.VersioningEnabled {
				newVersion := rr.Header().Get("X-Amz-Version-Id")
				require.NotEmpty(t, newVersion)
				storedPath += s3_constants.VersionsFolder + "/" + s3a.getVersionFileName(newVersion)
			}
			stored := filer.entries[storedPath]
			require.NotNil(t, stored)
			require.Equal(t, []byte("data"), stored.Content)
			require.Equal(t, []byte("retained"), stored.Extended["internal-other"])
			if tt.replace {
				require.NotContains(t, stored.Extended, "X-Amz-Meta-Source")
				require.Equal(t, []byte("new"), stored.Extended["X-Amz-Meta-Destination"])
			} else {
				require.Equal(t, []byte("kept"), stored.Extended["X-Amz-Meta-Source"])
			}
			wantOwner := writer
			if tt.ownership == s3_constants.OwnershipBucketOwnerEnforced || (tt.ownership == s3_constants.OwnershipBucketOwnerPreferred && tt.acl == "bucket-owner-full-control") {
				wantOwner = bucketOwner
			}
			require.Equal(t, wantOwner, string(stored.Extended[s3_constants.ExtAmzOwnerKey]))
			grants := GetAcpGrants(stored.Extended)
			ownerFullControl := false
			publicRead := false
			customPermission := map[string]string{s3_constants.AmzAclRead: s3_constants.PermissionRead, s3_constants.AmzAclFullControl: s3_constants.PermissionFullControl, s3_constants.AmzAclReadAcp: s3_constants.PermissionReadAcp, s3_constants.AmzAclWriteAcp: s3_constants.PermissionWriteAcp}[tt.grantHeader]
			for _, grant := range grants {
				require.NotEqual(t, sourceOwner, aws.StringValue(grant.Grantee.ID), "source owner grants must not transfer")
				ownerFullControl = ownerFullControl || (aws.StringValue(grant.Grantee.ID) == wantOwner && aws.StringValue(grant.Permission) == s3_constants.PermissionFullControl)
				publicRead = publicRead || (aws.StringValue(grant.Grantee.URI) == s3_constants.GranteeGroupAllUsers && aws.StringValue(grant.Permission) == s3_constants.PermissionRead)
			}
			require.True(t, ownerFullControl)
			require.Equal(t, tt.acl == "public-read", publicRead)
			if customPermission != "" {
				require.Len(t, grants, 2)
				require.Equal(t, bucketOwner, aws.StringValue(grants[0].Grantee.ID))
				require.Equal(t, customPermission, aws.StringValue(grants[0].Permission))
			} else if tt.acl == "" || tt.acl == "private" {
				require.Len(t, grants, 1, "private copies must not inherit any source grants")
			}
			wantMode := defaultFileMode
			if tt.acl == "public-read" {
				wantMode = 0644
			}
			require.Equal(t, wantMode, stored.Attributes.FileMode)
			if tt.mime != "" {
				require.Equal(t, tt.mime, stored.Attributes.Mime)
			}
			if tt.wantPatch {
				require.Len(t, filer.transactions, 1)
				patch := filer.transactions[0].Mutations[0]
				require.Equal(t, filer_pb.ObjectMutation_PATCH_EXTENDED, patch.Type)
				require.Equal(t, stored.Extended[s3_constants.ExtAmzOwnerKey], patch.SetExtended[s3_constants.ExtAmzOwnerKey])
				require.Equal(t, stored.Extended[s3_constants.ExtAmzAclKey], patch.SetExtended[s3_constants.ExtAmzAclKey])
			} else {
				require.Empty(t, filer.transactions)
			}
			if !tt.self || tt.sourceVersion {
				require.True(t, proto.Equal(originalSource, filer.entries[sourcePath]), "copy must preserve its source ACL and content")
			}
		})
	}
}
