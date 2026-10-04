package s3api

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestPutObjectUploadACL exercises signed uploads and inspects the actual filer
// entry, including rejection before any volume allocation or object replacement.
func TestPutObjectUploadACL(t *testing.T) {
	const bucket, object, writer, bucketOwner = "acl-bucket", "allowed/image.png", "upload-writer", "bucket-owner"
	tests := []struct {
		name, acl, grantHeader, grant, ownership, policy, versioning, errorCode  string
		writeOnly, wrongScope, marker, presigned, overwrite, unsigned, streaming bool
		status                                                                   int
	}{
		{name: "default private", status: 200},
		{name: "explicit private", acl: "private", status: 200},
		{name: "public read", acl: "public-read", status: 200},
		{name: "public read write", acl: "public-read-write", status: 200},
		{name: "authenticated read", acl: "authenticated-read", status: 200},
		{name: "custom read", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, status: 200},
		{name: "custom write", grantHeader: s3_constants.AmzAclWrite, grant: `id="bucket-owner"`, status: 200},
		{name: "custom read acp", grantHeader: s3_constants.AmzAclReadAcp, grant: `id="bucket-owner"`, status: 200},
		{name: "custom write acp", grantHeader: s3_constants.AmzAclWriteAcp, grant: `id="bucket-owner"`, status: 200},
		{name: "custom full control", grantHeader: s3_constants.AmzAclFullControl, grant: `id="bucket-owner"`, status: 200},
		{name: "unknown grantee", grantHeader: s3_constants.AmzAclRead, grant: `id="unknown"`, status: 400, errorCode: "InvalidRequest"},
		{name: "unknown canned acl", acl: "invalid", status: 400, errorCode: "InvalidRequest"},
		{name: "conflicting acl headers", acl: "public-read", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, status: 400, errorCode: "InvalidRequest"},
		{name: "bucket owner read", acl: "bucket-owner-read", status: 200},
		{name: "bucket owner full control", acl: "bucket-owner-full-control", status: 200},
		{name: "preferred ownership", acl: "bucket-owner-full-control", ownership: s3_constants.OwnershipBucketOwnerPreferred, status: 200},
		{name: "preferred default private", ownership: s3_constants.OwnershipBucketOwnerPreferred, status: 200},
		{name: "enforced default", ownership: s3_constants.OwnershipBucketOwnerEnforced, status: 200},
		{name: "enforced full control", acl: "bucket-owner-full-control", ownership: s3_constants.OwnershipBucketOwnerEnforced, status: 200},
		{name: "enforced rejects private", acl: "private", ownership: s3_constants.OwnershipBucketOwnerEnforced, status: 400, errorCode: "AccessControlListNotSupported"},
		{name: "enforced rejects public", acl: "public-read", ownership: s3_constants.OwnershipBucketOwnerEnforced, status: 400, errorCode: "AccessControlListNotSupported"},
		{name: "enforced rejects grants", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, ownership: s3_constants.OwnershipBucketOwnerEnforced, status: 400, errorCode: "AccessControlListNotSupported"},
		{name: "write only default", writeOnly: true, status: 200},
		{name: "write only rejects explicit private", acl: "private", writeOnly: true, status: 403, errorCode: "AccessDenied"},
		{name: "write only rejects public", acl: "public-read", writeOnly: true, status: 403, errorCode: "AccessDenied"},
		{name: "write only rejects grants", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, writeOnly: true, status: 403, errorCode: "AccessDenied"},
		{name: "acl permission outside prefix", acl: "public-read", wrongScope: true, status: 403, errorCode: "AccessDenied"},
		{name: "iam allows acl", acl: "public-read", writeOnly: true, policy: "iam-allow", status: 200},
		{name: "iam denies acl", acl: "public-read", policy: "iam-deny", status: 403, errorCode: "AccessDenied"},
		{name: "bucket allows acl", acl: "public-read", writeOnly: true, policy: "bucket-allow", status: 200},
		{name: "bucket denies acl", acl: "public-read", policy: "bucket-deny", status: 403, errorCode: "AccessDenied"},
		{name: "presigned public read", acl: "public-read", presigned: true, status: 200},
		{name: "presigned requires acl permission", acl: "public-read", presigned: true, writeOnly: true, status: 403, errorCode: "AccessDenied"},
		{name: "presigned custom read", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, presigned: true, status: 200},
		{name: "unsigned with authentication disabled", acl: "public-read", unsigned: true, status: 200},
		{name: "streaming unsigned payload", acl: "public-read", streaming: true, status: 200},
		{name: "directory marker", acl: "public-read", marker: true, status: 200},
		{name: "directory marker rejects acl", acl: "public-read", marker: true, writeOnly: true, status: 403, errorCode: "AccessDenied"},
		{name: "suspended version", acl: "public-read", versioning: s3_constants.VersioningSuspended, status: 200},
		{name: "enabled version", acl: "public-read", versioning: s3_constants.VersioningEnabled, status: 200},
		{name: "overwrite resets private", overwrite: true, status: 200},
		{name: "rejected overwrite preserves acl", overwrite: true, acl: "invalid", status: 400, errorCode: "InvalidRequest"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := object
			if tt.marker {
				key = "allowed/folder/"
			}
			volume := startFakeVolumeServer(t)
			filer := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
			s3a := newPutTestServer(t, startFakeFiler(t, filer))
			s3a.iam = NewIdentityAccessManagementWithStore(s3a.option, nil, "memory")
			t.Cleanup(s3a.iam.Shutdown)
			s3a.iam.isAuthEnabled = !tt.unsigned
			account := &Account{Id: writer, DisplayName: writer}
			identity := &Identity{Name: "upload-acl-test", Account: account, IsStatic: true,
				Actions:     []Action{"Write:acl-bucket/allowed/*"},
				Credentials: []*Credential{{AccessKey: routingTestAccessKey, SecretKey: routingTestSecretKey}}}
			if !tt.writeOnly {
				scope := "WriteAcp:acl-bucket/allowed/*"
				if tt.wrongScope {
					scope = "WriteAcp:acl-bucket/other/*"
				}
				identity.Actions = append(identity.Actions, Action(scope))
			}
			s3a.iam.accessKeyIdent[routingTestAccessKey] = identity
			s3a.iam.nameToIdentity[identity.Name] = identity
			s3a.iam.accounts[writer] = account
			s3a.iam.accounts[bucketOwner] = &Account{Id: bucketOwner, DisplayName: bucketOwner}
			ownership := tt.ownership
			if ownership == "" {
				ownership = s3_constants.OwnershipObjectWriter
			}
			bucketEntry := &filer_pb.Entry{Name: bucket, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{},
				Extended: map[string][]byte{s3_constants.ExtAmzOwnerKey: []byte(bucketOwner), s3_constants.ExtOwnershipKey: []byte(ownership)}}
			filer.entries["/buckets/"+bucket] = bucketEntry
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set(bucket, &BucketConfig{Name: bucket, Owner: bucketOwner, Ownership: ownership, Versioning: tt.versioning})
			s3a.bucketRegistry = NewBucketRegistry(s3a)
			s3a.bucketRegistry.LoadBucketMetadata(bucketEntry)
			if tt.versioning == s3_constants.VersioningEnabled {
				filer.entries["/buckets/"+bucket+"/"+key+s3_constants.VersionsFolder] = &filer_pb.Entry{Name: "image.png.versions", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
			}
			var original *filer_pb.Entry
			if tt.overwrite {
				original = &filer_pb.Entry{Name: "image.png", Attributes: &filer_pb.FuseAttributes{}, Extended: map[string][]byte{s3_constants.ExtAmzOwnerKey: []byte(bucketOwner), s3_constants.ExtAmzAclKey: []byte("old-acl")}}
				filer.entries["/buckets/"+bucket+"/"+key] = proto.Clone(original).(*filer_pb.Entry)
			}
			if tt.policy != "" {
				effect := "Allow"
				if strings.HasSuffix(tt.policy, "deny") {
					effect = "Deny"
				}
				statement := fmt.Sprintf(`{"Effect":%q,"Action":"s3:PutObjectAcl","Resource":"arn:aws:s3:::acl-bucket/allowed/*"}`, effect)
				if strings.HasPrefix(tt.policy, "iam") {
					require.NoError(t, s3a.iam.PutPolicy("upload-acl-policy", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::acl-bucket/allowed/*"},`+statement+`]}`))
					identity.PolicyNames = []string{"upload-acl-policy"}
				} else {
					s3a.policyEngine = NewBucketPolicyEngine()
					s3a.iam.policyEngine = s3a.policyEngine
					statement = strings.Replace(statement, `{"Effect":`, `{"Principal":"*","Effect":`, 1)
					require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy(bucket, `{"Version":"2012-10-17","Statement":[`+statement+`]}`))
				}
			}
			body := "uploaded content"
			wireBody := body
			if tt.streaming {
				checksum := crc32.NewIEEE()
				_, err := checksum.Write([]byte(body))
				require.NoError(t, err)
				wireBody = fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\nx-amz-checksum-crc32:%s\r\n\r\n", len(body), body, base64.StdEncoding.EncodeToString(checksum.Sum(nil)))
			}
			req := httptest.NewRequest(http.MethodPut, "http://s3/"+bucket+"/"+key, strings.NewReader(wireBody))
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": key})
			req.Header.Set("Content-Type", "text/plain")
			if tt.acl != "" {
				req.Header.Set(s3_constants.AmzCannedAcl, tt.acl)
			}
			if tt.grantHeader != "" {
				req.Header.Set(tt.grantHeader, tt.grant)
			}
			if tt.streaming {
				req.Header.Set("X-Amz-Content-Sha256", streamingUnsignedPayload)
				req.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
				req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(len(body)))
				req.Header.Set("Content-Encoding", "aws-chunked")
			}
			if tt.unsigned {
				// Disabled authentication uses the admin account, not a caller's
				// forged internal account header.
				req.Header.Set(s3_constants.AmzAccountId, "forged-account")
			} else if tt.presigned {
				// Exercise ACLs in the signed query rather than relying on a
				// particular SDK version's automatic header-hoisting behavior.
				query := req.URL.Query()
				if tt.acl != "" {
					query.Set(s3_constants.AmzCannedAcl, tt.acl)
					req.Header.Del(s3_constants.AmzCannedAcl)
				}
				if tt.grantHeader != "" {
					query.Set(tt.grantHeader, tt.grant)
					req.Header.Del(tt.grantHeader)
				}
				req.URL.RawQuery = query.Encode()
				signer := v4.NewSigner(credentials.NewStaticCredentials(routingTestAccessKey, routingTestSecretKey, ""))
				_, err := signer.Presign(req, strings.NewReader(wireBody), "s3", "us-east-1", time.Minute, time.Now())
				require.NoError(t, err)
			} else {
				signRoutingTestRequest(t, req, wireBody, "s3")
			}
			rr := httptest.NewRecorder()
			s3a.iam.Auth(s3a.PutObjectHandler, s3_constants.ACTION_WRITE)(rr, req)
			require.Equal(t, tt.status, rr.Code, rr.Body.String())
			filer.mu.Lock()
			defer filer.mu.Unlock()
			if tt.status != http.StatusOK {
				require.Contains(t, rr.Body.String(), "<Code>"+tt.errorCode+"</Code>")
				require.Zero(t, filer.nextKey, "rejected ACLs must not allocate chunks")
				require.True(t, proto.Equal(original, filer.entries["/buckets/"+bucket+"/"+strings.TrimSuffix(key, "/")]), "rejected uploads must not replace the object")
				return
			}
			stored := filer.entries["/buckets/"+bucket+"/"+strings.TrimSuffix(key, "/")]
			if tt.versioning == s3_constants.VersioningEnabled {
				versionID := rr.Header().Get("x-amz-version-id")
				require.NotEmpty(t, versionID)
				stored = nil
				for _, entry := range filer.entries {
					if string(entry.Extended[s3_constants.ExtVersionIdKey]) == versionID {
						stored = entry
						break
					}
				}
			}
			require.NotNil(t, stored)
			bodyMD5 := md5.Sum([]byte(body))
			require.Equal(t, bodyMD5[:], stored.Attributes.Md5, "ACL parsing must not consume or alter the upload body")
			wantOwner := writer
			if tt.unsigned {
				wantOwner = AccountAdmin.Id
			}
			if ownership == s3_constants.OwnershipBucketOwnerEnforced || (ownership == s3_constants.OwnershipBucketOwnerPreferred && tt.acl == "bucket-owner-full-control") {
				wantOwner = bucketOwner
			}
			require.Equal(t, wantOwner, string(stored.Extended[s3_constants.ExtAmzOwnerKey]))
			grants := GetAcpGrants(stored.Extended)
			require.NotEmpty(t, grants, "ACL must be persisted in the object create")
			if tt.grantHeader != "" {
				require.Len(t, grants, 1)
				require.Equal(t, bucketOwner, aws.StringValue(grants[0].Grantee.ID))
				wantPermission := map[string]string{
					s3_constants.AmzAclRead:        s3_constants.PermissionRead,
					s3_constants.AmzAclWrite:       s3_constants.PermissionWrite,
					s3_constants.AmzAclReadAcp:     s3_constants.PermissionReadAcp,
					s3_constants.AmzAclWriteAcp:    s3_constants.PermissionWriteAcp,
					s3_constants.AmzAclFullControl: s3_constants.PermissionFullControl,
				}[tt.grantHeader]
				require.Equal(t, wantPermission, aws.StringValue(grants[0].Permission))
			} else {
				require.Equal(t, wantOwner, aws.StringValue(grants[0].Grantee.ID))
				require.Equal(t, s3_constants.PermissionFullControl, aws.StringValue(grants[0].Permission))
				if tt.acl == "public-read" || tt.acl == "public-read-write" || tt.acl == "authenticated-read" {
					wantGrants := 2
					if tt.acl == "public-read-write" {
						wantGrants = 3
					}
					require.Len(t, grants, wantGrants)
					wantGroup := s3_constants.GranteeGroupAllUsers
					if tt.acl == "authenticated-read" {
						wantGroup = s3_constants.GranteeGroupAuthenticatedUsers
					}
					require.Equal(t, wantGroup, aws.StringValue(grants[1].Grantee.URI))
					require.Equal(t, s3_constants.PermissionRead, aws.StringValue(grants[1].Permission))
					if tt.acl == "public-read-write" {
						require.Equal(t, s3_constants.GranteeGroupAllUsers, aws.StringValue(grants[2].Grantee.URI))
						require.Equal(t, s3_constants.PermissionWrite, aws.StringValue(grants[2].Permission))
					}
				} else if ownership == s3_constants.OwnershipObjectWriter && strings.HasPrefix(tt.acl, "bucket-owner-") {
					require.Len(t, grants, 2)
					require.Equal(t, bucketOwner, aws.StringValue(grants[1].Grantee.ID))
					wantPermission := s3_constants.PermissionRead
					if tt.acl == "bucket-owner-full-control" {
						wantPermission = s3_constants.PermissionFullControl
					}
					require.Equal(t, wantPermission, aws.StringValue(grants[1].Permission))
				} else {
					require.Len(t, grants, 1)
				}
			}
		})
	}
}
