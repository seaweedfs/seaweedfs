package s3api

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
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
		signature                                                                string
		query, afterSigning                                                      url.Values
		grantees                                                                 []string
		repeatedGrant                                                            string
		conditionValue                                                           string
		defaultMode                                                              uint32
		unregisteredAccounts                                                     bool
		policyOnly                                                               bool
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
		// Preserve the upstream review fixes and legacy ownership behavior.
		{name: "multi grantee without space", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner",id="upload-writer"`, grantees: []string{bucketOwner, writer}, status: 200},
		{name: "unknown grantee key", grantHeader: s3_constants.AmzAclRead, grant: `account="bucket-owner"`, status: 400, errorCode: "InvalidRequest"},
		{name: "absent ownership default", ownership: "absent", status: 200},
		{name: "absent ownership public read", acl: "public-read", ownership: "absent", status: 200},
		{name: "absent ownership grants", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, ownership: "absent", status: 200},
		{name: "sigv2 ignores unsigned query acl", signature: "v2", afterSigning: url.Values{"X-Amz-Acl": {"public-read"}}, status: 200},
		{name: "external account uploader", acl: "public-read", unregisteredAccounts: true, status: 200},
		{name: "unknown grant type", grantHeader: s3_constants.AmzAclRead, grant: `account="bucket-owner"`, status: 400, errorCode: "InvalidRequest"},
		{name: "mixed unknown grant type", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner",principal="upload-writer"`, status: 400, errorCode: "InvalidRequest"},
		{name: "multiple grants without spaces", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner",id="upload-writer"`, grantees: []string{bucketOwner, writer}, status: 200},
		{name: "repeated grant headers", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, repeatedGrant: `id="upload-writer"`, grantees: []string{bucketOwner, writer}, status: 200},
		{name: "presigned multiple grants", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner",id="upload-writer"`, grantees: []string{bucketOwner, writer}, presigned: true, status: 200},
		{name: "presigned public read write", acl: "public-read-write", presigned: true, status: 200},
		{name: "presigned public version", acl: "public-read", presigned: true, versioning: s3_constants.VersioningEnabled, status: 200},
		{name: "default server mode", defaultMode: 0600, status: 200},
		{name: "enforced default server mode", ownership: s3_constants.OwnershipBucketOwnerEnforced, defaultMode: 0600, status: 200},
		{name: "v2 signed header", signature: "v2", acl: "public-read", status: 200},
		{name: "v2 presigned default", signature: "v2", presigned: true, status: 200},
		{name: "v2 presigned signed header", signature: "v2-header", presigned: true, acl: "public-read", status: 200},
		{name: "v2 unsigned canned query", signature: "v2", presigned: true, acl: "public-read", status: 200},
		{name: "v2 unsigned grant query", signature: "v2", presigned: true, grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, status: 200},
		{name: "v2 tampered mixed case query", signature: "v2", afterSigning: url.Values{"x-AMZ-aCl": {"public-read"}}, status: 200},
		{name: "v2 tampered presigned query", signature: "v2", presigned: true, afterSigning: url.Values{"x-amz-acl": {"public-read"}}, status: 200},
		{name: "v2 empty acl query", signature: "v2", afterSigning: url.Values{"x-amz-acl": {""}}, status: 200},
		{name: "v2 header cannot hide unsigned query", signature: "v2", acl: "private", afterSigning: url.Values{"x-amz-acl": {"public-read"}}, status: 200},
		{name: "v2 fake v4 marker cannot bypass", signature: "v2", afterSigning: url.Values{"x-amz-acl": {"public-read"}, "X-Amz-Credential": {"fake"}}, status: 200},
		{name: "v4 duplicate canned query", presigned: true, query: url.Values{"X-Amz-Acl": {"private", "public-read"}}, afterSigning: url.Values{"X-Amz-Acl": {"public-read", "private"}}, status: 400, errorCode: "InvalidRequest"},
		{name: "v4 case alias query", presigned: true, query: url.Values{"X-Amz-Acl": {"private"}, "x-amz-acl": {"public-read"}}, status: 400, errorCode: "InvalidRequest"},
		{name: "v4 conflicting header query", acl: "private", query: url.Values{"X-Amz-Acl": {"public-read"}}, status: 400, errorCode: "InvalidRequest"},
		{name: "v4 tampered signed query", presigned: true, acl: "private", afterSigning: url.Values{"X-Amz-Acl": {"public-read"}}, status: 403, errorCode: "SignatureDoesNotMatch"},
		{name: "dynamic default writer", unregisteredAccounts: true, status: 200},
		{name: "dynamic public writer", unregisteredAccounts: true, acl: "public-read", status: 200},
		{name: "dynamic bucket owner", unregisteredAccounts: true, acl: "bucket-owner-full-control", status: 200},
		{name: "dynamic preferred owner", unregisteredAccounts: true, acl: "bucket-owner-full-control", ownership: s3_constants.OwnershipBucketOwnerPreferred, status: 200},
		{name: "dynamic enforced default", unregisteredAccounts: true, ownership: s3_constants.OwnershipBucketOwnerEnforced, status: 200},
		{name: "dynamic enforced full control", unregisteredAccounts: true, acl: "bucket-owner-full-control", ownership: s3_constants.OwnershipBucketOwnerEnforced, status: 200},
		{name: "dynamic does not bypass custom validation", unregisteredAccounts: true, grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, status: 400, errorCode: "InvalidRequest"},
		{name: "disabled authentication bucket denies acl", unsigned: true, acl: "public-read", policy: "bucket-deny", status: 403, errorCode: "AccessDenied"},
		{name: "header bucket condition denies acl", acl: "public-read", policy: "bucket-condition-deny", status: 403, errorCode: "AccessDenied"},
		{name: "query bucket condition denies acl", acl: "public-read", presigned: true, policy: "bucket-condition-deny", status: 403, errorCode: "AccessDenied"},
		{name: "query bucket condition denies upload", acl: "public-read", presigned: true, policy: "bucket-put-condition-deny", status: 403, errorCode: "AccessDenied"},
		{name: "query iam condition denies acl", acl: "public-read", presigned: true, policy: "iam-condition-deny", status: 403, errorCode: "AccessDenied"},
		{name: "query iam condition denies upload", acl: "public-read", presigned: true, policy: "iam-put-condition-deny", status: 403, errorCode: "AccessDenied"},
		{name: "query bucket condition allows acl", acl: "public-read", presigned: true, writeOnly: true, policy: "bucket-condition-allow", status: 200},
		{name: "query bucket condition supplies all permissions", acl: "public-read", presigned: true, policyOnly: true, policy: "bucket-all-condition-allow", status: 200},
		{name: "query iam condition supplies all permissions", acl: "public-read", presigned: true, policyOnly: true, policy: "iam-all-condition-allow", status: 200},
		{name: "query grant bucket condition denies", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, presigned: true, policy: "bucket-condition-deny", status: 403, errorCode: "AccessDenied"},
		{name: "query grant iam condition denies", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, presigned: true, policy: "iam-condition-deny", status: 403, errorCode: "AccessDenied"},
		{name: "disabled authentication query condition denies acl", unsigned: true, presigned: true, acl: "public-read", policy: "bucket-condition-deny", status: 403, errorCode: "AccessDenied"},
		{name: "disabled authentication query condition denies upload", unsigned: true, presigned: true, acl: "public-read", policy: "bucket-put-condition-deny", status: 403, errorCode: "AccessDenied"},
		{name: "repeated grants preserve condition deny", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner"`, repeatedGrant: `id="upload-writer"`, policy: "bucket-condition-deny", status: 403, errorCode: "AccessDenied"},
		{name: "single line grants preserve condition deny", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner",id="upload-writer"`, policy: "bucket-condition-deny", conditionValue: `id="upload-writer"`, status: 403, errorCode: "AccessDenied"},
		{name: "presigned grants preserve condition deny", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner",id="upload-writer"`, presigned: true, policy: "bucket-condition-deny", conditionValue: `id="upload-writer"`, status: 403, errorCode: "AccessDenied"},
		{name: "joined grant list preserves condition deny", grantHeader: s3_constants.AmzAclRead, grant: `id="bucket-owner",id="upload-writer"`, policy: "bucket-condition-deny", conditionValue: `id="bucket-owner",id="upload-writer"`, status: 403, errorCode: "AccessDenied"},
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
			s3a.option.DefaultFileMode = tt.defaultMode
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
			if tt.policyOnly {
				identity.Actions = nil
			}
			s3a.iam.accessKeyIdent[routingTestAccessKey] = identity
			s3a.iam.nameToIdentity[identity.Name] = identity
			s3a.iam.accounts[writer] = account
			s3a.iam.accounts[bucketOwner] = &Account{Id: bucketOwner, DisplayName: bucketOwner}
			if tt.unregisteredAccounts {
				// JWT/STS authentication supplies trusted accounts dynamically;
				// their IDs are not registered in the static grantee directory.
				delete(s3a.iam.accounts, writer)
				delete(s3a.iam.accounts, bucketOwner)
			}
			ownership := tt.ownership
			if ownership == "" {
				ownership = s3_constants.OwnershipObjectWriter
			}
			bucketEntry := &filer_pb.Entry{Name: bucket, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{},
				Extended: map[string][]byte{s3_constants.ExtAmzOwnerKey: []byte(bucketOwner)}}
			storedOwnership := ownership
			if ownership == "absent" {
				storedOwnership = ""
			} else {
				bucketEntry.Extended[s3_constants.ExtOwnershipKey] = []byte(ownership)
			}
			filer.entries["/buckets/"+bucket] = bucketEntry
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set(bucket, &BucketConfig{Name: bucket, Owner: bucketOwner, Ownership: storedOwnership, Versioning: tt.versioning})
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
				if strings.Contains(tt.policy, "put-") {
					statement = strings.Replace(statement, `"s3:PutObjectAcl"`, `"s3:PutObject"`, 1)
				} else if strings.Contains(tt.policy, "all-") {
					statement = strings.Replace(statement, `"s3:PutObjectAcl"`, `["s3:PutObject","s3:PutObjectAcl"]`, 1)
				}
				if strings.Contains(tt.policy, "condition") {
					header, value := s3_constants.AmzCannedAcl, tt.acl
					if tt.grantHeader != "" {
						header, value = tt.grantHeader, tt.grant
					}
					if tt.conditionValue != "" {
						value = tt.conditionValue
					}
					condition := fmt.Sprintf(`,"Condition":{"StringEquals":{%q:%q}}}`, "s3:"+strings.ToLower(header), value)
					statement = strings.TrimSuffix(statement, "}") + condition
				}
				if strings.HasPrefix(tt.policy, "iam") {
					statements := statement
					if !tt.policyOnly {
						allowActions := `"s3:PutObject"`
						if strings.Contains(tt.policy, "condition") && effect == "Deny" {
							allowActions = `["s3:PutObject","s3:PutObjectAcl"]`
						}
						statements = `{"Effect":"Allow","Action":` + allowActions + `,"Resource":"arn:aws:s3:::acl-bucket/allowed/*"},` + statement
					}
					require.NoError(t, s3a.iam.PutPolicy("upload-acl-policy", `{"Version":"2012-10-17","Statement":[`+statements+`]}`))
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
				if tt.repeatedGrant != "" {
					req.Header.Add(tt.grantHeader, tt.repeatedGrant)
				}
			}
			req.URL.RawQuery = tt.query.Encode()
			if tt.streaming {
				req.Header.Set("X-Amz-Content-Sha256", streamingUnsignedPayload)
				req.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
				req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(len(body)))
				req.Header.Set("Content-Encoding", "aws-chunked")
			}
			if tt.presigned && tt.signature != "v2-header" {
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
			}
			if tt.unsigned {
				// Disabled authentication uses the admin account, not a caller's
				// forged internal account header.
				req.Header.Set(s3_constants.AmzAccountId, "forged-account")
			}
			if tt.signature != "" {
				cred := &Credential{AccessKey: routingTestAccessKey, SecretKey: routingTestSecretKey}
				if tt.presigned {
					query := req.URL.Query()
					expires := fmt.Sprint(time.Now().Add(time.Minute).Unix())
					query.Set("AWSAccessKeyId", routingTestAccessKey)
					query.Set("Expires", expires)
					query.Set("Signature", preSignatureV2(cred, req.Method, req.URL.EscapedPath(), query.Encode(), req.Header, expires))
					req.URL.RawQuery = query.Encode()
				} else {
					req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
					req.Header.Set("Authorization", signatureV2(cred, req.Method, req.URL.EscapedPath(), req.URL.RawQuery, req.Header))
				}
			} else if tt.presigned && !tt.unsigned {
				signer := v4.NewSigner(credentials.NewStaticCredentials(routingTestAccessKey, routingTestSecretKey, ""))
				_, err := signer.Presign(req, strings.NewReader(wireBody), "s3", "us-east-1", time.Minute, time.Now())
				require.NoError(t, err)
			} else if !tt.unsigned {
				signRoutingTestRequest(t, req, wireBody, "s3")
			}
			if tt.afterSigning != nil {
				query := req.URL.Query()
				for key, values := range tt.afterSigning {
					query[key] = values
				}
				req.URL.RawQuery = query.Encode()
				if tt.errorCode != "SignatureDoesNotMatch" {
					// These attacks preserve a valid signature. Unsigned V2 ACLs
					// must be ignored; ambiguous signed V4 ACLs must be rejected.
					_, code := s3a.iam.AuthenticateRequest(req.Clone(req.Context()))
					require.Equal(t, s3err.ErrNone, code)
				}
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
			wantACL, wantGrantHeader := tt.acl, tt.grantHeader
			if strings.HasPrefix(tt.signature, "v2") && tt.presigned && tt.signature != "v2-header" {
				wantACL, wantGrantHeader = "", ""
			}
			require.NotNil(t, stored)
			if !tt.marker {
				mode := defaultFileMode
				if tt.defaultMode != 0 && wantACL == "" {
					mode = tt.defaultMode
				}
				switch wantACL {
				case "public-read", "authenticated-read", "bucket-owner-read":
					mode = 0644
				case "public-read-write":
					mode = 0666
				}
				require.Equal(t, mode, stored.Attributes.FileMode, "header and signed query ACLs must use the same file mode")
			}
			bodyMD5 := md5.Sum([]byte(body))
			require.Equal(t, bodyMD5[:], stored.Attributes.Md5, "ACL parsing must not consume or alter the upload body")
			wantOwner := writer
			if tt.unsigned {
				wantOwner = AccountAdmin.Id
			}
			if s3_constants.EffectiveOwnership(storedOwnership) == s3_constants.OwnershipBucketOwnerEnforced || (ownership == s3_constants.OwnershipBucketOwnerPreferred && wantACL == "bucket-owner-full-control") {
				wantOwner = bucketOwner
			}
			require.Equal(t, wantOwner, string(stored.Extended[s3_constants.ExtAmzOwnerKey]))
			grants := GetAcpGrants(stored.Extended)
			require.NotEmpty(t, grants, "ACL must be persisted in the object create")
			if wantGrantHeader != "" {
				wantGrantees := tt.grantees
				if wantGrantees == nil {
					wantGrantees = []string{bucketOwner}
				}
				require.Len(t, grants, len(wantGrantees))
				wantPermission := map[string]string{
					s3_constants.AmzAclRead:        s3_constants.PermissionRead,
					s3_constants.AmzAclWrite:       s3_constants.PermissionWrite,
					s3_constants.AmzAclReadAcp:     s3_constants.PermissionReadAcp,
					s3_constants.AmzAclWriteAcp:    s3_constants.PermissionWriteAcp,
					s3_constants.AmzAclFullControl: s3_constants.PermissionFullControl,
				}[wantGrantHeader]
				for i, grantee := range wantGrantees {
					require.Equal(t, grantee, aws.StringValue(grants[i].Grantee.ID))
					require.Equal(t, wantPermission, aws.StringValue(grants[i].Permission))
				}
			} else {
				require.Equal(t, wantOwner, aws.StringValue(grants[0].Grantee.ID))
				require.Equal(t, s3_constants.PermissionFullControl, aws.StringValue(grants[0].Permission))
				if wantACL == "public-read" || wantACL == "public-read-write" || wantACL == "authenticated-read" {
					wantGrants := 2
					if wantACL == "public-read-write" {
						wantGrants = 3
					}
					require.Len(t, grants, wantGrants)
					wantGroup := s3_constants.GranteeGroupAllUsers
					if wantACL == "authenticated-read" {
						wantGroup = s3_constants.GranteeGroupAuthenticatedUsers
					}
					require.Equal(t, wantGroup, aws.StringValue(grants[1].Grantee.URI))
					require.Equal(t, s3_constants.PermissionRead, aws.StringValue(grants[1].Permission))
					if wantACL == "public-read-write" {
						require.Equal(t, s3_constants.GranteeGroupAllUsers, aws.StringValue(grants[2].Grantee.URI))
						require.Equal(t, s3_constants.PermissionWrite, aws.StringValue(grants[2].Permission))
					}
				} else if ownership == s3_constants.OwnershipObjectWriter && strings.HasPrefix(wantACL, "bucket-owner-") {
					require.Len(t, grants, 2)
					require.Equal(t, bucketOwner, aws.StringValue(grants[1].Grantee.ID))
					wantPermission := s3_constants.PermissionRead
					if wantACL == "bucket-owner-full-control" {
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

// TestPutObjectACLPolicyScope ensures query normalization cannot alter other
// operations or the original signed request passed to the upload handler.
func TestPutObjectACLPolicyScope(t *testing.T) {
	tests := []struct {
		name, method, object, subresource string
		action                            Action
		copy                              bool
		wantACL                           string
	}{
		{name: "upload", method: http.MethodPut, object: "key", action: s3_constants.ACTION_WRITE, wantACL: "public-read"},
		{name: "upload acl authorization", method: http.MethodPut, object: "key", action: s3_constants.ACTION_WRITE_ACP, wantACL: "public-read"},
		{name: "bucket", method: http.MethodPut, action: s3_constants.ACTION_WRITE},
		{name: "post form", method: http.MethodPost, object: "key", action: s3_constants.ACTION_WRITE},
		{name: "copy", method: http.MethodPut, object: "key", action: s3_constants.ACTION_WRITE, copy: true},
		{name: "multipart part", method: http.MethodPut, object: "key", subresource: "uploadId=upload&partNumber=1", action: s3_constants.ACTION_WRITE},
		{name: "standalone acl", method: http.MethodPut, object: "key", subresource: "acl=", action: s3_constants.ACTION_WRITE_ACP},
		{name: "tagging", method: http.MethodPut, object: "key", subresource: "tagging=", action: s3_constants.ACTION_WRITE},
		{name: "retention", method: http.MethodPut, object: "key", subresource: "retention=", action: s3_constants.ACTION_WRITE},
		{name: "other service", method: http.MethodPut, object: "key", action: "iam:CreateUser"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "http://s3/bucket/"+tt.object+"?x-amz-acl=public-read&"+tt.subresource, nil)
			if tt.copy {
				req.Header.Set("X-Amz-Copy-Source", "/source/key")
			}
			policyRequest, code := putObjectACLPolicyRequest(req, tt.action, "bucket", tt.object)
			require.Equal(t, s3err.ErrNone, code)
			require.Equal(t, tt.wantACL, policyRequest.Header.Get(s3_constants.AmzCannedAcl))
			require.Empty(t, req.Header.Get(s3_constants.AmzCannedAcl), "normalization must preserve signed headers")
		})
	}
}
