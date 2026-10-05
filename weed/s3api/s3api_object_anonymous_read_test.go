package s3api

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Read through the real gRPC lookup and HTTP handlers; replacing an entry does
// not mutate an earlier snapshot, just as a filer metadata update would not.
type anonymousReadFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu        sync.Mutex
	entries   map[string]*filer_pb.Entry
	lookupErr error
}

func (f *anonymousReadFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	entry := f.entries[path.Join(req.Directory, req.Name)]
	if entry == nil {
		return &filer_pb.LookupDirectoryEntryResponse{}, nil
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(entry).(*filer_pb.Entry)}, nil
}

func newAnonymousReadTestServer(t *testing.T) (*S3ApiServer, *anonymousReadFiler) {
	t.Helper()
	f := &anonymousReadFiler{entries: make(map[string]*filer_pb.Entry)}
	s3a := newPutTestServer(t, startFakeFiler(t, f))
	s3a.iam = &IdentityAccessManagement{isAuthEnabled: true}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b", Ownership: s3_constants.OwnershipObjectWriter})
	s3a.policyEngine = NewBucketPolicyEngine()
	s3a.iam.policyEngine = s3a.policyEngine
	return s3a, f
}

func anonymousReadGrant(permission string) []byte {
	grants := []*s3.Grant{{Grantee: &s3.Grantee{Type: aws.String("Group"), URI: aws.String(s3_constants.GranteeGroupAllUsers)}, Permission: aws.String(permission)}}
	data, _ := json.Marshal(grants)
	return data
}

func anonymousReadEntry(acl []byte) *filer_pb.Entry {
	sum := md5.Sum([]byte("hello world"))
	entry := &filer_pb.Entry{Name: "o", Attributes: &filer_pb.FuseAttributes{FileSize: 11, Md5: sum[:], Mtime: 1700000000, Mime: "text/plain"}, Content: []byte("hello world"), Extended: make(map[string][]byte)}
	if acl != nil {
		entry.Extended[s3_constants.ExtAmzAclKey] = acl
	}
	return entry
}

func serveAnonymousRead(s3a *S3ApiServer, method, object, query string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/b/"+object+"?"+query, nil)
	r = mux.SetURLVars(r, map[string]string{"bucket": "b", "object": object})
	for name, value := range headers {
		r.Header.Set(name, value)
	}
	handler := s3a.GetObjectHandler
	if method == http.MethodHead {
		handler = s3a.HeadObjectHandler
	}
	rr := httptest.NewRecorder()
	s3a.AuthWithPublicRead(handler, s3_constants.ACTION_READ)(rr, r)
	return rr
}

func TestAnonymousObjectACLRead(t *testing.T) {
	for _, tc := range []struct {
		name         string
		acl          []byte
		publicBucket bool
		enforced     bool
		want         int
	}{
		{"public read", anonymousReadGrant("READ"), false, false, 200},
		{"public full control", anonymousReadGrant("FULL_CONTROL"), false, false, 200},
		{"write is not read", anonymousReadGrant("WRITE"), false, false, 403},
		{"private", []byte(`[]`), false, false, 403},
		{"missing ACL private bucket", nil, false, false, 403},
		{"legacy public bucket", nil, true, false, 200},
		{"explicit private public bucket", []byte(`[]`), true, false, 403},
		{"malformed public bucket", []byte(`{broken`), true, false, 403},
		{"null grant", []byte(`[null]`), false, false, 403},
		{"wrong grantee type", []byte(`[{"Grantee":{"Type":"CanonicalUser","URI":"http://acs.amazonaws.com/groups/global/AllUsers"},"Permission":"READ"}]`), false, false, 403},
		{"authenticated users are not anonymous", []byte(`[ {"Grantee":{"Type":"Group","URI":"http://acs.amazonaws.com/groups/global/AuthenticatedUsers"},"Permission":"READ"}]`), false, false, 403},
		{"ACL disabled", anonymousReadGrant("READ"), false, true, 403},
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			t.Run(tc.name+"/"+method, func(t *testing.T) {
				s3a, f := newAnonymousReadTestServer(t)
				config, _ := s3a.getBucketConfig("b")
				config.IsPublicRead = tc.publicBucket
				if tc.enforced {
					config.Ownership = s3_constants.OwnershipBucketOwnerEnforced
				}
				f.entries["/buckets/b/o"] = anonymousReadEntry(tc.acl)
				rr := serveAnonymousRead(s3a, method, "o", "", nil)
				require.Equal(t, tc.want, rr.Code, rr.Body.String())
				if tc.want == 200 && method == http.MethodGet {
					require.Equal(t, "hello world", rr.Body.String())
				}
				if tc.want == 403 {
					require.NotContains(t, rr.Body.String(), "hello world")
					require.Empty(t, rr.Header().Get("ETag"))
				}
			})
		}
	}
}

func TestAnonymousObjectACLRangeConditionalAndRevocation(t *testing.T) {
	s3a, f := newAnonymousReadTestServer(t)
	f.entries["/buckets/b/o"] = anonymousReadEntry(anonymousReadGrant("READ"))
	rr := serveAnonymousRead(s3a, http.MethodGet, "o", "", map[string]string{"Range": "bytes=1-4"})
	require.Equal(t, 206, rr.Code, rr.Body.String())
	require.Equal(t, "ello", rr.Body.String())
	require.Equal(t, "bytes 1-4/11", rr.Header().Get("Content-Range"))
	etag := rr.Header().Get("ETag")
	require.NotEmpty(t, etag)
	rr = serveAnonymousRead(s3a, http.MethodGet, "o", "", map[string]string{"If-None-Match": etag})
	require.Equal(t, 304, rr.Code)
	f.mu.Lock()
	f.entries["/buckets/b/o"] = anonymousReadEntry([]byte(`[]`))
	f.mu.Unlock()
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		for _, headers := range []map[string]string{nil, {"Range": "bytes=1-4"}, {"If-None-Match": etag}, {"If-Match": "\"other\""}} {
			rr = serveAnonymousRead(s3a, method, "o", "", headers)
			require.Equal(t, 403, rr.Code, rr.Body.String())
			require.Empty(t, rr.Header().Get("ETag"))
		}
	}
}

func TestAnonymousObjectACLPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, statement string
		acl             []byte
		tag             string
		want            int
	}{
		{"explicit deny", `{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}`, anonymousReadGrant("READ"), "", 403},
		{"unrelated deny", `{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::b/*"}`, anonymousReadGrant("READ"), "", 200},
		{"tag deny matching", `{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*","Condition":{"StringNotEquals":{"s3:ExistingObjectTag/classification":"public"}}}`, anonymousReadGrant("READ"), "private", 403},
		{"tag deny nonmatching", `{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*","Condition":{"StringNotEquals":{"s3:ExistingObjectTag/classification":"public"}}}`, anonymousReadGrant("READ"), "public", 200},
		{"tag allow private ACL", `{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/classification":"public"}}}`, []byte(`[]`), "public", 200},
		{"tag allow nonmatching", `{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/classification":"public"}}}`, []byte(`[]`), "private", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s3a, f := newAnonymousReadTestServer(t)
			entry := anonymousReadEntry(tc.acl)
			entry.Extended[s3_constants.AmzObjectTaggingPrefix+"classification"] = []byte(tc.tag)
			f.entries["/buckets/b/o"] = entry
			require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("b", `{"Version":"2012-10-17","Statement":[`+tc.statement+`]}`))
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				rr := serveAnonymousRead(s3a, method, "o", "", nil)
				require.Equal(t, tc.want, rr.Code, rr.Body.String())
			}
		})
	}
}

func TestAnonymousObjectACLVersionAndDirectory(t *testing.T) {
	s3a, f := newAnonymousReadTestServer(t)
	config, _ := s3a.getBucketConfig("b")
	config.Versioning = "Enabled"
	public := anonymousReadEntry(anonymousReadGrant("READ"))
	public.Extended[s3_constants.ExtVersionIdKey] = []byte("public-version")
	public.Name = "v_public-version"
	private := anonymousReadEntry([]byte(`[]`))
	private.Extended[s3_constants.ExtVersionIdKey] = []byte("private-version")
	f.entries["/buckets/b/o.versions/v_public-version"] = public
	f.entries["/buckets/b/o.versions/v_private-version"] = private
	f.entries["/buckets/b/o"] = anonymousReadEntry([]byte(`[]`))
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		for _, tc := range []struct {
			query string
			want  int
		}{{"versionId=public-version", 200}, {"versionId=private-version", 403}, {"versionId=null", 403}, {"", 403}} {
			rr := serveAnonymousRead(s3a, method, "o", tc.query, nil)
			require.Equal(t, tc.want, rr.Code, rr.Body.String())
			if tc.want == 403 {
				require.Empty(t, rr.Header().Get("x-amz-version-id"))
			}
		}
	}
	require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("b", `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObjectVersion","Resource":"arn:aws:s3:::b/o"}]}`))
	rr := serveAnonymousRead(s3a, http.MethodGet, "o", "versionId=public-version", nil)
	require.Equal(t, 403, rr.Code)
	// The backing file is not an alternative key for bypassing GetObjectVersion policy.
	rr = serveAnonymousRead(s3a, http.MethodGet, "o.versions/v_public-version", "", nil)
	require.Equal(t, 403, rr.Code)
	// A versioned directory key must not return its public null-version marker.
	dir := anonymousReadEntry(anonymousReadGrant("READ"))
	dir.IsDirectory = true
	f.entries["/buckets/b/folder"] = dir
	f.entries["/buckets/b/folder/.versions/v_private-version"] = private
	rr = serveAnonymousRead(s3a, http.MethodGet, "folder/", "versionId=private-version", nil)
	require.Equal(t, 403, rr.Code)
}

func TestAnonymousObjectACLDoesNotReplaceAuthentication(t *testing.T) {
	s3a, f := newAnonymousReadTestServer(t)
	f.entries["/buckets/b/o"] = anonymousReadEntry(anonymousReadGrant("READ"))
	for _, header := range []string{"", "invalid", "AWS4-HMAC-SHA256 broken", "AWS invalid:signature"} {
		rr := serveAnonymousRead(s3a, http.MethodGet, "o", "", map[string]string{"Authorization": header})
		require.NotEqual(t, 200, rr.Code, rr.Body.String())
	}
	for _, query := range []string{"X-Amz-Credential=unknown", "AWSAccessKeyId=unknown"} {
		rr := serveAnonymousRead(s3a, http.MethodGet, "o", query, nil)
		require.NotEqual(t, 200, rr.Code, rr.Body.String())
	}
	// The metadata ACL must never provide a grant for another API operation.
	for _, query := range []string{"acl", "tagging", "attributes", "uploadId=upload", "uploads", "retention", "legal-hold"} {
		called := false
		r := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/b/o?"+query, nil), map[string]string{"bucket": "b", "object": "o"})
		rr := httptest.NewRecorder()
		s3a.AuthWithPublicRead(func(http.ResponseWriter, *http.Request) { called = true }, s3_constants.ACTION_READ)(rr, r)
		require.False(t, called, query)
	}
	// Keep the existing no-auth development mode, including objects without ACLs.
	s3a.iam.isAuthEnabled = false
	f.entries["/buckets/b/o"] = anonymousReadEntry(nil)
	rr := serveAnonymousRead(s3a, http.MethodGet, "o", "", nil)
	require.Equal(t, 200, rr.Code, rr.Body.String())
}

func TestAnonymousObjectACLMetadataFailure(t *testing.T) {
	s3a, f := newAnonymousReadTestServer(t)
	f.lookupErr = status.Error(codes.Internal, "metadata unavailable")
	rr := serveAnonymousRead(s3a, http.MethodGet, "o", "", nil)
	require.NotEqual(t, 200, rr.Code)
	require.NotContains(t, rr.Body.String(), "hello world")
	// Injected internal headers cannot select an authenticated principal.
	f.lookupErr = nil
	f.entries["/buckets/b/o"] = anonymousReadEntry([]byte(`[]`))
	require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("b", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::admin:user/admin"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}`))
	rr = serveAnonymousRead(s3a, http.MethodGet, "o", "", map[string]string{s3_constants.SeaweedFSPrincipalHeader: "arn:aws:iam::admin:user/admin", s3_constants.AmzAccountId: "admin"})
	require.Equal(t, 403, rr.Code)
	require.False(t, strings.Contains(rr.Body.String(), "hello world"))
}

func TestAnonymousObjectACLLegacyOwnership(t *testing.T) {
	s3a, f := newAnonymousReadTestServer(t)
	config, _ := s3a.getBucketConfig("b")
	f.entries["/buckets/b/o"] = anonymousReadEntry(anonymousReadGrant("READ"))
	for _, tc := range []struct {
		ownership string
		want      int
	}{
		{"", 200}, {s3_constants.OwnershipBucketOwnerPreferred, 200},
		{s3_constants.OwnershipBucketOwnerEnforced, 403}, {"invalid", 403},
	} {
		config.Ownership = tc.ownership
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			rr := serveAnonymousRead(s3a, method, "o", "", nil)
			require.Equal(t, tc.want, rr.Code, tc.ownership)
		}
	}
	// A literal user key containing .versions is allowed without internal version metadata.
	config.Ownership = ""
	f.entries["/buckets/b/o.versions/v_literal"] = anonymousReadEntry(anonymousReadGrant("READ"))
	rr := serveAnonymousRead(s3a, http.MethodGet, "o.versions/v_literal", "", nil)
	require.Equal(t, 200, rr.Code, rr.Body.String())
}

func TestAnonymousObjectACLConfiguredIdentity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		public   bool
		actions  []Action
		policy   string
		disabled bool
		want     int
	}{
		{"legacy anonymous permission", false, []Action{s3_constants.ACTION_READ}, "", false, 200},
		{"identity policy allow", false, nil, `{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}`, false, 200},
		{"identity deny beats object ACL", true, nil, `{"Effect":"Deny","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}`, false, 403},
		{"disabled identity cannot grant", false, []Action{s3_constants.ACTION_READ}, "", true, 403},
		{"public ACL does not require identity", true, nil, "", true, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s3a, f := newAnonymousReadTestServer(t)
			acl := []byte(`[]`)
			if tc.public {
				acl = anonymousReadGrant("READ")
			}
			f.entries["/buckets/b/o"] = anonymousReadEntry(acl)
			identity := &Identity{Name: s3_constants.AccountAnonymousId, Actions: tc.actions, Disabled: tc.disabled}
			if tc.policy != "" {
				identity.PolicyNames = []string{"anonymous-policy"}
				s3a.iam.iamPolicyEngine = policy_engine.NewPolicyEngine()
				require.NoError(t, s3a.iam.iamPolicyEngine.SetBucketPolicy("anonymous-policy", `{"Version":"2012-10-17","Statement":[`+tc.policy+`]}`))
			}
			s3a.iam.identityAnonymous = identity
			rr := serveAnonymousRead(s3a, http.MethodGet, "o", "", nil)
			require.Equal(t, tc.want, rr.Code, rr.Body.String())
		})
	}
}

func TestAnonymousObjectACLRemoteCacheReauthorization(t *testing.T) {
	for _, revokeACL := range []bool{true, false} {
		t.Run(map[bool]string{true: "ACL revoked", false: "policy tag changed"}[revokeACL], func(t *testing.T) {
			cached := anonymousReadEntry(anonymousReadGrant("READ"))
			cached.Content = nil
			cached.Chunks = []*filer_pb.FileChunk{{FileId: "1,abc", Size: 11}}
			if revokeACL {
				cached.Extended[s3_constants.ExtAmzAclKey] = []byte(`[]`)
			} else {
				cached.Extended[s3_constants.AmzObjectTaggingPrefix+"classification"] = []byte("private")
			}
			address := startFakeCacheFiler(t, &fakeCacheFiler{cache: func(context.Context, *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
				return &filer_pb.CacheRemoteObjectToLocalClusterResponse{Entry: cached}, nil
			}})
			s3a := newPutTestServer(t, address)
			s3a.iam = &IdentityAccessManagement{isAuthEnabled: true}
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b", Ownership: s3_constants.OwnershipObjectWriter})
			s3a.policyEngine = NewBucketPolicyEngine()
			if !revokeACL {
				require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("b", `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*","Condition":{"StringNotEquals":{"s3:ExistingObjectTag/classification":"public"}}}]}`))
			}
			r := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/b/o", nil), map[string]string{"bucket": "b", "object": "o"})
			r, deferred := s3a.deferAnonymousObjectRead(r, s3_constants.ACTION_READ, "b", "o")
			require.True(t, deferred)
			entry := anonymousReadEntry(anonymousReadGrant("READ"))
			entry.Content = nil
			entry.RemoteEntry = &filer_pb.RemoteEntry{RemoteSize: 11}
			entry.Extended[s3_constants.AmzObjectTaggingPrefix+"classification"] = []byte("public")
			require.Equal(t, s3err.ErrNone, s3a.authorizeAnonymousObjectRead(r, "b", "o", entry.Extended))
			w := httptest.NewRecorder()
			err := s3a.streamFromVolumeServers(w, r, entry, "", "b", "o", "")
			require.Error(t, err)
			require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), "hello world")
		})
	}
}
