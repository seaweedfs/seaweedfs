package s3api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetListBucketsArgs(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		wantMaxBuckets int
		wantPrefix     string
		wantStartAfter string
		wantErr        s3err.ErrorCode
	}{
		{name: "defaults", query: "", wantMaxBuckets: maxBucketsPerPage, wantErr: s3err.ErrNone},
		{name: "max-buckets", query: "max-buckets=25", wantMaxBuckets: 25, wantErr: s3err.ErrNone},
		{name: "max-buckets upper bound", query: "max-buckets=10000", wantMaxBuckets: 10000, wantErr: s3err.ErrNone},
		{name: "max-buckets zero", query: "max-buckets=0", wantErr: s3err.ErrInvalidMaxBuckets},
		{name: "max-buckets negative", query: "max-buckets=-1", wantErr: s3err.ErrInvalidMaxBuckets},
		{name: "max-buckets too large", query: "max-buckets=10001", wantErr: s3err.ErrInvalidMaxBuckets},
		{name: "max-buckets not a number", query: "max-buckets=abc", wantErr: s3err.ErrInvalidMaxBuckets},
		{name: "prefix", query: "prefix=team-", wantMaxBuckets: maxBucketsPerPage, wantPrefix: "team-", wantErr: s3err.ErrNone},
		{name: "continuation token", query: "continuation-token=" + url.QueryEscape(encodeContinuationToken("bucket-42")),
			wantMaxBuckets: maxBucketsPerPage, wantStartAfter: "bucket-42", wantErr: s3err.ErrNone},
		{name: "bad continuation token", query: "continuation-token=%25%25", wantErr: s3err.ErrInvalidContinuationToken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values, err := url.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("parse query: %v", err)
			}
			maxBuckets, prefix, startAfter, errCode := getListBucketsArgs(values)
			if errCode != tt.wantErr {
				t.Fatalf("errCode = %v, want %v", errCode, tt.wantErr)
			}
			if errCode != s3err.ErrNone {
				return
			}
			if maxBuckets != tt.wantMaxBuckets {
				t.Errorf("maxBuckets = %d, want %d", maxBuckets, tt.wantMaxBuckets)
			}
			if prefix != tt.wantPrefix {
				t.Errorf("prefix = %q, want %q", prefix, tt.wantPrefix)
			}
			if startAfter != tt.wantStartAfter {
				t.Errorf("startAfter = %q, want %q", startAfter, tt.wantStartAfter)
			}
		})
	}
}

func TestCanListBucketsFromOwnerIndex(t *testing.T) {
	iam := &IdentityAccessManagement{}
	r := func() *http.Request {
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		return req
	}

	tests := []struct {
		name        string
		identity    *Identity
		wantOk      bool
		wantGranted []string
	}{
		{name: "nil identity scans", identity: nil, wantOk: false},
		{name: "admin scans", identity: &Identity{Name: "root", Actions: []Action{"Admin"}}, wantOk: false},
		{name: "bare List scans", identity: &Identity{Name: "u", Actions: []Action{"List"}}, wantOk: false},
		{name: "wildcard scans", identity: &Identity{Name: "u", Actions: []Action{"List:team-*"}}, wantOk: false},
		{name: "no actions is owned-only", identity: &Identity{Name: "u"}, wantOk: true},
		{name: "attached policy with no loaded document scans", identity: &Identity{Name: "u", Actions: []Action{"List:b1"}, PolicyNames: []string{"p"}}, wantOk: false},
		{name: "named grants enumerate", identity: &Identity{Name: "u", Actions: []Action{"Read:b1", "List:b2", "Admin:b3/prefix", "Write"}},
			wantOk: true, wantGranted: []string{"b1", "b2", "b3"}},
		// grants come back in action order; resolveGrantedBuckets sorts and dedups
		{name: "unsorted grants keep action order", identity: &Identity{Name: "u", Actions: []Action{"List:c1", "List:a1", "List:b1"}},
			wantOk: true, wantGranted: []string{"c1", "a1", "b1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, granted := iam.canListBucketsFromOwnerIndex(r(), tt.identity)
			if ok != tt.wantOk {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOk)
			}
			if strings.Join(granted, ",") != strings.Join(tt.wantGranted, ",") {
				t.Errorf("granted = %v, want %v", granted, tt.wantGranted)
			}
		})
	}

	// A session token carries policies only the IAM integration can see.
	t.Run("session token scans", func(t *testing.T) {
		integrated := &IdentityAccessManagement{iamIntegration: &S3IAMIntegration{}}
		req := r()
		req.Header.Set("X-Amz-Security-Token", "tok")
		ok, granted := integrated.canListBucketsFromOwnerIndex(req, &Identity{Name: "u", Actions: []Action{"List:b1"}})
		if ok || granted != nil {
			t.Errorf("ok = %v granted = %v, want scan", ok, granted)
		}
	})
}

// An identity authorized by an attached IAM policy is listed from the buckets
// its policy names, and scanned for whenever the policy can reach a bucket it
// does not name.
func TestCanListBucketsFromOwnerIndexAttachedPolicy(t *testing.T) {
	// The policy from the issue report: List on one named bucket, plus the
	// account-wide ListAllMyBuckets that names no bucket at all.
	const namedBucket = `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["s3:GetBucketLocation","s3:ListBucket"],"Resource":["arn:aws:s3:::example-bucket"]},
		{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::example-bucket/*"]},
		{"Effect":"Allow","Action":["s3:ListAllMyBuckets"],"Resource":"*"}]}`
	const wildcardResource = `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::team-*"]}]}`
	const notResource = `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["s3:ListBucket"],"NotResource":["arn:aws:s3:::secret"]}]}`
	const allActionsOneBucket = `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::b1","arn:aws:s3:::b1/*"]}]}`
	const denyOne = `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::b1","arn:aws:s3:::b2"]},
		{"Effect":"Deny","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::b2"]}]}`
	const groupPolicy = `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::shared"]}]}`
	// A variable resolves per request, naming a bucket this cannot predict.
	const resourceVariable = `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::home-${aws:username}"]}]}`
	// A variable action may resolve to ListBucket, so the bucket it names counts.
	const actionVariable = `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["s3:${aws:username}"],"Resource":["arn:aws:s3:::b9"]}]}`
	// The IAM authorizer matches action names case-insensitively, so these
	// grant List and their buckets have to be listed.
	const mixedCaseAction = `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["S3:LISTBUCKET"],"Resource":["arn:aws:s3:::b7"]}]}`
	const mixedCaseWildcardAction = `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["S3:*"],"Resource":["arn:aws:s3:::b8"]}]}`

	engine := policy_engine.NewPolicyEngine()
	for name, document := range map[string]string{
		"named": namedBucket, "wildcard": wildcardResource, "not-resource": notResource,
		"all-actions": allActionsOneBucket, "deny-one": denyOne, "group": groupPolicy,
		"resource-variable": resourceVariable, "action-variable": actionVariable,
		"mixed-case": mixedCaseAction, "mixed-case-wildcard": mixedCaseWildcardAction,
	} {
		if err := engine.SetBucketPolicy(name, document); err != nil {
			t.Fatalf("load policy %s: %v", name, err)
		}
	}
	iam := &IdentityAccessManagement{
		iamPolicyEngine: engine,
		groups: map[string]*iam_pb.Group{
			"team":     {Name: "team", PolicyNames: []string{"group"}},
			"disabled": {Name: "disabled", PolicyNames: []string{"wildcard"}, Disabled: true},
		},
		userGroups: map[string][]string{"grouped": {"team"}, "in-disabled-group": {"disabled"}},
	}

	tests := []struct {
		name        string
		identity    *Identity
		wantOk      bool
		wantGranted []string
	}{
		{name: "named bucket enumerates", identity: &Identity{Name: "u", PolicyNames: []string{"named"}},
			wantOk: true, wantGranted: []string{"example-bucket"}},
		{name: "wildcard resource scans", identity: &Identity{Name: "u", PolicyNames: []string{"wildcard"}}},
		{name: "not-resource scans", identity: &Identity{Name: "u", PolicyNames: []string{"not-resource"}}},
		{name: "unknown policy scans", identity: &Identity{Name: "u", PolicyNames: []string{"missing"}}},
		{name: "one wildcard policy scans", identity: &Identity{Name: "u", PolicyNames: []string{"named", "wildcard"}}},
		{name: "action wildcard on a named bucket enumerates", identity: &Identity{Name: "u", PolicyNames: []string{"all-actions"}},
			wantOk: true, wantGranted: []string{"b1", "b1"}},
		// Deny narrows nothing here; resolveGrantedBuckets re-checks each name.
		{name: "denied bucket stays a candidate", identity: &Identity{Name: "u", PolicyNames: []string{"deny-one"}},
			wantOk: true, wantGranted: []string{"b1", "b2"}},
		{name: "group policy contributes", identity: &Identity{Name: "grouped", PolicyNames: []string{"named"}},
			wantOk: true, wantGranted: []string{"example-bucket", "shared"}},
		{name: "disabled group is ignored", identity: &Identity{Name: "in-disabled-group"}, wantOk: true},
		{name: "resource variable scans", identity: &Identity{Name: "u", PolicyNames: []string{"resource-variable"}}},
		{name: "action variable keeps its bucket", identity: &Identity{Name: "u", PolicyNames: []string{"action-variable"}},
			wantOk: true, wantGranted: []string{"b9"}},
		{name: "mixed case action enumerates", identity: &Identity{Name: "u", PolicyNames: []string{"mixed-case"}},
			wantOk: true, wantGranted: []string{"b7"}},
		{name: "mixed case wildcard action enumerates", identity: &Identity{Name: "u", PolicyNames: []string{"mixed-case-wildcard"}},
			wantOk: true, wantGranted: []string{"b8"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, "/", nil)
			ok, granted := iam.canListBucketsFromOwnerIndex(req, tt.identity)
			if ok != tt.wantOk {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOk)
			}
			if strings.Join(granted, ",") != strings.Join(tt.wantGranted, ",") {
				t.Errorf("granted = %v, want %v", granted, tt.wantGranted)
			}
		})
	}
}

func TestContinuationTokenRoundTrip(t *testing.T) {
	for _, name := range []string{"a", "bucket-42", "with.dots-and-dashes", "0123456789"} {
		decoded, err := decodeContinuationToken(encodeContinuationToken(name))
		if err != nil {
			t.Fatalf("decode(encode(%q)): %v", name, err)
		}
		if decoded != name {
			t.Errorf("round trip %q -> %q", name, decoded)
		}
	}
	if _, err := decodeContinuationToken(""); err == nil {
		t.Error("empty token should not decode")
	}
	if _, err := decodeContinuationToken("!!!"); err == nil {
		t.Error("non-base64 token should not decode")
	}
}

// With auth off every request may use every bucket, so ListBuckets lists them
// all instead of treating the caller as an unauthenticated user.
func TestListBucketsWithoutAuthListsEveryBucket(t *testing.T) {
	f := &fakeBucketDeleteFiler{entriesByDir: map[string][]*filer_pb.Entry{
		"/buckets": {
			{Name: ".system", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}},
			{Name: "alpha", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}},
			{Name: "beta", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}},
		},
	}}
	s3a := newBucketDeleteTestServer(t, f, false)

	for _, tt := range []struct {
		query string
		want  []string
	}{
		{query: "", want: []string{"alpha", "beta"}},
		{query: "?prefix=b", want: []string{"beta"}},
	} {
		rec := httptest.NewRecorder()
		s3a.ListBucketsHandler(rec, httptest.NewRequest(http.MethodGet, "/"+tt.query, nil))
		require.Equal(t, http.StatusOK, rec.Code, tt.query)
		var result ListAllMyBucketsResult
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result), tt.query)
		var names []string
		for _, b := range result.Buckets.Bucket {
			names = append(names, b.Name)
		}
		assert.Equal(t, tt.want, names, tt.query)
	}

	// A truncated page must hand back a token that reaches the next page.
	rec := httptest.NewRecorder()
	s3a.ListBucketsHandler(rec, httptest.NewRequest(http.MethodGet, "/?max-buckets=1", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var first ListAllMyBucketsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &first))
	require.Len(t, first.Buckets.Bucket, 1)
	assert.Equal(t, "alpha", first.Buckets.Bucket[0].Name)
	require.NotEmpty(t, first.ContinuationToken)

	rec = httptest.NewRecorder()
	s3a.ListBucketsHandler(rec, httptest.NewRequest(http.MethodGet, "/?max-buckets=1&continuation-token="+url.QueryEscape(first.ContinuationToken), nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var second ListAllMyBucketsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &second))
	require.Len(t, second.Buckets.Bucket, 1)
	assert.Equal(t, "beta", second.Buckets.Bucket[0].Name)
	assert.Empty(t, second.ContinuationToken)
}

// With auth on, a request carrying no identity still lists no buckets.
func TestListBucketsWithAuthListsNothingForAnonymous(t *testing.T) {
	f := &fakeBucketDeleteFiler{entriesByDir: map[string][]*filer_pb.Entry{
		"/buckets": {
			{Name: "alpha", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}},
		},
	}}
	s3a := newBucketDeleteTestServer(t, f, false)
	s3a.iam.isAuthEnabled = true

	rec := httptest.NewRecorder()
	s3a.ListBucketsHandler(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var result ListAllMyBucketsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.Empty(t, result.Buckets.Bucket)
}
