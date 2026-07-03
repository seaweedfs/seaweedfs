package s3api

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
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
		{name: "attached policies is owned-only", identity: &Identity{Name: "u", Actions: []Action{"List:b1"}, PolicyNames: []string{"p"}}, wantOk: true},
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

	t.Run("session token routes to policies when integrated", func(t *testing.T) {
		integrated := &IdentityAccessManagement{iamIntegration: &S3IAMIntegration{}}
		req := r()
		req.Header.Set("X-Amz-Security-Token", "tok")
		ok, granted := integrated.canListBucketsFromOwnerIndex(req, &Identity{Name: "u", Actions: []Action{"List:b1"}})
		if !ok || granted != nil {
			t.Errorf("ok = %v granted = %v, want owned-only", ok, granted)
		}
	})
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
