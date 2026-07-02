package s3api

import (
	"net/url"
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
