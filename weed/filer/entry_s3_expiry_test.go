package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestApplyS3ExpiryMetadata(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ttlSec   int32
		extended map[string][]byte
		want     bool
	}{
		{
			name:     "s3 entry with ttl",
			ttlSec:   3600,
			extended: map[string][]byte{s3_constants.ExtETagKey: []byte("abc123")},
			want:     true,
		},
		{
			name:     "no ttl",
			extended: map[string][]byte{s3_constants.ExtETagKey: []byte("abc123")},
		},
		{
			name:   "not an s3 entry",
			ttlSec: 3600,
		},
		{
			name:   "versioned entry keeps crtime expiry",
			ttlSec: 3600,
			extended: map[string][]byte{
				s3_constants.ExtETagKey:      []byte("abc123"),
				s3_constants.ExtVersionIdKey: []byte("v1"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := &Entry{Attr: Attr{TtlSec: tc.ttlSec}, Extended: tc.extended}
			entry.ApplyS3ExpiryMetadata()
			if got := entry.IsExpireS3Enabled(); got != tc.want {
				t.Fatalf("IsExpireS3Enabled() = %v, want %v", got, tc.want)
			}
		})
	}
}
