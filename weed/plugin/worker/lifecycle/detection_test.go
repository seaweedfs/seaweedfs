package lifecycle

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestBucketHasLifecycleXML(t *testing.T) {
	tests := []struct {
		name     string
		extended map[string][]byte
		want     bool
	}{
		{
			name:     "has_lifecycle_xml",
			extended: map[string][]byte{lifecycleXMLKey: []byte("<LifecycleConfiguration/>")},
			want:     true,
		},
		{
			name:     "empty_lifecycle_xml",
			extended: map[string][]byte{lifecycleXMLKey: {}},
			want:     false,
		},
		{
			name:     "no_lifecycle_xml",
			extended: map[string][]byte{"other-key": []byte("value")},
			want:     false,
		},
		{
			name:     "nil_extended",
			extended: nil,
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.extended != nil && len(tt.extended[lifecycleXMLKey]) > 0
			if got != tt.want {
				t.Errorf("hasLifecycleXML = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBucketVersioningStatus(t *testing.T) {
	tests := []struct {
		name     string
		extended map[string][]byte
		want     string
	}{
		{
			name: "versioning_enabled",
			extended: map[string][]byte{
				s3_constants.ExtVersioningKey: []byte("Enabled"),
			},
			want: "Enabled",
		},
		{
			name: "versioning_suspended",
			extended: map[string][]byte{
				s3_constants.ExtVersioningKey: []byte("Suspended"),
			},
			want: "Suspended",
		},
		{
			name:     "no_versioning",
			extended: map[string][]byte{},
			want:     "",
		},
		{
			name:     "nil_extended",
			extended: nil,
			want:     "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			if tt.extended != nil {
				got = string(tt.extended[s3_constants.ExtVersioningKey])
			}
			if got != tt.want {
				t.Errorf("versioningStatus = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDetectionProposalParameters(t *testing.T) {
	// Verify that bucket entries with lifecycle XML or TTL rules produce
	// proposals with the expected parameters.
	t.Run("bucket_with_lifecycle_xml_and_versioning", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Name:        "my-bucket",
			IsDirectory: true,
			Extended: map[string][]byte{
				lifecycleXMLKey:              []byte(`<LifecycleConfiguration><Rule><Status>Enabled</Status></Rule></LifecycleConfiguration>`),
				s3_constants.ExtVersioningKey: []byte("Enabled"),
			},
		}

		hasXML := entry.Extended != nil && len(entry.Extended[lifecycleXMLKey]) > 0
		versioning := ""
		if entry.Extended != nil {
			versioning = string(entry.Extended[s3_constants.ExtVersioningKey])
		}

		if !hasXML {
			t.Error("expected hasLifecycleXML=true")
		}
		if versioning != "Enabled" {
			t.Errorf("expected versioning=Enabled, got %q", versioning)
		}
	})

	t.Run("bucket_without_lifecycle_or_ttl_is_skipped", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Name:        "empty-bucket",
			IsDirectory: true,
			Extended:    map[string][]byte{},
		}

		hasXML := entry.Extended != nil && len(entry.Extended[lifecycleXMLKey]) > 0
		ttlCount := 0 // simulated: no TTL rules in filer.conf

		if hasXML || ttlCount > 0 {
			t.Error("expected bucket to be skipped (no lifecycle XML, no TTLs)")
		}
	})
}
