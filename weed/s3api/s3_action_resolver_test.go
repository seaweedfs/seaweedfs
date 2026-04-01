package s3api

import (
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestResolveS3Action_AttributesBeforeVersionId(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		method     string
		baseAction string
		object     string
		want       string
	}{
		{
			name:       "attributes only",
			query:      "attributes",
			method:     http.MethodGet,
			baseAction: s3_constants.ACTION_READ,
			object:     "key",
			want:       s3_constants.S3_ACTION_GET_OBJECT_ATTRIBUTES,
		},
		{
			name:       "attributes with versionId",
			query:      "attributes&versionId=abc123",
			method:     http.MethodGet,
			baseAction: s3_constants.ACTION_READ,
			object:     "key",
			want:       s3_constants.S3_ACTION_GET_OBJECT_ATTRIBUTES,
		},
		{
			name:       "versionId only GET",
			query:      "versionId=abc123",
			method:     http.MethodGet,
			baseAction: s3_constants.ACTION_READ,
			object:     "key",
			want:       s3_constants.S3_ACTION_GET_OBJECT_VERSION,
		},
		{
			name:       "versionId only DELETE",
			query:      "versionId=abc123",
			method:     http.MethodDelete,
			baseAction: s3_constants.ACTION_WRITE,
			object:     "key",
			want:       s3_constants.S3_ACTION_DELETE_OBJECT_VERSION,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := http.NewRequest(tt.method, "http://localhost/bucket/"+tt.object+"?"+tt.query, nil)
			got := ResolveS3Action(r, tt.baseAction, "bucket", tt.object)
			if got != tt.want {
				t.Errorf("ResolveS3Action() = %q, want %q", got, tt.want)
			}
		})
	}
}
