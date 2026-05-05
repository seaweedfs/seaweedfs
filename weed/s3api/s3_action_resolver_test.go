package s3api

import (
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestMapBaseActionToS3Format_ServicePrefixPassthrough verifies that actions
// with known service prefixes (s3:, iam:, sts:) are returned unchanged.
func TestMapBaseActionToS3Format_ServicePrefixPassthrough(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{"s3 prefix", "s3:GetObject", "s3:GetObject"},
		{"iam prefix", "iam:CreateUser", "iam:CreateUser"},
		{"sts:AssumeRole", "sts:AssumeRole", "sts:AssumeRole"},
		{"sts:GetFederationToken", "sts:GetFederationToken", "sts:GetFederationToken"},
		{"sts:GetCallerIdentity", "sts:GetCallerIdentity", "sts:GetCallerIdentity"},
		{"coarse Read maps to s3:GetObject", "Read", s3_constants.S3_ACTION_GET_OBJECT},
		{"coarse Write maps to s3:PutObject", "Write", s3_constants.S3_ACTION_PUT_OBJECT},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapBaseActionToS3Format(tt.input)
			if got != tt.expect {
				t.Errorf("mapBaseActionToS3Format(%q) = %q, want %q", tt.input, got, tt.expect)
			}
		})
	}
}

// TestResolveS3Action_STSActionsPassthrough verifies that STS actions flow
// through ResolveS3Action unchanged, both with and without an HTTP request.
func TestResolveS3Action_STSActionsPassthrough(t *testing.T) {
	stsActions := []string{
		"sts:AssumeRole",
		"sts:GetFederationToken",
		"sts:GetCallerIdentity",
	}

	for _, action := range stsActions {
		t.Run("nil_request_"+action, func(t *testing.T) {
			got := ResolveS3Action(nil, action, "", "")
			if got != action {
				t.Errorf("ResolveS3Action(nil, %q) = %q, want %q", action, got, action)
			}
		})
		t.Run("with_request_"+action, func(t *testing.T) {
			r, _ := http.NewRequest(http.MethodPost, "http://localhost/", nil)
			got := ResolveS3Action(r, action, "", "")
			if got != action {
				t.Errorf("ResolveS3Action(r, %q) = %q, want %q", action, got, action)
			}
		})
	}
}

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
