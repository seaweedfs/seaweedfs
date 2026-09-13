package s3tables

import "testing"

func TestValidateMetadataLocation(t *testing.T) {
	tests := []struct {
		name    string
		loc     string
		bucket  string
		wantErr bool
	}{
		{"empty allowed", "", "bkt", false},
		{"same bucket", "s3://bkt/ns/t/metadata/v1.metadata.json", "bkt", false},
		{"cross bucket rejected", "s3://other/ns/t/metadata/v1.metadata.json", "bkt", true},
		{"bucket only rejected", "s3://bkt", "bkt", true},
		{"bucket with trailing slash rejected", "s3://bkt/", "bkt", true},
		{"slash-only path rejected", "s3://bkt///", "bkt", true},
		{"dotdot in path rejected", "s3://bkt/../victim/metadata/v1.metadata.json", "bkt", true},
		{"dotdot mid path rejected", "s3://bkt/ns/../../victim/metadata/v1.metadata.json", "bkt", true},
		{"dot segment rejected", "s3://bkt/ns/./t/metadata/v1.metadata.json", "bkt", true},
		{"backslash segment rejected", "s3://bkt/ns/\\t/metadata/v1.metadata.json", "bkt", true},
		{"non-s3 scheme rejected", "file:///bkt/ns/t", "bkt", true},
		{"empty location string", "s3://", "bkt", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMetadataLocation(tt.loc, tt.bucket)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateMetadataLocation(%q, %q) err = %v, wantErr = %v", tt.loc, tt.bucket, err, tt.wantErr)
			}
		})
	}
}
