package s3tables

import "testing"

func TestValidateMetadataLocation(t *testing.T) {
	tests := []struct {
		name      string
		loc       string
		bucket    string
		namespace string
		wantErr   bool
	}{
		{"empty allowed", "", "bkt", "ns", false},
		{"own namespace", "s3://bkt/ns/t/metadata/v1.metadata.json", "bkt", "ns", false},
		{"own namespace arbitrary layout", "s3://bkt/ns/warehouse/t/metadata/v1.metadata.json", "bkt", "ns", false},
		{"cross bucket rejected", "s3://other/ns/t/metadata/v1.metadata.json", "bkt", "ns", true},
		{"cross namespace rejected", "s3://bkt/nsa/t/metadata/v1.metadata.json", "bkt", "ns", true},
		{"cross namespace data dir rejected", "s3://bkt/nsa/vtab/data/metadata/v1.metadata.json", "bkt", "ns", true},
		{"namespace prefix rejected", "s3://bkt/nsa/vtab/metadata/v1.metadata.json", "bkt", "ns", true},
		{"outside namespace tree rejected", "s3://bkt/shared/t/metadata/v1.metadata.json", "bkt", "ns", true},
		{"namespace dir without table path rejected", "s3://bkt/ns", "bkt", "ns", true},
		{"bucket only rejected", "s3://bkt", "bkt", "ns", true},
		{"bucket with trailing slash rejected", "s3://bkt/", "bkt", "ns", true},
		{"slash-only path rejected", "s3://bkt///", "bkt", "ns", true},
		{"dotdot in path rejected", "s3://bkt/../victim/metadata/v1.metadata.json", "bkt", "ns", true},
		{"dotdot mid path rejected", "s3://bkt/ns/../../victim/metadata/v1.metadata.json", "bkt", "ns", true},
		{"dot segment rejected", "s3://bkt/ns/./t/metadata/v1.metadata.json", "bkt", "ns", true},
		{"backslash segment rejected", "s3://bkt/ns/\\t/metadata/v1.metadata.json", "bkt", "ns", true},
		{"non-s3 scheme rejected", "file:///bkt/ns/t", "bkt", "ns", true},
		{"empty location string", "s3://", "bkt", "ns", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMetadataLocation(tt.loc, tt.bucket, tt.namespace)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateMetadataLocation(%q, %q, %q) err = %v, wantErr = %v", tt.loc, tt.bucket, tt.namespace, err, tt.wantErr)
			}
		})
	}
}
