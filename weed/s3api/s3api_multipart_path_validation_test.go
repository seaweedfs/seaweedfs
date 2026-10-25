package s3api

import (
	"strings"
	"testing"
)

func TestCheckUploadIDRequiresGeneratedFormat(t *testing.T) {
	s3a := &S3ApiServer{}
	const object = "dir/object"
	hash := s3a.generateUploadID(object)

	tests := []struct {
		name     string
		uploadID string
		wantErr  bool
	}{
		{"legacy hash", hash, false},
		{"current format", hash + "_" + strings.Repeat("a", 32), false},
		{"wrong object hash", strings.Repeat("0", 40) + "_" + strings.Repeat("a", 32), true},
		{"arbitrary suffix", hash + "_suffix", true},
		{"uppercase suffix", hash + "_" + strings.Repeat("A", 32), true},
		{"short suffix", hash + "_" + strings.Repeat("a", 31), true},
		{"long suffix", hash + "_" + strings.Repeat("a", 33), true},
		{"slash traversal", hash + "/../../../victim-bucket", true},
		{"suffix traversal", hash + "_" + strings.Repeat("a", 32) + "/../../../victim-bucket", true},
		{"backslash traversal", hash + `\..\..\victim-bucket`, true},
		{"nul suffix", hash + "_abc\x00def", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := s3a.checkUploadId(object, tt.uploadID) != nil
			if gotErr != tt.wantErr {
				t.Errorf("checkUploadId(%q) error = %v, want %v", tt.uploadID, gotErr, tt.wantErr)
			}
		})
	}
}
