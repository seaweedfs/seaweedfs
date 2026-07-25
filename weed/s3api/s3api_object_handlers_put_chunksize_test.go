package s3api

import "testing"

func TestUploadChunkSizeFollowsFilerMaxMB(t *testing.T) {
	cases := []struct {
		name     string
		maxMB    int32
		expected int32
	}{
		{"filer maxMB unset", 0, defaultUploadChunkSizeMB * 1024 * 1024},
		{"filer maxMB negative", -1, defaultUploadChunkSizeMB * 1024 * 1024},
		{"filer default", 4, 4 * 1024 * 1024},
		{"filer maxMB 32", 32, 32 * 1024 * 1024},
		{"clamped to int32 range", 4096, maxUploadChunkSizeMB * 1024 * 1024},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s3a := &S3ApiServer{option: &S3ApiServerOption{MaxMB: c.maxMB}}
			if got := s3a.uploadChunkSize(); got != c.expected {
				t.Errorf("uploadChunkSize() = %d, want %d", got, c.expected)
			}
		})
	}
}
