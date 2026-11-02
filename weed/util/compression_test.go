package util

import "testing"

func TestIsCompressableFileType(t *testing.T) {
	tests := []struct {
		ext, mtype       string
		compress, isSure bool
	}{
		{".json", "", true, true},
		{".JSON", "", true, true},
		{"", "application/json", true, true},
		{"", "application/json; charset=utf-8", true, true},
		{"", "application/ld+json", true, true},
		{"", "application/x-ndjson", true, true},
		{"", "application/json-seq", true, true},
		{"", "Application/JSON", true, true},
		{"", "text/plain; charset=utf-8", true, true},
		{"", "application/xml", true, true},
		{".gz", "application/json", false, true},
		{"", "application/gzip", false, false},
		{"", "image/png", false, true},
		{"", "", false, false},
	}
	for _, tt := range tests {
		compress, isSure := IsCompressableFileType(tt.ext, tt.mtype)
		if compress != tt.compress || isSure != tt.isSure {
			t.Errorf("IsCompressableFileType(%q, %q) = (%v, %v), want (%v, %v)", tt.ext, tt.mtype, compress, isSure, tt.compress, tt.isSure)
		}
	}
}
