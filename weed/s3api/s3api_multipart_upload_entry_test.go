package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestIsMultipartUploadEntry(t *testing.T) {
	tests := []struct {
		name  string
		entry *filer_pb.Entry
		want  bool
	}{
		{"aborted upload", nil, false},
		{"directory re-created by a part write", &filer_pb.Entry{IsDirectory: true}, false},
		{"key emptied", &filer_pb.Entry{IsDirectory: true, Extended: map[string][]byte{s3_constants.ExtMultipartObjectKey: {}}}, false},
		{"open upload", &filer_pb.Entry{IsDirectory: true, Extended: map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte("a.bin")}}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMultipartUploadEntry(tc.entry); got != tc.want {
				t.Errorf("isMultipartUploadEntry() = %v, want %v", got, tc.want)
			}
		})
	}
}
