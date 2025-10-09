package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util/constants"
)

func TestFilerErrorToS3Error(t *testing.T) {
	tests := []struct {
		name        string
		errString   string
		expectedErr s3err.ErrorCode
	}{
		{
			name:        "MD5 mismatch error",
			errString:   constants.ErrMsgBadDigest,
			expectedErr: s3err.ErrBadDigest,
		},
		{
			name:        "Context canceled error",
			errString:   "rpc error: code = Canceled desc = context canceled",
			expectedErr: s3err.ErrInvalidRequest,
		},
		{
			name:        "Context canceled error (simple)",
			errString:   "context canceled",
			expectedErr: s3err.ErrInvalidRequest,
		},
		{
			name:        "Directory exists error",
			errString:   "existing /path/to/file is a directory",
			expectedErr: s3err.ErrExistingObjectIsDirectory,
		},
		{
			name:        "File exists error",
			errString:   "/path/to/file is a file",
			expectedErr: s3err.ErrExistingObjectIsFile,
		},
		{
			name:        "Unknown error",
			errString:   "some random error",
			expectedErr: s3err.ErrInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filerErrorToS3Error(tt.errString)
			if result != tt.expectedErr {
				t.Errorf("filerErrorToS3Error(%q) = %v, want %v", tt.errString, result, tt.expectedErr)
			}
		})
	}
}
