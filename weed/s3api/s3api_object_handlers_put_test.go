package s3api

import (
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/util/constants"
)

func TestFilerErrorToS3Error(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		expectedErr s3err.ErrorCode
	}{
		{
			name:        "nil error",
			err:         nil,
			expectedErr: s3err.ErrNone,
		},
		{
			name:        "MD5 mismatch error",
			err:         errors.New(constants.ErrMsgBadDigest),
			expectedErr: s3err.ErrBadDigest,
		},
		{
			name:        "Read only error (direct)",
			err:         weed_server.ErrReadOnly,
			expectedErr: s3err.ErrAccessDenied,
		},
		{
			name:        "Read only error (wrapped)",
			err:         fmt.Errorf("create file /buckets/test/file.txt: %w", weed_server.ErrReadOnly),
			expectedErr: s3err.ErrAccessDenied,
		},
		{
			name:        "Context canceled error",
			err:         errors.New("rpc error: code = Canceled desc = context canceled"),
			expectedErr: s3err.ErrInvalidRequest,
		},
		{
			name:        "Context canceled error (simple)",
			err:         errors.New("context canceled"),
			expectedErr: s3err.ErrInvalidRequest,
		},
		{
			name:        "Directory exists error",
			err:         errors.New("existing /path/to/file is a directory"),
			expectedErr: s3err.ErrExistingObjectIsDirectory,
		},
		{
			name:        "File exists error",
			err:         errors.New("/path/to/file is a file"),
			expectedErr: s3err.ErrExistingObjectIsFile,
		},
		{
			name:        "Unknown error",
			err:         errors.New("some random error"),
			expectedErr: s3err.ErrInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filerErrorToS3Error(tt.err)
			if result != tt.expectedErr {
				t.Errorf("filerErrorToS3Error(%v) = %v, want %v", tt.err, result, tt.expectedErr)
			}
		})
	}
}
