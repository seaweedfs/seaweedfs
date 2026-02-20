package s3api

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
)

func TestEnsureRemoteErrorToS3(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		key         string
		versionId   string
		wantCode    string
		wantMessage string
	}{
		{
			name:        "filer not found returns NoSuchKey",
			err:         filer_pb.ErrNotFound,
			key:         "path/to/object",
			versionId:   "",
			wantCode:    s3err.GetAPIError(s3err.ErrNoSuchKey).Code,
			wantMessage: s3err.GetAPIError(s3err.ErrNoSuchKey).Description,
		},
		{
			name:        "filer not found with version id",
			err:         filer_pb.ErrNotFound,
			key:         "object.key",
			versionId:   "v123",
			wantCode:    s3err.GetAPIError(s3err.ErrNoSuchKey).Code,
			wantMessage: s3err.GetAPIError(s3err.ErrNoSuchKey).Description,
		},
		{
			name:        "wrapped filer not found returns NoSuchKey",
			err:         errors.Join(errors.New("wrapped"), filer_pb.ErrNotFound),
			key:         "key",
			versionId:   "",
			wantCode:    s3err.GetAPIError(s3err.ErrNoSuchKey).Code,
			wantMessage: s3err.GetAPIError(s3err.ErrNoSuchKey).Description,
		},
		{
			name:        "other error returns InternalError",
			err:         errors.New("some backend failure"),
			key:         "path/to/object",
			versionId:   "",
			wantCode:    s3err.GetAPIError(s3err.ErrInternalError).Code,
			wantMessage: "some backend failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ensureRemoteErrorToS3(tt.err, tt.key, tt.versionId)
			assert.Equal(t, tt.wantCode, got.Code)
			assert.Equal(t, tt.wantMessage, got.Message)
			assert.Equal(t, tt.key, got.Key)
			assert.Equal(t, tt.versionId, got.VersionId)
		})
	}
}
