package s3api

import (
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClassifyCopySourceError(t *testing.T) {
	deleteMarker := &filer_pb.Entry{
		Extended: map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
	}

	tests := []struct {
		name  string
		entry *filer_pb.Entry
		err   error
		want  s3err.ErrorCode
	}{
		{"regular entry", &filer_pb.Entry{Name: "o"}, nil, s3err.ErrNone},
		{"nil entry", nil, nil, s3err.ErrInvalidCopySource},
		{"directory entry", &filer_pb.Entry{IsDirectory: true}, nil, s3err.ErrInvalidCopySource},
		{"delete marker entry", deleteMarker, nil, s3err.ErrNoSuchKey},
		{"not found sentinel", nil, filer_pb.ErrNotFound, s3err.ErrInvalidCopySource},
		{"wrapped not found", nil, fmt.Errorf("read %s: %w", "o", filer_pb.ErrNotFound), s3err.ErrInvalidCopySource},
		{"grpc not found", nil, status.Error(codes.NotFound, "no entry"), s3err.ErrInvalidCopySource},
		{"invalid version id", nil, errInvalidVersionID, s3err.ErrInvalidCopySource},
		{"delete marker error", nil, ErrDeleteMarker, s3err.ErrInvalidCopySource},
		{"transient unavailable", nil, status.Error(codes.Unavailable, "filer down"), s3err.ErrServiceUnavailable},
		{"store error", nil, errors.New("connection reset"), s3err.ErrInternalError},
		// A message merely mentioning the sentinel text must not downgrade a store error.
		{"sentinel text in message", nil, fmt.Errorf("peer said: %s", filer_pb.ErrNotFound.Error()), s3err.ErrInternalError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyCopySourceError(tt.entry, tt.err); got != tt.want {
				t.Errorf("classifyCopySourceError() = %v, want %v", got, tt.want)
			}
		})
	}
}
