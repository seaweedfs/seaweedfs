package s3api

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestMapChunkedUploadErrorToS3Error(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want s3err.ErrorCode
	}{
		{
			// A truncated body (client abort or reverse-proxy timeout) reaches
			// putToFiler tagged exactly like UploadReaderInChunks reports it.
			name: "truncated source read maps to IncompleteBody",
			err:  fmt.Errorf("%w: read chunk at offset %d (got %d bytes): %w", operation.ErrTruncatedBody, 0, 8056500, io.ErrUnexpectedEOF),
			want: s3err.ErrIncompleteBody,
		},
		{
			// A volume-server upload dropping mid-write is a server fault, not a
			// client truncation, even though it also carries io.ErrUnexpectedEOF.
			name: "volume upload unexpected EOF maps to InternalError",
			err:  fmt.Errorf("upload chunk: %w", io.ErrUnexpectedEOF),
			want: s3err.ErrInternalError,
		},
		{
			name: "payload checksum mismatch maps to InvalidDigest",
			err:  errors.New(s3err.ErrMsgPayloadChecksumMismatch),
			want: s3err.ErrInvalidDigest,
		},
		{
			name: "other errors map to InternalError",
			err:  errors.New("assign volume: no free volumes"),
			want: s3err.ErrInternalError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapChunkedUploadErrorToS3Error(tt.err); got != tt.want {
				t.Errorf("mapChunkedUploadErrorToS3Error(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
