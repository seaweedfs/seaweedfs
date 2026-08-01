package s3api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

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
			if got := mapChunkedUploadErrorToS3Error(context.Background(), tt.err); got != tt.want {
				t.Errorf("mapChunkedUploadErrorToS3Error(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// A body that ends early because the peer vanished and one that ends early while the
// peer is still connected arrive as the same read error. Only the request context
// tells them apart, and they point at opposite causes, so they must not share a code.
func TestMapChunkedUploadErrorToS3ErrorClientDisconnect(t *testing.T) {
	truncated := fmt.Errorf("%w: read chunk at offset %d (got %d bytes): %w", operation.ErrTruncatedBody, 0, 0, io.ErrUnexpectedEOF)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	deadline, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelDeadline()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want s3err.ErrorCode
	}{
		{
			name: "peer gone maps to ClientDisconnected",
			ctx:  canceled,
			err:  truncated,
			want: s3err.ErrClientDisconnected,
		},
		{
			name: "peer still connected stays IncompleteBody",
			ctx:  context.Background(),
			err:  truncated,
			want: s3err.ErrIncompleteBody,
		},
		{
			// A deadline is the server giving up, not the peer leaving, so it must
			// not be laundered into a client-side code.
			name: "expired deadline stays IncompleteBody",
			ctx:  deadline,
			err:  truncated,
			want: s3err.ErrIncompleteBody,
		},
		{
			// Cancellation must not reclassify faults that are not truncations.
			name: "server fault under a canceled context stays InternalError",
			ctx:  canceled,
			err:  errors.New("assign volume: no free volumes"),
			want: s3err.ErrInternalError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapChunkedUploadErrorToS3Error(tt.ctx, tt.err); got != tt.want {
				t.Errorf("mapChunkedUploadErrorToS3Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

// 499 must stay distinguishable from the 400 it was split out of.
func TestClientDisconnectedAPIError(t *testing.T) {
	got := s3err.GetAPIError(s3err.ErrClientDisconnected)
	if got.HTTPStatusCode != 499 {
		t.Errorf("ClientDisconnected status = %d, want 499", got.HTTPStatusCode)
	}
	if got.Code != "ClientDisconnected" {
		t.Errorf("ClientDisconnected code = %q, want %q", got.Code, "ClientDisconnected")
	}
	if s3err.GetAPIError(s3err.ErrIncompleteBody).HTTPStatusCode != 400 {
		t.Error("IncompleteBody must remain a 400")
	}
}
