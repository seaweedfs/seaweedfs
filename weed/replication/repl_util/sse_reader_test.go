package repl_util

import (
	"bytes"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestDetectSSEType(t *testing.T) {
	tests := []struct {
		name      string
		entry     *filer_pb.Entry
		wantType  filer_pb.SSEType
		wantError bool
	}{
		{
			name:     "no chunks no extended",
			entry:    &filer_pb.Entry{},
			wantType: filer_pb.SSEType_NONE,
		},
		{
			name: "plaintext chunks",
			entry: &filer_pb.Entry{
				Chunks: []*filer_pb.FileChunk{
					{SseType: filer_pb.SSEType_NONE},
					{SseType: filer_pb.SSEType_NONE},
				},
			},
			wantType: filer_pb.SSEType_NONE,
		},
		{
			name: "uniform SSE-S3 chunks",
			entry: &filer_pb.Entry{
				Chunks: []*filer_pb.FileChunk{
					{SseType: filer_pb.SSEType_SSE_S3},
					{SseType: filer_pb.SSEType_SSE_S3},
				},
			},
			wantType: filer_pb.SSEType_SSE_S3,
		},
		{
			name: "uniform SSE-KMS chunks",
			entry: &filer_pb.Entry{
				Chunks: []*filer_pb.FileChunk{
					{SseType: filer_pb.SSEType_SSE_KMS},
				},
			},
			wantType: filer_pb.SSEType_SSE_KMS,
		},
		{
			name: "mixed chunk SSE types",
			entry: &filer_pb.Entry{
				Chunks: []*filer_pb.FileChunk{
					{SseType: filer_pb.SSEType_SSE_S3},
					{SseType: filer_pb.SSEType_SSE_KMS},
				},
			},
			wantError: true,
		},
		{
			name: "inline SSE-S3 via extended",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.SeaweedFSSSES3Key: {0x01},
				},
			},
			wantType: filer_pb.SSEType_SSE_S3,
		},
		{
			name: "inline SSE-KMS via extended",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.SeaweedFSSSEKMSKey: {0x01},
				},
			},
			wantType: filer_pb.SSEType_SSE_KMS,
		},
		{
			name: "inline SSE-C via extended",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.SeaweedFSSSEIV: {0x01},
				},
			},
			wantType: filer_pb.SSEType_SSE_C,
		},
		{
			name: "conflicting extended metadata",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.SeaweedFSSSES3Key:  {0x01},
					s3_constants.SeaweedFSSSEKMSKey: {0x02},
				},
			},
			wantError: true,
		},
		{
			name: "chunks take precedence over extended",
			entry: &filer_pb.Entry{
				Chunks: []*filer_pb.FileChunk{
					{SseType: filer_pb.SSEType_SSE_S3},
				},
				Extended: map[string][]byte{
					s3_constants.SeaweedFSSSEKMSKey: {0x01},
				},
			},
			wantType: filer_pb.SSEType_SSE_S3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := detectSSEType(tt.entry)
			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error, got type %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantType {
				t.Errorf("got %v, want %v", got, tt.wantType)
			}
		})
	}
}

func TestMaybeDecryptReader_Plaintext(t *testing.T) {
	content := []byte("hello world")
	entry := &filer_pb.Entry{}
	reader := bytes.NewReader(content)

	got, err := MaybeDecryptReader(reader, entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, _ := io.ReadAll(got)
	if !bytes.Equal(result, content) {
		t.Errorf("got %q, want %q", result, content)
	}
}

func TestMaybeDecryptReader_NilEntry(t *testing.T) {
	content := []byte("hello")
	reader := bytes.NewReader(content)

	got, err := MaybeDecryptReader(reader, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, _ := io.ReadAll(got)
	if !bytes.Equal(result, content) {
		t.Errorf("got %q, want %q", result, content)
	}
}

func TestMaybeDecryptReader_SSEC_Error(t *testing.T) {
	entry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.SeaweedFSSSEIV: {0x01},
		},
	}
	reader := bytes.NewReader([]byte("data"))

	_, err := MaybeDecryptReader(reader, entry)
	if err == nil {
		t.Fatal("expected error for SSE-C")
	}
}

func TestMaybeDecryptContent_Plaintext(t *testing.T) {
	content := []byte("hello world")
	entry := &filer_pb.Entry{}

	got, err := MaybeDecryptContent(content, entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("got %q, want %q", got, content)
	}
}

func TestMaybeDecryptContent_NilEntry(t *testing.T) {
	content := []byte("data")
	got, err := MaybeDecryptContent(content, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("got %q, want %q", got, content)
	}
}

func TestMaybeDecryptContent_Empty(t *testing.T) {
	got, err := MaybeDecryptContent(nil, &filer_pb.Entry{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestMaybeDecryptContent_SSEC_Error(t *testing.T) {
	entry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.SeaweedFSSSEIV: {0x01},
		},
	}

	_, err := MaybeDecryptContent([]byte("data"), entry)
	if err == nil {
		t.Fatal("expected error for SSE-C")
	}
}

func TestMaybeDecryptContent_MixedExtended_Error(t *testing.T) {
	entry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.SeaweedFSSSES3Key:  {0x01},
			s3_constants.SeaweedFSSSEKMSKey: {0x02},
		},
	}

	_, err := MaybeDecryptContent([]byte("data"), entry)
	if err == nil {
		t.Fatal("expected error for conflicting SSE metadata")
	}
}
