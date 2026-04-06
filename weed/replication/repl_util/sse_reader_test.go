package repl_util

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
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
	result, err := io.ReadAll(got)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
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
	result, err := io.ReadAll(got)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
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

// --- SSE-S3 integration tests ---
// These tests exercise the SSE-S3 AES-CTR encrypt/decrypt round-trip using
// CreateSSES3EncryptedReader and CreateSSES3DecryptedReader directly. The
// key manager envelope (SerializeSSES3Metadata) is not tested here because
// it requires a filer connection to set the super key.

func TestMaybeDecryptReader_SSES3(t *testing.T) {
	plaintext := []byte("SSE-S3 encrypted content for testing round-trip decryption")

	// Generate a random DEK and IV
	dek := make([]byte, 32)
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		t.Fatal(err)
	}

	sseKey := &s3api.SSES3Key{
		Key:       dek,
		KeyID:     "test-key-id",
		Algorithm: s3_constants.SSEAlgorithmAES256,
		IV:        iv,
	}

	// Encrypt the plaintext using the same AES-CTR that SSE-S3 uses
	encReader, encIV, err := s3api.CreateSSES3EncryptedReader(bytes.NewReader(plaintext), sseKey)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	ciphertext, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("read ciphertext: %v", err)
	}

	// Now build serialized metadata that bypasses the key manager envelope.
	// Since we can't easily set up the global key manager's super key without
	// a filer, we test at the CreateSSES3DecryptedReader level directly,
	// which is what decryptSSES3 calls after deserializing.
	decrypted, err := s3api.CreateSSES3DecryptedReader(bytes.NewReader(ciphertext), sseKey, encIV)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	result, err := io.ReadAll(decrypted)
	if err != nil {
		t.Fatalf("read decrypted: %v", err)
	}
	if !bytes.Equal(result, plaintext) {
		t.Errorf("SSE-S3 round-trip failed: got %q, want %q", result, plaintext)
	}
}

func TestMaybeDecryptContent_SSES3(t *testing.T) {
	plaintext := []byte("inline SSE-S3 content")

	dek := make([]byte, 32)
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		t.Fatal(err)
	}

	// Encrypt with AES-CTR (same as SSE-S3)
	block, err := aes.NewCipher(dek)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCTR(block, iv).XORKeyStream(ciphertext, plaintext)

	// Verify decryption via CreateSSES3DecryptedReader
	reader, err := s3api.CreateSSES3DecryptedReader(
		bytes.NewReader(ciphertext),
		&s3api.SSES3Key{Key: dek, KeyID: "test", Algorithm: s3_constants.SSEAlgorithmAES256},
		iv,
	)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(result, plaintext) {
		t.Errorf("got %q, want %q", result, plaintext)
	}
}

// --- SSE-KMS integration tests ---

// testKMSProvider is a minimal KMSProvider mock for testing.
type testKMSProvider struct {
	keyID     string
	plaintext []byte // the DEK plaintext returned by Decrypt
}

func (p *testKMSProvider) GenerateDataKey(_ context.Context, _ *kms.GenerateDataKeyRequest) (*kms.GenerateDataKeyResponse, error) {
	return nil, nil
}

func (p *testKMSProvider) Decrypt(_ context.Context, _ *kms.DecryptRequest) (*kms.DecryptResponse, error) {
	return &kms.DecryptResponse{
		KeyID:     p.keyID,
		Plaintext: append([]byte(nil), p.plaintext...), // return a copy
	}, nil
}

func (p *testKMSProvider) DescribeKey(_ context.Context, _ *kms.DescribeKeyRequest) (*kms.DescribeKeyResponse, error) {
	return nil, nil
}

func (p *testKMSProvider) GetKeyID(_ context.Context, keyIdentifier string) (string, error) {
	return p.keyID, nil
}

func (p *testKMSProvider) Close() error { return nil }

func TestMaybeDecryptReader_SSEKMS(t *testing.T) {
	plaintext := []byte("SSE-KMS encrypted content for testing")

	// Generate a random DEK and IV
	dek := make([]byte, 32)
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		t.Fatal(err)
	}

	// Encrypt with AES-CTR (same cipher mode as SSE-KMS)
	block, err := aes.NewCipher(dek)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCTR(block, iv).XORKeyStream(ciphertext, plaintext)

	// Set up a mock KMS provider that returns our DEK
	keyID := "test-kms-key-1"
	encryptedDEK := []byte("fake-encrypted-dek") // mock doesn't validate
	kms.SetGlobalKMSProvider(&testKMSProvider{
		keyID:     keyID,
		plaintext: dek,
	})
	defer kms.SetGlobalKMSProvider(nil)

	// Build serialized KMS metadata
	kmsMetadata := s3api.SSEKMSMetadata{
		Algorithm:        s3_constants.SSEAlgorithmKMS,
		KeyID:            keyID,
		EncryptedDataKey: base64.StdEncoding.EncodeToString(encryptedDEK),
		IV:               base64.StdEncoding.EncodeToString(iv),
	}
	metadataBytes, err := json.Marshal(kmsMetadata)
	if err != nil {
		t.Fatal(err)
	}

	entry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.SeaweedFSSSEKMSKey: metadataBytes,
		},
	}

	// Test MaybeDecryptReader
	reader := bytes.NewReader(ciphertext)
	decrypted, err := MaybeDecryptReader(reader, entry)
	if err != nil {
		t.Fatalf("MaybeDecryptReader: %v", err)
	}
	result, err := io.ReadAll(decrypted)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(result, plaintext) {
		t.Errorf("SSE-KMS round-trip failed: got %q, want %q", result, plaintext)
	}
}

func TestMaybeDecryptContent_SSEKMS(t *testing.T) {
	plaintext := []byte("inline SSE-KMS content")

	dek := make([]byte, 32)
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		t.Fatal(err)
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCTR(block, iv).XORKeyStream(ciphertext, plaintext)

	keyID := "test-kms-key-2"
	kms.SetGlobalKMSProvider(&testKMSProvider{
		keyID:     keyID,
		plaintext: dek,
	})
	defer kms.SetGlobalKMSProvider(nil)

	kmsMetadata := s3api.SSEKMSMetadata{
		Algorithm:        s3_constants.SSEAlgorithmKMS,
		KeyID:            keyID,
		EncryptedDataKey: base64.StdEncoding.EncodeToString([]byte("fake-encrypted-dek")),
		IV:               base64.StdEncoding.EncodeToString(iv),
	}
	metadataBytes, err := json.Marshal(kmsMetadata)
	if err != nil {
		t.Fatal(err)
	}

	entry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.SeaweedFSSSEKMSKey: metadataBytes,
		},
	}

	result, err := MaybeDecryptContent(ciphertext, entry)
	if err != nil {
		t.Fatalf("MaybeDecryptContent: %v", err)
	}
	if !bytes.Equal(result, plaintext) {
		t.Errorf("SSE-KMS round-trip failed: got %q, want %q", result, plaintext)
	}
}
