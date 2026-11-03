package s3api

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Writes a known-answer fixture for readers that decode SSE-S3 objects and
// filer-encrypted chunks outside the gateway, from this package's own
// encryption: DIRECT_READ_FIXTURE=/path go test -run TestGenerateDirectReadFixture
func TestGenerateDirectReadFixture(t *testing.T) {
	out := os.Getenv("DIRECT_READ_FIXTURE")
	if out == "" {
		t.Skip("set DIRECT_READ_FIXTURE to write the fixture")
	}
	kek, err := deriveKeyFromSecret("s3vectors-test")
	if err != nil {
		t.Fatal(err)
	}
	globalSSES3KeyManager.mu.Lock()
	previous := globalSSES3KeyManager.superKey
	globalSSES3KeyManager.superKey = kek
	globalSSES3KeyManager.mu.Unlock()
	t.Cleanup(func() {
		globalSSES3KeyManager.mu.Lock()
		globalSSES3KeyManager.superKey = previous
		globalSSES3KeyManager.mu.Unlock()
	})

	plain := make([]byte, 5000)
	for i := range plain {
		plain[i] = byte(i*7 + i/13)
	}
	key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatal(err)
	}
	baseIV := []byte("0123456789abcdef")

	type chunk struct {
		Offset      int64  `json:"offset"`
		Ciphertext  string `json:"ciphertext"`
		SseMetadata string `json:"sse_metadata"`
	}
	var chunks []chunk
	for _, part := range [][2]int64{{0, 2048}, {2048, 2952}} {
		off, size := part[0], part[1]
		enc, iv, err := CreateSSES3EncryptedReaderWithBaseIV(bytes.NewReader(plain[off:off+size]), key, baseIV, off)
		if err != nil {
			t.Fatal(err)
		}
		ct, err := io.ReadAll(enc)
		if err != nil {
			t.Fatal(err)
		}
		meta, err := SerializeSSES3Metadata(&SSES3Key{Key: key.Key, KeyID: key.KeyID, Algorithm: key.Algorithm, IV: iv})
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, chunk{off, hex.EncodeToString(ct), string(meta)})
	}
	objectMeta, err := SerializeSSES3Metadata(&SSES3Key{Key: key.Key, KeyID: key.KeyID, Algorithm: key.Algorithm, IV: baseIV})
	if err != nil {
		t.Fatal(err)
	}
	encWhole, _, err := CreateSSES3EncryptedReaderWithBaseIV(bytes.NewReader(plain), key, baseIV, 0)
	if err != nil {
		t.Fatal(err)
	}
	whole, err := io.ReadAll(encWhole)
	if err != nil {
		t.Fatal(err)
	}

	ck := util.GenCipherKey()
	cipherPlain, err := util.Encrypt(plain, ck)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := util.GzipData(plain)
	if err != nil {
		t.Fatal(err)
	}
	cipherGzip, err := util.Encrypt(gz, ck)
	if err != nil {
		t.Fatal(err)
	}

	km := NewSSES3KeyManager("pass-phrase")
	v2, err := km.wrapKEK(kek)
	if err != nil {
		t.Fatal(err)
	}
	wrappingKey, err := km.deriveWrappingKey(legacyKEKWrappingSalt)
	if err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(wrappingKey)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		t.Fatal(err)
	}
	v1 := base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, kek, nil))
	for _, wrapped := range []struct {
		data []byte
		isV2 bool
	}{{[]byte(v1), false}, {v2, true}} {
		unwrapped, isV2, err := km.unwrapKEK(wrapped.data)
		if err != nil || isV2 != wrapped.isV2 || !bytes.Equal(unwrapped, kek) {
			t.Fatalf("unwrapKEK(v2=%v) = v2=%v, err=%v; want the KEK back", wrapped.isV2, isV2, err)
		}
	}

	fixture := map[string]any{
		"kek_secret":        "s3vectors-test",
		"kek_hex":           hex.EncodeToString(kek),
		"kek_passphrase":    "pass-phrase",
		"kek_wrapped_v1":    v1,
		"kek_wrapped_v2":    string(v2),
		"plaintext":         hex.EncodeToString(plain),
		"object_metadata":   string(objectMeta),
		"chunks":            chunks,
		"whole_ciphertext":  hex.EncodeToString(whole),
		"cipher_key":        hex.EncodeToString(ck),
		"cipher_plain":      hex.EncodeToString(cipherPlain),
		"cipher_gzip":       hex.EncodeToString(cipherGzip),
		"commitment_sample": hex.EncodeToString(ComputeKeyCommitment(key.Key, baseIV, key.Algorithm)),
	}
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", " ")
	if err := enc.Encode(fixture); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
