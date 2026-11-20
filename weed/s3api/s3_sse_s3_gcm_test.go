package s3api

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/fips140"
	"crypto/rand"
	"encoding/base64"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// legacySealDEK is the AES-GCM step of encryptKeyWithSuperKey before
// NewGCMWithRandomNonce: it returns ciphertext || tag and the 12-byte nonce.
func legacySealDEK(t *testing.T, superKey, dek []byte) (encryptedDEK, nonce []byte) {
	t.Helper()
	gcm := legacyGCM(t, superKey)
	nonce = make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		t.Fatal(err)
	}
	return gcm.Seal(nil, nonce, dek, nil), nonce
}

// legacyWrapKEK is wrapKEK before NewGCMWithRandomNonce, producing the v2
// layout base64(magic || salt || nonce || ciphertext+tag).
func legacyWrapKEK(t *testing.T, km *SSES3KeyManager, kek []byte) []byte {
	t.Helper()
	salt := make([]byte, kekRandomSaltSize)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		t.Fatal(err)
	}
	wrappingKey, err := km.deriveWrappingKey(salt)
	if err != nil {
		t.Fatal(err)
	}
	gcm := legacyGCM(t, wrappingKey)
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		t.Fatal(err)
	}
	header := append(append([]byte{}, kekWrappedV2Magic...), salt...)
	sealed := gcm.Seal(append(header, nonce...), nonce, kek, nil)
	return []byte(base64.StdEncoding.EncodeToString(sealed))
}

// legacyUnwrapKEKV2 is the v2 branch of unwrapKEK before NewGCMWithRandomNonce.
func legacyUnwrapKEKV2(t *testing.T, km *SSES3KeyManager, wrapped []byte) ([]byte, error) {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(string(wrapped))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, kekWrappedV2Magic) {
		t.Fatalf("wrapped KEK is not in the v2 layout")
	}
	salt := raw[len(kekWrappedV2Magic) : len(kekWrappedV2Magic)+kekRandomSaltSize]
	payload := raw[len(kekWrappedV2Magic)+kekRandomSaltSize:]
	wrappingKey, err := km.deriveWrappingKey(salt)
	if err != nil {
		t.Fatal(err)
	}
	gcm := legacyGCM(t, wrappingKey)
	return gcm.Open(nil, payload[:gcm.NonceSize()], payload[gcm.NonceSize():], nil)
}

func legacyGCM(t *testing.T, key []byte) cipher.AEAD {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	return gcm
}

// TestSuperKeyDEKLayoutMatchesLegacy checks that the encryptedDEK and nonce
// stored in SSE-S3 object metadata keep their layout, so objects written
// before and after the switch to NewGCMWithRandomNonce open with either version.
func TestSuperKeyDEKLayoutMatchesLegacy(t *testing.T) {
	km := initSSES3KeyManagerForTest(t)
	dek := bytes.Repeat([]byte{0x42}, SSES3KeySize)

	oldDEK, oldNonce := legacySealDEK(t, km.superKey, dek)
	if got, err := km.decryptKeyWithSuperKey(oldDEK, oldNonce); err != nil || !bytes.Equal(got, dek) {
		t.Fatalf("decryptKeyWithSuperKey(legacy output) = %x, %v", got, err)
	}

	encryptedDEK, nonce, err := km.encryptKeyWithSuperKey(dek)
	if err != nil {
		t.Fatal(err)
	}
	if len(nonce) != len(oldNonce) || len(encryptedDEK) != len(oldDEK) {
		t.Fatalf("nonce/encryptedDEK are %d/%d bytes, legacy %d/%d",
			len(nonce), len(encryptedDEK), len(oldNonce), len(oldDEK))
	}
	if got, err := legacyGCM(t, km.superKey).Open(nil, nonce, encryptedDEK, nil); err != nil || !bytes.Equal(got, dek) {
		t.Fatalf("legacy open of encryptKeyWithSuperKey output = %x, %v", got, err)
	}
}

// TestWrapKEKLayoutMatchesLegacy checks that the v2 wrapped-KEK layout is
// unchanged in both directions.
func TestWrapKEKLayoutMatchesLegacy(t *testing.T) {
	km := NewSSES3KeyManager("test-passphrase-for-kek-layout")
	kek := bytes.Repeat([]byte{0xAB}, SSES3KeySize)

	got, isV2, err := km.unwrapKEK(legacyWrapKEK(t, km, kek))
	if err != nil || !isV2 || !bytes.Equal(got, kek) {
		t.Fatalf("unwrapKEK(legacy output) = %x, isV2=%v, %v", got, isV2, err)
	}

	wrapped, err := km.wrapKEK(kek)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := legacyUnwrapKEKV2(t, km, wrapped); err != nil || !bytes.Equal(got, kek) {
		t.Fatalf("legacy unwrap of wrapKEK output = %x, %v", got, err)
	}
}

// TestSSES3KeyWrappingUnderFIPS140Only runs the SSE-S3 metadata and KEK
// round trips with GODEBUG=fips140=only, where cipher.NewGCM refuses
// caller-chosen nonces.
func TestSSES3KeyWrappingUnderFIPS140Only(t *testing.T) {
	if !runUnderFIPS140Only(t) {
		return
	}
	km := initSSES3KeyManagerForTest(t)
	key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := SerializeSSES3Metadata(key)
	if err != nil {
		t.Fatalf("SerializeSSES3Metadata: %v", err)
	}
	restored, err := DeserializeSSES3Metadata(metadata, km)
	if err != nil || !bytes.Equal(restored.Key, key.Key) {
		t.Fatalf("DeserializeSSES3Metadata = %v, %v", restored, err)
	}

	km = NewSSES3KeyManager("test-passphrase-for-kek-layout")
	wrapped, err := km.wrapKEK(key.Key)
	if err != nil {
		t.Fatalf("wrapKEK: %v", err)
	}
	if got, _, err := km.unwrapKEK(wrapped); err != nil || !bytes.Equal(got, key.Key) {
		t.Fatalf("unwrapKEK = %x, %v", got, err)
	}
}

// runUnderFIPS140Only reports whether the test is running with
// GODEBUG=fips140=only. If it is not, it re-runs the calling test in a child
// process with that setting (GODEBUG fips140 is fixed at program start) and
// fails if the child fails. It skips when the test binary cannot start in
// that mode, which happens when a package init uses a non-approved algorithm.
func runUnderFIPS140Only(t *testing.T) bool {
	t.Helper()
	if fips140.Enforced() {
		return true
	}
	godebug := "fips140=only"
	if v := os.Getenv("GODEBUG"); v != "" {
		godebug = v + "," + godebug
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), "GODEBUG="+godebug)
	out, err := cmd.CombinedOutput()
	if !strings.Contains(string(out), "=== RUN   "+t.Name()) {
		t.Skipf("test binary does not start with GODEBUG=fips140=only: %v\n%s", err, out)
	}
	if err != nil || !strings.Contains(string(out), "--- PASS: "+t.Name()) {
		t.Fatalf("with GODEBUG=fips140=only: %v\n%s", err, out)
	}
	return false
}
