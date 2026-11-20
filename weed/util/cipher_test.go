package util

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

func TestSameAsJavaImplementation(t *testing.T) {
	str := "QVVhmqg112NMT7F+G/7QPynqSln3xPIhKdFGmTVKZD6IS0noyr2Z5kXFF6fPjZ/7Hq8kRhlmLeeqZUccxyaZHezOdgkjS6d4NTdHf5IjXzk7"
	cipherText, _ := base64.StdEncoding.DecodeString(str)
	secretKey := []byte("256-bit key for AES 256 GCM encr")
	plaintext, err := Decrypt(cipherText, CipherKey(secretKey))
	if err != nil {
		println(err.Error())
	}
	println(string(plaintext))
}

// legacyEncrypt is Encrypt as it was before NewGCMWithRandomNonce: a
// caller-generated 12-byte nonce prepended to ciphertext || tag.
func legacyEncrypt(plaintext []byte, key CipherKey) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// legacyDecrypt is Decrypt as it was before NewGCMWithRandomNonce.
func legacyDecrypt(ciphertext []byte, key CipherKey) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	return gcm.Open(nil, ciphertext[:nonceSize], ciphertext[nonceSize:], nil)
}

// TestCipherReadsAndWritesLegacyLayout checks that chunks encrypted before and
// after the switch to NewGCMWithRandomNonce decrypt with either version.
func TestCipherReadsAndWritesLegacyLayout(t *testing.T) {
	key := GenCipherKey()
	plaintext := []byte("chunk data written by -encryptVolumeData")

	old, err := legacyEncrypt(plaintext, key)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := Decrypt(old, key); err != nil || !bytes.Equal(got, plaintext) {
		t.Fatalf("Decrypt(legacy output) = %q, %v", got, err)
	}

	sealed, err := Encrypt(plaintext, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != len(old) {
		t.Fatalf("Encrypt output is %d bytes, legacy output is %d", len(sealed), len(old))
	}
	if got, err := legacyDecrypt(sealed, key); err != nil || !bytes.Equal(got, plaintext) {
		t.Fatalf("legacy decrypt of Encrypt output = %q, %v", got, err)
	}
}

// TestCipherUnderFIPS140Only runs an Encrypt/Decrypt round trip with
// GODEBUG=fips140=only, where cipher.NewGCM refuses caller-chosen nonces.
func TestCipherUnderFIPS140Only(t *testing.T) {
	if !runUnderFIPS140Only(t) {
		return
	}
	key := GenCipherKey()
	plaintext := []byte("chunk data written by -encryptVolumeData")
	sealed, err := Encrypt(plaintext, key)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if got, err := Decrypt(sealed, key); err != nil || !bytes.Equal(got, plaintext) {
		t.Fatalf("Decrypt = %q, %v", got, err)
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

// otherAESBlock is an AES block that is not the standard library's own
// *aes.Block, like the one aes.NewCipher returns in a GOEXPERIMENT=boringcrypto
// build, which cipher.NewGCMWithRandomNonce refuses.
type otherAESBlock struct{ cipher.Block }

// TestGCMFallsBackForOtherAESBlocks checks that the fallback for such blocks
// writes and reads the same nonce || ciphertext || tag layout as
// cipher.NewGCMWithRandomNonce, with and without additional data.
func TestGCMFallsBackForOtherAESBlocks(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cipher.NewGCMWithRandomNonce(otherAESBlock{block}); err == nil {
		t.Fatal("cipher.NewGCMWithRandomNonce accepted a non-standard block; the fallback is never used")
	}
	fallback, err := newGCMWithRandomNonce(otherAESBlock{block})
	if err != nil {
		t.Fatal(err)
	}
	if fallback.NonceSize() != 0 || fallback.Overhead() != 12+16 {
		t.Fatalf("fallback NonceSize/Overhead %d/%d, want 0/28", fallback.NonceSize(), fallback.Overhead())
	}

	// A GOEXPERIMENT=boringcrypto build refuses aes.NewCipher's block too; compare
	// there with cipher.NewGCM and the nonce in front, as Encrypt wrote it before.
	var refSeal func(plaintext, ad []byte) []byte
	var refOpen func(sealed, ad []byte) ([]byte, error)
	if native, err := cipher.NewGCMWithRandomNonce(block); err == nil {
		refSeal = func(plaintext, ad []byte) []byte { return native.Seal(nil, nil, plaintext, ad) }
		refOpen = func(sealed, ad []byte) ([]byte, error) { return native.Open(nil, nil, sealed, ad) }
	} else {
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			t.Fatal(err)
		}
		refSeal = func(plaintext, ad []byte) []byte {
			nonce := make([]byte, gcm.NonceSize())
			if _, err := rand.Read(nonce); err != nil {
				t.Fatal(err)
			}
			return gcm.Seal(nonce, nonce, plaintext, ad)
		}
		refOpen = func(sealed, ad []byte) ([]byte, error) {
			return gcm.Open(nil, sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():], ad)
		}
	}

	plaintext := []byte("SeaweedFS chunk")
	prefix := []byte("prefix")
	for _, ad := range [][]byte{nil, []byte("additional data")} {
		sealed := fallback.Seal(append([]byte{}, prefix...), nil, plaintext, ad)
		if !bytes.HasPrefix(sealed, prefix) {
			t.Fatalf("fallback Seal did not append to dst")
		}
		if got, err := refOpen(sealed[len(prefix):], ad); err != nil || !bytes.Equal(got, plaintext) {
			t.Fatalf("reference Open of fallback output: %q, %v", got, err)
		}
		if got, err := fallback.Open(nil, nil, refSeal(plaintext, ad), ad); err != nil || !bytes.Equal(got, plaintext) {
			t.Fatalf("fallback Open of reference output: %q, %v", got, err)
		}
	}
	if _, err := fallback.Open(nil, nil, make([]byte, 5), nil); err == nil {
		t.Fatal("fallback Open accepted a ciphertext shorter than the nonce")
	}
}
