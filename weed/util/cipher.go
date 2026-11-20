package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

type CipherKey []byte

func GenCipherKey() CipherKey {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		glog.Fatalf("random key gen: %v", err)
	}
	return CipherKey(key)
}

// Encrypt seals plaintext with AES-GCM under a random 12-byte nonce. The
// output is nonce || ciphertext || tag. NewGCMWithRandomNonce generates the
// nonce itself, which GODEBUG=fips140=only requires.
func Encrypt(plaintext []byte, key CipherKey) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCMWithRandomNonce(c)
	if err != nil {
		return nil, err
	}

	return gcm.Seal(nil, nil, plaintext, nil), nil
}

// Decrypt opens the nonce || ciphertext || tag layout that Encrypt produces.
func Decrypt(ciphertext []byte, key CipherKey) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCMWithRandomNonce(c)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < gcm.Overhead() {
		return nil, errors.New("ciphertext too short")
	}

	return gcm.Open(nil, nil, ciphertext, nil)
}
