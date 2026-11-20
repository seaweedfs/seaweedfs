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

	gcm, err := newGCMWithRandomNonce(c)
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

	gcm, err := newGCMWithRandomNonce(c)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < gcm.Overhead() {
		return nil, errors.New("ciphertext too short")
	}

	return gcm.Open(nil, nil, ciphertext, nil)
}

// newGCMWithRandomNonce returns cipher.NewGCMWithRandomNonce(block). That
// function accepts only the standard library's own AES block, so for an AES
// block from another implementation, such as a GOEXPERIMENT=boringcrypto
// build's, it falls back to cipher.NewGCM with a nonce from crypto/rand. The
// output is nonce || ciphertext || tag either way.
func newGCMWithRandomNonce(block cipher.Block) (cipher.AEAD, error) {
	if gcm, err := cipher.NewGCMWithRandomNonce(block); err == nil {
		return gcm, nil
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return randomNonceGCM{gcm}, nil
}

// randomNonceGCM gives an AEAD from cipher.NewGCM the interface of one from
// cipher.NewGCMWithRandomNonce: Seal takes no nonce and puts a random one in
// front of its output, and Open takes it from there.
type randomNonceGCM struct {
	aead cipher.AEAD
}

func (g randomNonceGCM) NonceSize() int { return 0 }

func (g randomNonceGCM) Overhead() int { return g.aead.NonceSize() + g.aead.Overhead() }

func (g randomNonceGCM) Seal(dst, nonce, plaintext, additionalData []byte) []byte {
	if len(nonce) != 0 {
		panic("util: randomNonceGCM.Seal takes no nonce")
	}
	nonce = make([]byte, g.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		panic(err)
	}
	return g.aead.Seal(append(dst, nonce...), nonce, plaintext, additionalData)
}

func (g randomNonceGCM) Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
	if len(nonce) != 0 {
		return nil, errors.New("util: randomNonceGCM.Open takes no nonce")
	}
	n := g.aead.NonceSize()
	if len(ciphertext) < n {
		return nil, errors.New("ciphertext too short")
	}
	return g.aead.Open(dst, ciphertext[:n], ciphertext[n:], additionalData)
}
