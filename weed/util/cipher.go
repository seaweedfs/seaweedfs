package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

type CipherKey []byte

func GenCipherKey() CipherKey {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		glog.Fatalf("random key gen: %v", err)
	}
	return CipherKey(key)
}

func Encrypt(plaintext []byte, key CipherKey) ([]byte, error) {
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

func Decrypt(ciphertext []byte, key CipherKey) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}

func EncryptReader(clearReader io.Reader) (cipherKey CipherKey, encryptedReader io.ReadCloser, clearDataLen, encryptedDataLen int, err error) {
	clearData, err := ioutil.ReadAll(clearReader)
	if err != nil {
		err = fmt.Errorf("read raw input: %v", err)
		return
	}
	clearDataLen = len(clearData)
	cipherKey = GenCipherKey()
	encryptedData, err := Encrypt(clearData, cipherKey)
	if err != nil {
		err = fmt.Errorf("encrypt input: %v", err)
		return
	}
	encryptedDataLen = len(encryptedData)
	encryptedReader = ioutil.NopCloser(bytes.NewReader(encryptedData))
	return
}
