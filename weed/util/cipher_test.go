package util

import (
	"encoding/base64"
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
