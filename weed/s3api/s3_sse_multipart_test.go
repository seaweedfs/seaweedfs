package s3api

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestSSECMultipartUpload tests SSE-C with multipart uploads
func TestSSECMultipartUpload(t *testing.T) {
	keyPair := GenerateTestSSECKey(1)
	customerKey := &SSECustomerKey{
		Algorithm: "AES256",
		Key:       keyPair.Key,
		KeyMD5:    keyPair.KeyMD5,
	}

	// Test data larger than typical part size
	testData := strings.Repeat("Hello, SSE-C multipart world! ", 1000) // ~30KB

	t.Run("Single part encryption/decryption", func(t *testing.T) {
		// Encrypt the data
		encryptedReader, iv, err := CreateSSECEncryptedReader(strings.NewReader(testData), customerKey)
		if err != nil {
			t.Fatalf("Failed to create encrypted reader: %v", err)
		}

		encryptedData, err := io.ReadAll(encryptedReader)
		if err != nil {
			t.Fatalf("Failed to read encrypted data: %v", err)
		}

		// Decrypt the data
		decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), customerKey, iv)
		if err != nil {
			t.Fatalf("Failed to create decrypted reader: %v", err)
		}

		decryptedData, err := io.ReadAll(decryptedReader)
		if err != nil {
			t.Fatalf("Failed to read decrypted data: %v", err)
		}

		if string(decryptedData) != testData {
			t.Error("Decrypted data doesn't match original")
		}
	})

	t.Run("Simulated multipart upload parts", func(t *testing.T) {
		// Simulate multiple parts (each part gets encrypted separately)
		partSize := 5 * 1024 // 5KB parts
		var encryptedParts [][]byte
		var partIVs [][]byte

		for i := 0; i < len(testData); i += partSize {
			end := i + partSize
			if end > len(testData) {
				end = len(testData)
			}

			partData := testData[i:end]

			// Each part is encrypted separately in multipart uploads
			encryptedReader, iv, err := CreateSSECEncryptedReader(strings.NewReader(partData), customerKey)
			if err != nil {
				t.Fatalf("Failed to create encrypted reader for part %d: %v", i/partSize, err)
			}

			encryptedPart, err := io.ReadAll(encryptedReader)
			if err != nil {
				t.Fatalf("Failed to read encrypted part %d: %v", i/partSize, err)
			}

			encryptedParts = append(encryptedParts, encryptedPart)
			partIVs = append(partIVs, iv)
		}

		// Simulate reading back the multipart object
		var reconstructedData strings.Builder

		for i, encryptedPart := range encryptedParts {
			decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedPart), customerKey, partIVs[i])
			if err != nil {
				t.Fatalf("Failed to create decrypted reader for part %d: %v", i, err)
			}

			decryptedPart, err := io.ReadAll(decryptedReader)
			if err != nil {
				t.Fatalf("Failed to read decrypted part %d: %v", i, err)
			}

			reconstructedData.Write(decryptedPart)
		}

		if reconstructedData.String() != testData {
			t.Error("Reconstructed multipart data doesn't match original")
		}
	})

	t.Run("Multipart with different part sizes", func(t *testing.T) {
		partSizes := []int{1024, 2048, 4096, 8192} // Various part sizes

		for _, partSize := range partSizes {
			t.Run(fmt.Sprintf("PartSize_%d", partSize), func(t *testing.T) {
				var encryptedParts [][]byte
				var partIVs [][]byte

				for i := 0; i < len(testData); i += partSize {
					end := i + partSize
					if end > len(testData) {
						end = len(testData)
					}

					partData := testData[i:end]

					encryptedReader, iv, err := CreateSSECEncryptedReader(strings.NewReader(partData), customerKey)
					if err != nil {
						t.Fatalf("Failed to create encrypted reader: %v", err)
					}

					encryptedPart, err := io.ReadAll(encryptedReader)
					if err != nil {
						t.Fatalf("Failed to read encrypted part: %v", err)
					}

					encryptedParts = append(encryptedParts, encryptedPart)
					partIVs = append(partIVs, iv)
				}

				// Verify reconstruction
				var reconstructedData strings.Builder

				for j, encryptedPart := range encryptedParts {
					decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedPart), customerKey, partIVs[j])
					if err != nil {
						t.Fatalf("Failed to create decrypted reader: %v", err)
					}

					decryptedPart, err := io.ReadAll(decryptedReader)
					if err != nil {
						t.Fatalf("Failed to read decrypted part: %v", err)
					}

					reconstructedData.Write(decryptedPart)
				}

				if reconstructedData.String() != testData {
					t.Errorf("Reconstructed data doesn't match original for part size %d", partSize)
				}
			})
		}
	})
}

// TestSSEKMSMultipartUpload tests SSE-KMS with multipart uploads
func TestSSEKMSMultipartUpload(t *testing.T) {
	kmsKey := SetupTestKMS(t)
	defer kmsKey.Cleanup()

	// Test data larger than typical part size
	testData := strings.Repeat("Hello, SSE-KMS multipart world! ", 1000) // ~30KB
	encryptionContext := BuildEncryptionContext("test-bucket", "test-object", false)

	t.Run("Single part encryption/decryption", func(t *testing.T) {
		// Encrypt the data
		encryptedReader, sseKey, err := CreateSSEKMSEncryptedReader(strings.NewReader(testData), kmsKey.KeyID, encryptionContext)
		if err != nil {
			t.Fatalf("Failed to create encrypted reader: %v", err)
		}

		encryptedData, err := io.ReadAll(encryptedReader)
		if err != nil {
			t.Fatalf("Failed to read encrypted data: %v", err)
		}

		// Decrypt the data
		decryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(encryptedData), sseKey)
		if err != nil {
			t.Fatalf("Failed to create decrypted reader: %v", err)
		}

		decryptedData, err := io.ReadAll(decryptedReader)
		if err != nil {
			t.Fatalf("Failed to read decrypted data: %v", err)
		}

		if string(decryptedData) != testData {
			t.Error("Decrypted data doesn't match original")
		}
	})

	t.Run("Simulated multipart upload parts", func(t *testing.T) {
		// Simulate multiple parts (each part might use the same or different KMS operations)
		partSize := 5 * 1024 // 5KB parts
		var encryptedParts [][]byte
		var sseKeys []*SSEKMSKey

		for i := 0; i < len(testData); i += partSize {
			end := i + partSize
			if end > len(testData) {
				end = len(testData)
			}

			partData := testData[i:end]

			// Each part might get its own data key in KMS multipart uploads
			encryptedReader, sseKey, err := CreateSSEKMSEncryptedReader(strings.NewReader(partData), kmsKey.KeyID, encryptionContext)
			if err != nil {
				t.Fatalf("Failed to create encrypted reader for part %d: %v", i/partSize, err)
			}

			encryptedPart, err := io.ReadAll(encryptedReader)
			if err != nil {
				t.Fatalf("Failed to read encrypted part %d: %v", i/partSize, err)
			}

			encryptedParts = append(encryptedParts, encryptedPart)
			sseKeys = append(sseKeys, sseKey)
		}

		// Simulate reading back the multipart object
		var reconstructedData strings.Builder

		for i, encryptedPart := range encryptedParts {
			decryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(encryptedPart), sseKeys[i])
			if err != nil {
				t.Fatalf("Failed to create decrypted reader for part %d: %v", i, err)
			}

			decryptedPart, err := io.ReadAll(decryptedReader)
			if err != nil {
				t.Fatalf("Failed to read decrypted part %d: %v", i, err)
			}

			reconstructedData.Write(decryptedPart)
		}

		if reconstructedData.String() != testData {
			t.Error("Reconstructed multipart data doesn't match original")
		}
	})

	t.Run("Multipart consistency checks", func(t *testing.T) {
		// Test that all parts use the same KMS key ID but different data keys
		partSize := 5 * 1024
		var sseKeys []*SSEKMSKey

		for i := 0; i < len(testData); i += partSize {
			end := i + partSize
			if end > len(testData) {
				end = len(testData)
			}

			partData := testData[i:end]

			_, sseKey, err := CreateSSEKMSEncryptedReader(strings.NewReader(partData), kmsKey.KeyID, encryptionContext)
			if err != nil {
				t.Fatalf("Failed to create encrypted reader: %v", err)
			}

			sseKeys = append(sseKeys, sseKey)
		}

		// Verify all parts use the same KMS key ID
		for i, sseKey := range sseKeys {
			if sseKey.KeyID != kmsKey.KeyID {
				t.Errorf("Part %d has wrong KMS key ID: expected %s, got %s", i, kmsKey.KeyID, sseKey.KeyID)
			}
		}

		// Verify each part has different encrypted data keys (they should be unique)
		for i := 0; i < len(sseKeys); i++ {
			for j := i + 1; j < len(sseKeys); j++ {
				if bytes.Equal(sseKeys[i].EncryptedDataKey, sseKeys[j].EncryptedDataKey) {
					t.Errorf("Parts %d and %d have identical encrypted data keys (should be unique)", i, j)
				}
			}
		}
	})
}

// TestMultipartSSEMixedScenarios tests edge cases with multipart and SSE
func TestMultipartSSEMixedScenarios(t *testing.T) {
	t.Run("Empty parts handling", func(t *testing.T) {
		keyPair := GenerateTestSSECKey(1)
		customerKey := &SSECustomerKey{
			Algorithm: "AES256",
			Key:       keyPair.Key,
			KeyMD5:    keyPair.KeyMD5,
		}

		// Test empty part
		encryptedReader, iv, err := CreateSSECEncryptedReader(strings.NewReader(""), customerKey)
		if err != nil {
			t.Fatalf("Failed to create encrypted reader for empty data: %v", err)
		}

		encryptedData, err := io.ReadAll(encryptedReader)
		if err != nil {
			t.Fatalf("Failed to read encrypted empty data: %v", err)
		}

		// Empty part should produce empty encrypted data, but still have a valid IV
		if len(encryptedData) != 0 {
			t.Errorf("Expected empty encrypted data for empty part, got %d bytes", len(encryptedData))
		}
		if len(iv) != s3_constants.AESBlockSize {
			t.Errorf("Expected IV of size %d, got %d", s3_constants.AESBlockSize, len(iv))
		}

		// Decrypt and verify
		decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), customerKey, iv)
		if err != nil {
			t.Fatalf("Failed to create decrypted reader for empty data: %v", err)
		}

		decryptedData, err := io.ReadAll(decryptedReader)
		if err != nil {
			t.Fatalf("Failed to read decrypted empty data: %v", err)
		}

		if len(decryptedData) != 0 {
			t.Errorf("Expected empty decrypted data, got %d bytes", len(decryptedData))
		}
	})

	t.Run("Single byte parts", func(t *testing.T) {
		keyPair := GenerateTestSSECKey(1)
		customerKey := &SSECustomerKey{
			Algorithm: "AES256",
			Key:       keyPair.Key,
			KeyMD5:    keyPair.KeyMD5,
		}

		testData := "ABCDEFGHIJ"
		var encryptedParts [][]byte
		var partIVs [][]byte

		// Encrypt each byte as a separate part
		for i, b := range []byte(testData) {
			partData := string(b)

			encryptedReader, iv, err := CreateSSECEncryptedReader(strings.NewReader(partData), customerKey)
			if err != nil {
				t.Fatalf("Failed to create encrypted reader for byte %d: %v", i, err)
			}

			encryptedPart, err := io.ReadAll(encryptedReader)
			if err != nil {
				t.Fatalf("Failed to read encrypted byte %d: %v", i, err)
			}

			encryptedParts = append(encryptedParts, encryptedPart)
			partIVs = append(partIVs, iv)
		}

		// Reconstruct
		var reconstructedData strings.Builder

		for i, encryptedPart := range encryptedParts {
			decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedPart), customerKey, partIVs[i])
			if err != nil {
				t.Fatalf("Failed to create decrypted reader for byte %d: %v", i, err)
			}

			decryptedPart, err := io.ReadAll(decryptedReader)
			if err != nil {
				t.Fatalf("Failed to read decrypted byte %d: %v", i, err)
			}

			reconstructedData.Write(decryptedPart)
		}

		if reconstructedData.String() != testData {
			t.Errorf("Expected %s, got %s", testData, reconstructedData.String())
		}
	})

	t.Run("Very large parts", func(t *testing.T) {
		keyPair := GenerateTestSSECKey(1)
		customerKey := &SSECustomerKey{
			Algorithm: "AES256",
			Key:       keyPair.Key,
			KeyMD5:    keyPair.KeyMD5,
		}

		// Create a large part (1MB)
		largeData := make([]byte, 1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		// Encrypt
		encryptedReader, iv, err := CreateSSECEncryptedReader(bytes.NewReader(largeData), customerKey)
		if err != nil {
			t.Fatalf("Failed to create encrypted reader for large data: %v", err)
		}

		encryptedData, err := io.ReadAll(encryptedReader)
		if err != nil {
			t.Fatalf("Failed to read encrypted large data: %v", err)
		}

		// Decrypt
		decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), customerKey, iv)
		if err != nil {
			t.Fatalf("Failed to create decrypted reader for large data: %v", err)
		}

		decryptedData, err := io.ReadAll(decryptedReader)
		if err != nil {
			t.Fatalf("Failed to read decrypted large data: %v", err)
		}

		if !bytes.Equal(decryptedData, largeData) {
			t.Error("Large data doesn't match after encryption/decryption")
		}
	})
}

// TestMultipartSSEPerformance tests performance characteristics of SSE with multipart
func TestMultipartSSEPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	t.Run("SSE-C performance with multiple parts", func(t *testing.T) {
		keyPair := GenerateTestSSECKey(1)
		customerKey := &SSECustomerKey{
			Algorithm: "AES256",
			Key:       keyPair.Key,
			KeyMD5:    keyPair.KeyMD5,
		}

		partSize := 64 * 1024 // 64KB parts
		numParts := 10

		for partNum := 0; partNum < numParts; partNum++ {
			partData := make([]byte, partSize)
			for i := range partData {
				partData[i] = byte((partNum + i) % 256)
			}

			// Encrypt
			encryptedReader, iv, err := CreateSSECEncryptedReader(bytes.NewReader(partData), customerKey)
			if err != nil {
				t.Fatalf("Failed to create encrypted reader for part %d: %v", partNum, err)
			}

			encryptedData, err := io.ReadAll(encryptedReader)
			if err != nil {
				t.Fatalf("Failed to read encrypted data for part %d: %v", partNum, err)
			}

			// Decrypt
			decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), customerKey, iv)
			if err != nil {
				t.Fatalf("Failed to create decrypted reader for part %d: %v", partNum, err)
			}

			decryptedData, err := io.ReadAll(decryptedReader)
			if err != nil {
				t.Fatalf("Failed to read decrypted data for part %d: %v", partNum, err)
			}

			if !bytes.Equal(decryptedData, partData) {
				t.Errorf("Data mismatch for part %d", partNum)
			}
		}
	})

	t.Run("SSE-KMS performance with multiple parts", func(t *testing.T) {
		kmsKey := SetupTestKMS(t)
		defer kmsKey.Cleanup()

		partSize := 64 * 1024 // 64KB parts
		numParts := 5         // Fewer parts for KMS due to overhead
		encryptionContext := BuildEncryptionContext("test-bucket", "test-object", false)

		for partNum := 0; partNum < numParts; partNum++ {
			partData := make([]byte, partSize)
			for i := range partData {
				partData[i] = byte((partNum + i) % 256)
			}

			// Encrypt
			encryptedReader, sseKey, err := CreateSSEKMSEncryptedReader(bytes.NewReader(partData), kmsKey.KeyID, encryptionContext)
			if err != nil {
				t.Fatalf("Failed to create encrypted reader for part %d: %v", partNum, err)
			}

			encryptedData, err := io.ReadAll(encryptedReader)
			if err != nil {
				t.Fatalf("Failed to read encrypted data for part %d: %v", partNum, err)
			}

			// Decrypt
			decryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(encryptedData), sseKey)
			if err != nil {
				t.Fatalf("Failed to create decrypted reader for part %d: %v", partNum, err)
			}

			decryptedData, err := io.ReadAll(decryptedReader)
			if err != nil {
				t.Fatalf("Failed to read decrypted data for part %d: %v", partNum, err)
			}

			if !bytes.Equal(decryptedData, partData) {
				t.Errorf("Data mismatch for part %d", partNum)
			}
		}
	})
}
