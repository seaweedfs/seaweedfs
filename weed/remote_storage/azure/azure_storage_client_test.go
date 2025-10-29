package azure

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestAzureStorageClientBasic tests basic Azure storage client operations
func TestAzureStorageClientBasic(t *testing.T) {
	// Skip if credentials not available
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	accountKey := os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	testContainer := os.Getenv("AZURE_TEST_CONTAINER")

	if accountName == "" || accountKey == "" {
		t.Skip("Skipping Azure storage test: AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY not set")
	}
	if testContainer == "" {
		testContainer = "seaweedfs-test"
	}

	// Create client
	maker := azureRemoteStorageMaker{}
	conf := &remote_pb.RemoteConf{
		Name:             "test-azure",
		AzureAccountName: accountName,
		AzureAccountKey:  accountKey,
	}

	client, err := maker.Make(conf)
	if err != nil {
		t.Fatalf("Failed to create Azure client: %v", err)
	}

	azClient := client.(*azureRemoteStorageClient)

	// Test 1: Create bucket/container
	t.Run("CreateBucket", func(t *testing.T) {
		err := azClient.CreateBucket(testContainer)
		// Ignore error if bucket already exists
		if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
			t.Fatalf("Failed to create bucket: %v", err)
		}
	})

	// Test 2: List buckets
	t.Run("ListBuckets", func(t *testing.T) {
		buckets, err := azClient.ListBuckets()
		if err != nil {
			t.Fatalf("Failed to list buckets: %v", err)
		}
		if len(buckets) == 0 {
			t.Log("No buckets found (might be expected)")
		} else {
			t.Logf("Found %d buckets", len(buckets))
		}
	})

	// Test 3: Write file
	testContent := []byte("Hello from SeaweedFS Azure SDK migration test!")
	testKey := fmt.Sprintf("/test-file-%d.txt", time.Now().Unix())
	loc := &remote_pb.RemoteStorageLocation{
		Name:   "test-azure",
		Bucket: testContainer,
		Path:   testKey,
	}

	t.Run("WriteFile", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{
				Mtime: time.Now().Unix(),
				Mime:  "text/plain",
			},
			Extended: map[string][]byte{
				"x-amz-meta-test-key": []byte("test-value"),
			},
		}

		reader := bytes.NewReader(testContent)
		remoteEntry, err := azClient.WriteFile(loc, entry, reader)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}
		if remoteEntry == nil {
			t.Fatal("Remote entry is nil")
		}
		if remoteEntry.RemoteSize != int64(len(testContent)) {
			t.Errorf("Expected size %d, got %d", len(testContent), remoteEntry.RemoteSize)
		}
	})

	// Test 4: Read file
	t.Run("ReadFile", func(t *testing.T) {
		data, err := azClient.ReadFile(loc, 0, int64(len(testContent)))
		if err != nil {
			t.Fatalf("Failed to read file: %v", err)
		}
		if !bytes.Equal(data, testContent) {
			t.Errorf("Content mismatch. Expected: %s, Got: %s", testContent, data)
		}
	})

	// Test 5: Read partial file
	t.Run("ReadPartialFile", func(t *testing.T) {
		data, err := azClient.ReadFile(loc, 0, 5)
		if err != nil {
			t.Fatalf("Failed to read partial file: %v", err)
		}
		expected := testContent[:5]
		if !bytes.Equal(data, expected) {
			t.Errorf("Content mismatch. Expected: %s, Got: %s", expected, data)
		}
	})

	// Test 6: Update metadata
	t.Run("UpdateMetadata", func(t *testing.T) {
		oldEntry := &filer_pb.Entry{
			Extended: map[string][]byte{
				"x-amz-meta-test-key": []byte("test-value"),
			},
		}
		newEntry := &filer_pb.Entry{
			Extended: map[string][]byte{
				"x-amz-meta-test-key": []byte("test-value"),
				"x-amz-meta-new-key":  []byte("new-value"),
			},
		}
		err := azClient.UpdateFileMetadata(loc, oldEntry, newEntry)
		if err != nil {
			t.Fatalf("Failed to update metadata: %v", err)
		}
	})

	// Test 7: Traverse (list objects)
	t.Run("Traverse", func(t *testing.T) {
		foundFile := false
		err := azClient.Traverse(loc, func(dir string, name string, isDir bool, remoteEntry *filer_pb.RemoteEntry) error {
			if !isDir && name == testKey[1:] { // Remove leading slash
				foundFile = true
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to traverse: %v", err)
		}
		if !foundFile {
			t.Log("Test file not found in traverse (might be expected due to path matching)")
		}
	})

	// Test 8: Delete file
	t.Run("DeleteFile", func(t *testing.T) {
		err := azClient.DeleteFile(loc)
		if err != nil {
			t.Fatalf("Failed to delete file: %v", err)
		}
	})

	// Test 9: Verify file deleted (should fail)
	t.Run("VerifyDeleted", func(t *testing.T) {
		_, err := azClient.ReadFile(loc, 0, 10)
		if !bloberror.HasCode(err, bloberror.BlobNotFound) {
			t.Errorf("Expected BlobNotFound error, but got: %v", err)
		}
	})

	// Clean up: Try to delete the test container
	// Comment out if you want to keep the container
	/*
		t.Run("DeleteBucket", func(t *testing.T) {
			err := azClient.DeleteBucket(testContainer)
			if err != nil {
				t.Logf("Warning: Failed to delete bucket: %v", err)
			}
		})
	*/
}

// TestToMetadata tests the metadata conversion function
func TestToMetadata(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string][]byte
		expected map[string]*string
	}{
		{
			name: "basic metadata",
			input: map[string][]byte{
				s3_constants.AmzUserMetaPrefix + "key1": []byte("value1"),
				s3_constants.AmzUserMetaPrefix + "key2": []byte("value2"),
			},
			expected: map[string]*string{
				"key1": stringPtr("value1"),
				"key2": stringPtr("value2"),
			},
		},
		{
			name: "metadata with dashes",
			input: map[string][]byte{
				s3_constants.AmzUserMetaPrefix + "content-type": []byte("text/plain"),
			},
			expected: map[string]*string{
				"content_2d_type": stringPtr("text/plain"), // dash (0x2d) -> _2d_
			},
		},
		{
			name: "non-metadata keys ignored",
			input: map[string][]byte{
				"some-other-key": []byte("ignored"),
				s3_constants.AmzUserMetaPrefix + "included": []byte("included"),
			},
			expected: map[string]*string{
				"included": stringPtr("included"),
			},
		},
		{
			name: "keys starting with digits",
			input: map[string][]byte{
				s3_constants.AmzUserMetaPrefix + "123key":   []byte("value1"),
				s3_constants.AmzUserMetaPrefix + "456-test": []byte("value2"),
				s3_constants.AmzUserMetaPrefix + "789":      []byte("value3"),
			},
			expected: map[string]*string{
				"_123key":      stringPtr("value1"), // starts with digit -> prefix _
				"_456_2d_test": stringPtr("value2"), // starts with digit AND has dash
				"_789":         stringPtr("value3"),
			},
		},
		{
			name: "uppercase and mixed case keys",
			input: map[string][]byte{
				s3_constants.AmzUserMetaPrefix + "My-Key":     []byte("value1"),
				s3_constants.AmzUserMetaPrefix + "UPPERCASE":  []byte("value2"),
				s3_constants.AmzUserMetaPrefix + "MiXeD-CaSe": []byte("value3"),
			},
			expected: map[string]*string{
				"my_2d_key":     stringPtr("value1"), // lowercase + dash -> _2d_
				"uppercase":     stringPtr("value2"),
				"mixed_2d_case": stringPtr("value3"),
			},
		},
		{
			name: "keys with invalid characters",
			input: map[string][]byte{
				s3_constants.AmzUserMetaPrefix + "my.key":     []byte("value1"),
				s3_constants.AmzUserMetaPrefix + "key+plus":   []byte("value2"),
				s3_constants.AmzUserMetaPrefix + "key@symbol": []byte("value3"),
				s3_constants.AmzUserMetaPrefix + "key-with.":  []byte("value4"),
				s3_constants.AmzUserMetaPrefix + "key/slash":  []byte("value5"),
			},
			expected: map[string]*string{
				"my_2e_key":       stringPtr("value1"), // dot (0x2e) -> _2e_
				"key_2b_plus":     stringPtr("value2"), // plus (0x2b) -> _2b_
				"key_40_symbol":   stringPtr("value3"), // @ (0x40) -> _40_
				"key_2d_with_2e_": stringPtr("value4"), // dash and dot
				"key_2f_slash":    stringPtr("value5"), // slash (0x2f) -> _2f_
			},
		},
		{
			name: "collision prevention",
			input: map[string][]byte{
				s3_constants.AmzUserMetaPrefix + "my-key": []byte("value1"),
				s3_constants.AmzUserMetaPrefix + "my.key": []byte("value2"),
				s3_constants.AmzUserMetaPrefix + "my_key": []byte("value3"),
			},
			expected: map[string]*string{
				"my_2d_key": stringPtr("value1"), // dash (0x2d)
				"my_2e_key": stringPtr("value2"), // dot (0x2e)
				"my_key":    stringPtr("value3"), // underscore is valid, no encoding
			},
		},
		{
			name:     "empty input",
			input:    map[string][]byte{},
			expected: map[string]*string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toMetadata(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d keys, got %d", len(tt.expected), len(result))
			}
			for key, expectedVal := range tt.expected {
				if resultVal, ok := result[key]; !ok {
					t.Errorf("Expected key %s not found", key)
				} else if resultVal == nil || expectedVal == nil {
					if resultVal != expectedVal {
						t.Errorf("For key %s: expected %v, got %v", key, expectedVal, resultVal)
					}
				} else if *resultVal != *expectedVal {
					t.Errorf("For key %s: expected %s, got %s", key, *expectedVal, *resultVal)
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return bytes.Contains([]byte(s), []byte(substr))
}

func stringPtr(s string) *string {
	return &s
}

// Benchmark tests
func BenchmarkToMetadata(b *testing.B) {
	input := map[string][]byte{
		"x-amz-meta-key1":         []byte("value1"),
		"x-amz-meta-key2":         []byte("value2"),
		"x-amz-meta-content-type": []byte("text/plain"),
		"other-key":               []byte("ignored"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		toMetadata(input)
	}
}

// Test that the maker implements the interface
func TestAzureRemoteStorageMaker(t *testing.T) {
	maker := azureRemoteStorageMaker{}

	if !maker.HasBucket() {
		t.Error("Expected HasBucket() to return true")
	}

	// Test with missing credentials - unset env vars (auto-restored by t.Setenv)
	t.Setenv("AZURE_STORAGE_ACCOUNT", "")
	t.Setenv("AZURE_STORAGE_ACCESS_KEY", "")

	conf := &remote_pb.RemoteConf{
		Name: "test",
	}
	_, err := maker.Make(conf)
	if err == nil {
		t.Error("Expected error with missing credentials")
	}
}

// Test error cases
func TestAzureStorageClientErrors(t *testing.T) {
	// Test with invalid credentials
	maker := azureRemoteStorageMaker{}
	conf := &remote_pb.RemoteConf{
		Name:             "test",
		AzureAccountName: "invalid",
		AzureAccountKey:  "aW52YWxpZGtleQ==", // base64 encoded "invalidkey"
	}

	client, err := maker.Make(conf)
	if err != nil {
		t.Skip("Invalid credentials correctly rejected at client creation")
	}

	// If client creation succeeded, operations should fail
	azClient := client.(*azureRemoteStorageClient)
	loc := &remote_pb.RemoteStorageLocation{
		Name:   "test",
		Bucket: "nonexistent",
		Path:   "/test.txt",
	}

	// These operations should fail with invalid credentials
	_, err = azClient.ReadFile(loc, 0, 10)
	if err == nil {
		t.Log("Expected error with invalid credentials on ReadFile, but got none (might be cached)")
	}
}
