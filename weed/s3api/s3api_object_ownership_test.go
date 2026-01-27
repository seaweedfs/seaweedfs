package s3api

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestSetObjectOwnerFromRequest(t *testing.T) {
	tests := []struct {
		name                string
		bucketRegistryNil   bool
		bucketMetadata      *BucketMetaData
		bucketMetadataError s3err.ErrorCode
		uploaderAccountId   string
		expectedOwnerId     string
		description         string
	}{
		{
			name:              "BucketOwnerEnforced - use bucket owner",
			bucketRegistryNil: false,
			bucketMetadata: &BucketMetaData{
				Name:            "test-bucket",
				ObjectOwnership: s3_constants.OwnershipBucketOwnerEnforced,
				Owner: &s3.Owner{
					ID:          stringPtr("bucket-owner-123"),
					DisplayName: stringPtr("Bucket Owner"),
				},
			},
			bucketMetadataError: s3err.ErrNone,
			uploaderAccountId:   "uploader-456",
			expectedOwnerId:     "bucket-owner-123",
			description:         "Should use bucket owner when BucketOwnerEnforced",
		},
		{
			name:              "ObjectWriter - use uploader",
			bucketRegistryNil: false,
			bucketMetadata: &BucketMetaData{
				Name:            "test-bucket",
				ObjectOwnership: s3_constants.OwnershipObjectWriter,
				Owner: &s3.Owner{
					ID:          stringPtr("bucket-owner-123"),
					DisplayName: stringPtr("Bucket Owner"),
				},
			},
			bucketMetadataError: s3err.ErrNone,
			uploaderAccountId:   "uploader-456",
			expectedOwnerId:     "uploader-456",
			description:         "Should use uploader when ObjectWriter mode",
		},
		{
			name:              "BucketOwnerPreferred - use uploader",
			bucketRegistryNil: false,
			bucketMetadata: &BucketMetaData{
				Name:            "test-bucket",
				ObjectOwnership: s3_constants.OwnershipBucketOwnerPreferred,
				Owner: &s3.Owner{
					ID:          stringPtr("bucket-owner-123"),
					DisplayName: stringPtr("Bucket Owner"),
				},
			},
			bucketMetadataError: s3err.ErrNone,
			uploaderAccountId:   "uploader-456",
			expectedOwnerId:     "uploader-456",
			description:         "Should use uploader when BucketOwnerPreferred mode",
		},
		{
			name:              "BucketOwnerEnforced but owner is nil - fallback to uploader",
			bucketRegistryNil: false,
			bucketMetadata: &BucketMetaData{
				Name:            "test-bucket",
				ObjectOwnership: s3_constants.OwnershipBucketOwnerEnforced,
				Owner:           nil,
			},
			bucketMetadataError: s3err.ErrNone,
			uploaderAccountId:   "uploader-456",
			expectedOwnerId:     "uploader-456",
			description:         "Should fallback to uploader when bucket owner is nil",
		},
		{
			name:              "BucketOwnerEnforced but owner ID is nil - fallback to uploader",
			bucketRegistryNil: false,
			bucketMetadata: &BucketMetaData{
				Name:            "test-bucket",
				ObjectOwnership: s3_constants.OwnershipBucketOwnerEnforced,
				Owner: &s3.Owner{
					ID:          nil,
					DisplayName: stringPtr("Bucket Owner"),
				},
			},
			bucketMetadataError: s3err.ErrNone,
			uploaderAccountId:   "uploader-456",
			expectedOwnerId:     "uploader-456",
			description:         "Should fallback to uploader when bucket owner ID is nil",
		},
		{
			name:                "Bucket metadata error - fallback to uploader",
			bucketRegistryNil:   false,
			bucketMetadata:      nil,
			bucketMetadataError: s3err.ErrNoSuchBucket,
			uploaderAccountId:   "uploader-456",
			expectedOwnerId:     "uploader-456",
			description:         "Should fallback to uploader when bucket metadata unavailable",
		},
		{
			name:              "Bucket registry is nil - fallback to uploader",
			bucketRegistryNil: true,
			uploaderAccountId: "uploader-456",
			expectedOwnerId:   "uploader-456",
			description:       "Should fallback to uploader when bucketRegistry is nil",
		},
		{
			name:              "Empty uploader account ID - no owner set",
			bucketRegistryNil: false,
			bucketMetadata: &BucketMetaData{
				Name:            "test-bucket",
				ObjectOwnership: s3_constants.OwnershipObjectWriter,
				Owner: &s3.Owner{
					ID:          stringPtr("bucket-owner-123"),
					DisplayName: stringPtr("Bucket Owner"),
				},
			},
			bucketMetadataError: s3err.ErrNone,
			uploaderAccountId:   "",
			expectedOwnerId:     "",
			description:         "Should not set owner when uploader account ID is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock S3ApiServer
			s3a := &S3ApiServer{}

			// Setup bucket registry with mock behavior
			if !tt.bucketRegistryNil {
				// Create a minimal BucketRegistry with overridden GetBucketMetadata
				s3a.bucketRegistry = &BucketRegistry{
					metadataCache: map[string]*BucketMetaData{},
					notFound:      map[string]struct{}{},
				}

				// Pre-populate the cache with test metadata
				if tt.bucketMetadata != nil && tt.bucketMetadataError == s3err.ErrNone {
					s3a.bucketRegistry.metadataCache["test-bucket"] = tt.bucketMetadata
				} else if tt.bucketMetadataError == s3err.ErrNoSuchBucket {
					s3a.bucketRegistry.notFound["test-bucket"] = struct{}{}
				}
			}

			// Create mock request with uploader account ID
			req, _ := http.NewRequest("PUT", "/test-bucket/test-object", nil)
			req.Header.Set(s3_constants.AmzAccountId, tt.uploaderAccountId)

			// Create entry
			entry := &filer_pb.Entry{
				Name: "test-object",
			}

			// Call the function
			s3a.setObjectOwnerFromRequest(req, "test-bucket", entry)

			// Verify the owner ID
			if tt.expectedOwnerId == "" {
				if entry.Extended != nil {
					if _, exists := entry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
						t.Errorf("%s: Expected no owner to be set, but owner was set", tt.description)
					}
				}
			} else {
				if entry.Extended == nil {
					t.Errorf("%s: Expected owner to be set, but Extended is nil", tt.description)
					return
				}
				ownerBytes, exists := entry.Extended[s3_constants.ExtAmzOwnerKey]
				if !exists {
					t.Errorf("%s: Expected owner to be set, but ExtAmzOwnerKey not found", tt.description)
					return
				}
				actualOwnerId := string(ownerBytes)
				if actualOwnerId != tt.expectedOwnerId {
					t.Errorf("%s: Expected owner ID %s, got %s", tt.description, tt.expectedOwnerId, actualOwnerId)
				}
			}
		})
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
