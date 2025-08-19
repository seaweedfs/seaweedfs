package s3api

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// ServerSideEncryptionConfiguration represents the bucket encryption configuration
type ServerSideEncryptionConfiguration struct {
	XMLName xml.Name                   `xml:"ServerSideEncryptionConfiguration"`
	Rules   []ServerSideEncryptionRule `xml:"Rule"`
}

// ServerSideEncryptionRule represents a single encryption rule
type ServerSideEncryptionRule struct {
	ApplyServerSideEncryptionByDefault ApplyServerSideEncryptionByDefault `xml:"ApplyServerSideEncryptionByDefault"`
	BucketKeyEnabled                   *bool                              `xml:"BucketKeyEnabled,omitempty"`
}

// ApplyServerSideEncryptionByDefault specifies the default encryption settings
type ApplyServerSideEncryptionByDefault struct {
	SSEAlgorithm   string `xml:"SSEAlgorithm"`
	KMSMasterKeyID string `xml:"KMSMasterKeyID,omitempty"`
}

// BucketEncryptionConfig represents the internal bucket encryption configuration
type BucketEncryptionConfig struct {
	SSEAlgorithm     string `json:"sseAlgorithm"`     // "AES256" or "aws:kms"
	KMSKeyID         string `json:"kmsKeyId"`         // KMS key ID (optional for aws:kms)
	BucketKeyEnabled bool   `json:"bucketKeyEnabled"` // S3 Bucket Keys optimization
}

// Default encryption algorithms
const (
	EncryptionTypeAES256 = "AES256"
	EncryptionTypeKMS    = "aws:kms"
)

// GetBucketEncryption handles GET bucket encryption requests
func (s3a *S3ApiServer) GetBucketEncryption(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	// Load bucket encryption configuration
	config, err := s3a.loadBucketEncryptionConfig(bucket)
	if err != nil {
		if err == ErrBucketEncryptionNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucketEncryptionConfiguration)
			return
		}
		glog.Errorf("Failed to load bucket encryption config for %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Convert internal config to S3 XML response
	response := &ServerSideEncryptionConfiguration{
		Rules: []ServerSideEncryptionRule{
			{
				ApplyServerSideEncryptionByDefault: ApplyServerSideEncryptionByDefault{
					SSEAlgorithm:   config.SSEAlgorithm,
					KMSMasterKeyID: config.KMSKeyID,
				},
				BucketKeyEnabled: &config.BucketKeyEnabled,
			},
		},
	}

	w.Header().Set("Content-Type", "application/xml")
	if err := xml.NewEncoder(w).Encode(response); err != nil {
		glog.Errorf("Failed to encode bucket encryption response: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
}

// PutBucketEncryption handles PUT bucket encryption requests
func (s3a *S3ApiServer) PutBucketEncryption(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	// Read and parse the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		glog.Errorf("Failed to read request body: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}
	defer r.Body.Close()

	var config ServerSideEncryptionConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		glog.Errorf("Failed to parse bucket encryption configuration: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	// Validate the configuration
	if len(config.Rules) == 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	rule := config.Rules[0] // AWS S3 supports only one rule

	// Validate SSE algorithm
	if rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm != EncryptionTypeAES256 &&
		rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm != EncryptionTypeKMS {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidEncryptionAlgorithm)
		return
	}

	// For aws:kms, validate KMS key if provided
	if rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm == EncryptionTypeKMS {
		keyID := rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID
		if keyID != "" && !isValidKMSKeyID(keyID) {
			s3err.WriteErrorResponse(w, r, s3err.ErrKMSKeyNotFound)
			return
		}
	}

	// Convert to internal configuration format
	bucketConfig := &BucketEncryptionConfig{
		SSEAlgorithm:     rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm,
		KMSKeyID:         rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID,
		BucketKeyEnabled: rule.BucketKeyEnabled != nil && *rule.BucketKeyEnabled,
	}

	// Save the configuration
	if err := s3a.saveBucketEncryptionConfig(bucket, bucketConfig); err != nil {
		glog.Errorf("Failed to save bucket encryption config for %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// DeleteBucketEncryption handles DELETE bucket encryption requests
func (s3a *S3ApiServer) DeleteBucketEncryption(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	if err := s3a.deleteBucketEncryptionConfig(bucket); err != nil {
		if err == ErrBucketEncryptionNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucketEncryptionConfiguration)
			return
		}
		glog.Errorf("Failed to delete bucket encryption config for %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetBucketEncryptionConfig retrieves the bucket encryption configuration for internal use
func (s3a *S3ApiServer) GetBucketEncryptionConfig(bucket string) (*BucketEncryptionConfig, error) {
	return s3a.loadBucketEncryptionConfig(bucket)
}

// Storage layer functions for bucket encryption configuration

var ErrBucketEncryptionNotFound = fmt.Errorf("bucket encryption configuration not found")

// loadBucketEncryptionConfig loads the encryption configuration for a bucket
func (s3a *S3ApiServer) loadBucketEncryptionConfig(bucket string) (*BucketEncryptionConfig, error) {
	var config *BucketEncryptionConfig

	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: fmt.Sprintf("/.seaweedfs/buckets/%s", bucket),
			Name:      "encryption",
		}

		response, err := client.LookupDirectoryEntry(context.Background(), request)
		if err != nil {
			return err
		}

		if response.Entry == nil || response.Entry.Content == nil {
			return ErrBucketEncryptionNotFound
		}

		var cfg BucketEncryptionConfig
		if err := json.Unmarshal(response.Entry.Content, &cfg); err != nil {
			return fmt.Errorf("failed to unmarshal bucket encryption config: %v", err)
		}

		config = &cfg
		return nil
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, ErrBucketEncryptionNotFound
		}
		return nil, err
	}

	return config, nil
}

// saveBucketEncryptionConfig saves the encryption configuration for a bucket
func (s3a *S3ApiServer) saveBucketEncryptionConfig(bucket string, config *BucketEncryptionConfig) error {
	// Serialize the configuration
	configData, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket encryption config: %v", err)
	}

	// Save to filer
	return s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Ensure the bucket metadata directory exists
		bucketDir := fmt.Sprintf("/.seaweedfs/buckets/%s", bucket)

		_, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: bucketDir,
			Entry: &filer_pb.Entry{
				Name:        "encryption",
				IsDirectory: false,
				Content:     configData,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					FileMode: 0644,
					FileSize: uint64(len(configData)),
				},
			},
		})

		return err
	})
}

// deleteBucketEncryptionConfig deletes the encryption configuration for a bucket
func (s3a *S3ApiServer) deleteBucketEncryptionConfig(bucket string) error {
	return s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.DeleteEntryRequest{
			Directory:    fmt.Sprintf("/.seaweedfs/buckets/%s", bucket),
			Name:         "encryption",
			IsDeleteData: true,
		}

		_, err := client.DeleteEntry(context.Background(), request)
		if err != nil && strings.Contains(err.Error(), "not found") {
			return ErrBucketEncryptionNotFound
		}

		return err
	})
}

// IsDefaultEncryptionEnabled checks if default encryption is enabled for a bucket
func (s3a *S3ApiServer) IsDefaultEncryptionEnabled(bucket string) bool {
	config, err := s3a.GetBucketEncryptionConfig(bucket)
	if err != nil || config == nil {
		return false
	}
	return config.SSEAlgorithm != ""
}

// GetDefaultEncryptionHeaders returns the default encryption headers for a bucket
func (s3a *S3ApiServer) GetDefaultEncryptionHeaders(bucket string) map[string]string {
	config, err := s3a.GetBucketEncryptionConfig(bucket)
	if err != nil || config == nil {
		return nil
	}

	headers := make(map[string]string)
	headers[s3_constants.AmzServerSideEncryption] = config.SSEAlgorithm

	if config.SSEAlgorithm == EncryptionTypeKMS && config.KMSKeyID != "" {
		headers[s3_constants.AmzServerSideEncryptionAwsKmsKeyId] = config.KMSKeyID
	}

	if config.BucketKeyEnabled {
		headers[s3_constants.AmzServerSideEncryptionBucketKeyEnabled] = "true"
	}

	return headers
}
