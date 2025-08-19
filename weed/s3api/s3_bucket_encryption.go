package s3api

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/cors"
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

// encryptionConfigToProto converts EncryptionConfiguration to protobuf format
func encryptionConfigToProto(config *s3_pb.EncryptionConfiguration) *s3_pb.EncryptionConfiguration {
	if config == nil {
		return nil
	}
	return &s3_pb.EncryptionConfiguration{
		SseAlgorithm:     config.SseAlgorithm,
		KmsKeyId:         config.KmsKeyId,
		BucketKeyEnabled: config.BucketKeyEnabled,
	}
}

// encryptionConfigFromXML converts XML ServerSideEncryptionConfiguration to protobuf
func encryptionConfigFromXML(xmlConfig *ServerSideEncryptionConfiguration) *s3_pb.EncryptionConfiguration {
	if xmlConfig == nil || len(xmlConfig.Rules) == 0 {
		return nil
	}

	rule := xmlConfig.Rules[0] // AWS S3 supports only one rule
	return &s3_pb.EncryptionConfiguration{
		SseAlgorithm:     rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm,
		KmsKeyId:         rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID,
		BucketKeyEnabled: rule.BucketKeyEnabled != nil && *rule.BucketKeyEnabled,
	}
}

// encryptionConfigToXML converts protobuf EncryptionConfiguration to XML
func encryptionConfigToXML(config *s3_pb.EncryptionConfiguration) *ServerSideEncryptionConfiguration {
	if config == nil {
		return nil
	}

	return &ServerSideEncryptionConfiguration{
		Rules: []ServerSideEncryptionRule{
			{
				ApplyServerSideEncryptionByDefault: ApplyServerSideEncryptionByDefault{
					SSEAlgorithm:   config.SseAlgorithm,
					KMSMasterKeyID: config.KmsKeyId,
				},
				BucketKeyEnabled: &config.BucketKeyEnabled,
			},
		},
	}
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
	config, errCode := s3a.getEncryptionConfiguration(bucket)
	if errCode != s3err.ErrNone {
		if errCode == s3err.ErrNoSuchBucketEncryptionConfiguration {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucketEncryptionConfiguration)
			return
		}
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Convert protobuf config to S3 XML response
	response := encryptionConfigToXML(config)
	if response == nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucketEncryptionConfiguration)
		return
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

	var xmlConfig ServerSideEncryptionConfiguration
	if err := xml.Unmarshal(body, &xmlConfig); err != nil {
		glog.Errorf("Failed to parse bucket encryption configuration: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	// Validate the configuration
	if len(xmlConfig.Rules) == 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	rule := xmlConfig.Rules[0] // AWS S3 supports only one rule

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

	// Convert XML to protobuf configuration
	encryptionConfig := encryptionConfigFromXML(&xmlConfig)

	// Update the bucket configuration
	errCode := s3a.updateEncryptionConfiguration(bucket, encryptionConfig)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// DeleteBucketEncryption handles DELETE bucket encryption requests
func (s3a *S3ApiServer) DeleteBucketEncryption(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	errCode := s3a.removeEncryptionConfiguration(bucket)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetBucketEncryptionConfig retrieves the bucket encryption configuration for internal use
func (s3a *S3ApiServer) GetBucketEncryptionConfig(bucket string) (*s3_pb.EncryptionConfiguration, error) {
	config, errCode := s3a.getEncryptionConfiguration(bucket)
	if errCode != s3err.ErrNone {
		if errCode == s3err.ErrNoSuchBucketEncryptionConfiguration {
			return nil, fmt.Errorf("no encryption configuration found")
		}
		return nil, fmt.Errorf("failed to get encryption configuration")
	}
	return config, nil
}

// Internal methods following the bucket configuration pattern

// getEncryptionConfiguration retrieves encryption configuration with caching
func (s3a *S3ApiServer) getEncryptionConfiguration(bucket string) (*s3_pb.EncryptionConfiguration, s3err.ErrorCode) {
	// Get existing metadata
	_, _, encryptionConfig, err := s3a.getBucketEncryptionMetadata(bucket)
	if err != nil {
		if err.Error() == "no encryption configuration found" {
			return nil, s3err.ErrNoSuchBucketEncryptionConfiguration
		}
		glog.Errorf("getEncryptionConfiguration: failed to get bucket metadata for bucket %s: %v", bucket, err)
		return nil, s3err.ErrInternalError
	}

	return encryptionConfig, s3err.ErrNone
}

// updateEncryptionConfiguration updates the encryption configuration for a bucket
func (s3a *S3ApiServer) updateEncryptionConfiguration(bucket string, encryptionConfig *s3_pb.EncryptionConfiguration) s3err.ErrorCode {
	// Get existing metadata
	existingTags, existingCors, _, err := s3a.getBucketEncryptionMetadata(bucket)
	if err != nil {
		glog.Errorf("updateEncryptionConfiguration: failed to get bucket metadata for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	// Store updated metadata
	if err := s3a.setBucketEncryptionMetadata(bucket, existingTags, existingCors, encryptionConfig); err != nil {
		glog.Errorf("updateEncryptionConfiguration: failed to persist encryption config to bucket content for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	// Cache will be updated automatically via metadata subscription
	return s3err.ErrNone
}

// removeEncryptionConfiguration removes the encryption configuration for a bucket
func (s3a *S3ApiServer) removeEncryptionConfiguration(bucket string) s3err.ErrorCode {
	// Get existing metadata
	existingTags, existingCors, existingEncryption, err := s3a.getBucketEncryptionMetadata(bucket)
	if err != nil {
		if err.Error() == "no encryption configuration found" {
			return s3err.ErrNoSuchBucketEncryptionConfiguration
		}
		glog.Errorf("removeEncryptionConfiguration: failed to get bucket metadata for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	if existingEncryption == nil {
		return s3err.ErrNoSuchBucketEncryptionConfiguration
	}

	// Store metadata without encryption config
	if err := s3a.setBucketEncryptionMetadata(bucket, existingTags, existingCors, nil); err != nil {
		glog.Errorf("removeEncryptionConfiguration: failed to remove encryption config from bucket content for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	// Cache will be updated automatically via metadata subscription
	return s3err.ErrNone
}

// getBucketEncryptionMetadata retrieves bucket metadata including encryption configuration
func (s3a *S3ApiServer) getBucketEncryptionMetadata(bucket string) (map[string]string, *s3_pb.CORSConfiguration, *s3_pb.EncryptionConfiguration, error) {
	// Convert CORS from internal type to protobuf type
	tags, corsConfigInternal, encryptionConfig, err := s3a.getBucketMetadataWithEncryption(bucket)
	if err != nil {
		return nil, nil, nil, err
	}

	// Convert internal CORS to protobuf CORS
	var corsConfig *s3_pb.CORSConfiguration
	if corsConfigInternal != nil {
		corsConfig = corsConfigToProto(corsConfigInternal)
	}

	// If no encryption config, return error to indicate missing configuration
	if encryptionConfig == nil {
		return tags, corsConfig, nil, fmt.Errorf("no encryption configuration found")
	}

	return tags, corsConfig, encryptionConfig, nil
}

// setBucketEncryptionMetadata stores bucket metadata including encryption configuration
func (s3a *S3ApiServer) setBucketEncryptionMetadata(bucket string, tags map[string]string, corsConfig *s3_pb.CORSConfiguration, encryptionConfig *s3_pb.EncryptionConfiguration) error {
	// Convert protobuf CORS to internal CORS
	var corsConfigInternal *cors.CORSConfiguration
	if corsConfig != nil {
		corsConfigInternal = corsConfigFromProto(corsConfig)
	}

	return s3a.setBucketMetadataWithEncryption(bucket, tags, corsConfigInternal, encryptionConfig)
}

// IsDefaultEncryptionEnabled checks if default encryption is enabled for a bucket
func (s3a *S3ApiServer) IsDefaultEncryptionEnabled(bucket string) bool {
	config, err := s3a.GetBucketEncryptionConfig(bucket)
	if err != nil || config == nil {
		return false
	}
	return config.SseAlgorithm != ""
}

// GetDefaultEncryptionHeaders returns the default encryption headers for a bucket
func (s3a *S3ApiServer) GetDefaultEncryptionHeaders(bucket string) map[string]string {
	config, err := s3a.GetBucketEncryptionConfig(bucket)
	if err != nil || config == nil {
		return nil
	}

	headers := make(map[string]string)
	headers[s3_constants.AmzServerSideEncryption] = config.SseAlgorithm

	if config.SseAlgorithm == EncryptionTypeKMS && config.KmsKeyId != "" {
		headers[s3_constants.AmzServerSideEncryptionAwsKmsKeyId] = config.KmsKeyId
	}

	if config.BucketKeyEnabled {
		headers[s3_constants.AmzServerSideEncryptionBucketKeyEnabled] = "true"
	}

	return headers
}
