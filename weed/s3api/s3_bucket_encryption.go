package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// ErrNoEncryptionConfig is returned when a bucket has no encryption configuration
var ErrNoEncryptionConfig = errors.New("no encryption configuration found")

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

// GetBucketEncryptionHandler handles GET bucket encryption requests
func (s3a *S3ApiServer) GetBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
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

// PutBucketEncryptionHandler handles PUT bucket encryption requests
func (s3a *S3ApiServer) PutBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
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

// DeleteBucketEncryptionHandler handles DELETE bucket encryption requests
func (s3a *S3ApiServer) DeleteBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
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
			return nil, ErrNoEncryptionConfig
		}
		return nil, fmt.Errorf("failed to get encryption configuration")
	}
	return config, nil
}

// Internal methods following the bucket configuration pattern

// getEncryptionConfiguration retrieves encryption configuration with caching
func (s3a *S3ApiServer) getEncryptionConfiguration(bucket string) (*s3_pb.EncryptionConfiguration, s3err.ErrorCode) {
	// Get metadata using structured API
	metadata, err := s3a.GetBucketMetadata(bucket)
	if err != nil {
		glog.Errorf("getEncryptionConfiguration: failed to get bucket metadata for bucket %s: %v", bucket, err)
		return nil, s3err.ErrInternalError
	}

	if metadata.Encryption == nil {
		return nil, s3err.ErrNoSuchBucketEncryptionConfiguration
	}

	return metadata.Encryption, s3err.ErrNone
}

// updateEncryptionConfiguration updates the encryption configuration for a bucket
func (s3a *S3ApiServer) updateEncryptionConfiguration(bucket string, encryptionConfig *s3_pb.EncryptionConfiguration) s3err.ErrorCode {
	// Update using structured API
	// Note: UpdateBucketEncryption -> UpdateBucketMetadata -> setBucketMetadata
	// already invalidates the cache synchronously after successful update
	err := s3a.UpdateBucketEncryption(bucket, encryptionConfig)
	if err != nil {
		glog.Errorf("updateEncryptionConfiguration: failed to update encryption config for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	return s3err.ErrNone
}

// removeEncryptionConfiguration removes the encryption configuration for a bucket
func (s3a *S3ApiServer) removeEncryptionConfiguration(bucket string) s3err.ErrorCode {
	// Check if encryption configuration exists
	metadata, err := s3a.GetBucketMetadata(bucket)
	if err != nil {
		glog.Errorf("removeEncryptionConfiguration: failed to get bucket metadata for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	if metadata.Encryption == nil {
		return s3err.ErrNoSuchBucketEncryptionConfiguration
	}

	// Update using structured API
	// Note: ClearBucketEncryption -> UpdateBucketMetadata -> setBucketMetadata
	// already invalidates the cache synchronously after successful update
	err = s3a.ClearBucketEncryption(bucket)
	if err != nil {
		glog.Errorf("removeEncryptionConfiguration: failed to remove encryption config for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	return s3err.ErrNone
}

// IsDefaultEncryptionEnabled checks if default encryption is enabled for a bucket
func (s3a *S3ApiServer) IsDefaultEncryptionEnabled(bucket string) bool {
	config, err := s3a.GetBucketEncryptionConfig(bucket)
	if err != nil {
		glog.V(4).Infof("IsDefaultEncryptionEnabled: failed to get encryption config for bucket %s: %v", bucket, err)
		return false
	}
	if config == nil {
		return false
	}
	return config.SseAlgorithm != ""
}

// GetDefaultEncryptionHeaders returns the default encryption headers for a bucket
func (s3a *S3ApiServer) GetDefaultEncryptionHeaders(bucket string) map[string]string {
	config, err := s3a.GetBucketEncryptionConfig(bucket)
	if err != nil {
		glog.V(4).Infof("GetDefaultEncryptionHeaders: failed to get encryption config for bucket %s: %v", bucket, err)
		return nil
	}
	if config == nil {
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

// IsDefaultEncryptionEnabled checks if default encryption is enabled for a configuration
func IsDefaultEncryptionEnabled(config *s3_pb.EncryptionConfiguration) bool {
	return config != nil && config.SseAlgorithm != ""
}

// GetDefaultEncryptionHeaders generates default encryption headers from configuration
func GetDefaultEncryptionHeaders(config *s3_pb.EncryptionConfiguration) map[string]string {
	if config == nil || config.SseAlgorithm == "" {
		return nil
	}

	headers := make(map[string]string)
	headers[s3_constants.AmzServerSideEncryption] = config.SseAlgorithm

	if config.SseAlgorithm == "aws:kms" && config.KmsKeyId != "" {
		headers[s3_constants.AmzServerSideEncryptionAwsKmsKeyId] = config.KmsKeyId
	}

	return headers
}

// encryptionConfigFromXMLBytes parses XML bytes to encryption configuration
func encryptionConfigFromXMLBytes(xmlBytes []byte) (*s3_pb.EncryptionConfiguration, error) {
	var xmlConfig ServerSideEncryptionConfiguration
	if err := xml.Unmarshal(xmlBytes, &xmlConfig); err != nil {
		return nil, err
	}

	// Validate namespace - should be empty or the standard AWS namespace
	if xmlConfig.XMLName.Space != "" && xmlConfig.XMLName.Space != "http://s3.amazonaws.com/doc/2006-03-01/" {
		return nil, fmt.Errorf("invalid XML namespace: %s", xmlConfig.XMLName.Space)
	}

	// Validate the configuration
	if len(xmlConfig.Rules) == 0 {
		return nil, fmt.Errorf("encryption configuration must have at least one rule")
	}

	rule := xmlConfig.Rules[0]
	if rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm == "" {
		return nil, fmt.Errorf("encryption algorithm is required")
	}

	// Validate algorithm
	validAlgorithms := map[string]bool{
		"AES256":  true,
		"aws:kms": true,
	}

	if !validAlgorithms[rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm] {
		return nil, fmt.Errorf("unsupported encryption algorithm: %s", rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm)
	}

	config := encryptionConfigFromXML(&xmlConfig)
	return config, nil
}

// encryptionConfigToXMLBytes converts encryption configuration to XML bytes
func encryptionConfigToXMLBytes(config *s3_pb.EncryptionConfiguration) ([]byte, error) {
	if config == nil {
		return nil, fmt.Errorf("encryption configuration is nil")
	}

	xmlConfig := encryptionConfigToXML(config)
	return xml.Marshal(xmlConfig)
}
