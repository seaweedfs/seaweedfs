package gcp

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"google.golang.org/api/option"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	seaweedkms "github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	// Register the Google Cloud KMS provider
	seaweedkms.RegisterProvider("gcp", NewGCPKMSProvider)
}

// GCPKMSProvider implements the KMSProvider interface using Google Cloud KMS
type GCPKMSProvider struct {
	client    *kms.KeyManagementClient
	projectID string
}

// GCPKMSConfig contains configuration for the Google Cloud KMS provider
type GCPKMSConfig struct {
	ProjectID             string `json:"project_id"`              // GCP project ID
	CredentialsFile       string `json:"credentials_file"`        // Path to service account JSON file
	CredentialsJSON       string `json:"credentials_json"`        // Service account JSON content (base64 encoded)
	UseDefaultCredentials bool   `json:"use_default_credentials"` // Use default GCP credentials (metadata service, gcloud, etc.)
	RequestTimeout        int    `json:"request_timeout"`         // Request timeout in seconds (default: 30)
}

// NewGCPKMSProvider creates a new Google Cloud KMS provider
func NewGCPKMSProvider(config util.Configuration) (seaweedkms.KMSProvider, error) {
	if config == nil {
		return nil, fmt.Errorf("Google Cloud KMS configuration is required")
	}

	// Extract configuration
	projectID := config.GetString("project_id")
	if projectID == "" {
		return nil, fmt.Errorf("project_id is required for Google Cloud KMS provider")
	}

	credentialsFile := config.GetString("credentials_file")
	credentialsJSON := config.GetString("credentials_json")
	useDefaultCredentials := config.GetBool("use_default_credentials")

	requestTimeout := config.GetInt("request_timeout")
	if requestTimeout == 0 {
		requestTimeout = 30 // Default 30 seconds
	}

	// Prepare client options
	var clientOptions []option.ClientOption

	// Configure credentials
	if credentialsFile != "" {
		clientOptions = append(clientOptions, option.WithCredentialsFile(credentialsFile))
		glog.V(1).Infof("GCP KMS: Using credentials file %s", credentialsFile)
	} else if credentialsJSON != "" {
		// Decode base64 credentials if provided
		credBytes, err := base64.StdEncoding.DecodeString(credentialsJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to decode credentials JSON: %w", err)
		}
		clientOptions = append(clientOptions, option.WithCredentialsJSON(credBytes))
		glog.V(1).Infof("GCP KMS: Using provided credentials JSON")
	} else if !useDefaultCredentials {
		return nil, fmt.Errorf("either credentials_file, credentials_json, or use_default_credentials=true must be provided")
	} else {
		glog.V(1).Infof("GCP KMS: Using default credentials")
	}

	// Set request timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(requestTimeout)*time.Second)
	defer cancel()

	// Create KMS client
	client, err := kms.NewKeyManagementClient(ctx, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Google Cloud KMS client: %w", err)
	}

	provider := &GCPKMSProvider{
		client:    client,
		projectID: projectID,
	}

	glog.V(1).Infof("Google Cloud KMS provider initialized for project %s", projectID)
	return provider, nil
}

// GenerateDataKey generates a new data encryption key using Google Cloud KMS
func (p *GCPKMSProvider) GenerateDataKey(ctx context.Context, req *seaweedkms.GenerateDataKeyRequest) (*seaweedkms.GenerateDataKeyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("GenerateDataKeyRequest cannot be nil")
	}

	if req.KeyID == "" {
		return nil, fmt.Errorf("KeyID is required")
	}

	// Validate key spec
	var keySize int
	switch req.KeySpec {
	case seaweedkms.KeySpecAES256:
		keySize = 32 // 256 bits
	default:
		return nil, fmt.Errorf("unsupported key spec: %s", req.KeySpec)
	}

	// Generate data key locally (GCP KMS doesn't have GenerateDataKey like AWS)
	dataKey := make([]byte, keySize)
	if _, err := rand.Read(dataKey); err != nil {
		return nil, fmt.Errorf("failed to generate random data key: %w", err)
	}

	// Encrypt the data key using GCP KMS
	glog.V(4).Infof("GCP KMS: Encrypting data key using key %s", req.KeyID)

	// Build the encryption request
	encryptReq := &kmspb.EncryptRequest{
		Name:      req.KeyID,
		Plaintext: dataKey,
	}

	// Add additional authenticated data from encryption context
	if len(req.EncryptionContext) > 0 {
		// Convert encryption context to additional authenticated data
		aad := p.encryptionContextToAAD(req.EncryptionContext)
		encryptReq.AdditionalAuthenticatedData = []byte(aad)
	}

	// Call GCP KMS to encrypt the data key
	encryptResp, err := p.client.Encrypt(ctx, encryptReq)
	if err != nil {
		return nil, p.convertGCPError(err, req.KeyID)
	}

	// Create standardized envelope format for consistent API behavior
	envelopeBlob, err := seaweedkms.CreateEnvelope("gcp", encryptResp.Name, string(encryptResp.Ciphertext), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create ciphertext envelope: %w", err)
	}

	response := &seaweedkms.GenerateDataKeyResponse{
		KeyID:          encryptResp.Name, // GCP returns the full resource name
		Plaintext:      dataKey,
		CiphertextBlob: envelopeBlob, // Store in standardized envelope format
	}

	glog.V(4).Infof("GCP KMS: Generated and encrypted data key using key %s", req.KeyID)
	return response, nil
}

// Decrypt decrypts an encrypted data key using Google Cloud KMS
func (p *GCPKMSProvider) Decrypt(ctx context.Context, req *seaweedkms.DecryptRequest) (*seaweedkms.DecryptResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("DecryptRequest cannot be nil")
	}

	if len(req.CiphertextBlob) == 0 {
		return nil, fmt.Errorf("CiphertextBlob cannot be empty")
	}

	// Parse the ciphertext envelope to extract key information
	envelope, err := seaweedkms.ParseEnvelope(req.CiphertextBlob)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ciphertext envelope: %w", err)
	}

	keyName := envelope.KeyID
	if keyName == "" {
		return nil, fmt.Errorf("envelope missing key ID")
	}

	// Convert string back to bytes
	ciphertext := []byte(envelope.Ciphertext)

	// Build the decryption request
	decryptReq := &kmspb.DecryptRequest{
		Name:       keyName,
		Ciphertext: ciphertext,
	}

	// Add additional authenticated data from encryption context
	if len(req.EncryptionContext) > 0 {
		aad := p.encryptionContextToAAD(req.EncryptionContext)
		decryptReq.AdditionalAuthenticatedData = []byte(aad)
	}

	// Call GCP KMS to decrypt the data key
	glog.V(4).Infof("GCP KMS: Decrypting data key using key %s", keyName)
	decryptResp, err := p.client.Decrypt(ctx, decryptReq)
	if err != nil {
		return nil, p.convertGCPError(err, keyName)
	}

	response := &seaweedkms.DecryptResponse{
		KeyID:     keyName,
		Plaintext: decryptResp.Plaintext,
	}

	glog.V(4).Infof("GCP KMS: Decrypted data key using key %s", keyName)
	return response, nil
}

// DescribeKey validates that a key exists and returns its metadata
func (p *GCPKMSProvider) DescribeKey(ctx context.Context, req *seaweedkms.DescribeKeyRequest) (*seaweedkms.DescribeKeyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("DescribeKeyRequest cannot be nil")
	}

	if req.KeyID == "" {
		return nil, fmt.Errorf("KeyID is required")
	}

	// Build the request to get the crypto key
	getKeyReq := &kmspb.GetCryptoKeyRequest{
		Name: req.KeyID,
	}

	// Call GCP KMS to get key information
	glog.V(4).Infof("GCP KMS: Describing key %s", req.KeyID)
	key, err := p.client.GetCryptoKey(ctx, getKeyReq)
	if err != nil {
		return nil, p.convertGCPError(err, req.KeyID)
	}

	response := &seaweedkms.DescribeKeyResponse{
		KeyID:       key.Name,
		ARN:         key.Name, // GCP uses resource names instead of ARNs
		Description: "Google Cloud KMS key",
	}

	// Map GCP key purpose to our usage enum
	if key.Purpose == kmspb.CryptoKey_ENCRYPT_DECRYPT {
		response.KeyUsage = seaweedkms.KeyUsageEncryptDecrypt
	}

	// Map GCP key state to our state enum
	// Get the primary version to check its state
	if key.Primary != nil && key.Primary.State == kmspb.CryptoKeyVersion_ENABLED {
		response.KeyState = seaweedkms.KeyStateEnabled
	} else {
		response.KeyState = seaweedkms.KeyStateDisabled
	}

	// GCP KMS keys are managed by Google Cloud
	response.Origin = seaweedkms.KeyOriginGCP

	glog.V(4).Infof("GCP KMS: Described key %s (state: %s)", req.KeyID, response.KeyState)
	return response, nil
}

// GetKeyID resolves a key name to the full resource name
func (p *GCPKMSProvider) GetKeyID(ctx context.Context, keyIdentifier string) (string, error) {
	if keyIdentifier == "" {
		return "", fmt.Errorf("key identifier cannot be empty")
	}

	// If it's already a full resource name, return as-is
	if strings.HasPrefix(keyIdentifier, "projects/") {
		return keyIdentifier, nil
	}

	// Otherwise, try to construct the full resource name or validate via DescribeKey
	descReq := &seaweedkms.DescribeKeyRequest{KeyID: keyIdentifier}
	descResp, err := p.DescribeKey(ctx, descReq)
	if err != nil {
		return "", fmt.Errorf("failed to resolve key identifier %s: %w", keyIdentifier, err)
	}

	return descResp.KeyID, nil
}

// Close cleans up any resources used by the provider
func (p *GCPKMSProvider) Close() error {
	if p.client != nil {
		err := p.client.Close()
		if err != nil {
			glog.Errorf("Error closing GCP KMS client: %v", err)
			return err
		}
	}
	glog.V(2).Infof("Google Cloud KMS provider closed")
	return nil
}

// encryptionContextToAAD converts encryption context map to additional authenticated data
// This is a simplified implementation - in production, you might want a more robust serialization
func (p *GCPKMSProvider) encryptionContextToAAD(context map[string]string) string {
	if len(context) == 0 {
		return ""
	}

	// Simple key=value&key=value format
	var parts []string
	for k, v := range context {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(parts, "&")
}

// convertGCPError converts Google Cloud KMS errors to our standard KMS errors
func (p *GCPKMSProvider) convertGCPError(err error, keyID string) error {
	// Google Cloud SDK uses gRPC status codes
	errMsg := err.Error()

	if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "NotFound") {
		return &seaweedkms.KMSError{
			Code:    seaweedkms.ErrCodeNotFoundException,
			Message: fmt.Sprintf("Key not found in Google Cloud KMS: %v", err),
			KeyID:   keyID,
		}
	}

	if strings.Contains(errMsg, "permission") || strings.Contains(errMsg, "access") || strings.Contains(errMsg, "Forbidden") {
		return &seaweedkms.KMSError{
			Code:    seaweedkms.ErrCodeAccessDenied,
			Message: fmt.Sprintf("Access denied to Google Cloud KMS: %v", err),
			KeyID:   keyID,
		}
	}

	if strings.Contains(errMsg, "disabled") || strings.Contains(errMsg, "unavailable") {
		return &seaweedkms.KMSError{
			Code:    seaweedkms.ErrCodeKeyUnavailable,
			Message: fmt.Sprintf("Key unavailable in Google Cloud KMS: %v", err),
			KeyID:   keyID,
		}
	}

	// For unknown errors, wrap as internal failure
	return &seaweedkms.KMSError{
		Code:    seaweedkms.ErrCodeKMSInternalFailure,
		Message: fmt.Sprintf("Google Cloud KMS error: %v", err),
		KeyID:   keyID,
	}
}
