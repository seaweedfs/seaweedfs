package aws

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	seaweedkms "github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	// Register the AWS KMS provider
	seaweedkms.RegisterProvider("aws", NewAWSKMSProvider)
}

// AWSKMSProvider implements the KMSProvider interface using AWS KMS
type AWSKMSProvider struct {
	client   *kms.KMS
	region   string
	endpoint string // For testing with LocalStack or custom endpoints
}

// AWSKMSConfig contains configuration for the AWS KMS provider
type AWSKMSConfig struct {
	Region         string `json:"region"`          // AWS region (e.g., "us-east-1")
	AccessKey      string `json:"access_key"`      // AWS access key (optional if using IAM roles)
	SecretKey      string `json:"secret_key"`      // AWS secret key (optional if using IAM roles)
	SessionToken   string `json:"session_token"`   // AWS session token (optional for STS)
	Endpoint       string `json:"endpoint"`        // Custom endpoint (optional, for LocalStack/testing)
	Profile        string `json:"profile"`         // AWS profile name (optional)
	RoleARN        string `json:"role_arn"`        // IAM role ARN to assume (optional)
	ExternalID     string `json:"external_id"`     // External ID for role assumption (optional)
	ConnectTimeout int    `json:"connect_timeout"` // Connection timeout in seconds (default: 10)
	RequestTimeout int    `json:"request_timeout"` // Request timeout in seconds (default: 30)
	MaxRetries     int    `json:"max_retries"`     // Maximum number of retries (default: 3)
}

// NewAWSKMSProvider creates a new AWS KMS provider
func NewAWSKMSProvider(config util.Configuration) (seaweedkms.KMSProvider, error) {
	if config == nil {
		return nil, fmt.Errorf("AWS KMS configuration is required")
	}

	// Extract configuration
	region := config.GetString("region")
	if region == "" {
		region = "us-east-1" // Default region
	}

	accessKey := config.GetString("access_key")
	secretKey := config.GetString("secret_key")
	sessionToken := config.GetString("session_token")
	endpoint := config.GetString("endpoint")
	profile := config.GetString("profile")

	// Timeouts and retries
	connectTimeout := config.GetInt("connect_timeout")
	if connectTimeout == 0 {
		connectTimeout = 10 // Default 10 seconds
	}

	requestTimeout := config.GetInt("request_timeout")
	if requestTimeout == 0 {
		requestTimeout = 30 // Default 30 seconds
	}

	maxRetries := config.GetInt("max_retries")
	if maxRetries == 0 {
		maxRetries = 3 // Default 3 retries
	}

	// Create AWS session
	awsConfig := &aws.Config{
		Region:     aws.String(region),
		MaxRetries: aws.Int(maxRetries),
		HTTPClient: &http.Client{
			Timeout: time.Duration(requestTimeout) * time.Second,
		},
	}

	// Set custom endpoint if provided (for testing with LocalStack)
	if endpoint != "" {
		awsConfig.Endpoint = aws.String(endpoint)
		awsConfig.DisableSSL = aws.Bool(strings.HasPrefix(endpoint, "http://"))
	}

	// Configure credentials
	if accessKey != "" && secretKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, sessionToken)
	} else if profile != "" {
		awsConfig.Credentials = credentials.NewSharedCredentials("", profile)
	}
	// If neither are provided, use default credential chain (IAM roles, etc.)

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	provider := &AWSKMSProvider{
		client:   kms.New(sess),
		region:   region,
		endpoint: endpoint,
	}

	glog.V(1).Infof("AWS KMS provider initialized for region %s", region)
	return provider, nil
}

// GenerateDataKey generates a new data encryption key using AWS KMS
func (p *AWSKMSProvider) GenerateDataKey(ctx context.Context, req *seaweedkms.GenerateDataKeyRequest) (*seaweedkms.GenerateDataKeyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("GenerateDataKeyRequest cannot be nil")
	}

	if req.KeyID == "" {
		return nil, fmt.Errorf("KeyID is required")
	}

	// Validate key spec
	var keySpec string
	switch req.KeySpec {
	case seaweedkms.KeySpecAES256:
		keySpec = "AES_256"
	default:
		return nil, fmt.Errorf("unsupported key spec: %s", req.KeySpec)
	}

	// Build KMS request
	kmsReq := &kms.GenerateDataKeyInput{
		KeyId:   aws.String(req.KeyID),
		KeySpec: aws.String(keySpec),
	}

	// Add encryption context if provided
	if len(req.EncryptionContext) > 0 {
		kmsReq.EncryptionContext = aws.StringMap(req.EncryptionContext)
	}

	// Call AWS KMS
	glog.V(4).Infof("AWS KMS: Generating data key for key ID %s", req.KeyID)
	result, err := p.client.GenerateDataKeyWithContext(ctx, kmsReq)
	if err != nil {
		return nil, p.convertAWSError(err, req.KeyID)
	}

	// Extract the actual key ID from the response (resolves aliases)
	actualKeyID := ""
	if result.KeyId != nil {
		actualKeyID = *result.KeyId
	}

	// Create standardized envelope format for consistent API behavior
	envelopeBlob, err := seaweedkms.CreateEnvelope("aws", actualKeyID, base64.StdEncoding.EncodeToString(result.CiphertextBlob), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create ciphertext envelope: %w", err)
	}

	response := &seaweedkms.GenerateDataKeyResponse{
		KeyID:          actualKeyID,
		Plaintext:      result.Plaintext,
		CiphertextBlob: envelopeBlob, // Store in standardized envelope format
	}

	glog.V(4).Infof("AWS KMS: Generated data key for key ID %s (actual: %s)", req.KeyID, actualKeyID)
	return response, nil
}

// Decrypt decrypts an encrypted data key using AWS KMS
func (p *AWSKMSProvider) Decrypt(ctx context.Context, req *seaweedkms.DecryptRequest) (*seaweedkms.DecryptResponse, error) {
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

	if envelope.Provider != "aws" {
		return nil, fmt.Errorf("invalid provider in envelope: expected 'aws', got '%s'", envelope.Provider)
	}

	ciphertext, err := base64.StdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ciphertext from envelope: %w", err)
	}

	// Build KMS request
	kmsReq := &kms.DecryptInput{
		CiphertextBlob: ciphertext,
	}

	// Add encryption context if provided
	if len(req.EncryptionContext) > 0 {
		kmsReq.EncryptionContext = aws.StringMap(req.EncryptionContext)
	}

	// Call AWS KMS
	glog.V(4).Infof("AWS KMS: Decrypting data key (blob size: %d bytes)", len(req.CiphertextBlob))
	result, err := p.client.DecryptWithContext(ctx, kmsReq)
	if err != nil {
		return nil, p.convertAWSError(err, "")
	}

	// Extract the key ID that was used for encryption
	keyID := ""
	if result.KeyId != nil {
		keyID = *result.KeyId
	}

	response := &seaweedkms.DecryptResponse{
		KeyID:     keyID,
		Plaintext: result.Plaintext,
	}

	glog.V(4).Infof("AWS KMS: Decrypted data key using key ID %s", keyID)
	return response, nil
}

// DescribeKey validates that a key exists and returns its metadata
func (p *AWSKMSProvider) DescribeKey(ctx context.Context, req *seaweedkms.DescribeKeyRequest) (*seaweedkms.DescribeKeyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("DescribeKeyRequest cannot be nil")
	}

	if req.KeyID == "" {
		return nil, fmt.Errorf("KeyID is required")
	}

	// Build KMS request
	kmsReq := &kms.DescribeKeyInput{
		KeyId: aws.String(req.KeyID),
	}

	// Call AWS KMS
	glog.V(4).Infof("AWS KMS: Describing key %s", req.KeyID)
	result, err := p.client.DescribeKeyWithContext(ctx, kmsReq)
	if err != nil {
		return nil, p.convertAWSError(err, req.KeyID)
	}

	if result.KeyMetadata == nil {
		return nil, fmt.Errorf("no key metadata returned from AWS KMS")
	}

	metadata := result.KeyMetadata
	response := &seaweedkms.DescribeKeyResponse{
		KeyID:       aws.StringValue(metadata.KeyId),
		ARN:         aws.StringValue(metadata.Arn),
		Description: aws.StringValue(metadata.Description),
	}

	// Convert AWS key usage to our enum
	if metadata.KeyUsage != nil {
		switch *metadata.KeyUsage {
		case "ENCRYPT_DECRYPT":
			response.KeyUsage = seaweedkms.KeyUsageEncryptDecrypt
		case "GENERATE_DATA_KEY":
			response.KeyUsage = seaweedkms.KeyUsageGenerateDataKey
		}
	}

	// Convert AWS key state to our enum
	if metadata.KeyState != nil {
		switch *metadata.KeyState {
		case "Enabled":
			response.KeyState = seaweedkms.KeyStateEnabled
		case "Disabled":
			response.KeyState = seaweedkms.KeyStateDisabled
		case "PendingDeletion":
			response.KeyState = seaweedkms.KeyStatePendingDeletion
		case "Unavailable":
			response.KeyState = seaweedkms.KeyStateUnavailable
		}
	}

	// Convert AWS origin to our enum
	if metadata.Origin != nil {
		switch *metadata.Origin {
		case "AWS_KMS":
			response.Origin = seaweedkms.KeyOriginAWS
		case "EXTERNAL":
			response.Origin = seaweedkms.KeyOriginExternal
		case "AWS_CLOUDHSM":
			response.Origin = seaweedkms.KeyOriginCloudHSM
		}
	}

	glog.V(4).Infof("AWS KMS: Described key %s (actual: %s, state: %s)", req.KeyID, response.KeyID, response.KeyState)
	return response, nil
}

// GetKeyID resolves a key alias or ARN to the actual key ID
func (p *AWSKMSProvider) GetKeyID(ctx context.Context, keyIdentifier string) (string, error) {
	if keyIdentifier == "" {
		return "", fmt.Errorf("key identifier cannot be empty")
	}

	// Use DescribeKey to resolve the key identifier
	descReq := &seaweedkms.DescribeKeyRequest{KeyID: keyIdentifier}
	descResp, err := p.DescribeKey(ctx, descReq)
	if err != nil {
		return "", fmt.Errorf("failed to resolve key identifier %s: %w", keyIdentifier, err)
	}

	return descResp.KeyID, nil
}

// Close cleans up any resources used by the provider
func (p *AWSKMSProvider) Close() error {
	// AWS SDK clients don't require explicit cleanup
	glog.V(2).Infof("AWS KMS provider closed")
	return nil
}

// convertAWSError converts AWS KMS errors to our standard KMS errors
func (p *AWSKMSProvider) convertAWSError(err error, keyID string) error {
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case "NotFoundException":
			return &seaweedkms.KMSError{
				Code:    seaweedkms.ErrCodeNotFoundException,
				Message: awsErr.Message(),
				KeyID:   keyID,
			}
		case "DisabledException", "KeyUnavailableException":
			return &seaweedkms.KMSError{
				Code:    seaweedkms.ErrCodeKeyUnavailable,
				Message: awsErr.Message(),
				KeyID:   keyID,
			}
		case "AccessDeniedException":
			return &seaweedkms.KMSError{
				Code:    seaweedkms.ErrCodeAccessDenied,
				Message: awsErr.Message(),
				KeyID:   keyID,
			}
		case "InvalidKeyUsageException":
			return &seaweedkms.KMSError{
				Code:    seaweedkms.ErrCodeInvalidKeyUsage,
				Message: awsErr.Message(),
				KeyID:   keyID,
			}
		case "InvalidCiphertextException":
			return &seaweedkms.KMSError{
				Code:    seaweedkms.ErrCodeInvalidCiphertext,
				Message: awsErr.Message(),
				KeyID:   keyID,
			}
		case "KMSInternalException", "KMSInvalidStateException":
			return &seaweedkms.KMSError{
				Code:    seaweedkms.ErrCodeKMSInternalFailure,
				Message: awsErr.Message(),
				KeyID:   keyID,
			}
		default:
			// For unknown AWS errors, wrap them as internal failures
			return &seaweedkms.KMSError{
				Code:    seaweedkms.ErrCodeKMSInternalFailure,
				Message: fmt.Sprintf("AWS KMS error %s: %s", awsErr.Code(), awsErr.Message()),
				KeyID:   keyID,
			}
		}
	}

	// For non-AWS errors (network issues, etc.), wrap as internal failure
	return &seaweedkms.KMSError{
		Code:    seaweedkms.ErrCodeKMSInternalFailure,
		Message: fmt.Sprintf("AWS KMS provider error: %v", err),
		KeyID:   keyID,
	}
}
