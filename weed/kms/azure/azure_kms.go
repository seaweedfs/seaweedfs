//go:build azurekms

package azure

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	seaweedkms "github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	// Register the Azure Key Vault provider
	seaweedkms.RegisterProvider("azure", NewAzureKMSProvider)
}

// AzureKMSProvider implements the KMSProvider interface using Azure Key Vault
type AzureKMSProvider struct {
	client       *azkeys.Client
	vaultURL     string
	tenantID     string
	clientID     string
	clientSecret string
}

// AzureKMSConfig contains configuration for the Azure Key Vault provider
type AzureKMSConfig struct {
	VaultURL        string `json:"vault_url"`         // Azure Key Vault URL (e.g., "https://myvault.vault.azure.net/")
	TenantID        string `json:"tenant_id"`         // Azure AD tenant ID
	ClientID        string `json:"client_id"`         // Service principal client ID
	ClientSecret    string `json:"client_secret"`     // Service principal client secret
	Certificate     string `json:"certificate"`       // Certificate path for cert-based auth (alternative to client secret)
	UseDefaultCreds bool   `json:"use_default_creds"` // Use default Azure credentials (managed identity)
	RequestTimeout  int    `json:"request_timeout"`   // Request timeout in seconds (default: 30)
}

// NewAzureKMSProvider creates a new Azure Key Vault provider
func NewAzureKMSProvider(config util.Configuration) (seaweedkms.KMSProvider, error) {
	if config == nil {
		return nil, fmt.Errorf("Azure Key Vault configuration is required")
	}

	// Extract configuration
	vaultURL := config.GetString("vault_url")
	if vaultURL == "" {
		return nil, fmt.Errorf("vault_url is required for Azure Key Vault provider")
	}

	tenantID := config.GetString("tenant_id")
	clientID := config.GetString("client_id")
	clientSecret := config.GetString("client_secret")
	useDefaultCreds := config.GetBool("use_default_creds")

	requestTimeout := config.GetInt("request_timeout")
	if requestTimeout == 0 {
		requestTimeout = 30 // Default 30 seconds
	}

	// Create credential based on configuration
	var credential azcore.TokenCredential
	var err error

	if useDefaultCreds {
		// Use default Azure credentials (managed identity, Azure CLI, etc.)
		credential, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create default Azure credentials: %w", err)
		}
		glog.V(1).Infof("Azure KMS: Using default Azure credentials")
	} else if clientID != "" && clientSecret != "" {
		// Use service principal credentials
		if tenantID == "" {
			return nil, fmt.Errorf("tenant_id is required when using client credentials")
		}
		credential, err = azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client secret credential: %w", err)
		}
		glog.V(1).Infof("Azure KMS: Using client secret credentials for client ID %s", clientID)
	} else {
		return nil, fmt.Errorf("either use_default_creds=true or client_id+client_secret must be provided")
	}

	// Create Key Vault client
	clientOptions := &azkeys.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			PerCallPolicies: []policy.Policy{},
			Transport: &http.Client{
				Timeout: time.Duration(requestTimeout) * time.Second,
			},
		},
	}

	client, err := azkeys.NewClient(vaultURL, credential, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Key Vault client: %w", err)
	}

	provider := &AzureKMSProvider{
		client:       client,
		vaultURL:     vaultURL,
		tenantID:     tenantID,
		clientID:     clientID,
		clientSecret: clientSecret,
	}

	glog.V(1).Infof("Azure Key Vault provider initialized for vault %s", vaultURL)
	return provider, nil
}

// GenerateDataKey generates a new data encryption key using Azure Key Vault
func (p *AzureKMSProvider) GenerateDataKey(ctx context.Context, req *seaweedkms.GenerateDataKeyRequest) (*seaweedkms.GenerateDataKeyResponse, error) {
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

	// Generate data key locally (Azure Key Vault doesn't have GenerateDataKey like AWS)
	dataKey := make([]byte, keySize)
	if _, err := rand.Read(dataKey); err != nil {
		return nil, fmt.Errorf("failed to generate random data key: %w", err)
	}

	// Encrypt the data key using Azure Key Vault
	glog.V(4).Infof("Azure KMS: Encrypting data key using key %s", req.KeyID)

	// Prepare encryption parameters
	algorithm := azkeys.JSONWebKeyEncryptionAlgorithmRSAOAEP256
	encryptParams := azkeys.KeyOperationsParameters{
		Algorithm: &algorithm, // Default encryption algorithm
		Value:     dataKey,
	}

	// Add encryption context as Additional Authenticated Data (AAD) if provided
	if len(req.EncryptionContext) > 0 {
		// Marshal encryption context to JSON for deterministic AAD
		aadBytes, err := json.Marshal(req.EncryptionContext)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encryption context: %w", err)
		}
		encryptParams.AAD = aadBytes
		glog.V(4).Infof("Azure KMS: Using encryption context as AAD for key %s", req.KeyID)
	}

	// Call Azure Key Vault to encrypt the data key
	encryptResult, err := p.client.Encrypt(ctx, req.KeyID, "", encryptParams, nil)
	if err != nil {
		return nil, p.convertAzureError(err, req.KeyID)
	}

	// Get the actual key ID from the response
	actualKeyID := req.KeyID
	if encryptResult.KID != nil {
		actualKeyID = string(*encryptResult.KID)
	}

	// Create standardized envelope format for consistent API behavior
	envelopeBlob, err := seaweedkms.CreateEnvelope("azure", actualKeyID, string(encryptResult.Result), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create ciphertext envelope: %w", err)
	}

	response := &seaweedkms.GenerateDataKeyResponse{
		KeyID:          actualKeyID,
		Plaintext:      dataKey,
		CiphertextBlob: envelopeBlob, // Store in standardized envelope format
	}

	glog.V(4).Infof("Azure KMS: Generated and encrypted data key using key %s", actualKeyID)
	return response, nil
}

// Decrypt decrypts an encrypted data key using Azure Key Vault
func (p *AzureKMSProvider) Decrypt(ctx context.Context, req *seaweedkms.DecryptRequest) (*seaweedkms.DecryptResponse, error) {
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

	keyID := envelope.KeyID
	if keyID == "" {
		return nil, fmt.Errorf("envelope missing key ID")
	}

	// Convert string back to bytes
	ciphertext := []byte(envelope.Ciphertext)

	// Prepare decryption parameters
	decryptAlgorithm := azkeys.JSONWebKeyEncryptionAlgorithmRSAOAEP256
	decryptParams := azkeys.KeyOperationsParameters{
		Algorithm: &decryptAlgorithm, // Must match encryption algorithm
		Value:     ciphertext,
	}

	// Add encryption context as Additional Authenticated Data (AAD) if provided
	if len(req.EncryptionContext) > 0 {
		// Marshal encryption context to JSON for deterministic AAD (must match encryption)
		aadBytes, err := json.Marshal(req.EncryptionContext)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encryption context: %w", err)
		}
		decryptParams.AAD = aadBytes
		glog.V(4).Infof("Azure KMS: Using encryption context as AAD for decryption of key %s", keyID)
	}

	// Call Azure Key Vault to decrypt the data key
	glog.V(4).Infof("Azure KMS: Decrypting data key using key %s", keyID)
	decryptResult, err := p.client.Decrypt(ctx, keyID, "", decryptParams, nil)
	if err != nil {
		return nil, p.convertAzureError(err, keyID)
	}

	// Get the actual key ID from the response
	actualKeyID := keyID
	if decryptResult.KID != nil {
		actualKeyID = string(*decryptResult.KID)
	}

	response := &seaweedkms.DecryptResponse{
		KeyID:     actualKeyID,
		Plaintext: decryptResult.Result,
	}

	glog.V(4).Infof("Azure KMS: Decrypted data key using key %s", actualKeyID)
	return response, nil
}

// DescribeKey validates that a key exists and returns its metadata
func (p *AzureKMSProvider) DescribeKey(ctx context.Context, req *seaweedkms.DescribeKeyRequest) (*seaweedkms.DescribeKeyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("DescribeKeyRequest cannot be nil")
	}

	if req.KeyID == "" {
		return nil, fmt.Errorf("KeyID is required")
	}

	// Get key from Azure Key Vault
	glog.V(4).Infof("Azure KMS: Describing key %s", req.KeyID)
	result, err := p.client.GetKey(ctx, req.KeyID, "", nil)
	if err != nil {
		return nil, p.convertAzureError(err, req.KeyID)
	}

	if result.Key == nil {
		return nil, fmt.Errorf("no key returned from Azure Key Vault")
	}

	key := result.Key
	response := &seaweedkms.DescribeKeyResponse{
		KeyID:       req.KeyID,
		Description: "Azure Key Vault key", // Azure doesn't provide description in the same way
	}

	// Set ARN-like identifier for Azure
	if key.KID != nil {
		response.ARN = string(*key.KID)
		response.KeyID = string(*key.KID)
	}

	// Set key usage based on key operations
	if key.KeyOps != nil && len(key.KeyOps) > 0 {
		// Azure keys can have multiple operations, check if encrypt/decrypt are supported
		for _, op := range key.KeyOps {
			if op != nil && (*op == string(azkeys.JSONWebKeyOperationEncrypt) || *op == string(azkeys.JSONWebKeyOperationDecrypt)) {
				response.KeyUsage = seaweedkms.KeyUsageEncryptDecrypt
				break
			}
		}
	}

	// Set key state based on enabled status
	if result.Attributes != nil {
		if result.Attributes.Enabled != nil && *result.Attributes.Enabled {
			response.KeyState = seaweedkms.KeyStateEnabled
		} else {
			response.KeyState = seaweedkms.KeyStateDisabled
		}
	}

	// Azure Key Vault keys are managed by Azure
	response.Origin = seaweedkms.KeyOriginAzure

	glog.V(4).Infof("Azure KMS: Described key %s (state: %s)", req.KeyID, response.KeyState)
	return response, nil
}

// GetKeyID resolves a key name to the full key identifier
func (p *AzureKMSProvider) GetKeyID(ctx context.Context, keyIdentifier string) (string, error) {
	if keyIdentifier == "" {
		return "", fmt.Errorf("key identifier cannot be empty")
	}

	// Use DescribeKey to resolve and validate the key identifier
	descReq := &seaweedkms.DescribeKeyRequest{KeyID: keyIdentifier}
	descResp, err := p.DescribeKey(ctx, descReq)
	if err != nil {
		return "", fmt.Errorf("failed to resolve key identifier %s: %w", keyIdentifier, err)
	}

	return descResp.KeyID, nil
}

// Close cleans up any resources used by the provider
func (p *AzureKMSProvider) Close() error {
	// Azure SDK clients don't require explicit cleanup
	glog.V(2).Infof("Azure Key Vault provider closed")
	return nil
}

// convertAzureError converts Azure Key Vault errors to our standard KMS errors
func (p *AzureKMSProvider) convertAzureError(err error, keyID string) error {
	// Azure SDK uses different error types, need to check for specific conditions
	errMsg := err.Error()

	if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "NotFound") {
		return &seaweedkms.KMSError{
			Code:    seaweedkms.ErrCodeNotFoundException,
			Message: fmt.Sprintf("Key not found in Azure Key Vault: %v", err),
			KeyID:   keyID,
		}
	}

	if strings.Contains(errMsg, "access") || strings.Contains(errMsg, "Forbidden") || strings.Contains(errMsg, "Unauthorized") {
		return &seaweedkms.KMSError{
			Code:    seaweedkms.ErrCodeAccessDenied,
			Message: fmt.Sprintf("Access denied to Azure Key Vault: %v", err),
			KeyID:   keyID,
		}
	}

	if strings.Contains(errMsg, "disabled") || strings.Contains(errMsg, "unavailable") {
		return &seaweedkms.KMSError{
			Code:    seaweedkms.ErrCodeKeyUnavailable,
			Message: fmt.Sprintf("Key unavailable in Azure Key Vault: %v", err),
			KeyID:   keyID,
		}
	}

	// For unknown errors, wrap as internal failure
	return &seaweedkms.KMSError{
		Code:    seaweedkms.ErrCodeKMSInternalFailure,
		Message: fmt.Sprintf("Azure Key Vault error: %v", err),
		KeyID:   keyID,
	}
}
