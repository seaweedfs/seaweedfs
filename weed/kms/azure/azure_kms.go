//go:build azurekms

package azure

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
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

// vaultHost returns the host of a configured Key Vault URL, or "" when the URL
// carries none. Hosts are compared case-insensitively, without a trailing dot,
// and without an explicit :443, which names the same vault as no port.
func vaultHost(vaultURL string) string {
	parsed, err := url.Parse(vaultURL)
	if err != nil {
		return ""
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Host), ":443")
	return strings.TrimSuffix(host, ".")
}

// splitKeyID turns a Key Vault key identifier into the (name, version) pair the
// azkeys client expects. A plain key name, or anything that is not a Key Vault
// URL, is returned unchanged together with an empty version, which the client
// resolves to the latest version.
//
// A URL naming a different vault than the one this provider is configured for
// is rejected: the client addresses only its own vault, so dropping the host
// would silently encrypt under this vault's same-named key.
func (p *AzureKMSProvider) splitKeyID(keyID string) (string, string, error) {
	parsed, err := url.Parse(keyID)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return keyID, "", nil
	}
	if host := vaultHost(p.vaultURL); host != "" && vaultHost(keyID) != host {
		return "", "", fmt.Errorf("key ID %q names vault %q, but this provider is configured for %q", keyID, parsed.Host, host)
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "keys" {
		return keyID, "", nil
	}
	if len(parts) >= 3 {
		return parts[1], parts[2], nil
	}
	return parts[1], "", nil
}

// encodeCiphertext stores the wrapped data key in the JSON envelope as base64.
// The envelope is JSON, so raw binary would be replaced by U+FFFD on the way in.
func encodeCiphertext(ciphertext []byte) string {
	return base64.StdEncoding.EncodeToString(ciphertext)
}

// contextDigest digests the encryption context for storage in the envelope.
// RSA-OAEP takes no AAD, so the context is bound to the wrapped key by
// recording its hash and checking it on decrypt instead.
func contextDigest(context map[string]string) string {
	encoded, _ := json.Marshal(context) // map keys marshal in sorted order
	sum := sha256.Sum256(encoded)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// checkContext verifies the request's encryption context matches the context
// recorded when the key was wrapped.
func checkContext(envelope *seaweedkms.CiphertextEnvelope, context map[string]string) error {
	recorded, _ := envelope.ProviderSpecific["encryption_context_sha256"].(string)
	if recorded == "" {
		if len(context) == 0 {
			return nil
		}
	} else if recorded == contextDigest(context) {
		return nil
	}
	return fmt.Errorf("encryption context does not match the wrapped key")
}

// decodeCiphertext reads back what encodeCiphertext wrote.
func decodeCiphertext(ciphertext string) ([]byte, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(ciphertext)
	if err != nil {
		return nil, fmt.Errorf("wrapped data key is not valid base64: %w", err)
	}
	if len(decoded) == 0 {
		return nil, fmt.Errorf("wrapped data key is empty")
	}
	return decoded, nil
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

	// RSA-OAEP takes no additional authenticated data: Key Vault rejects the
	// request with BadParameter when AAD is set on an RSA-OAEP key. The
	// context is bound to the wrapped key via a digest in the envelope
	// instead, which Decrypt verifies.
	providerSpecific := map[string]interface{}{}
	if len(req.EncryptionContext) > 0 {
		providerSpecific["encryption_context_sha256"] = contextDigest(req.EncryptionContext)
	}

	// Call Azure Key Vault to encrypt the data key
	keyName, keyVersion, err := p.splitKeyID(req.KeyID)
	if err != nil {
		return nil, err
	}
	encryptResult, err := p.client.Encrypt(ctx, keyName, keyVersion, encryptParams, nil)
	if err != nil {
		return nil, p.convertAzureError(err, req.KeyID)
	}

	// Get the actual key ID from the response
	actualKeyID := req.KeyID
	if encryptResult.KID != nil {
		actualKeyID = string(*encryptResult.KID)
	}

	// Create standardized envelope format for consistent API behavior
	envelopeBlob, err := seaweedkms.CreateEnvelope("azure", actualKeyID, encodeCiphertext(encryptResult.Result), providerSpecific)
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

	// Convert the base64 envelope field back to the raw wrapped key
	ciphertext, err := decodeCiphertext(envelope.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("invalid Azure ciphertext envelope: %w", err)
	}

	// Prepare decryption parameters
	decryptAlgorithm := azkeys.JSONWebKeyEncryptionAlgorithmRSAOAEP256
	decryptParams := azkeys.KeyOperationsParameters{
		Algorithm: &decryptAlgorithm, // Must match encryption algorithm
		Value:     ciphertext,
	}

	// RSA-OAEP takes no AAD, so the encryption context is bound via a digest
	// recorded in the envelope rather than authenticated by the vault.
	if err := checkContext(envelope, req.EncryptionContext); err != nil {
		return nil, err
	}

	// Call Azure Key Vault to decrypt the data key
	glog.V(4).Infof("Azure KMS: Decrypting data key using key %s", keyID)
	decryptName, decryptVersion, err := p.splitKeyID(keyID)
	if err != nil {
		return nil, err
	}
	decryptResult, err := p.client.Decrypt(ctx, decryptName, decryptVersion, decryptParams, nil)
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
	describeName, describeVersion, err := p.splitKeyID(req.KeyID)
	if err != nil {
		return nil, err
	}
	result, err := p.client.GetKey(ctx, describeName, describeVersion, nil)
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
