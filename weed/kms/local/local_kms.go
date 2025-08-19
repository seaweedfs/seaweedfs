package local

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// LocalKMSProvider implements a local, in-memory KMS for development and testing
// WARNING: This is NOT suitable for production use - keys are stored in memory
type LocalKMSProvider struct {
	mu           sync.RWMutex
	keys         map[string]*LocalKey
	defaultKeyID string
}

// LocalKey represents a key stored in the local KMS
type LocalKey struct {
	KeyID       string            `json:"keyId"`
	ARN         string            `json:"arn"`
	Description string            `json:"description"`
	KeyMaterial []byte            `json:"keyMaterial"` // 256-bit master key
	KeyUsage    kms.KeyUsage      `json:"keyUsage"`
	KeyState    kms.KeyState      `json:"keyState"`
	Origin      kms.KeyOrigin     `json:"origin"`
	CreatedAt   time.Time         `json:"createdAt"`
	Aliases     []string          `json:"aliases"`
	Metadata    map[string]string `json:"metadata"`
}

// LocalKMSConfig contains configuration for the local KMS provider
type LocalKMSConfig struct {
	DefaultKeyID string               `json:"defaultKeyId"`
	Keys         map[string]*LocalKey `json:"keys"`
}

func init() {
	// Register the local KMS provider
	kms.RegisterProvider("local", NewLocalKMSProvider)
}

// NewLocalKMSProvider creates a new local KMS provider
func NewLocalKMSProvider(config util.Configuration) (kms.KMSProvider, error) {
	provider := &LocalKMSProvider{
		keys: make(map[string]*LocalKey),
	}

	// Load configuration if provided
	if config != nil {
		if err := provider.loadConfig(config); err != nil {
			return nil, fmt.Errorf("failed to load local KMS config: %v", err)
		}
	}

	// Create a default key if none exists
	if len(provider.keys) == 0 {
		defaultKey, err := provider.createDefaultKey()
		if err != nil {
			return nil, fmt.Errorf("failed to create default key: %v", err)
		}
		provider.defaultKeyID = defaultKey.KeyID
		glog.V(1).Infof("Local KMS: Created default key %s", defaultKey.KeyID)
	}

	return provider, nil
}

// loadConfig loads configuration from the provided config
func (p *LocalKMSProvider) loadConfig(config util.Configuration) error {
	// This would typically load from a configuration file or environment variables
	// For now, we'll create a simple default configuration
	return nil
}

// createDefaultKey creates a default master key for the local KMS
func (p *LocalKMSProvider) createDefaultKey() (*LocalKey, error) {
	keyID := generateKeyID()
	keyMaterial := make([]byte, 32) // 256-bit key
	if _, err := io.ReadFull(rand.Reader, keyMaterial); err != nil {
		return nil, fmt.Errorf("failed to generate key material: %v", err)
	}

	key := &LocalKey{
		KeyID:       keyID,
		ARN:         fmt.Sprintf("arn:aws:kms:local:000000000000:key/%s", keyID),
		Description: "Default local KMS key for SeaweedFS",
		KeyMaterial: keyMaterial,
		KeyUsage:    kms.KeyUsageEncryptDecrypt,
		KeyState:    kms.KeyStateEnabled,
		Origin:      kms.KeyOriginAWS,
		CreatedAt:   time.Now(),
		Aliases:     []string{"alias/seaweedfs-default"},
		Metadata:    make(map[string]string),
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys[keyID] = key

	// Also register aliases
	for _, alias := range key.Aliases {
		p.keys[alias] = key
	}

	return key, nil
}

// GenerateDataKey implements the KMSProvider interface
func (p *LocalKMSProvider) GenerateDataKey(ctx context.Context, req *kms.GenerateDataKeyRequest) (*kms.GenerateDataKeyResponse, error) {
	if req.KeySpec != kms.KeySpecAES256 {
		return nil, &kms.KMSError{
			Code:    kms.ErrCodeInvalidKeyUsage,
			Message: fmt.Sprintf("Unsupported key spec: %s", req.KeySpec),
			KeyID:   req.KeyID,
		}
	}

	// Resolve the key
	key, err := p.getKey(req.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != kms.KeyStateEnabled {
		return nil, &kms.KMSError{
			Code:    kms.ErrCodeKeyUnavailable,
			Message: fmt.Sprintf("Key %s is in state %s", key.KeyID, key.KeyState),
			KeyID:   key.KeyID,
		}
	}

	// Generate a random 256-bit data key
	dataKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dataKey); err != nil {
		return nil, &kms.KMSError{
			Code:    kms.ErrCodeKMSInternalFailure,
			Message: "Failed to generate data key",
			KeyID:   key.KeyID,
		}
	}

	// Encrypt the data key with the master key
	encryptedDataKey, err := p.encryptDataKey(dataKey, key, req.EncryptionContext)
	if err != nil {
		kms.ClearSensitiveData(dataKey)
		return nil, &kms.KMSError{
			Code:    kms.ErrCodeKMSInternalFailure,
			Message: fmt.Sprintf("Failed to encrypt data key: %v", err),
			KeyID:   key.KeyID,
		}
	}

	return &kms.GenerateDataKeyResponse{
		KeyID:          key.KeyID,
		Plaintext:      dataKey,
		CiphertextBlob: encryptedDataKey,
	}, nil
}

// Decrypt implements the KMSProvider interface
func (p *LocalKMSProvider) Decrypt(ctx context.Context, req *kms.DecryptRequest) (*kms.DecryptResponse, error) {
	// Parse the encrypted data key to extract metadata
	metadata, err := p.parseEncryptedDataKey(req.CiphertextBlob)
	if err != nil {
		return nil, &kms.KMSError{
			Code:    kms.ErrCodeInvalidCiphertext,
			Message: fmt.Sprintf("Invalid ciphertext format: %v", err),
		}
	}

	// Verify encryption context matches
	if !p.encryptionContextMatches(metadata.EncryptionContext, req.EncryptionContext) {
		return nil, &kms.KMSError{
			Code:    kms.ErrCodeInvalidCiphertext,
			Message: "Encryption context mismatch",
			KeyID:   metadata.KeyID,
		}
	}

	// Get the master key
	key, err := p.getKey(metadata.KeyID)
	if err != nil {
		return nil, err
	}

	if key.KeyState != kms.KeyStateEnabled {
		return nil, &kms.KMSError{
			Code:    kms.ErrCodeKeyUnavailable,
			Message: fmt.Sprintf("Key %s is in state %s", key.KeyID, key.KeyState),
			KeyID:   key.KeyID,
		}
	}

	// Decrypt the data key
	dataKey, err := p.decryptDataKey(metadata, key)
	if err != nil {
		return nil, &kms.KMSError{
			Code:    kms.ErrCodeInvalidCiphertext,
			Message: fmt.Sprintf("Failed to decrypt data key: %v", err),
			KeyID:   key.KeyID,
		}
	}

	return &kms.DecryptResponse{
		KeyID:     key.KeyID,
		Plaintext: dataKey,
	}, nil
}

// DescribeKey implements the KMSProvider interface
func (p *LocalKMSProvider) DescribeKey(ctx context.Context, req *kms.DescribeKeyRequest) (*kms.DescribeKeyResponse, error) {
	key, err := p.getKey(req.KeyID)
	if err != nil {
		return nil, err
	}

	return &kms.DescribeKeyResponse{
		KeyID:       key.KeyID,
		ARN:         key.ARN,
		Description: key.Description,
		KeyUsage:    key.KeyUsage,
		KeyState:    key.KeyState,
		Origin:      key.Origin,
	}, nil
}

// GetKeyID implements the KMSProvider interface
func (p *LocalKMSProvider) GetKeyID(ctx context.Context, keyIdentifier string) (string, error) {
	key, err := p.getKey(keyIdentifier)
	if err != nil {
		return "", err
	}
	return key.KeyID, nil
}

// Close implements the KMSProvider interface
func (p *LocalKMSProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Clear all key material from memory
	for _, key := range p.keys {
		kms.ClearSensitiveData(key.KeyMaterial)
	}
	p.keys = make(map[string]*LocalKey)
	return nil
}

// getKey retrieves a key by ID or alias
func (p *LocalKMSProvider) getKey(keyIdentifier string) (*LocalKey, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Try direct lookup first
	if key, exists := p.keys[keyIdentifier]; exists {
		return key, nil
	}

	// Try with default key if no identifier provided
	if keyIdentifier == "" && p.defaultKeyID != "" {
		if key, exists := p.keys[p.defaultKeyID]; exists {
			return key, nil
		}
	}

	return nil, &kms.KMSError{
		Code:    kms.ErrCodeNotFoundException,
		Message: fmt.Sprintf("Key not found: %s", keyIdentifier),
		KeyID:   keyIdentifier,
	}
}

// encryptedDataKeyMetadata represents the metadata stored with encrypted data keys
type encryptedDataKeyMetadata struct {
	KeyID             string            `json:"keyId"`
	EncryptionContext map[string]string `json:"encryptionContext"`
	EncryptedData     []byte            `json:"encryptedData"`
	IV                []byte            `json:"iv"`
}

// encryptDataKey encrypts a data key using the master key
func (p *LocalKMSProvider) encryptDataKey(dataKey []byte, masterKey *LocalKey, encryptionContext map[string]string) ([]byte, error) {
	block, err := aes.NewCipher(masterKey.KeyMaterial)
	if err != nil {
		return nil, err
	}

	// Generate a random IV
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	// Encrypt using AES-CTR
	stream := cipher.NewCTR(block, iv)
	encryptedData := make([]byte, len(dataKey))
	stream.XORKeyStream(encryptedData, dataKey)

	// Create metadata structure
	metadata := &encryptedDataKeyMetadata{
		KeyID:             masterKey.KeyID,
		EncryptionContext: encryptionContext,
		EncryptedData:     encryptedData,
		IV:                iv,
	}

	// Serialize metadata to JSON
	return json.Marshal(metadata)
}

// decryptDataKey decrypts a data key using the master key
func (p *LocalKMSProvider) decryptDataKey(metadata *encryptedDataKeyMetadata, masterKey *LocalKey) ([]byte, error) {
	block, err := aes.NewCipher(masterKey.KeyMaterial)
	if err != nil {
		return nil, err
	}

	// Decrypt using AES-CTR
	stream := cipher.NewCTR(block, metadata.IV)
	dataKey := make([]byte, len(metadata.EncryptedData))
	stream.XORKeyStream(dataKey, metadata.EncryptedData)

	return dataKey, nil
}

// parseEncryptedDataKey parses the encrypted data key blob
func (p *LocalKMSProvider) parseEncryptedDataKey(ciphertextBlob []byte) (*encryptedDataKeyMetadata, error) {
	var metadata encryptedDataKeyMetadata
	if err := json.Unmarshal(ciphertextBlob, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse ciphertext blob: %v", err)
	}
	return &metadata, nil
}

// encryptionContextMatches checks if two encryption contexts match
func (p *LocalKMSProvider) encryptionContextMatches(ctx1, ctx2 map[string]string) bool {
	if len(ctx1) != len(ctx2) {
		return false
	}
	for k, v := range ctx1 {
		if ctx2[k] != v {
			return false
		}
	}
	return true
}

// generateKeyID generates a random key ID
func generateKeyID() string {
	// Generate a UUID-like key ID
	b := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(err) // This should never happen
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// CreateKey creates a new key in the local KMS (for testing)
func (p *LocalKMSProvider) CreateKey(description string, aliases []string) (*LocalKey, error) {
	keyID := generateKeyID()
	keyMaterial := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, keyMaterial); err != nil {
		return nil, err
	}

	key := &LocalKey{
		KeyID:       keyID,
		ARN:         fmt.Sprintf("arn:aws:kms:local:000000000000:key/%s", keyID),
		Description: description,
		KeyMaterial: keyMaterial,
		KeyUsage:    kms.KeyUsageEncryptDecrypt,
		KeyState:    kms.KeyStateEnabled,
		Origin:      kms.KeyOriginAWS,
		CreatedAt:   time.Now(),
		Aliases:     aliases,
		Metadata:    make(map[string]string),
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.keys[keyID] = key
	for i, alias := range aliases {
		// Ensure alias has proper format
		if !strings.HasPrefix(alias, "alias/") {
			aliases[i] = "alias/" + alias
		}
		p.keys[aliases[i]] = key
	}

	return key, nil
}
