package local

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"sort"
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
	mu                   sync.RWMutex
	keys                 map[string]*LocalKey
	defaultKeyID         string
	enableOnDemandCreate bool // Whether to create keys on-demand for missing key IDs
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
	DefaultKeyID         string               `json:"defaultKeyId"`
	Keys                 map[string]*LocalKey `json:"keys"`
	EnableOnDemandCreate bool                 `json:"enableOnDemandCreate"`
}

func init() {
	// Register the local KMS provider
	kms.RegisterProvider("local", NewLocalKMSProvider)
}

// NewLocalKMSProvider creates a new local KMS provider
func NewLocalKMSProvider(config util.Configuration) (kms.KMSProvider, error) {
	provider := &LocalKMSProvider{
		keys:                 make(map[string]*LocalKey),
		enableOnDemandCreate: true, // Default to true for development/testing convenience
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
	if config == nil {
		return nil
	}

	p.enableOnDemandCreate = config.GetBool("enableOnDemandCreate")

	// TODO: Load pre-existing keys from configuration if provided
	// For now, rely on default key creation in constructor

	glog.V(2).Infof("Local KMS: enableOnDemandCreate = %v", p.enableOnDemandCreate)
	return nil
}

// createDefaultKey creates a default master key for the local KMS
func (p *LocalKMSProvider) createDefaultKey() (*LocalKey, error) {
	keyID, err := generateKeyID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate key ID: %w", err)
	}
	keyMaterial := make([]byte, 32) // 256-bit key
	if _, err := io.ReadFull(rand.Reader, keyMaterial); err != nil {
		return nil, fmt.Errorf("failed to generate key material: %w", err)
	}

	key := &LocalKey{
		KeyID:       keyID,
		ARN:         fmt.Sprintf("arn:aws:kms:local:000000000000:key/%s", keyID),
		Description: "Default local KMS key for SeaweedFS",
		KeyMaterial: keyMaterial,
		KeyUsage:    kms.KeyUsageEncryptDecrypt,
		KeyState:    kms.KeyStateEnabled,
		Origin:      kms.KeyOriginLocal,
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

// getKey retrieves a key by ID or alias, creating it on-demand if it doesn't exist
func (p *LocalKMSProvider) getKey(keyIdentifier string) (*LocalKey, error) {
	p.mu.RLock()

	// Try direct lookup first
	if key, exists := p.keys[keyIdentifier]; exists {
		p.mu.RUnlock()
		return key, nil
	}

	// Try with default key if no identifier provided
	if keyIdentifier == "" && p.defaultKeyID != "" {
		if key, exists := p.keys[p.defaultKeyID]; exists {
			p.mu.RUnlock()
			return key, nil
		}
	}

	p.mu.RUnlock()

	// Key doesn't exist - create on-demand if enabled and key identifier is reasonable
	if keyIdentifier != "" && p.enableOnDemandCreate && p.isReasonableKeyIdentifier(keyIdentifier) {
		glog.V(1).Infof("Creating on-demand local KMS key: %s", keyIdentifier)
		key, err := p.CreateKeyWithID(keyIdentifier, fmt.Sprintf("Auto-created local KMS key: %s", keyIdentifier))
		if err != nil {
			return nil, &kms.KMSError{
				Code:    kms.ErrCodeKMSInternalFailure,
				Message: fmt.Sprintf("Failed to create on-demand key %s: %v", keyIdentifier, err),
				KeyID:   keyIdentifier,
			}
		}
		return key, nil
	}

	return nil, &kms.KMSError{
		Code:    kms.ErrCodeNotFoundException,
		Message: fmt.Sprintf("Key not found: %s", keyIdentifier),
		KeyID:   keyIdentifier,
	}
}

// isReasonableKeyIdentifier determines if a key identifier is reasonable for on-demand creation
func (p *LocalKMSProvider) isReasonableKeyIdentifier(keyIdentifier string) bool {
	// Basic validation: reasonable length and character set
	if len(keyIdentifier) < 3 || len(keyIdentifier) > 100 {
		return false
	}

	// Allow alphanumeric characters, hyphens, underscores, and forward slashes
	// This covers most reasonable key identifier formats without being overly restrictive
	for _, r := range keyIdentifier {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' || r == '/') {
			return false
		}
	}

	// Reject keys that start or end with separators
	if keyIdentifier[0] == '-' || keyIdentifier[0] == '_' || keyIdentifier[0] == '/' ||
		keyIdentifier[len(keyIdentifier)-1] == '-' || keyIdentifier[len(keyIdentifier)-1] == '_' || keyIdentifier[len(keyIdentifier)-1] == '/' {
		return false
	}

	return true
}

// encryptedDataKeyMetadata represents the metadata stored with encrypted data keys
type encryptedDataKeyMetadata struct {
	KeyID             string            `json:"keyId"`
	EncryptionContext map[string]string `json:"encryptionContext"`
	EncryptedData     []byte            `json:"encryptedData"`
	Nonce             []byte            `json:"nonce"` // Renamed from IV to be more explicit about AES-GCM usage
}

// encryptDataKey encrypts a data key using the master key with AES-GCM for authenticated encryption
func (p *LocalKMSProvider) encryptDataKey(dataKey []byte, masterKey *LocalKey, encryptionContext map[string]string) ([]byte, error) {
	block, err := aes.NewCipher(masterKey.KeyMaterial)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Generate a random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Prepare additional authenticated data (AAD) from the encryption context
	// Use deterministic marshaling to ensure consistent AAD
	var aad []byte
	if len(encryptionContext) > 0 {
		var err error
		aad, err = marshalEncryptionContextDeterministic(encryptionContext)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encryption context for AAD: %w", err)
		}
	}

	// Encrypt using AES-GCM
	encryptedData := gcm.Seal(nil, nonce, dataKey, aad)

	// Create metadata structure
	metadata := &encryptedDataKeyMetadata{
		KeyID:             masterKey.KeyID,
		EncryptionContext: encryptionContext,
		EncryptedData:     encryptedData,
		Nonce:             nonce,
	}

	// Serialize metadata to JSON
	return json.Marshal(metadata)
}

// decryptDataKey decrypts a data key using the master key with AES-GCM for authenticated decryption
func (p *LocalKMSProvider) decryptDataKey(metadata *encryptedDataKeyMetadata, masterKey *LocalKey) ([]byte, error) {
	block, err := aes.NewCipher(masterKey.KeyMaterial)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Prepare additional authenticated data (AAD)
	var aad []byte
	if len(metadata.EncryptionContext) > 0 {
		var err error
		aad, err = marshalEncryptionContextDeterministic(metadata.EncryptionContext)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encryption context for AAD: %w", err)
		}
	}

	// Decrypt using AES-GCM
	nonce := metadata.Nonce
	if len(nonce) != gcm.NonceSize() {
		return nil, fmt.Errorf("invalid nonce size: expected %d, got %d", gcm.NonceSize(), len(nonce))
	}

	dataKey, err := gcm.Open(nil, nonce, metadata.EncryptedData, aad)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt with GCM: %w", err)
	}

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
func generateKeyID() (string, error) {
	// Generate a UUID-like key ID
	b := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", fmt.Errorf("failed to generate random bytes for key ID: %w", err)
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// CreateKey creates a new key in the local KMS (for testing)
func (p *LocalKMSProvider) CreateKey(description string, aliases []string) (*LocalKey, error) {
	keyID, err := generateKeyID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate key ID: %w", err)
	}
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
		Origin:      kms.KeyOriginLocal,
		CreatedAt:   time.Now(),
		Aliases:     aliases,
		Metadata:    make(map[string]string),
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.keys[keyID] = key
	for _, alias := range aliases {
		// Ensure alias has proper format
		if !strings.HasPrefix(alias, "alias/") {
			alias = "alias/" + alias
		}
		p.keys[alias] = key
	}

	return key, nil
}

// CreateKeyWithID creates a key with a specific keyID (for testing only)
func (p *LocalKMSProvider) CreateKeyWithID(keyID, description string) (*LocalKey, error) {
	keyMaterial := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, keyMaterial); err != nil {
		return nil, fmt.Errorf("failed to generate key material: %w", err)
	}

	key := &LocalKey{
		KeyID:       keyID,
		ARN:         fmt.Sprintf("arn:aws:kms:local:000000000000:key/%s", keyID),
		Description: description,
		KeyMaterial: keyMaterial,
		KeyUsage:    kms.KeyUsageEncryptDecrypt,
		KeyState:    kms.KeyStateEnabled,
		Origin:      kms.KeyOriginLocal,
		CreatedAt:   time.Now(),
		Aliases:     []string{}, // No aliases by default
		Metadata:    make(map[string]string),
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Register key with the exact keyID provided
	p.keys[keyID] = key

	return key, nil
}

// marshalEncryptionContextDeterministic creates a deterministic byte representation of encryption context
// This ensures that the same encryption context always produces the same AAD for AES-GCM
func marshalEncryptionContextDeterministic(encryptionContext map[string]string) ([]byte, error) {
	if len(encryptionContext) == 0 {
		return nil, nil
	}

	// Sort keys to ensure deterministic output
	keys := make([]string, 0, len(encryptionContext))
	for k := range encryptionContext {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build deterministic representation with proper JSON escaping
	var buf strings.Builder
	buf.WriteString("{")
	for i, k := range keys {
		if i > 0 {
			buf.WriteString(",")
		}
		// Marshal key and value to get proper JSON string escaping
		keyBytes, err := json.Marshal(k)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encryption context key '%s': %w", k, err)
		}
		valueBytes, err := json.Marshal(encryptionContext[k])
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encryption context value for key '%s': %w", k, err)
		}
		buf.Write(keyBytes)
		buf.WriteString(":")
		buf.Write(valueBytes)
	}
	buf.WriteString("}")

	return []byte(buf.String()), nil
}
