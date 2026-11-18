package openbao

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	vault "github.com/hashicorp/vault/api"
	"golang.org/x/sync/singleflight"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	seaweedkms "github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	// Register the OpenBao/Vault KMS provider
	seaweedkms.RegisterProvider("openbao", NewOpenBaoKMSProvider)
	seaweedkms.RegisterProvider("vault", NewOpenBaoKMSProvider) // Alias for compatibility
}

// OpenBaoKMSProvider implements the KMSProvider interface using OpenBao/Vault Transit engine
type OpenBaoKMSProvider struct {
	client      *vault.Client
	transitPath string // Transit engine mount path (default: "transit")
	address     string

	// AppRole credentials, kept to log in again when the token expires or is revoked
	roleID   string
	secretID string
	logins   singleflight.Group // keyed by the refused token
	now      func() time.Time

	reloginTimeout time.Duration // bounds the shared check and login when the client has no timeout
	reloginWaiting func()        // test hook: called when a caller starts waiting for a re-login

	tokenMu         sync.Mutex
	canLookupSelf   bool      // the token could call auth/token/lookup-self when it was issued
	reloginAfter    time.Time // without lookup-self: until then a refusal is taken for a policy denial
	loginRetryAfter time.Time // after a failed login, no new attempt before this
}

// OpenBao/Vault answer 403 both to a token that is no longer valid (expired, revoked, out
// of uses) and to a key the token may not use; only the first is fixed by a new login, and
// a login per denied request would mint a new token each time. Right after each login the
// provider checks that the new token may call auth/token/lookup-self; if so, a refusal is
// settled by asking lookup-self again. If it may not (lookup-self taken out of the default
// policy, a proxy that does not pass it on), or lookup-self later fails for another reason,
// a refusal within reloginMinTokenAge of the login (or half the token's TTL, if shorter)
// is taken for a policy denial.
const reloginMinTokenAge = time.Minute

// defaultReloginTimeout bounds the logins and lookup-self checks when the client has no
// timeout of its own: a request_timeout that is not positive leaves Vault requests without
// one. A variable so that tests can shorten it.
var defaultReloginTimeout = 30 * time.Second

// reloginFailureBackoff is how long refused requests wait for a new login attempt after one failed.
const reloginFailureBackoff = 5 * time.Second

var (
	errPolicyDenied = errors.New("the token is valid but was refused: policy denial")
	errLoginBackoff = errors.New("an AppRole login failed moments ago")
)

// OpenBaoKMSConfig contains configuration for the OpenBao/Vault KMS provider
type OpenBaoKMSConfig struct {
	Address        string `json:"address"`         // Vault address (e.g., "http://localhost:8200")
	Token          string `json:"token"`           // Vault token for authentication
	RoleID         string `json:"role_id"`         // AppRole role ID (alternative to token)
	SecretID       string `json:"secret_id"`       // AppRole secret ID (alternative to token)
	TransitPath    string `json:"transit_path"`    // Transit engine mount path (default: "transit")
	TLSSkipVerify  bool   `json:"tls_skip_verify"` // Skip TLS verification (for testing)
	CACert         string `json:"ca_cert"`         // Path to CA certificate
	ClientCert     string `json:"client_cert"`     // Path to client certificate
	ClientKey      string `json:"client_key"`      // Path to client private key
	RequestTimeout int    `json:"request_timeout"` // Request timeout in seconds (default: 30)
}

// NewOpenBaoKMSProvider creates a new OpenBao/Vault KMS provider
func NewOpenBaoKMSProvider(config util.Configuration) (seaweedkms.KMSProvider, error) {
	if config == nil {
		return nil, fmt.Errorf("OpenBao/Vault KMS configuration is required")
	}

	// Extract configuration
	address := config.GetString("address")
	if address == "" {
		address = "http://localhost:8200" // Default OpenBao address
	}

	token := config.GetString("token")
	roleID := config.GetString("role_id")
	secretID := config.GetString("secret_id")
	transitPath := config.GetString("transit_path")
	if transitPath == "" {
		transitPath = "transit" // Default transit path
	}

	tlsSkipVerify := config.GetBool("tls_skip_verify")
	caCert := config.GetString("ca_cert")
	clientCert := config.GetString("client_cert")
	clientKey := config.GetString("client_key")

	requestTimeout := config.GetInt("request_timeout")
	if requestTimeout == 0 {
		requestTimeout = 30 // Default 30 seconds
	}

	// Create Vault client configuration
	vaultConfig := vault.DefaultConfig()
	vaultConfig.Address = address
	vaultConfig.Timeout = time.Duration(requestTimeout) * time.Second

	// Configure TLS
	if tlsSkipVerify || caCert != "" || (clientCert != "" && clientKey != "") {
		tlsConfig := &vault.TLSConfig{
			Insecure: tlsSkipVerify,
		}
		if caCert != "" {
			tlsConfig.CACert = caCert
		}
		if clientCert != "" && clientKey != "" {
			tlsConfig.ClientCert = clientCert
			tlsConfig.ClientKey = clientKey
		}

		if err := vaultConfig.ConfigureTLS(tlsConfig); err != nil {
			return nil, fmt.Errorf("failed to configure TLS: %w", err)
		}
	}

	// Create Vault client
	client, err := vault.NewClient(vaultConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenBao/Vault client: %w", err)
	}

	provider := &OpenBaoKMSProvider{
		client:      client,
		transitPath: transitPath,
		address:     address,
		now:         time.Now,

		reloginTimeout: defaultReloginTimeout,
	}

	// Authenticate
	if token != "" {
		client.SetToken(token)
		glog.V(1).Infof("OpenBao KMS: Using token authentication")
	} else if roleID != "" && secretID != "" {
		provider.roleID = roleID
		provider.secretID = secretID
		if err := provider.login(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to authenticate with AppRole: %w", err)
		}
		glog.V(1).Infof("OpenBao KMS: Using AppRole authentication")
	} else {
		return nil, fmt.Errorf("either token or role_id+secret_id must be provided")
	}

	glog.V(1).Infof("OpenBao/Vault KMS provider initialized at %s", address)
	return provider, nil
}

// authenticateAppRole authenticates using AppRole method and returns the new token, unset
func authenticateAppRole(ctx context.Context, client *vault.Client, roleID, secretID string) (*vault.SecretAuth, error) {
	data := map[string]interface{}{
		"role_id":   roleID,
		"secret_id": secretID,
	}

	secret, err := client.Logical().WriteWithContext(ctx, "auth/approle/login", data)
	if err != nil {
		return nil, fmt.Errorf("AppRole authentication failed: %w", err)
	}

	if secret == nil || secret.Auth == nil {
		return nil, fmt.Errorf("AppRole authentication returned empty token")
	}

	return secret.Auth, nil
}

// login logs in with AppRole and installs the new token together with what tells a later
// refusal of it from a policy denial.
func (p *OpenBaoKMSProvider) login(ctx context.Context) error {
	start := p.now() // the token's TTL may start before its response arrives
	auth, err := authenticateAppRole(ctx, p.client, p.roleID, p.secretID)
	if err != nil {
		p.tokenMu.Lock()
		p.loginRetryAfter = p.now().Add(reloginFailureBackoff)
		p.tokenMu.Unlock()
		return err
	}
	minAge := reloginMinTokenAge
	if ttl := time.Duration(auth.LeaseDuration) * time.Second; ttl > 0 && ttl/2 < minAge {
		minAge = ttl / 2
	}
	probeCtx, cancel := context.WithTimeout(ctx, p.boundedTimeout())
	canLookupSelf := p.lookupSelf(probeCtx, auth.ClientToken) == nil
	cancel()
	p.tokenMu.Lock()
	p.client.SetToken(auth.ClientToken)
	p.canLookupSelf = canLookupSelf
	p.reloginAfter = start.Add(minAge)
	p.tokenMu.Unlock()
	return nil
}

// boundedTimeout is the client's timeout, or reloginTimeout when the client has none.
func (p *OpenBaoKMSProvider) boundedTimeout() time.Duration {
	if timeout := p.client.ClientTimeout(); timeout > 0 {
		return timeout
	}
	return p.reloginTimeout
}

// lookupSelf calls auth/token/lookup-self with the given token.
func (p *OpenBaoKMSProvider) lookupSelf(ctx context.Context, token string) error {
	client, err := p.client.Clone()
	if err != nil {
		return err
	}
	client.SetToken(token)
	_, err = client.Auth().Token().LookupSelfWithContext(ctx)
	return err
}

// tokenStillValid tells whether a refused token is still valid, which makes the refusal
// a policy denial.
func (p *OpenBaoKMSProvider) tokenStillValid(ctx context.Context, token string, canLookupSelf, fresh bool) bool {
	if canLookupSelf {
		err := p.lookupSelf(ctx, token)
		if err == nil {
			return true
		}
		if isPermissionDenied(err) {
			return false // it could look itself up when issued, so it is no longer valid
		}
		glog.V(1).Infof("OpenBao KMS: lookup-self failed, judging the refusal by the token's age: %v", err)
	}
	return fresh
}

// withRelogin runs a request and, with AppRole authentication, logs in again once and
// retries when OpenBao/Vault refuses the token: an AppRole token expires after its TTL
// and is never renewed, so without this every request fails until a restart.
func (p *OpenBaoKMSProvider) withRelogin(ctx context.Context, call func() (*vault.Secret, error)) (*vault.Secret, error) {
	token := p.client.Token()
	secret, err := call()
	if err == nil || p.roleID == "" || !isPermissionDenied(err) {
		return secret, err
	}
	if loginErr := p.relogin(ctx, token); loginErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if !errors.Is(loginErr, errPolicyDenied) && !errors.Is(loginErr, errLoginBackoff) {
			glog.Warningf("OpenBao KMS: %v", loginErr)
		}
		return nil, err
	}
	return call()
}

// relogin logs in again, unless another request already did so after the refused token
// was used, or the token is still valid. Concurrent callers refused with the same token
// share one check and login and their result, failure included; that work is bounded by
// the client's timeout rather than by any one caller's context, and each caller stops
// waiting when its own context ends.
func (p *OpenBaoKMSProvider) relogin(ctx context.Context, refusedToken string) error {
	result := p.logins.DoChan(refusedToken, func() (interface{}, error) {
		p.tokenMu.Lock()
		current, canLookupSelf := p.client.Token(), p.canLookupSelf
		fresh, backoff := p.now().Before(p.reloginAfter), p.now().Before(p.loginRetryAfter)
		p.tokenMu.Unlock()
		if current != refusedToken {
			return nil, nil
		}
		if backoff {
			return nil, errLoginBackoff
		}
		sharedCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.boundedTimeout())
		defer cancel()
		if p.tokenStillValid(sharedCtx, refusedToken, canLookupSelf, fresh) {
			return nil, errPolicyDenied
		}
		glog.V(1).Infof("OpenBao KMS: token refused, logging in again with AppRole")
		return nil, p.login(sharedCtx)
	})
	if p.reloginWaiting != nil {
		p.reloginWaiting()
	}
	select {
	case r := <-result:
		return r.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func isPermissionDenied(err error) bool {
	var respErr *vault.ResponseError
	return errors.As(err, &respErr) && respErr.StatusCode == http.StatusForbidden
}

// GenerateDataKey generates a new data encryption key using OpenBao/Vault Transit
func (p *OpenBaoKMSProvider) GenerateDataKey(ctx context.Context, req *seaweedkms.GenerateDataKeyRequest) (*seaweedkms.GenerateDataKeyResponse, error) {
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

	// Generate data key locally (similar to Azure/GCP approach)
	dataKey := make([]byte, keySize)
	if _, err := rand.Read(dataKey); err != nil {
		return nil, fmt.Errorf("failed to generate random data key: %w", err)
	}

	// Encrypt the data key using OpenBao/Vault Transit
	glog.V(4).Infof("OpenBao KMS: Encrypting data key using key %s", req.KeyID)

	// Prepare encryption data
	encryptData := map[string]interface{}{
		"plaintext": base64.StdEncoding.EncodeToString(dataKey),
	}

	// Add encryption context if provided
	if len(req.EncryptionContext) > 0 {
		contextJSON, err := json.Marshal(req.EncryptionContext)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encryption context: %w", err)
		}
		encryptData["context"] = base64.StdEncoding.EncodeToString(contextJSON)
	}

	// Call OpenBao/Vault Transit encrypt endpoint
	path := fmt.Sprintf("%s/encrypt/%s", p.transitPath, req.KeyID)
	secret, err := p.withRelogin(ctx, func() (*vault.Secret, error) {
		return p.client.Logical().WriteWithContext(ctx, path, encryptData)
	})
	if err != nil {
		return nil, p.convertVaultError(err, req.KeyID)
	}

	if secret == nil || secret.Data == nil {
		return nil, fmt.Errorf("no data returned from OpenBao/Vault encrypt operation")
	}

	ciphertext, ok := secret.Data["ciphertext"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid ciphertext format from OpenBao/Vault")
	}

	// Create standardized envelope format for consistent API behavior
	envelopeBlob, err := seaweedkms.CreateEnvelope("openbao", req.KeyID, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create ciphertext envelope: %w", err)
	}

	response := &seaweedkms.GenerateDataKeyResponse{
		KeyID:          req.KeyID,
		Plaintext:      dataKey,
		CiphertextBlob: envelopeBlob, // Store in standardized envelope format
	}

	glog.V(4).Infof("OpenBao KMS: Generated and encrypted data key using key %s", req.KeyID)
	return response, nil
}

// Decrypt decrypts an encrypted data key using OpenBao/Vault Transit
func (p *OpenBaoKMSProvider) Decrypt(ctx context.Context, req *seaweedkms.DecryptRequest) (*seaweedkms.DecryptResponse, error) {
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

	// Use the ciphertext from envelope
	ciphertext := envelope.Ciphertext

	// Prepare decryption data
	decryptData := map[string]interface{}{
		"ciphertext": ciphertext,
	}

	// Add encryption context if provided
	if len(req.EncryptionContext) > 0 {
		contextJSON, err := json.Marshal(req.EncryptionContext)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal encryption context: %w", err)
		}
		decryptData["context"] = base64.StdEncoding.EncodeToString(contextJSON)
	}

	// Call OpenBao/Vault Transit decrypt endpoint
	path := fmt.Sprintf("%s/decrypt/%s", p.transitPath, keyID)
	glog.V(4).Infof("OpenBao KMS: Decrypting data key using key %s", keyID)
	secret, err := p.withRelogin(ctx, func() (*vault.Secret, error) {
		return p.client.Logical().WriteWithContext(ctx, path, decryptData)
	})
	if err != nil {
		return nil, p.convertVaultError(err, keyID)
	}

	if secret == nil || secret.Data == nil {
		return nil, fmt.Errorf("no data returned from OpenBao/Vault decrypt operation")
	}

	plaintextB64, ok := secret.Data["plaintext"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid plaintext format from OpenBao/Vault")
	}

	plaintext, err := base64.StdEncoding.DecodeString(plaintextB64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode plaintext from OpenBao/Vault: %w", err)
	}

	response := &seaweedkms.DecryptResponse{
		KeyID:     keyID,
		Plaintext: plaintext,
	}

	glog.V(4).Infof("OpenBao KMS: Decrypted data key using key %s", keyID)
	return response, nil
}

// DescribeKey validates that a key exists and returns its metadata
func (p *OpenBaoKMSProvider) DescribeKey(ctx context.Context, req *seaweedkms.DescribeKeyRequest) (*seaweedkms.DescribeKeyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("DescribeKeyRequest cannot be nil")
	}

	if req.KeyID == "" {
		return nil, fmt.Errorf("KeyID is required")
	}

	// Get key information from OpenBao/Vault
	path := fmt.Sprintf("%s/keys/%s", p.transitPath, req.KeyID)
	glog.V(4).Infof("OpenBao KMS: Describing key %s", req.KeyID)
	secret, err := p.withRelogin(ctx, func() (*vault.Secret, error) {
		return p.client.Logical().ReadWithContext(ctx, path)
	})
	if err != nil {
		return nil, p.convertVaultError(err, req.KeyID)
	}

	if secret == nil || secret.Data == nil {
		return nil, &seaweedkms.KMSError{
			Code:    seaweedkms.ErrCodeNotFoundException,
			Message: fmt.Sprintf("Key not found: %s", req.KeyID),
			KeyID:   req.KeyID,
		}
	}

	response := &seaweedkms.DescribeKeyResponse{
		KeyID:       req.KeyID,
		ARN:         fmt.Sprintf("openbao:%s:key:%s", p.address, req.KeyID),
		Description: "OpenBao/Vault Transit engine key",
	}

	// Check key type and set usage
	if keyType, ok := secret.Data["type"].(string); ok {
		if keyType == "aes256-gcm96" || keyType == "aes128-gcm96" || keyType == "chacha20-poly1305" {
			response.KeyUsage = seaweedkms.KeyUsageEncryptDecrypt
		} else {
			// Default to data key generation if not an encrypt/decrypt type
			response.KeyUsage = seaweedkms.KeyUsageGenerateDataKey
		}
	} else {
		// If type is missing, default to data key generation
		response.KeyUsage = seaweedkms.KeyUsageGenerateDataKey
	}

	// OpenBao/Vault keys are enabled by default (no disabled state in transit)
	response.KeyState = seaweedkms.KeyStateEnabled

	// Keys in OpenBao/Vault transit are service-managed
	response.Origin = seaweedkms.KeyOriginOpenBao

	glog.V(4).Infof("OpenBao KMS: Described key %s (state: %s)", req.KeyID, response.KeyState)
	return response, nil
}

// GetKeyID resolves a key name (already the full key ID in OpenBao/Vault)
func (p *OpenBaoKMSProvider) GetKeyID(ctx context.Context, keyIdentifier string) (string, error) {
	if keyIdentifier == "" {
		return "", fmt.Errorf("key identifier cannot be empty")
	}

	// Use DescribeKey to validate the key exists
	descReq := &seaweedkms.DescribeKeyRequest{KeyID: keyIdentifier}
	descResp, err := p.DescribeKey(ctx, descReq)
	if err != nil {
		return "", fmt.Errorf("failed to resolve key identifier %s: %w", keyIdentifier, err)
	}

	return descResp.KeyID, nil
}

// Close cleans up any resources used by the provider
func (p *OpenBaoKMSProvider) Close() error {
	// OpenBao/Vault client doesn't require explicit cleanup
	glog.V(2).Infof("OpenBao/Vault KMS provider closed")
	return nil
}

// convertVaultError converts OpenBao/Vault errors to our standard KMS errors
func (p *OpenBaoKMSProvider) convertVaultError(err error, keyID string) error {
	errMsg := err.Error()

	if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "no handler") {
		return &seaweedkms.KMSError{
			Code:    seaweedkms.ErrCodeNotFoundException,
			Message: fmt.Sprintf("Key not found in OpenBao/Vault: %v", err),
			KeyID:   keyID,
		}
	}

	if strings.Contains(errMsg, "permission") || strings.Contains(errMsg, "denied") || strings.Contains(errMsg, "forbidden") {
		return &seaweedkms.KMSError{
			Code:    seaweedkms.ErrCodeAccessDenied,
			Message: fmt.Sprintf("Access denied to OpenBao/Vault: %v", err),
			KeyID:   keyID,
		}
	}

	if strings.Contains(errMsg, "disabled") || strings.Contains(errMsg, "unavailable") {
		return &seaweedkms.KMSError{
			Code:    seaweedkms.ErrCodeKeyUnavailable,
			Message: fmt.Sprintf("Key unavailable in OpenBao/Vault: %v", err),
			KeyID:   keyID,
		}
	}

	// For unknown errors, wrap as internal failure
	return &seaweedkms.KMSError{
		Code:    seaweedkms.ErrCodeKMSInternalFailure,
		Message: fmt.Sprintf("OpenBao/Vault error: %v", err),
		KeyID:   keyID,
	}
}
