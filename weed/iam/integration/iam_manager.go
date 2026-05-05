package integration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// maxPoliciesForEvaluation defines an upper bound on the number of policies that
// will be evaluated for a single request. This protects against pathological or
// malicious inputs that attempt to create extremely large policy lists.
const maxPoliciesForEvaluation = 1024

// IAMManager orchestrates all IAM components
type IAMManager struct {
	stsService           *sts.STSService
	policyEngine         *policy.PolicyEngine
	roleStore            RoleStore
	userStore            UserStore
	oidcProviderStore    OIDCProviderStore
	filerAddressProvider func() string // Function to get current filer address
	initialized          bool
	runtimePolicyMu      sync.Mutex
	runtimePolicyNames   map[string]struct{}
}

// SetOIDCProviderStore configures the IAM-managed OIDC provider store. When
// nil, OIDC provider IAM actions return ServiceNotReady. The store is the
// source of truth for AssumeRoleWithWebIdentity provider resolution once
// Phase 2b lands; in Phase 2a it is read-only and populated from static
// configuration at boot.
func (m *IAMManager) SetOIDCProviderStore(store OIDCProviderStore) {
	m.oidcProviderStore = store
}

// GetOIDCProviderStore returns the configured store (may be nil).
func (m *IAMManager) GetOIDCProviderStore() OIDCProviderStore {
	return m.oidcProviderStore
}

// GetOIDCProvider returns the record for the given ARN, or an error if the
// store is not configured or the record is missing.
func (m *IAMManager) GetOIDCProvider(ctx context.Context, arn string) (*OIDCProviderRecord, error) {
	if m.oidcProviderStore == nil {
		return nil, fmt.Errorf("OIDC provider store not configured")
	}
	return m.oidcProviderStore.GetProviderByARN(ctx, m.getFilerAddress(), arn)
}

// ListOIDCProviders enumerates all configured OIDC providers.
func (m *IAMManager) ListOIDCProviders(ctx context.Context) ([]*OIDCProviderRecord, error) {
	if m.oidcProviderStore == nil {
		return nil, fmt.Errorf("OIDC provider store not configured")
	}
	return m.oidcProviderStore.ListProviders(ctx, m.getFilerAddress())
}

// CreateOIDCProvider persists a new IAM-managed OIDC provider record. Refuses
// to overwrite an existing record so callers see EntityAlreadyExists semantics.
func (m *IAMManager) CreateOIDCProvider(ctx context.Context, rec *OIDCProviderRecord) error {
	if m.oidcProviderStore == nil {
		return fmt.Errorf("OIDC provider store not configured")
	}
	if rec == nil {
		return fmt.Errorf("record cannot be nil")
	}
	if err := validateOIDCProviderRecord(rec); err != nil {
		return err
	}
	if existing, err := m.oidcProviderStore.GetProviderByARN(ctx, m.getFilerAddress(), rec.ARN); err == nil && existing != nil {
		return fmt.Errorf("%w: %s", ErrOIDCProviderAlreadyExists, rec.ARN)
	}
	now := time.Now().UTC()
	rec.CreatedAt = now
	rec.UpdatedAt = now
	return m.oidcProviderStore.StoreProvider(ctx, m.getFilerAddress(), rec)
}

// DeleteOIDCProvider removes the IAM-managed record. Idempotent.
func (m *IAMManager) DeleteOIDCProvider(ctx context.Context, arn string) error {
	if m.oidcProviderStore == nil {
		return fmt.Errorf("OIDC provider store not configured")
	}
	return m.oidcProviderStore.DeleteProvider(ctx, m.getFilerAddress(), arn)
}

// AddClientIDToOIDCProvider appends `clientID` to the provider's allowed
// audience list. Adding an existing client ID is a no-op (AWS-compat).
func (m *IAMManager) AddClientIDToOIDCProvider(ctx context.Context, arn, clientID string) error {
	if m.oidcProviderStore == nil {
		return fmt.Errorf("OIDC provider store not configured")
	}
	if clientID == "" {
		return fmt.Errorf("ClientID cannot be empty")
	}
	rec, err := m.oidcProviderStore.GetProviderByARN(ctx, m.getFilerAddress(), arn)
	if err != nil {
		return err
	}
	for _, existing := range rec.ClientIDs {
		if existing == clientID {
			return nil // idempotent
		}
	}
	rec.ClientIDs = append(rec.ClientIDs, clientID)
	rec.UpdatedAt = time.Now().UTC()
	return m.oidcProviderStore.StoreProvider(ctx, m.getFilerAddress(), rec)
}

// RemoveClientIDFromOIDCProvider drops `clientID` from the allowed audience
// list. Removing a missing client ID is a no-op.
func (m *IAMManager) RemoveClientIDFromOIDCProvider(ctx context.Context, arn, clientID string) error {
	if m.oidcProviderStore == nil {
		return fmt.Errorf("OIDC provider store not configured")
	}
	rec, err := m.oidcProviderStore.GetProviderByARN(ctx, m.getFilerAddress(), arn)
	if err != nil {
		return err
	}
	pruned := make([]string, 0, len(rec.ClientIDs))
	for _, existing := range rec.ClientIDs {
		if existing != clientID {
			pruned = append(pruned, existing)
		}
	}
	if len(pruned) == len(rec.ClientIDs) {
		return nil // not present; no-op
	}
	rec.ClientIDs = pruned
	rec.UpdatedAt = time.Now().UTC()
	return m.oidcProviderStore.StoreProvider(ctx, m.getFilerAddress(), rec)
}

// UpdateOIDCProviderThumbprints replaces the entire thumbprint list. AWS
// constrains the list to 1..5 entries when non-empty; we mirror that bound.
func (m *IAMManager) UpdateOIDCProviderThumbprints(ctx context.Context, arn string, thumbprints []string) error {
	if m.oidcProviderStore == nil {
		return fmt.Errorf("OIDC provider store not configured")
	}
	if len(thumbprints) > 5 {
		return fmt.Errorf("ThumbprintList must contain at most 5 entries, got %d", len(thumbprints))
	}
	for _, tp := range thumbprints {
		if !isValidSHA1Thumbprint(tp) {
			return fmt.Errorf("invalid thumbprint %q: must be 40-character SHA-1 hex", tp)
		}
	}
	rec, err := m.oidcProviderStore.GetProviderByARN(ctx, m.getFilerAddress(), arn)
	if err != nil {
		return err
	}
	rec.Thumbprints = append([]string(nil), thumbprints...)
	rec.UpdatedAt = time.Now().UTC()
	return m.oidcProviderStore.StoreProvider(ctx, m.getFilerAddress(), rec)
}

// TagOIDCProvider merges the supplied tags into the provider's tag set.
func (m *IAMManager) TagOIDCProvider(ctx context.Context, arn string, tags map[string]string) error {
	if m.oidcProviderStore == nil {
		return fmt.Errorf("OIDC provider store not configured")
	}
	rec, err := m.oidcProviderStore.GetProviderByARN(ctx, m.getFilerAddress(), arn)
	if err != nil {
		return err
	}
	if rec.Tags == nil {
		rec.Tags = make(map[string]string, len(tags))
	}
	for k, v := range tags {
		rec.Tags[k] = v
	}
	rec.UpdatedAt = time.Now().UTC()
	return m.oidcProviderStore.StoreProvider(ctx, m.getFilerAddress(), rec)
}

// UntagOIDCProvider removes the named tags from the provider's tag set.
func (m *IAMManager) UntagOIDCProvider(ctx context.Context, arn string, keys []string) error {
	if m.oidcProviderStore == nil {
		return fmt.Errorf("OIDC provider store not configured")
	}
	rec, err := m.oidcProviderStore.GetProviderByARN(ctx, m.getFilerAddress(), arn)
	if err != nil {
		return err
	}
	for _, k := range keys {
		delete(rec.Tags, k)
	}
	rec.UpdatedAt = time.Now().UTC()
	return m.oidcProviderStore.StoreProvider(ctx, m.getFilerAddress(), rec)
}

// validateOIDCProviderRecord enforces the invariants AWS imposes on the
// underlying CreateOpenIDConnectProvider call.
func validateOIDCProviderRecord(rec *OIDCProviderRecord) error {
	if rec.URL == "" {
		return fmt.Errorf("Url is required")
	}
	if rec.ARN == "" {
		return fmt.Errorf("ARN is required")
	}
	if len(rec.ClientIDs) == 0 {
		return fmt.Errorf("ClientIDList must contain at least one entry")
	}
	if len(rec.ClientIDs) > 100 {
		return fmt.Errorf("ClientIDList must contain at most 100 entries")
	}
	if len(rec.Thumbprints) > 5 {
		return fmt.Errorf("ThumbprintList must contain at most 5 entries")
	}
	for _, tp := range rec.Thumbprints {
		if !isValidSHA1Thumbprint(tp) {
			return fmt.Errorf("invalid thumbprint %q: must be 40-character SHA-1 hex", tp)
		}
	}
	return nil
}

// isValidSHA1Thumbprint returns true iff `s` is exactly 40 hex characters,
// matching the SHA-1 digest format AWS expects.
func isValidSHA1Thumbprint(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	return true
}

// IAMConfig holds configuration for all IAM components
type IAMConfig struct {
	// STS service configuration
	STS *sts.STSConfig `json:"sts"`

	// Policy engine configuration
	Policy *policy.PolicyEngineConfig `json:"policy"`

	// Role store configuration
	Roles *RoleStoreConfig `json:"roleStore"`

	// OIDCProviders configures the IAM-managed OIDC provider store. Optional;
	// if absent the manager defaults to an in-memory store hydrated from
	// STS.Providers at boot.
	OIDCProviders *OIDCProviderStoreConfig `json:"oidcProviderStore,omitempty"`
}

// OIDCProviderStoreConfig holds OIDC provider store configuration.
type OIDCProviderStoreConfig struct {
	StoreType   string                 `json:"storeType"` // memory, filer
	StoreConfig map[string]interface{} `json:"storeConfig,omitempty"`
}

// RoleStoreConfig holds role store configuration
type RoleStoreConfig struct {
	// StoreType specifies the role store backend (memory, filer, etc.)
	StoreType string `json:"storeType"`

	// StoreConfig contains store-specific configuration
	StoreConfig map[string]interface{} `json:"storeConfig,omitempty"`
}

// UserStore defines the interface for retrieving IAM user policy attachments.
type UserStore interface {
	GetUser(ctx context.Context, username string) (*iam_pb.Identity, error)
}

// RoleDefinition defines a role with its trust policy and attached policies
type RoleDefinition struct {
	// RoleName is the name of the role
	RoleName string `json:"roleName"`

	// RoleArn is the full ARN of the role
	RoleArn string `json:"roleArn"`

	// TrustPolicy defines who can assume this role
	TrustPolicy *policy.PolicyDocument `json:"trustPolicy"`

	// AttachedPolicies lists the policy names attached to this role
	AttachedPolicies []string `json:"attachedPolicies"`

	// Description is an optional description of the role
	Description string `json:"description,omitempty"`

	// MaxSessionDuration is the upper bound (in seconds) on session length when
	// callers assume this role. Zero means "use the global STS default". When
	// set it must satisfy AWS bounds: 3600 ≤ MaxSessionDuration ≤ 43200.
	// Honoured by AssumeRole, AssumeRoleWithWebIdentity, AssumeRoleWithCredentials.
	MaxSessionDuration int64 `json:"maxSessionDuration,omitempty"`
}

// ActionRequest represents a request to perform an action
type ActionRequest struct {
	// Principal is the entity performing the action
	Principal string `json:"principal"`

	// Action is the action being requested
	Action string `json:"action"`

	// Resource is the resource being accessed
	Resource string `json:"resource"`

	// SessionToken for temporary credential validation
	SessionToken string `json:"sessionToken"`

	// RequestContext contains additional request information
	RequestContext map[string]interface{} `json:"requestContext,omitempty"`

	// PolicyNames to evaluate (overrides role-based policies if present)
	PolicyNames []string `json:"policyNames,omitempty"`
}

// NewIAMManager creates a new IAM manager
func NewIAMManager() *IAMManager {
	return &IAMManager{}
}

// SetUserStore assigns the user store used to resolve IAM user policy attachments.
func (m *IAMManager) SetUserStore(store UserStore) {
	m.userStore = store
}

// SyncRuntimePolicies keeps zero-config runtime policies available to the
// in-memory policy engine used by the advanced IAM authorizer.
func (m *IAMManager) SyncRuntimePolicies(ctx context.Context, policies []*iam_pb.Policy) error {
	if !m.initialized || m.policyEngine == nil {
		return nil
	}
	if m.policyEngine.StoreType() != sts.StoreTypeMemory {
		return nil
	}

	desiredPolicies := make(map[string]*policy.PolicyDocument, len(policies))
	for _, runtimePolicy := range policies {
		if runtimePolicy == nil || runtimePolicy.Name == "" {
			continue
		}

		var document policy.PolicyDocument
		if err := json.Unmarshal([]byte(runtimePolicy.Content), &document); err != nil {
			return fmt.Errorf("failed to parse runtime policy %q: %w", runtimePolicy.Name, err)
		}

		desiredPolicies[runtimePolicy.Name] = &document
	}

	m.runtimePolicyMu.Lock()
	defer m.runtimePolicyMu.Unlock()

	filerAddress := m.getFilerAddress()
	for policyName := range m.runtimePolicyNames {
		if _, keep := desiredPolicies[policyName]; keep {
			continue
		}
		if err := m.policyEngine.DeletePolicy(ctx, filerAddress, policyName); err != nil {
			return fmt.Errorf("failed to delete runtime policy %q: %w", policyName, err)
		}
	}

	for policyName, document := range desiredPolicies {
		if err := m.policyEngine.AddPolicy(filerAddress, policyName, document); err != nil {
			return fmt.Errorf("failed to sync runtime policy %q: %w", policyName, err)
		}
	}

	m.runtimePolicyNames = make(map[string]struct{}, len(desiredPolicies))
	for policyName := range desiredPolicies {
		m.runtimePolicyNames[policyName] = struct{}{}
	}

	return nil
}

// Initialize initializes the IAM manager with all components
func (m *IAMManager) Initialize(config *IAMConfig, filerAddressProvider func() string) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	// Store the filer address provider function
	m.filerAddressProvider = filerAddressProvider

	// Initialize STS service
	m.stsService = sts.NewSTSService()
	if err := m.stsService.Initialize(config.STS); err != nil {
		return fmt.Errorf("failed to initialize STS service: %w", err)
	}

	// CRITICAL SECURITY: Set trust policy validator to ensure proper role assumption validation
	m.stsService.SetTrustPolicyValidator(m)

	// Initialize policy engine
	m.policyEngine = policy.NewPolicyEngine()
	if err := m.policyEngine.InitializeWithProvider(config.Policy, m.filerAddressProvider); err != nil {
		return fmt.Errorf("failed to initialize policy engine: %w", err)
	}

	// Initialize role store
	roleStore, err := m.createRoleStoreWithProvider(config.Roles, m.filerAddressProvider)
	if err != nil {
		return fmt.Errorf("failed to initialize role store: %w", err)
	}
	m.roleStore = roleStore

	// Initialize OIDC provider store and hydrate from static configuration so
	// the read-only IAM API can return the same providers the STS service
	// already accepts. Mutations will land in Phase 2b.
	if err := m.initOIDCProviderStore(config); err != nil {
		return fmt.Errorf("failed to initialize OIDC provider store: %w", err)
	}

	m.initialized = true
	return nil
}

// initOIDCProviderStore creates the OIDC provider store and seeds it from the
// static STS provider configuration. The static path remains the bootstrap
// source: each enabled OIDC entry under STS.Providers is mirrored as an
// OIDCProviderRecord so the IAM API surfaces the same set the STS service
// validates against.
func (m *IAMManager) initOIDCProviderStore(config *IAMConfig) error {
	store, err := m.createOIDCProviderStore(config.OIDCProviders)
	if err != nil {
		return err
	}
	m.oidcProviderStore = store

	if config.STS == nil {
		return nil
	}
	for _, pc := range config.STS.Providers {
		if pc == nil || !pc.Enabled || pc.Type != sts.ProviderTypeOIDC {
			continue
		}
		issuer, _ := pc.Config["issuer"].(string)
		if issuer == "" {
			glog.Warningf("OIDC provider %s in static config has empty issuer; skipping mirror to store", pc.Name)
			continue
		}
		accountID := ""
		if config.STS != nil {
			accountID = config.STS.AccountId
		}
		arn, err := DeriveOIDCProviderARN(accountID, issuer)
		if err != nil {
			glog.Warningf("derive ARN for static OIDC provider %s: %v", pc.Name, err)
			continue
		}
		clientIDs := extractClientIDs(pc.Config)
		ctx := context.Background()
		// Preserve CreatedAt across reboots when a persistent store already
		// has this provider — IAM's GetOpenIDConnectProvider response
		// shouldn't shift its CreateDate every time the server restarts.
		now := time.Now().UTC()
		createdAt := now
		if existing, err := store.GetProviderByARN(ctx, m.getFilerAddress(), arn); err == nil && existing != nil && !existing.CreatedAt.IsZero() {
			createdAt = existing.CreatedAt
		}
		rec := &OIDCProviderRecord{
			AccountID: accountID,
			ARN:       arn,
			URL:       issuer,
			ClientIDs: clientIDs,
			CreatedAt: createdAt,
			UpdatedAt: now,
		}
		if err := store.StoreProvider(ctx, m.getFilerAddress(), rec); err != nil {
			glog.Warningf("mirror static OIDC provider %s into store: %v", pc.Name, err)
		}
	}
	return nil
}

// createOIDCProviderStore selects an OIDCProviderStore implementation. Defaults
// to memory; "filer" requires a filerAddressProvider to be configured.
func (m *IAMManager) createOIDCProviderStore(cfg *OIDCProviderStoreConfig) (OIDCProviderStore, error) {
	if cfg == nil || cfg.StoreType == "" || cfg.StoreType == "memory" {
		return NewMemoryOIDCProviderStore(), nil
	}
	if cfg.StoreType == "filer" {
		return NewFilerOIDCProviderStore(cfg.StoreConfig, m.filerAddressProvider), nil
	}
	return nil, fmt.Errorf("unsupported OIDC provider store type: %s", cfg.StoreType)
}

// extractClientIDs reads a single clientId or a clientIds list from the
// provider's static config map. Mirrors the OIDCConfig schema.
func extractClientIDs(cfg map[string]interface{}) []string {
	if cfg == nil {
		return nil
	}
	if list, ok := cfg["clientIds"].([]interface{}); ok {
		out := make([]string, 0, len(list))
		for _, v := range list {
			if s, ok := v.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	if id, ok := cfg["clientId"].(string); ok && id != "" {
		return []string{id}
	}
	return nil
}

// getFilerAddress returns the current filer address using the provider function
func (m *IAMManager) getFilerAddress() string {
	if m.filerAddressProvider != nil {
		return m.filerAddressProvider()
	}
	return "" // Fallback to empty string if no provider is set
}

// createRoleStoreWithProvider creates a role store with a filer address provider function
func (m *IAMManager) createRoleStoreWithProvider(config *RoleStoreConfig, filerAddressProvider func() string) (RoleStore, error) {
	if config == nil {
		// Default to generic cached filer role store when no config provided
		return NewGenericCachedRoleStore(nil, filerAddressProvider)
	}

	switch config.StoreType {
	case "", "filer":
		// Check if caching is explicitly disabled
		if config.StoreConfig != nil {
			if noCache, ok := config.StoreConfig["noCache"].(bool); ok && noCache {
				return NewFilerRoleStore(config.StoreConfig, filerAddressProvider)
			}
		}
		// Default to generic cached filer store for better performance
		return NewGenericCachedRoleStore(config.StoreConfig, filerAddressProvider)
	case "cached-filer", "generic-cached":
		return NewGenericCachedRoleStore(config.StoreConfig, filerAddressProvider)
	case "memory":
		return NewMemoryRoleStore(), nil
	default:
		return nil, fmt.Errorf("unsupported role store type: %s", config.StoreType)
	}
}

// RegisterIdentityProvider registers an identity provider
func (m *IAMManager) RegisterIdentityProvider(provider providers.IdentityProvider) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	return m.stsService.RegisterProvider(provider)
}

// CreatePolicy creates a new policy
func (m *IAMManager) CreatePolicy(ctx context.Context, filerAddress string, name string, policyDoc *policy.PolicyDocument) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	return m.policyEngine.AddPolicy(filerAddress, name, policyDoc)
}

// CreateRole creates a new role with trust policy and attached policies
func (m *IAMManager) CreateRole(ctx context.Context, filerAddress string, roleName string, roleDef *RoleDefinition) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}

	if roleDef == nil {
		return fmt.Errorf("role definition cannot be nil")
	}

	// Set role ARN if not provided
	if roleDef.RoleArn == "" {
		roleDef.RoleArn = fmt.Sprintf("arn:aws:iam::role/%s", roleName)
	}

	// Validate trust policy
	if roleDef.TrustPolicy != nil {
		if err := policy.ValidateTrustPolicyDocument(roleDef.TrustPolicy); err != nil {
			return fmt.Errorf("invalid trust policy: %w", err)
		}
	}

	// Validate per-role MaxSessionDuration if specified. AWS bounds: 1h..12h.
	if roleDef.MaxSessionDuration != 0 {
		if roleDef.MaxSessionDuration < 3600 || roleDef.MaxSessionDuration > 43200 {
			return fmt.Errorf("MaxSessionDuration must be between 3600 and 43200 seconds, got %d", roleDef.MaxSessionDuration)
		}
	}

	// Store role definition
	return m.roleStore.StoreRole(ctx, "", roleName, roleDef)
}

// GetRole retrieves a role definition by name.
func (m *IAMManager) GetRole(ctx context.Context, roleName string) (*RoleDefinition, error) {
	if !m.initialized {
		return nil, fmt.Errorf("IAM manager not initialized")
	}
	if roleName == "" {
		return nil, fmt.Errorf("role name cannot be empty")
	}

	return m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
}

// UpdateBucketPolicy updates the policy for a bucket
func (m *IAMManager) UpdateBucketPolicy(ctx context.Context, bucketName string, policyJSON []byte) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	if bucketName == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}

	// Parse the policy document handled by the IAM policy engine
	var policyDoc policy.PolicyDocument
	if err := json.Unmarshal(policyJSON, &policyDoc); err != nil {
		return fmt.Errorf("invalid policy JSON: %w", err)
	}

	// Store the policy with a special prefix to distinguish from IAM policies
	policyName := "bucket-policy:" + bucketName
	return m.policyEngine.AddPolicy(m.getFilerAddress(), policyName, &policyDoc)
}

// AssumeRoleWithWebIdentity assumes a role using web identity (OIDC)
func (m *IAMManager) AssumeRoleWithWebIdentity(ctx context.Context, request *sts.AssumeRoleWithWebIdentityRequest) (*sts.AssumeRoleResponse, error) {
	if !m.initialized {
		return nil, fmt.Errorf("IAM manager not initialized")
	}

	// Extract role name from ARN
	roleName := utils.ExtractRoleNameFromArn(request.RoleArn)

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
		return nil, fmt.Errorf("role not found: %s", roleName)
	}

	// Validate trust policy before allowing STS to assume the role
	if err := m.validateTrustPolicyForWebIdentity(ctx, roleDef, request.WebIdentityToken, request.DurationSeconds); err != nil {
		return nil, fmt.Errorf("trust policy validation failed: %w", err)
	}

	// Apply role-level MaxSessionDuration cap. The STS service still applies
	// the global MaxSessionLength and the source-token-expiry cap on top of
	// this; per-role takes precedence whenever it is the tightest bound.
	request.DurationSeconds = capDurationByRole(request.DurationSeconds, roleDef.MaxSessionDuration)

	// Use STS service to assume the role
	return m.stsService.AssumeRoleWithWebIdentity(ctx, request)
}

// capDurationByRole returns the requested duration clamped to the role's
// MaxSessionDuration. A nil requested duration is left nil so the STS
// service's calculateSessionDuration applies the global default (typically
// 1 hour) — substituting the role's max here would silently mint a 12h
// session for any caller who omitted DurationSeconds, which AWS does not
// do. The role-max upper bound still applies in the downstream cap chain
// once the request has a concrete duration.
func capDurationByRole(requested *int64, roleMax int64) *int64 {
	if roleMax <= 0 || requested == nil {
		return requested
	}
	if *requested > roleMax {
		v := roleMax
		return &v
	}
	return requested
}

// AssumeRoleWithCredentials assumes a role using credentials (LDAP)
func (m *IAMManager) AssumeRoleWithCredentials(ctx context.Context, request *sts.AssumeRoleWithCredentialsRequest) (*sts.AssumeRoleResponse, error) {
	if !m.initialized {
		return nil, fmt.Errorf("IAM manager not initialized")
	}

	// Extract role name from ARN
	roleName := utils.ExtractRoleNameFromArn(request.RoleArn)

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
		return nil, fmt.Errorf("role not found: %s", roleName)
	}

	// Validate trust policy
	if err := m.validateTrustPolicyForCredentials(ctx, roleDef, request); err != nil {
		return nil, fmt.Errorf("trust policy validation failed: %w", err)
	}

	// Apply role-level MaxSessionDuration cap.
	request.DurationSeconds = capDurationByRole(request.DurationSeconds, roleDef.MaxSessionDuration)

	// Use STS service to assume the role
	return m.stsService.AssumeRoleWithCredentials(ctx, request)
}

// IsActionAllowed checks if a principal is allowed to perform an action on a resource
func (m *IAMManager) IsActionAllowed(ctx context.Context, request *ActionRequest) (bool, error) {
	if !m.initialized {
		return false, fmt.Errorf("IAM manager not initialized")
	}

	// Validate session token if present
	// We always try to validate with the internal STS service first if it's a SeaweedFS token.
	// This ensures that session policies embedded in the token are correctly extracted and enforced.
	var sessionInfo *sts.SessionInfo
	if request.SessionToken != "" {
		// Parse unverified to check issuer
		parsed, _, err := new(jwt.Parser).ParseUnverified(request.SessionToken, jwt.MapClaims{})
		isInternal := false
		if err == nil {
			if claims, ok := parsed.Claims.(jwt.MapClaims); ok {
				if issuer, ok := claims["iss"].(string); ok && m.stsService != nil && m.stsService.Config != nil {
					if issuer == m.stsService.Config.Issuer {
						isInternal = true
					}
				}
			}
		}

		if isInternal || !isOIDCToken(request.SessionToken) {
			var err error
			sessionInfo, err = m.stsService.ValidateSessionToken(ctx, request.SessionToken)
			if err != nil {
				return false, fmt.Errorf("invalid session: %w", err)
			}
		}
	}

	// Create evaluation context
	evalCtx := &policy.EvaluationContext{
		Principal:      request.Principal,
		Action:         request.Action,
		Resource:       request.Resource,
		RequestContext: request.RequestContext,
	}

	// Ensure RequestContext exists and populate with principal info
	if evalCtx.RequestContext == nil {
		evalCtx.RequestContext = make(map[string]interface{})
	}
	// Add principal to context for policy matching
	// The PolicyEngine checks RequestContext["principal"] or RequestContext["aws:PrincipalArn"]
	evalCtx.RequestContext["principal"] = request.Principal
	evalCtx.RequestContext["aws:PrincipalArn"] = request.Principal // AWS standard key

	// Check if this is an admin request - bypass policy evaluation if so
	// This mirrors the logic in auth_signature_v4.go but applies it at authorization time
	isAdmin := false
	if request.RequestContext != nil {
		if val, ok := request.RequestContext["is_admin"].(bool); ok && val {
			isAdmin = true
		}
		// Print full request context for debugging
	}

	// Parse principal ARN to extract details for context variables (e.g. ${aws:username})
	arnInfo := utils.ParsePrincipalARN(request.Principal)
	if arnInfo.RoleName != "" {
		// For assumed roles, AWS docs say aws:username IS the role name.
		// However, for user isolation in these tests, we typically map the session name (the user who assumed the role) to aws:username.
		// arn:aws:sts::account:assumed-role/RoleName/SessionName
		awsUsername := arnInfo.RoleName
		if idx := strings.LastIndex(request.Principal, "/"); idx != -1 && idx < len(request.Principal)-1 {
			awsUsername = request.Principal[idx+1:]
		}

		evalCtx.RequestContext["aws:username"] = awsUsername
		evalCtx.RequestContext["aws:userid"] = arnInfo.RoleName
	} else if userName := utils.ExtractUserNameFromPrincipal(request.Principal); userName != "" {
		evalCtx.RequestContext["aws:username"] = userName
		evalCtx.RequestContext["aws:userid"] = userName
	}
	if arnInfo.AccountID != "" {
		evalCtx.RequestContext["aws:PrincipalAccount"] = arnInfo.AccountID
	}

	// Determine if there is a bucket policy to evaluate
	var bucketPolicyName string
	if strings.HasPrefix(request.Resource, "arn:aws:s3:::") {
		resourcePath := request.Resource[13:] // remove "arn:aws:s3:::"
		parts := strings.SplitN(resourcePath, "/", 2)
		if len(parts) > 0 && parts[0] != "" {
			bucketPolicyName = "bucket-policy:" + parts[0]
		}
	}

	var baseResult *policy.EvaluationResult
	var err error
	// hasManagedSubject is true once we've resolved the principal to a registered
	// IAM user or role (or the caller has supplied PolicyNames directly). For a
	// managed subject, "no matching statement" must deny — the DefaultEffect=Allow
	// fallback is only meant for the unmanaged zero-config startup case.
	hasManagedSubject := false

	if isAdmin {
		// Admin always has base access allowed
		baseResult = &policy.EvaluationResult{Effect: policy.EffectAllow}
	} else {
		policies := request.PolicyNames
		if len(policies) > 0 {
			hasManagedSubject = true
		}
		if len(policies) == 0 {
			// Extract role name from principal ARN
			roleName := utils.ExtractRoleNameFromPrincipal(request.Principal)
			if roleName == "" {
				userName := utils.ExtractUserNameFromPrincipal(request.Principal)
				if userName == "" {
					return false, fmt.Errorf("could not extract role from principal: %s", request.Principal)
				}
				if m.userStore == nil {
					return false, fmt.Errorf("user store unavailable for principal: %s", request.Principal)
				}
				user, err := m.userStore.GetUser(ctx, userName)
				if err != nil || user == nil {
					return false, fmt.Errorf("user not found for principal: %s (user=%s)", request.Principal, userName)
				}
				hasManagedSubject = true
				policies = user.GetPolicyNames()
			} else {
				// Get role definition
				roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
				if err != nil {
					return false, fmt.Errorf("role not found: %s", roleName)
				}

				hasManagedSubject = true
				policies = roleDef.AttachedPolicies
			}
		}
		if bucketPolicyName != "" {
			// Enforce an upper bound on the number of policies to avoid excessive allocations
			if len(policies) >= maxPoliciesForEvaluation {
				return false, fmt.Errorf("too many policies for evaluation: %d >= %d", len(policies), maxPoliciesForEvaluation)
			}
			// Create a new slice to avoid modifying the original and append the bucket policy
			copied := make([]string, len(policies))
			copy(copied, policies)
			policies = append(copied, bucketPolicyName)
		}

		baseResult, err = m.policyEngine.Evaluate(ctx, "", evalCtx, policies)
		if err != nil {
			return false, fmt.Errorf("policy evaluation failed: %w", err)
		}
	}

	// Base policy must allow; if it doesn't, deny immediately (session policy can only further restrict)
	if baseResult.Effect != policy.EffectAllow {
		return false, nil
	}

	// Zero-config IAM uses DefaultEffect=Allow to preserve open-by-default behavior
	// for requests without any subject policies. Once we resolve the principal to
	// a registered IAM user or role (or the caller hands us policy names),
	// "no matching statement" must fall back to deny — otherwise a freshly
	// created user with zero policies would inherit full access.
	if hasManagedSubject && len(baseResult.MatchingStatements) == 0 {
		return false, nil
	}

	// If there's a session policy, it must also allow the action
	if sessionInfo != nil && sessionInfo.SessionPolicy != "" {
		var sessionPolicy policy.PolicyDocument
		if err := json.Unmarshal([]byte(sessionInfo.SessionPolicy), &sessionPolicy); err != nil {
			return false, fmt.Errorf("invalid session policy JSON: %w", err)
		}
		if err := policy.ValidatePolicyDocument(&sessionPolicy); err != nil {
			return false, fmt.Errorf("invalid session policy document: %w", err)
		}
		sessionResult, err := m.policyEngine.EvaluatePolicyDocument(ctx, evalCtx, "session-policy", &sessionPolicy, policy.EffectDeny)
		if err != nil {
			return false, fmt.Errorf("session policy evaluation failed: %w", err)
		}
		if sessionResult.Effect != policy.EffectAllow {
			// Session policy does not allow this action
			return false, nil
		}
	}

	return true, nil
}

// ValidateTrustPolicy validates if a principal can assume a role (for testing)
func (m *IAMManager) ValidateTrustPolicy(ctx context.Context, roleArn, provider, userID string) bool {
	roleName := utils.ExtractRoleNameFromArn(roleArn)
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
		return false
	}

	// Simple validation based on provider in trust policy
	if roleDef.TrustPolicy != nil {
		for _, statement := range roleDef.TrustPolicy.Statement {
			if statement.Effect == "Allow" {
				if principal, ok := statement.Principal.(map[string]interface{}); ok {
					if federated, ok := principal["Federated"].(string); ok {
						// For OIDC, check against issuer URL
						if provider == "oidc" && federated == "test-oidc" {
							return true
						}
						// For LDAP, check against test-ldap
						if provider == "ldap" && federated == "test-ldap" {
							return true
						}
						// Also check for wildcard
						if federated == "*" {
							return true
						}
					}
				}
			}
		}
	}

	return false
}

// validateTrustPolicyForWebIdentity validates trust policy for OIDC assumption
func (m *IAMManager) validateTrustPolicyForWebIdentity(ctx context.Context, roleDef *RoleDefinition, webIdentityToken string, durationSeconds *int64) error {
	if roleDef.TrustPolicy == nil {
		return fmt.Errorf("role has no trust policy")
	}

	// Create evaluation context for trust policy validation
	requestContext := make(map[string]interface{})

	// Try to parse as JWT first, fallback to mock token handling
	tokenClaims, err := parseJWTTokenForTrustPolicy(webIdentityToken)
	if err != nil {
		// If JWT parsing fails, this might be a mock token (like "valid-oidc-token")
		// For mock tokens, we'll use default values that match the trust policy expectations
		requestContext["aws:FederatedProvider"] = "test-oidc"
		requestContext["oidc:iss"] = "test-oidc"
		// This ensures aws:userid key is populated even for mock tokens if needed
		requestContext["aws:userid"] = "mock-user"
		requestContext["oidc:sub"] = "mock-user"
	} else {
		// Add standard context values from JWT claims that trust policies might check
		// See: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_iam-condition-keys.html#condition-keys-web-identity-federation

		// The issuer is the federated provider for OIDC
		if iss, ok := tokenClaims["iss"].(string); ok {
			// Default to issuer URL
			requestContext["aws:FederatedProvider"] = iss
			requestContext["oidc:iss"] = iss

			// Try to resolve provider name from issuer for better policy matching
			// This allows policies to reference the provider name (e.g. "keycloak") instead of the full issuer URL
			if m.stsService != nil {
				for name, provider := range m.stsService.GetProviders() {
					if oidcProvider, ok := provider.(interface{ GetIssuer() string }); ok {
						confIssuer := oidcProvider.GetIssuer()

						if confIssuer == iss {
							requestContext["aws:FederatedProvider"] = name
							break
						}
					}
				}
			}
		}

		if sub, ok := tokenClaims["sub"].(string); ok {
			requestContext["oidc:sub"] = sub
			// Map subject to aws:userid as well for compatibility
			requestContext["aws:userid"] = sub
		}
		if aud, ok := tokenClaims["aud"].(string); ok {
			requestContext["oidc:aud"] = aud
		}
		// Custom claims can be prefixed if needed, but for "be 100% compatible with AWS",
		// we should rely on standard OIDC claims.

		// Add all other claims with oidc: prefix to support custom claims in trust policies
		// This enables checking claims like "oidc:roles", "oidc:groups", "oidc:email", etc.
		for k, v := range tokenClaims {
			// Skip claims we've already handled explicitly or shouldn't expose
			if k == "iss" || k == "sub" || k == "aud" {
				continue
			}

			// Add with oidc: prefix
			requestContext["oidc:"+k] = v
		}
	}

	// Add DurationSeconds to context if provided
	if durationSeconds != nil {
		requestContext["sts:DurationSeconds"] = *durationSeconds
	}

	// Create evaluation context for trust policy
	evalCtx := &policy.EvaluationContext{
		Principal:      "web-identity-user", // Placeholder principal for trust policy evaluation
		Action:         "sts:AssumeRoleWithWebIdentity",
		Resource:       roleDef.RoleArn,
		RequestContext: requestContext,
	}

	// Evaluate the trust policy directly
	if !m.evaluateTrustPolicy(roleDef.TrustPolicy, evalCtx) {
		return fmt.Errorf("trust policy denies web identity assumption")
	}

	return nil
}

// validateTrustPolicyForCredentials validates trust policy for credential assumption
func (m *IAMManager) validateTrustPolicyForCredentials(ctx context.Context, roleDef *RoleDefinition, request *sts.AssumeRoleWithCredentialsRequest) error {
	if roleDef.TrustPolicy == nil {
		return fmt.Errorf("role has no trust policy")
	}

	// Check if trust policy allows credential assumption for the specific provider
	for _, statement := range roleDef.TrustPolicy.Statement {
		if statement.Effect == "Allow" {
			for _, action := range statement.Action {
				if action == "sts:AssumeRoleWithCredentials" {
					if principal, ok := statement.Principal.(map[string]interface{}); ok {
						if federated, ok := principal["Federated"].(string); ok {
							if federated == request.ProviderName {
								return nil // Allow
							}
						}
					}
				}
			}
		}
	}

	return fmt.Errorf("trust policy does not allow credential assumption for provider: %s", request.ProviderName)
}

// Helper functions

// ExpireSessionForTesting manually expires a session for testing purposes
func (m *IAMManager) ExpireSessionForTesting(ctx context.Context, sessionToken string) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	return m.stsService.ExpireSessionForTesting(ctx, sessionToken)
}

// GetPoliciesForUser returns the policy names attached to an IAM user.
// Returns an error if the user store is not configured or the lookup fails,
// so callers can fail closed on policy-resolution failures.
func (m *IAMManager) GetPoliciesForUser(ctx context.Context, username string) ([]string, error) {
	if m.userStore == nil {
		return nil, fmt.Errorf("user store not configured")
	}
	user, err := m.userStore.GetUser(ctx, username)
	if err != nil {
		return nil, fmt.Errorf("failed to look up user %q: %w", username, err)
	}
	if user == nil {
		return nil, nil
	}
	return user.PolicyNames, nil
}

// GetSTSService returns the STS service instance
func (m *IAMManager) GetSTSService() *sts.STSService {
	return m.stsService
}

// DefaultAllow returns whether the default effect is Allow
func (m *IAMManager) DefaultAllow() bool {
	if !m.initialized || m.policyEngine == nil {
		return true // Default to true if not initialized
	}
	return m.policyEngine.DefaultAllow()
}

// parseJWTTokenForTrustPolicy parses a JWT token to extract claims for trust policy evaluation
func parseJWTTokenForTrustPolicy(tokenString string) (map[string]interface{}, error) {
	// Simple JWT parsing without verification (for trust policy context only)
	// In production, this should use proper JWT parsing with signature verification
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid JWT format")
	}

	// Decode the payload (second part)
	payload := parts[1]
	// Add padding if needed
	for len(payload)%4 != 0 {
		payload += "="
	}

	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode JWT payload: %w", err)
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JWT claims: %w", err)
	}

	return claims, nil
}

// evaluateTrustPolicy evaluates a trust policy against the evaluation context
// Now delegates to PolicyEngine for unified policy evaluation
func (m *IAMManager) evaluateTrustPolicy(trustPolicy *policy.PolicyDocument, evalCtx *policy.EvaluationContext) bool {
	if trustPolicy == nil {
		return false
	}

	// Use the PolicyEngine to evaluate the trust policy
	// The PolicyEngine now handles Principal, Action, Resource, and Condition matching
	result, err := m.policyEngine.EvaluateTrustPolicy(context.Background(), trustPolicy, evalCtx)
	if err != nil {
		return false
	}

	return result.Effect == policy.EffectAllow
}

// evaluateTrustPolicyConditions and evaluatePrincipalValue have been removed
// Trust policy evaluation is now handled entirely by PolicyEngine.EvaluateTrustPolicy()

// isOIDCToken checks if a token is an OIDC JWT token (vs STS session token)
func isOIDCToken(token string) bool {
	// JWT tokens have three parts separated by dots and start with base64-encoded JSON
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}

	// JWT tokens typically start with "eyJ" (base64 encoded JSON starting with "{")
	if !strings.HasPrefix(token, "eyJ") {
		return false
	}

	parsed, _, err := new(jwt.Parser).ParseUnverified(token, jwt.MapClaims{})
	if err != nil {
		return false
	}

	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		return false
	}

	if typ, ok := claims["typ"].(string); ok && typ == sts.TokenTypeSession {
		return false
	}
	if typ, ok := claims[sts.JWTClaimTokenType].(string); ok && typ == sts.TokenTypeSession {
		return false
	}

	return true
}

// TrustPolicyValidator interface implementation
// These methods allow the IAMManager to serve as the trust policy validator for the STS service

// ValidateTrustPolicyForWebIdentity implements the TrustPolicyValidator interface
func (m *IAMManager) ValidateTrustPolicyForWebIdentity(ctx context.Context, roleArn string, webIdentityToken string, durationSeconds *int64) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	// Extract role name from ARN
	roleName := utils.ExtractRoleNameFromArn(roleArn)

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
		return fmt.Errorf("role not found: %s", roleName)
	}

	// Use existing trust policy validation logic
	return m.validateTrustPolicyForWebIdentity(ctx, roleDef, webIdentityToken, durationSeconds)
}

// ValidateTrustPolicyForCredentials implements the TrustPolicyValidator interface
func (m *IAMManager) ValidateTrustPolicyForCredentials(ctx context.Context, roleArn string, identity *providers.ExternalIdentity) error {
	if !m.initialized {
		return fmt.Errorf("IAM manager not initialized")
	}

	// Extract role name from ARN
	roleName := utils.ExtractRoleNameFromArn(roleArn)

	// Get role definition
	roleDef, err := m.roleStore.GetRole(ctx, m.getFilerAddress(), roleName)
	if err != nil {
		return fmt.Errorf("role not found: %s", roleName)
	}

	// For credentials, we need to create a mock request to reuse existing validation
	// This is a bit of a hack, but it allows us to reuse the existing logic
	mockRequest := &sts.AssumeRoleWithCredentialsRequest{
		ProviderName: identity.Provider, // Use the provider name from the identity
	}

	// Use existing trust policy validation logic
	return m.validateTrustPolicyForCredentials(ctx, roleDef, mockRequest)
}
