package credential

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

// FilerAddressSetter is an interface for credential stores that need a dynamic filer address
type FilerAddressSetter interface {
	SetFilerAddressFunc(getFiler func() pb.ServerAddress, grpcDialOption grpc.DialOption)
}

// CredentialManager manages user credentials using a configurable store
type CredentialManager struct {
	Store CredentialStore
	// staticMu protects staticIdentities and staticNames, which are written
	// by SetStaticIdentities (startup + config reload) and read concurrently
	// by LoadConfiguration, SaveConfiguration, GetStaticUsernames, and IsStaticIdentity.
	staticMu sync.RWMutex
	// staticIdentities holds identities loaded from a static config file (-s3.config).
	// These are included in LoadConfiguration so that listing operations
	// return all configured identities, not just dynamic ones from the store.
	staticIdentities []*iam_pb.Identity
	staticNames      map[string]bool
}

// NewCredentialManager creates a new credential manager with the specified store
func NewCredentialManager(storeName CredentialStoreTypeName, configuration util.Configuration, prefix string) (*CredentialManager, error) {
	var store CredentialStore

	// Find the requested store implementation
	for _, s := range Stores {
		if s.GetName() == storeName {
			store = s
			break
		}
	}

	if store == nil {
		return nil, fmt.Errorf("credential store '%s' not found. Available stores: %s",
			storeName, getAvailableStores())
	}

	// Initialize the store
	if err := store.Initialize(configuration, prefix); err != nil {
		return nil, fmt.Errorf("failed to initialize credential store '%s': %v", storeName, err)
	}

	return &CredentialManager{
		Store: store,
	}, nil
}

func (cm *CredentialManager) SetMasterClient(masterClient *wdclient.MasterClient, grpcDialOption grpc.DialOption) {
	cm.Store = NewPropagatingCredentialStore(cm.Store, masterClient, grpcDialOption)
}

// SetFilerAddressFunc sets the function to get the current filer address
func (cm *CredentialManager) SetFilerAddressFunc(getFiler func() pb.ServerAddress, grpcDialOption grpc.DialOption) {
	if s, ok := cm.Store.(FilerAddressSetter); ok {
		s.SetFilerAddressFunc(getFiler, grpcDialOption)
	}
}

// GetStore returns the underlying credential store
func (cm *CredentialManager) GetStore() CredentialStore {
	return cm.Store
}

// GetStoreName returns the name of the underlying credential store
func (cm *CredentialManager) GetStoreName() string {
	if cm.Store != nil {
		return string(cm.Store.GetName())
	}
	return ""
}

// SetStaticIdentities registers identities loaded from a static config file.
// These identities are included in LoadConfiguration and ListUsers results
// but are never persisted to the dynamic store.
func (cm *CredentialManager) SetStaticIdentities(identities []*iam_pb.Identity) {
	filtered := make([]*iam_pb.Identity, 0, len(identities))
	names := make(map[string]bool, len(identities))
	for _, ident := range identities {
		if ident != nil {
			filtered = append(filtered, ident)
			names[ident.Name] = true
		}
	}
	cm.staticMu.Lock()
	cm.staticIdentities = filtered
	cm.staticNames = names
	cm.staticMu.Unlock()
}

// IsStaticIdentity returns true if the named identity was loaded from static config.
func (cm *CredentialManager) IsStaticIdentity(name string) bool {
	cm.staticMu.RLock()
	defer cm.staticMu.RUnlock()
	return cm.staticNames[name]
}

// GetStaticUsernames returns the names of all static identities.
func (cm *CredentialManager) GetStaticUsernames() []string {
	cm.staticMu.RLock()
	defer cm.staticMu.RUnlock()
	names := make([]string, 0, len(cm.staticIdentities))
	for _, ident := range cm.staticIdentities {
		names = append(names, ident.Name)
	}
	return names
}

// LoadConfiguration loads the S3 API configuration from the store and merges
// in any static identities so that listing operations show all users.
func (cm *CredentialManager) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	config, err := cm.Store.LoadConfiguration(ctx)
	if err != nil {
		return config, err
	}
	// Merge static identities that are not already in the dynamic config
	cm.staticMu.RLock()
	staticIdents := cm.staticIdentities
	cm.staticMu.RUnlock()
	if len(staticIdents) > 0 {
		dynamicNames := make(map[string]bool, len(config.Identities))
		for _, ident := range config.Identities {
			dynamicNames[ident.Name] = true
		}
		for _, si := range staticIdents {
			if !dynamicNames[si.Name] {
				config.Identities = append(config.Identities, si)
			}
		}
	}
	return config, nil
}

// SaveConfiguration saves the S3 API configuration.
// Static identities are filtered out before saving to the store.
// The caller's config is not mutated.
func (cm *CredentialManager) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	cm.staticMu.RLock()
	staticNames := cm.staticNames
	cm.staticMu.RUnlock()
	if len(staticNames) > 0 {
		var dynamicOnly []*iam_pb.Identity
		for _, ident := range config.Identities {
			if !staticNames[ident.Name] {
				dynamicOnly = append(dynamicOnly, ident)
			}
		}
		configCopy := *config
		configCopy.Identities = dynamicOnly
		return cm.Store.SaveConfiguration(ctx, &configCopy)
	}
	return cm.Store.SaveConfiguration(ctx, config)
}

// CreateUser creates a new user
func (cm *CredentialManager) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	return cm.Store.CreateUser(ctx, identity)
}

// GetUser retrieves a user by username
func (cm *CredentialManager) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	return cm.Store.GetUser(ctx, username)
}

// UpdateUser updates an existing user
func (cm *CredentialManager) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	return cm.Store.UpdateUser(ctx, username, identity)
}

// DeleteUser removes a user
func (cm *CredentialManager) DeleteUser(ctx context.Context, username string) error {
	return cm.Store.DeleteUser(ctx, username)
}

// ListUsers returns usernames from the dynamic store via cm.Store.ListUsers.
// On store error the error is returned directly without merging static entries.
// Static identities (cm.staticIdentities) are NOT included here because
// internal callers (e.g. DeletePolicy) look up each user in the store and
// would fail on non-existent static entries. External callers that need the
// full list should merge GetStaticUsernames separately.
func (cm *CredentialManager) ListUsers(ctx context.Context) ([]string, error) {
	return cm.Store.ListUsers(ctx)
}

// GetUserByAccessKey retrieves a user by access key
func (cm *CredentialManager) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	return cm.Store.GetUserByAccessKey(ctx, accessKey)
}

// CreateAccessKey creates a new access key for a user
func (cm *CredentialManager) CreateAccessKey(ctx context.Context, username string, credential *iam_pb.Credential) error {
	return cm.Store.CreateAccessKey(ctx, username, credential)
}

// DeleteAccessKey removes an access key for a user
func (cm *CredentialManager) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	return cm.Store.DeleteAccessKey(ctx, username, accessKey)
}

// GetPolicies returns all policies
func (cm *CredentialManager) GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error) {
	return cm.Store.GetPolicies(ctx)
}

// PutPolicy creates or updates a policy
func (cm *CredentialManager) PutPolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return cm.Store.PutPolicy(ctx, name, document)
}

// DeletePolicy removes a policy
func (cm *CredentialManager) DeletePolicy(ctx context.Context, name string) error {
	return cm.Store.DeletePolicy(ctx, name)
}

// GetPolicy retrieves a policy by name
func (cm *CredentialManager) GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error) {
	return cm.Store.GetPolicy(ctx, name)
}

// ListPolicyNames returns the names of all policies
func (cm *CredentialManager) ListPolicyNames(ctx context.Context) ([]string, error) {
	return cm.Store.ListPolicyNames(ctx)
}

// CreatePolicy creates a new policy (if supported by the store)
func (cm *CredentialManager) CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	// Check if the store implements PolicyManager interface with CreatePolicy
	if policyStore, ok := cm.Store.(PolicyManager); ok {
		return policyStore.CreatePolicy(ctx, name, document)
	}
	// Fallback to PutPolicy for stores that only implement CredentialStore
	return cm.Store.PutPolicy(ctx, name, document)
}

// UpdatePolicy updates an existing policy (if supported by the store)
func (cm *CredentialManager) UpdatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	// Check if the store implements PolicyManager interface with UpdatePolicy
	if policyStore, ok := cm.Store.(PolicyManager); ok {
		return policyStore.UpdatePolicy(ctx, name, document)
	}
	// Fallback to PutPolicy for stores that only implement CredentialStore
	return cm.Store.PutPolicy(ctx, name, document)
}

// LoadS3ConfigFile reads a static S3 identity config file and registers
// the identities so they appear in LoadConfiguration and listing results.
func (cm *CredentialManager) LoadS3ConfigFile(path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	config := &iam_pb.S3ApiConfiguration{}
	opts := protojson.UnmarshalOptions{DiscardUnknown: true, AllowPartial: true}
	if err := opts.Unmarshal(content, config); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	cm.SetStaticIdentities(config.Identities)
	glog.V(1).Infof("Loaded %d static identities from %s", len(config.Identities), path)
	return nil
}

// Shutdown performs cleanup
func (cm *CredentialManager) Shutdown() {
	if cm.Store != nil {
		cm.Store.Shutdown()
	}
}

// getAvailableStores returns a comma-separated list of available store names
func getAvailableStores() string {
	var storeNames []string
	for _, store := range Stores {
		storeNames = append(storeNames, string(store.GetName()))
	}
	return strings.Join(storeNames, ", ")
}

// GetAvailableStores returns a list of available credential store names
func GetAvailableStores() []CredentialStoreTypeName {
	var storeNames []CredentialStoreTypeName
	for _, store := range Stores {
		storeNames = append(storeNames, store.GetName())
	}
	if storeNames == nil {
		return []CredentialStoreTypeName{}
	}
	return storeNames
}

// CreateServiceAccount creates a new service account
func (cm *CredentialManager) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	return cm.Store.CreateServiceAccount(ctx, sa)
}

// UpdateServiceAccount updates an existing service account
func (cm *CredentialManager) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	return cm.Store.UpdateServiceAccount(ctx, id, sa)
}

// DeleteServiceAccount removes a service account
func (cm *CredentialManager) DeleteServiceAccount(ctx context.Context, id string) error {
	return cm.Store.DeleteServiceAccount(ctx, id)
}

// GetServiceAccount retrieves a service account by ID
func (cm *CredentialManager) GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error) {
	return cm.Store.GetServiceAccount(ctx, id)
}

// ListServiceAccounts returns all service accounts
func (cm *CredentialManager) ListServiceAccounts(ctx context.Context) ([]*iam_pb.ServiceAccount, error) {
	return cm.Store.ListServiceAccounts(ctx)
}

// AttachUserPolicy attaches a managed policy to a user
func (cm *CredentialManager) AttachUserPolicy(ctx context.Context, username string, policyName string) error {
	return cm.Store.AttachUserPolicy(ctx, username, policyName)
}

// DetachUserPolicy detaches a managed policy from a user
func (cm *CredentialManager) DetachUserPolicy(ctx context.Context, username string, policyName string) error {
	return cm.Store.DetachUserPolicy(ctx, username, policyName)
}

// ListAttachedUserPolicies returns the list of policy names attached to a user
func (cm *CredentialManager) ListAttachedUserPolicies(ctx context.Context, username string) ([]string, error) {
	return cm.Store.ListAttachedUserPolicies(ctx, username)
}

// Group Management

func (cm *CredentialManager) CreateGroup(ctx context.Context, group *iam_pb.Group) error {
	return cm.Store.CreateGroup(ctx, group)
}

func (cm *CredentialManager) GetGroup(ctx context.Context, groupName string) (*iam_pb.Group, error) {
	return cm.Store.GetGroup(ctx, groupName)
}

func (cm *CredentialManager) DeleteGroup(ctx context.Context, groupName string) error {
	return cm.Store.DeleteGroup(ctx, groupName)
}

func (cm *CredentialManager) ListGroups(ctx context.Context) ([]string, error) {
	return cm.Store.ListGroups(ctx)
}

func (cm *CredentialManager) UpdateGroup(ctx context.Context, group *iam_pb.Group) error {
	return cm.Store.UpdateGroup(ctx, group)
}
