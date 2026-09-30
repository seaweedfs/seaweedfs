package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/karlseguin/ccache/v2"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrRoleNotFound is wrapped by every RoleStore's GetRole when the role does
// not exist, so callers can tell a missing role from a store that could not
// be read.
var ErrRoleNotFound = errors.New("role not found")

// ErrRoleStatic refuses a change to a role defined in the server's IAM config
// file: change it there instead.
var ErrRoleStatic = errors.New("role is defined in the IAM config file")

// ErrRoleExists is returned by an UpdateRole whose update refuses a role that
// already exists (CreateRole).
var ErrRoleExists = errors.New("role already exists")

// RoleUpdate computes a role's new definition from its current one, nil when
// the role does not exist. It returns nil to delete the role, and an error to
// leave it unchanged. It may run more than once: it is called again with the
// fresh definition when another writer changed the role in between.
type RoleUpdate func(current *RoleDefinition) (*RoleDefinition, error)

// maxRoleUpdateAttempts bounds UpdateRole's retries under contention.
const maxRoleUpdateAttempts = 10

// errRoleUpdateContended is returned when the role kept changing under
// UpdateRole for maxRoleUpdateAttempts reads.
var errRoleUpdateContended = errors.New("role changed concurrently; retry")

// RoleStore defines the interface for storing IAM role definitions
type RoleStore interface {
	// StoreRole stores a role definition (filerAddress ignored for memory stores)
	StoreRole(ctx context.Context, filerAddress string, roleName string, role *RoleDefinition) error

	// GetRole retrieves a role definition (filerAddress ignored for memory stores)
	GetRole(ctx context.Context, filerAddress string, roleName string) (*RoleDefinition, error)

	// ListRoles lists all role names (filerAddress ignored for memory stores)
	ListRoles(ctx context.Context, filerAddress string) ([]string, error)

	// DeleteRole deletes a role definition (filerAddress ignored for memory stores)
	DeleteRole(ctx context.Context, filerAddress string, roleName string) error

	// UpdateRole replaces a role with update's result, atomically: the write
	// lands only if the role is still as update saw it, absent included, and
	// update is retried against the current role otherwise. Every change the
	// IAM API and the filer IAM service make to a role goes through it, so
	// writers on different servers neither lose each other's changes nor bring
	// back a role deleted in between.
	UpdateRole(ctx context.Context, filerAddress string, roleName string, update RoleUpdate) error
}

// MemoryRoleStore implements RoleStore using in-memory storage
type MemoryRoleStore struct {
	roles map[string]*RoleDefinition
	mutex sync.RWMutex
}

// NewMemoryRoleStore creates a new memory-based role store
func NewMemoryRoleStore() *MemoryRoleStore {
	return &MemoryRoleStore{
		roles: make(map[string]*RoleDefinition),
	}
}

// StoreRole stores a role definition in memory (filerAddress ignored for memory store)
func (m *MemoryRoleStore) StoreRole(ctx context.Context, filerAddress string, roleName string, role *RoleDefinition) error {
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}
	if role == nil {
		return fmt.Errorf("role cannot be nil")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Deep copy the role to prevent external modifications
	m.roles[roleName] = copyRoleDefinition(role)
	return nil
}

// GetRole retrieves a role definition from memory (filerAddress ignored for memory store)
func (m *MemoryRoleStore) GetRole(ctx context.Context, filerAddress string, roleName string) (*RoleDefinition, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name cannot be empty")
	}

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	role, exists := m.roles[roleName]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrRoleNotFound, roleName)
	}

	// Return a copy to prevent external modifications
	return copyRoleDefinition(role), nil
}

// ListRoles lists all role names in memory (filerAddress ignored for memory store)
func (m *MemoryRoleStore) ListRoles(ctx context.Context, filerAddress string) ([]string, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	names := make([]string, 0, len(m.roles))
	for name := range m.roles {
		names = append(names, name)
	}

	return names, nil
}

// DeleteRole deletes a role definition from memory (filerAddress ignored for memory store)
func (m *MemoryRoleStore) DeleteRole(ctx context.Context, filerAddress string, roleName string) error {
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.roles, roleName)
	return nil
}

// UpdateRole applies update under the store's lock (filerAddress ignored for
// memory store).
func (m *MemoryRoleStore) UpdateRole(ctx context.Context, filerAddress string, roleName string, update RoleUpdate) error {
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	next, err := update(copyRoleDefinition(m.roles[roleName]))
	if err != nil {
		return err
	}
	if next == nil {
		delete(m.roles, roleName)
		return nil
	}
	m.roles[roleName] = copyRoleDefinition(next)
	return nil
}

// copyRoleDefinition creates a deep copy of a role definition
func copyRoleDefinition(original *RoleDefinition) *RoleDefinition {
	if original == nil {
		return nil
	}

	copied := &RoleDefinition{
		RoleName:           original.RoleName,
		RoleArn:            original.RoleArn,
		Description:        original.Description,
		MaxSessionDuration: original.MaxSessionDuration,
		Source:             original.Source,
		CreatedAt:          original.CreatedAt,
		RoleId:             original.RoleId,
	}

	// Deep copy trust policy if it exists
	if original.TrustPolicy != nil {
		// Use JSON marshaling for deep copy of the complex policy structure
		trustPolicyData, _ := json.Marshal(original.TrustPolicy)
		var trustPolicyCopy policy.PolicyDocument
		json.Unmarshal(trustPolicyData, &trustPolicyCopy)
		copied.TrustPolicy = &trustPolicyCopy
	}

	// Copy attached policies slice
	if original.AttachedPolicies != nil {
		copied.AttachedPolicies = make([]string, len(original.AttachedPolicies))
		copy(copied.AttachedPolicies, original.AttachedPolicies)
	}

	return copied
}

// roleListPageSize is the number of entries FilerRoleStore.ListRoles asks the
// filer for per page.
const roleListPageSize = 1000

// FilerRoleStore implements RoleStore using SeaweedFS filer
type FilerRoleStore struct {
	grpcDialOption       grpc.DialOption
	basePath             string
	filerAddressProvider func() string
}

// NewFilerRoleStore creates a new filer-based role store
func NewFilerRoleStore(config map[string]interface{}, filerAddressProvider func() string) (*FilerRoleStore, error) {
	store := &FilerRoleStore{
		basePath:             "/etc/iam/roles", // Default path for role storage - aligned with /etc/ convention
		filerAddressProvider: filerAddressProvider,
	}

	// Parse configuration - only basePath and other settings, NOT filerAddress
	if config != nil {
		if basePath, ok := config["basePath"].(string); ok && basePath != "" {
			store.basePath = strings.TrimSuffix(basePath, "/")
		}
	}

	glog.V(2).Infof("Initialized FilerRoleStore with basePath %s", store.basePath)

	return store, nil
}

// StoreRole stores a role definition in filer
func (f *FilerRoleStore) StoreRole(ctx context.Context, filerAddress string, roleName string, role *RoleDefinition) error {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerRoleStore")
	}
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}
	if role == nil {
		return fmt.Errorf("role cannot be nil")
	}

	// Serialize role to JSON
	roleData, err := json.MarshalIndent(role, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize role: %v", err)
	}

	rolePath := f.getRolePath(roleName)

	// Store in filer
	return f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: f.basePath,
			Entry: &filer_pb.Entry{
				Name:        f.getRoleFileName(roleName),
				IsDirectory: false,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0600), // Read/write for owner only
					Uid:      uint32(0),
					Gid:      uint32(0),
				},
				Content: roleData,
			},
		}

		glog.V(3).Infof("Storing role %s at %s", roleName, rolePath)
		_, err := client.CreateEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to store role %s: %v", roleName, err)
		}

		return nil
	})
}

// UpdateRole reads the role's entry, applies update, and writes the result on
// the condition that the entry is unchanged since the read — absent, when the
// role did not exist — so the filer refuses a write racing another writer's
// change or delete, and update is applied again to what that writer left. A
// delete is made on the same condition, so it removes the role update saw and
// not one written after it.
func (f *FilerRoleStore) UpdateRole(ctx context.Context, filerAddress string, roleName string, update RoleUpdate) error {
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerRoleStore")
	}
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}
	return f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		for attempt := 0; attempt < maxRoleUpdateAttempts; attempt++ {
			var entry *filer_pb.Entry
			var current *RoleDefinition
			resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
				Directory: f.basePath,
				Name:      f.getRoleFileName(roleName),
			})
			switch {
			case errors.Is(err, filer_pb.ErrNotFound):
			case err != nil:
				return fmt.Errorf("lookup role %s: %w", roleName, err)
			case resp.Entry != nil:
				entry = resp.Entry
				current = &RoleDefinition{}
				if err := json.Unmarshal(entry.Content, current); err != nil {
					return fmt.Errorf("failed to deserialize role %s: %v", roleName, err)
				}
			}

			next, err := update(current)
			if err != nil {
				return err
			}
			if next == nil {
				if entry == nil {
					return nil
				}
				deleted, err := f.deleteRoleEntryIfUnchanged(ctx, client, entry)
				if err != nil {
					return fmt.Errorf("failed to delete role %s: %w", roleName, err)
				}
				if !deleted {
					glog.V(3).Infof("Role %s changed before its delete; retrying", roleName)
					continue
				}
				return nil
			}
			roleData, err := json.MarshalIndent(next, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to serialize role: %v", err)
			}

			clause := &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}
			if entry != nil {
				clause = &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ENTRY_EQUAL, ExpectedEntry: entry}
			}
			now := time.Now().Unix()
			created, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
				Directory: f.basePath,
				Entry: &filer_pb.Entry{
					Name: f.getRoleFileName(roleName),
					Attributes: &filer_pb.FuseAttributes{
						Mtime:    now,
						Crtime:   now,
						FileMode: uint32(0600),
					},
					Content: roleData,
				},
				Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{clause}},
			})
			if isRoleWriteConflict(created, err) {
				glog.V(3).Infof("Role %s changed during update; retrying", roleName)
				continue
			}
			if err != nil {
				return fmt.Errorf("failed to store role %s: %v", roleName, err)
			}
			if created.Error != "" {
				return fmt.Errorf("failed to store role %s: %s", roleName, created.Error)
			}
			return nil
		}
		return fmt.Errorf("update role %s: %w", roleName, errRoleUpdateContended)
	})
}

// deleteRoleEntryIfUnchanged deletes the role's entry if it still equals
// entry, reporting false when it changed. The delete is routed and locked as
// the conditional CreateEntry of the same path is, so the two serialize.
func (f *FilerRoleStore) deleteRoleEntryIfUnchanged(ctx context.Context, client filer_pb.SeaweedFilerClient, entry *filer_pb.Entry) (bool, error) {
	fullPath := f.basePath + "/" + entry.Name
	resp, err := client.ObjectTransaction(ctx, &filer_pb.ObjectTransactionRequest{
		LockKey:  fullPath,
		RouteKey: s3_constants.ObjectWriteRouteKeyPrefix + fullPath,
		Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{
			Kind: filer_pb.WriteCondition_IF_ENTRY_EQUAL, ExpectedEntry: entry,
		}}},
		Mutations: []*filer_pb.ObjectMutation{{
			Type: filer_pb.ObjectMutation_DELETE, Directory: f.basePath, Name: entry.Name, IsDeleteData: true,
		}},
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			return false, nil
		}
		return false, err
	}
	if resp.ErrorCode == filer_pb.FilerError_PRECONDITION_FAILED {
		return false, nil
	}
	if resp.Error != "" {
		return false, errors.New(resp.Error)
	}
	return true, nil
}

// isRoleWriteConflict reports a write the filer refused because its condition
// no longer held: in the response, or as FailedPrecondition when the write
// was forwarded to the entry's owner filer.
func isRoleWriteConflict(resp *filer_pb.CreateEntryResponse, err error) bool {
	if err != nil {
		return status.Code(err) == codes.FailedPrecondition
	}
	return resp != nil && resp.ErrorCode == filer_pb.FilerError_PRECONDITION_FAILED
}

// GetRole retrieves a role definition from filer
func (f *FilerRoleStore) GetRole(ctx context.Context, filerAddress string, roleName string) (*RoleDefinition, error) {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerRoleStore")
	}
	if roleName == "" {
		return nil, fmt.Errorf("role name cannot be empty")
	}

	var roleData []byte
	err := f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: f.basePath,
			Name:      f.getRoleFileName(roleName),
		}

		glog.V(3).Infof("Looking up role %s", roleName)
		response, err := filer_pb.LookupEntry(ctx, client, request)
		if errors.Is(err, filer_pb.ErrNotFound) {
			return fmt.Errorf("%w: %s", ErrRoleNotFound, roleName)
		}
		if err != nil {
			return fmt.Errorf("lookup role %s: %w", roleName, err)
		}

		if response.Entry == nil {
			return fmt.Errorf("%w: %s", ErrRoleNotFound, roleName)
		}

		roleData = response.Entry.Content
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Deserialize role from JSON
	var role RoleDefinition
	if err := json.Unmarshal(roleData, &role); err != nil {
		return nil, fmt.Errorf("failed to deserialize role: %v", err)
	}

	return &role, nil
}

// ListRoles lists all role names in filer
func (f *FilerRoleStore) ListRoles(ctx context.Context, filerAddress string) ([]string, error) {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerRoleStore")
	}

	// Page through the directory, and fail on a broken stream rather than
	// return what arrived: a truncated list would let DeletePolicy miss a role
	// that still attaches the policy (RolesAttachingPolicy).
	var roleNames []string
	err := f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		glog.V(3).Infof("Listing roles in %s", f.basePath)
		startFrom := ""
		for {
			stream, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
				Directory:         f.basePath,
				StartFromFileName: startFrom,
				Limit:             roleListPageSize,
			})
			if err != nil {
				return err
			}
			received := 0
			for {
				resp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return err
				}
				if resp.Entry == nil {
					continue
				}
				received++
				startFrom = resp.Entry.Name
				if resp.Entry.IsDirectory {
					continue
				}
				if name, ok := strings.CutSuffix(resp.Entry.Name, ".json"); ok {
					roleNames = append(roleNames, name)
				}
			}
			if received < roleListPageSize {
				return nil
			}
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list roles: %w", err)
	}

	return roleNames, nil
}

// DeleteRole deletes a role definition from filer
func (f *FilerRoleStore) DeleteRole(ctx context.Context, filerAddress string, roleName string) error {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerRoleStore")
	}
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}

	return f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.DeleteEntryRequest{
			Directory:    f.basePath,
			Name:         f.getRoleFileName(roleName),
			IsDeleteData: true,
		}

		glog.V(3).Infof("Deleting role %s", roleName)
		resp, err := client.DeleteEntry(ctx, request)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return nil // Idempotent: deletion of non-existent role is successful
			}
			return fmt.Errorf("failed to delete role %s: %v", roleName, err)
		}

		if resp.Error != "" {
			if strings.Contains(resp.Error, "not found") {
				return nil // Idempotent: deletion of non-existent role is successful
			}
			return fmt.Errorf("failed to delete role %s: %s", roleName, resp.Error)
		}

		return nil
	})
}

// Helper methods for FilerRoleStore

func (f *FilerRoleStore) getRoleFileName(roleName string) string {
	return roleName + ".json"
}

func (f *FilerRoleStore) getRolePath(roleName string) string {
	return f.basePath + "/" + f.getRoleFileName(roleName)
}

func (f *FilerRoleStore) withFilerClient(filerAddress string, fn func(filer_pb.SeaweedFilerClient) error) error {
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerRoleStore")
	}
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(filerAddress), f.grpcDialOption, fn)
}

// CachedFilerRoleStore implements RoleStore with TTL caching on top of FilerRoleStore
type CachedFilerRoleStore struct {
	filerStore *FilerRoleStore
	cache      *ccache.Cache
	listCache  *ccache.Cache
	ttl        time.Duration
	listTTL    time.Duration
}

// CachedFilerRoleStoreConfig holds configuration for the cached role store
type CachedFilerRoleStoreConfig struct {
	BasePath     string `json:"basePath,omitempty"`
	TTL          string `json:"ttl,omitempty"`          // e.g., "5m", "1h"
	ListTTL      string `json:"listTtl,omitempty"`      // e.g., "1m", "30s"
	MaxCacheSize int    `json:"maxCacheSize,omitempty"` // Maximum number of cached roles
}

// RolesAttachingPolicy returns the names of the roles that attach the policy.
// A policy is attached to a role by name, so deleting it while attached would
// let a policy created later under the same name take effect on the role;
// callers refuse the delete instead, as AWS does (DeleteConflict).
//
// It sees the stored roles, which every server sharing the store sees, and
// this server's config-file roles. A role defined only in another server's
// config file is invisible here: servers sharing a role store may have
// different config files, but a config-file role that attaches a managed
// (API-created) policy is protected only on the servers whose file defines it.
// Keep such roles in every server's file, or attach only config-file policies
// to config-file roles.
func RolesAttachingPolicy(ctx context.Context, store RoleStore, policyName string) ([]string, error) {
	names, err := store.ListRoles(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list roles: %w", err)
	}
	var attaching []string
	for _, name := range names {
		role, err := store.GetRole(ctx, "", name)
		if errors.Is(err, ErrRoleNotFound) {
			continue // deleted between list and read
		}
		if err != nil {
			return nil, fmt.Errorf("get role %s: %w", name, err)
		}
		for _, attached := range role.AttachedPolicies {
			if attached == policyName {
				attaching = append(attaching, name)
				break
			}
		}
	}
	return attaching, nil
}

// MaxManagedPoliciesPerRole caps the managed policies attached to one role,
// as AWS's default quota does (and as MaxManagedPoliciesPerUser does for users).
const MaxManagedPoliciesPerRole = 10

var roleNamePattern = regexp.MustCompile(`^[\w+=,.@-]{1,64}$`)

// ValidateRoleName checks a role name against AWS's rules. A role is stored as
// <name>.json in the filer, so the rules also keep a name from leaving the
// role store's directory.
func ValidateRoleName(name string) error {
	if !roleNamePattern.MatchString(name) {
		return fmt.Errorf("invalid role name %q: must be 1-64 characters of letters, digits and +=,.@_-", name)
	}
	return nil
}

// staticRoleOverlay serves the IAM config file's roles from memory beside a
// persistent store, which never holds them (see IAMManager.LoadStaticRoles).
// A role stored under the same name takes precedence, as a stored OIDC
// provider does over a config-file one; deleting it brings the config-file
// role back. A config-file role itself cannot be stored over or deleted.
type staticRoleOverlay struct {
	static map[string]*RoleDefinition
	inner  RoleStore
}

// storedRoleExists reports whether the store itself holds the role.
func (o *staticRoleOverlay) storedRoleExists(ctx context.Context, filerAddress, roleName string) (bool, error) {
	_, err := o.inner.GetRole(ctx, filerAddress, roleName)
	if errors.Is(err, ErrRoleNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (o *staticRoleOverlay) StoreRole(ctx context.Context, filerAddress string, roleName string, role *RoleDefinition) error {
	if _, ok := o.static[roleName]; ok {
		stored, err := o.storedRoleExists(ctx, filerAddress, roleName)
		if err != nil {
			return err
		}
		if !stored {
			return fmt.Errorf("%w: %s", ErrRoleStatic, roleName)
		}
	}
	return o.inner.StoreRole(ctx, filerAddress, roleName, role)
}

// UpdateRole refuses to create a stored role over a config-file role, as
// StoreRole does; a stored role of that name may be changed.
func (o *staticRoleOverlay) UpdateRole(ctx context.Context, filerAddress string, roleName string, update RoleUpdate) error {
	_, static := o.static[roleName]
	return o.inner.UpdateRole(ctx, filerAddress, roleName, func(current *RoleDefinition) (*RoleDefinition, error) {
		if current == nil && static {
			return nil, fmt.Errorf("%w: %s", ErrRoleStatic, roleName)
		}
		return update(current)
	})
}

func (o *staticRoleOverlay) GetRole(ctx context.Context, filerAddress string, roleName string) (*RoleDefinition, error) {
	role, err := o.inner.GetRole(ctx, filerAddress, roleName)
	if errors.Is(err, ErrRoleNotFound) {
		if static, ok := o.static[roleName]; ok {
			return copyRoleDefinition(static), nil
		}
	}
	return role, err
}

func (o *staticRoleOverlay) ListRoles(ctx context.Context, filerAddress string) ([]string, error) {
	names, err := o.inner.ListRoles(ctx, filerAddress)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		seen[name] = true
	}
	for name := range o.static {
		if !seen[name] {
			names = append(names, name)
		}
	}
	return names, nil
}

func (o *staticRoleOverlay) DeleteRole(ctx context.Context, filerAddress string, roleName string) error {
	if _, ok := o.static[roleName]; ok {
		stored, err := o.storedRoleExists(ctx, filerAddress, roleName)
		if err != nil {
			return err
		}
		if !stored {
			return fmt.Errorf("%w: %s", ErrRoleStatic, roleName)
		}
	}
	return o.inner.DeleteRole(ctx, filerAddress, roleName)
}

// ClearCache forwards cache invalidation to the store underneath.
func (o *staticRoleOverlay) ClearCache() {
	if cached, ok := o.inner.(interface{ ClearCache() }); ok {
		cached.ClearCache()
	}
}
