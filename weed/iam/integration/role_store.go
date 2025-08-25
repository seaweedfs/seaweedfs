package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// RoleStore defines the interface for storing IAM role definitions
type RoleStore interface {
	// StoreRole stores a role definition
	StoreRole(ctx context.Context, roleName string, role *RoleDefinition) error

	// GetRole retrieves a role definition
	GetRole(ctx context.Context, roleName string) (*RoleDefinition, error)

	// ListRoles lists all role names
	ListRoles(ctx context.Context) ([]string, error)

	// DeleteRole deletes a role definition
	DeleteRole(ctx context.Context, roleName string) error
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

// StoreRole stores a role definition in memory
func (m *MemoryRoleStore) StoreRole(ctx context.Context, roleName string, role *RoleDefinition) error {
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

// GetRole retrieves a role definition from memory
func (m *MemoryRoleStore) GetRole(ctx context.Context, roleName string) (*RoleDefinition, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name cannot be empty")
	}

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	role, exists := m.roles[roleName]
	if !exists {
		return nil, fmt.Errorf("role not found: %s", roleName)
	}

	// Return a copy to prevent external modifications
	return copyRoleDefinition(role), nil
}

// ListRoles lists all role names in memory
func (m *MemoryRoleStore) ListRoles(ctx context.Context) ([]string, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	names := make([]string, 0, len(m.roles))
	for name := range m.roles {
		names = append(names, name)
	}

	return names, nil
}

// DeleteRole deletes a role definition from memory
func (m *MemoryRoleStore) DeleteRole(ctx context.Context, roleName string) error {
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.roles, roleName)
	return nil
}

// copyRoleDefinition creates a deep copy of a role definition
func copyRoleDefinition(original *RoleDefinition) *RoleDefinition {
	if original == nil {
		return nil
	}

	copied := &RoleDefinition{
		RoleName:    original.RoleName,
		RoleArn:     original.RoleArn,
		Description: original.Description,
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

// FilerRoleStore implements RoleStore using SeaweedFS filer
type FilerRoleStore struct {
	filerGrpcAddress string
	grpcDialOption   grpc.DialOption
	basePath         string
}

// NewFilerRoleStore creates a new filer-based role store
func NewFilerRoleStore(config map[string]interface{}) (*FilerRoleStore, error) {
	store := &FilerRoleStore{
		basePath: "/etc/iam/roles", // Default path for role storage - aligned with /etc/ convention
	}

	// Parse configuration
	if config != nil {
		if filerAddr, ok := config["filerAddress"].(string); ok {
			store.filerGrpcAddress = filerAddr
		}
		if basePath, ok := config["basePath"].(string); ok {
			store.basePath = strings.TrimSuffix(basePath, "/")
		}
	}

	// Validate configuration
	if store.filerGrpcAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerRoleStore")
	}

	glog.V(2).Infof("Initialized FilerRoleStore with filer %s, basePath %s",
		store.filerGrpcAddress, store.basePath)

	return store, nil
}

// StoreRole stores a role definition in filer
func (f *FilerRoleStore) StoreRole(ctx context.Context, roleName string, role *RoleDefinition) error {
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
	return f.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
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

// GetRole retrieves a role definition from filer
func (f *FilerRoleStore) GetRole(ctx context.Context, roleName string) (*RoleDefinition, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name cannot be empty")
	}

	var roleData []byte
	err := f.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: f.basePath,
			Name:      f.getRoleFileName(roleName),
		}

		glog.V(3).Infof("Looking up role %s", roleName)
		response, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("role not found: %v", err)
		}

		if response.Entry == nil {
			return fmt.Errorf("role not found")
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
func (f *FilerRoleStore) ListRoles(ctx context.Context) ([]string, error) {
	var roleNames []string

	err := f.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.ListEntriesRequest{
			Directory:          f.basePath,
			Prefix:             "",
			StartFromFileName:  "",
			InclusiveStartFrom: false,
			Limit:              1000, // Process in batches of 1000
		}

		glog.V(3).Infof("Listing roles in %s", f.basePath)
		stream, err := client.ListEntries(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to list roles: %v", err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				break // End of stream or error
			}

			if resp.Entry == nil || resp.Entry.IsDirectory {
				continue
			}

			// Extract role name from filename
			filename := resp.Entry.Name
			if strings.HasSuffix(filename, ".json") {
				roleName := strings.TrimSuffix(filename, ".json")
				roleNames = append(roleNames, roleName)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return roleNames, nil
}

// DeleteRole deletes a role definition from filer
func (f *FilerRoleStore) DeleteRole(ctx context.Context, roleName string) error {
	if roleName == "" {
		return fmt.Errorf("role name cannot be empty")
	}

	return f.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.DeleteEntryRequest{
			Directory:    f.basePath,
			Name:         f.getRoleFileName(roleName),
			IsDeleteData: true,
		}

		glog.V(3).Infof("Deleting role %s", roleName)
		_, err := client.DeleteEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to delete role %s: %v", roleName, err)
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

func (f *FilerRoleStore) withFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(f.filerGrpcAddress), f.grpcDialOption, fn)
}
