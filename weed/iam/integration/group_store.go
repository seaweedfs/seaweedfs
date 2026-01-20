package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
)

// GroupDefinition defines an IAM group
type GroupDefinition struct {
	// GroupName is the name of the group
	GroupName string `json:"groupName"`

	// GroupId is the unique identifier for the group
	GroupId string `json:"groupId"`

	// Arn is the full ARN of the group
	Arn string `json:"arn"`

	// CreateDate is the creation timestamp
	CreateDate time.Time `json:"createDate"`

	// Members is a list of user names belonging to this group
	Members []string `json:"members"`

	// AttachedPolicies lists the policy names attached to this group
	AttachedPolicies []string `json:"attachedPolicies"`
}

// GroupStore defines the interface for storing IAM group definitions
type GroupStore interface {
	// StoreGroup stores a group definition
	StoreGroup(ctx context.Context, filerAddress string, groupName string, group *GroupDefinition) error

	// GetGroup retrieves a group definition
	GetGroup(ctx context.Context, filerAddress string, groupName string) (*GroupDefinition, error)

	// ListGroups lists all group names
	ListGroups(ctx context.Context, filerAddress string) ([]string, error)

	// DeleteGroup deletes a group definition
	DeleteGroup(ctx context.Context, filerAddress string, groupName string) error
}

// MemoryGroupStore implements GroupStore using in-memory storage
type MemoryGroupStore struct {
	groups map[string]*GroupDefinition
	mutex  sync.RWMutex
}

// NewMemoryGroupStore creates a new memory-based group store
func NewMemoryGroupStore() *MemoryGroupStore {
	return &MemoryGroupStore{
		groups: make(map[string]*GroupDefinition),
	}
}

// StoreGroup stores a group definition in memory
func (m *MemoryGroupStore) StoreGroup(ctx context.Context, filerAddress string, groupName string, group *GroupDefinition) error {
	if groupName == "" {
		return fmt.Errorf("group name cannot be empty")
	}
	if group == nil {
		return fmt.Errorf("group cannot be nil")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.groups[groupName] = copyGroupDefinition(group)
	return nil
}

// GetGroup retrieves a group definition from memory
func (m *MemoryGroupStore) GetGroup(ctx context.Context, filerAddress string, groupName string) (*GroupDefinition, error) {
	if groupName == "" {
		return nil, fmt.Errorf("group name cannot be empty")
	}

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	group, exists := m.groups[groupName]
	if !exists {
		return nil, fmt.Errorf("group not found: %s", groupName)
	}

	return copyGroupDefinition(group), nil
}

// ListGroups lists all group names in memory
func (m *MemoryGroupStore) ListGroups(ctx context.Context, filerAddress string) ([]string, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	names := make([]string, 0, len(m.groups))
	for name := range m.groups {
		names = append(names, name)
	}
	return names, nil
}

// DeleteGroup deletes a group definition from memory
func (m *MemoryGroupStore) DeleteGroup(ctx context.Context, filerAddress string, groupName string) error {
	if groupName == "" {
		return fmt.Errorf("group name cannot be empty")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.groups, groupName)
	return nil
}

// FilerGroupStore implements GroupStore using SeaweedFS filer
type FilerGroupStore struct {
	grpcDialOption       grpc.DialOption
	basePath             string
	filerAddressProvider func() string
	masterClient         *wdclient.MasterClient
}

// NewFilerGroupStore creates a new filer-based group store
func NewFilerGroupStore(config map[string]interface{}, filerAddressProvider func() string, masterClient *wdclient.MasterClient) (*FilerGroupStore, error) {
	store := &FilerGroupStore{
		basePath:             "/etc/iam/groups",
		filerAddressProvider: filerAddressProvider,
		masterClient:         masterClient,
	}

	if config != nil {
		if basePath, ok := config["basePath"].(string); ok && basePath != "" {
			store.basePath = strings.TrimSuffix(basePath, "/")
		}
	}

	return store, nil
}

func (f *FilerGroupStore) getGroupFileName(groupName string) string {
	return groupName + ".json"
}

func (f *FilerGroupStore) withFilerClient(filerAddress string, fn func(filer_pb.SeaweedFilerClient) error) error {
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerGroupStore")
	}
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(filerAddress), f.grpcDialOption, fn)
}

// StoreGroup stores a group definition in filer
func (f *FilerGroupStore) StoreGroup(ctx context.Context, filerAddress string, groupName string, group *GroupDefinition) error {
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerGroupStore")
	}
	if groupName == "" {
		return fmt.Errorf("group name cannot be empty")
	}

	groupData, err := json.MarshalIndent(group, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize group: %v", err)
	}

	return f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, f.basePath, f.getGroupFileName(groupName), groupData)
	})
}

// GetGroup retrieves a group definition from filer
func (f *FilerGroupStore) GetGroup(ctx context.Context, filerAddress string, groupName string) (*GroupDefinition, error) {
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerGroupStore")
	}

	var groupData []byte
	err := f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: f.basePath,
			Name:      f.getGroupFileName(groupName),
		}

		response, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("group not found: %v", err)
		}

		if response.Entry == nil {
			return fmt.Errorf("group not found")
		}

		groupData = response.Entry.Content
		
		if len(groupData) == 0 && len(response.Entry.Chunks) > 0 {
			if f.masterClient != nil {
				var buf bytes.Buffer
				if err := filer.ReadEntry(f.masterClient, client, f.basePath, f.getGroupFileName(groupName), &buf); err != nil {
					return fmt.Errorf("failed to read chunked group: %v", err)
				}
				groupData = buf.Bytes()
			} else {
				glog.Warningf("Group %s has chunks but no masterClient available", groupName)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	var group GroupDefinition
	if err := json.Unmarshal(groupData, &group); err != nil {
		return nil, fmt.Errorf("failed to deserialize group: %v", err)
	}

	return &group, nil
}

// ListGroups lists all group names in filer
func (f *FilerGroupStore) ListGroups(ctx context.Context, filerAddress string) ([]string, error) {
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerGroupStore")
	}

	var groupNames []string

	err := f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.ListEntriesRequest{
			Directory:          f.basePath,
			Limit:              10000,
		}

		stream, err := client.ListEntries(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to list groups: %v", err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				break
			}

			if resp.Entry == nil || resp.Entry.IsDirectory {
				continue
			}

			filename := resp.Entry.Name
			if strings.HasSuffix(filename, ".json") {
				groupName := strings.TrimSuffix(filename, ".json")
				groupNames = append(groupNames, groupName)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return groupNames, nil
}

// DeleteGroup deletes a group definition from filer
func (f *FilerGroupStore) DeleteGroup(ctx context.Context, filerAddress string, groupName string) error {
	if filerAddress == "" && f.filerAddressProvider != nil {
		filerAddress = f.filerAddressProvider()
	}
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerGroupStore")
	}

	return f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.DeleteEntryRequest{
			Directory:    f.basePath,
			Name:         f.getGroupFileName(groupName),
			IsDeleteData: true,
		}

		resp, err := client.DeleteEntry(ctx, request)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return nil
			}
			return fmt.Errorf("failed to delete group %s: %v", groupName, err)
		}
		if resp.Error != "" {
			if strings.Contains(resp.Error, "not found") {
				return nil
			}
			return fmt.Errorf("failed to delete group %s: %s", groupName, resp.Error)
		}
		return nil
	})
}



// copyGroupDefinition creates a deep copy
func copyGroupDefinition(original *GroupDefinition) *GroupDefinition {
	if original == nil {
		return nil
	}

	copied := &GroupDefinition{
		GroupName:  original.GroupName,
		GroupId:    original.GroupId,
		Arn:        original.Arn,
		CreateDate: original.CreateDate,
	}

	if original.Members != nil {
		copied.Members = make([]string, len(original.Members))
		copy(copied.Members, original.Members)
	}

	if original.AttachedPolicies != nil {
		copied.AttachedPolicies = make([]string, len(original.AttachedPolicies))
		copy(copied.AttachedPolicies, original.AttachedPolicies)
	}

	return copied
}
