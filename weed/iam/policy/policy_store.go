package policy

import (
	"context"
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// MemoryPolicyStore implements PolicyStore using in-memory storage
type MemoryPolicyStore struct {
	policies map[string]*PolicyDocument
	mutex    sync.RWMutex
}

// NewMemoryPolicyStore creates a new memory-based policy store
func NewMemoryPolicyStore() *MemoryPolicyStore {
	return &MemoryPolicyStore{
		policies: make(map[string]*PolicyDocument),
	}
}

// StorePolicy stores a policy document in memory (filerAddress ignored for memory store)
func (s *MemoryPolicyStore) StorePolicy(ctx context.Context, filerAddress string, name string, policy *PolicyDocument) error {
	if name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}

	if policy == nil {
		return fmt.Errorf("policy cannot be nil")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Deep copy the policy to prevent external modifications
	s.policies[name] = copyPolicyDocument(policy)
	return nil
}

// GetPolicy retrieves a policy document from memory (filerAddress ignored for memory store)
func (s *MemoryPolicyStore) GetPolicy(ctx context.Context, filerAddress string, name string) (*PolicyDocument, error) {
	if name == "" {
		return nil, fmt.Errorf("policy name cannot be empty")
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	policy, exists := s.policies[name]
	if !exists {
		return nil, fmt.Errorf("policy not found: %s", name)
	}

	// Return a copy to prevent external modifications
	return copyPolicyDocument(policy), nil
}

// DeletePolicy deletes a policy document from memory (filerAddress ignored for memory store)
func (s *MemoryPolicyStore) DeletePolicy(ctx context.Context, filerAddress string, name string) error {
	if name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.policies, name)
	return nil
}

// PolicyMetadata represents policy metadata
type PolicyMetadata struct {
	Name        string    `json:"name"`
	PolicyId    string    `json:"policyId"`
	Arn         string    `json:"arn"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// ListPolicies lists all policies in memory (filerAddress ignored for memory store)
func (s *MemoryPolicyStore) ListPolicies(ctx context.Context, filerAddress string) ([]PolicyMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	policies := make([]PolicyMetadata, 0, len(s.policies))
	for name := range s.policies {
		policies = append(policies, PolicyMetadata{
			Name:      name,
			CreatedAt: time.Now(), // Memory store doesn't track creation time
			UpdatedAt: time.Now(),
		})
	}

	return policies, nil
}

// copyPolicyDocument creates a deep copy of a policy document
func copyPolicyDocument(original *PolicyDocument) *PolicyDocument {
	if original == nil {
		return nil
	}

	copied := &PolicyDocument{
		Version: original.Version,
		Id:      original.Id,
	}

	// Copy statements
	copied.Statement = make([]Statement, len(original.Statement))
	for i, stmt := range original.Statement {
		copied.Statement[i] = Statement{
			Sid:          stmt.Sid,
			Effect:       stmt.Effect,
			Principal:    stmt.Principal,
			NotPrincipal: stmt.NotPrincipal,
		}

		// Copy action slice
		if stmt.Action != nil {
			copied.Statement[i].Action = make([]string, len(stmt.Action))
			copy(copied.Statement[i].Action, stmt.Action)
		}

		// Copy NotAction slice
		if stmt.NotAction != nil {
			copied.Statement[i].NotAction = make([]string, len(stmt.NotAction))
			copy(copied.Statement[i].NotAction, stmt.NotAction)
		}

		// Copy resource slice
		if stmt.Resource != nil {
			copied.Statement[i].Resource = make([]string, len(stmt.Resource))
			copy(copied.Statement[i].Resource, stmt.Resource)
		}

		// Copy NotResource slice
		if stmt.NotResource != nil {
			copied.Statement[i].NotResource = make([]string, len(stmt.NotResource))
			copy(copied.Statement[i].NotResource, stmt.NotResource)
		}

		// Copy condition map (shallow copy for now)
		if stmt.Condition != nil {
			copied.Statement[i].Condition = make(map[string]map[string]interface{})
			for k, v := range stmt.Condition {
				copied.Statement[i].Condition[k] = v
			}
		}
	}

	return copied
}

// FilerPolicyStore implements PolicyStore using SeaweedFS filer
type FilerPolicyStore struct {
	grpcDialOption       grpc.DialOption
	basePath             string
	filerAddressProvider func() string
	masterClient         *wdclient.MasterClient
}

// NewFilerPolicyStore creates a new filer-based policy store
func NewFilerPolicyStore(config map[string]interface{}, filerAddressProvider func() string, masterClient *wdclient.MasterClient) (*FilerPolicyStore, error) {
	store := &FilerPolicyStore{
		basePath:             "/etc/iam/policies", // Default path for Filer-backed IAM
		filerAddressProvider: filerAddressProvider,
		masterClient:         masterClient,
	}

	// Parse configuration - only basePath and other settings, NOT filerAddress
	if config != nil {
		if basePath, ok := config["basePath"].(string); ok && basePath != "" {
			store.basePath = strings.TrimSuffix(basePath, "/")
		}
	}

	glog.V(2).Infof("Initialized FilerPolicyStore with basePath %s", store.basePath)

	return store, nil
}

// SetMasterClient sets the master client for the policy store
func (s *FilerPolicyStore) SetMasterClient(masterClient *wdclient.MasterClient) {
	s.masterClient = masterClient
}

// StorePolicy stores a policy document in filer
func (s *FilerPolicyStore) StorePolicy(ctx context.Context, filerAddress string, name string, policy *PolicyDocument) error {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && s.filerAddressProvider != nil {
		filerAddress = s.filerAddressProvider()
	}
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerPolicyStore")
	}
	if name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}
	if policy == nil {
		return fmt.Errorf("policy cannot be nil")
	}

	// Serialize policy to JSON
	policyData, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize policy: %v", err)
	}



	// Store in filer
	return s.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		glog.V(3).Infof("Storing policy %s at %s/%s", name, s.basePath, s.getPolicyFileName(name))
		return filer.SaveInsideFiler(client, s.basePath, s.getPolicyFileName(name), policyData)
	})
}

// GetPolicy retrieves a policy document from filer
func (s *FilerPolicyStore) GetPolicy(ctx context.Context, filerAddress string, name string) (*PolicyDocument, error) {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && s.filerAddressProvider != nil {
		filerAddress = s.filerAddressProvider()
	}
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerPolicyStore")
	}
	if name == "" {
		return nil, fmt.Errorf("policy name cannot be empty")
	}

	var policyData []byte
	err := s.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		glog.V(3).Infof("Looking up policy %s at %s/%s", name, s.basePath, s.getPolicyFileName(name))
		
		// Try reading directly from Filer metadata first (fastest for small policies)
		glog.V(0).Infof("GetPolicy %s: trying ReadInsideFiler", name)
		data, err := filer.ReadInsideFiler(client, s.basePath, s.getPolicyFileName(name))
		if err == nil && len(data) > 0 {
			policyData = data
			return nil
		}
		
		// If direct read failed or returned empty (and we have masterClient), try chunked read
		if s.masterClient != nil {
			glog.V(0).Infof("GetPolicy %s: ReadInsideFiler returned empty/error, using masterClient for chunked read", name)
			var buf bytes.Buffer
			if err := filer.ReadEntry(s.masterClient, client, s.basePath, s.getPolicyFileName(name), &buf); err != nil {
				// If both failed, return the original error if strict, or this one
				return fmt.Errorf("read entry/policy not found: %v", err)
			}
			policyData = buf.Bytes()
			return nil
		}

		// If no masterClient and ReadInsideFiler failed
		if err != nil {
			return fmt.Errorf("policy not found: %v", err)
		}
		
		// If we got here, data is empty and no masterClient
		policyData = data
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Deserialize policy from JSON
	var policy PolicyDocument
	if err := json.Unmarshal(policyData, &policy); err != nil {
		return nil, fmt.Errorf("failed to deserialize policy: %v", err)
	}

	return &policy, nil
}

// DeletePolicy deletes a policy document from filer
func (s *FilerPolicyStore) DeletePolicy(ctx context.Context, filerAddress string, name string) error {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && s.filerAddressProvider != nil {
		filerAddress = s.filerAddressProvider()
	}
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerPolicyStore")
	}
	if name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}

	return s.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.DeleteEntryRequest{
			Directory:            s.basePath,
			Name:                 s.getPolicyFileName(name),
			IsDeleteData:         true,
			IsRecursive:          false,
			IgnoreRecursiveError: false,
		}

		glog.V(3).Infof("Deleting policy %s", name)
		resp, err := client.DeleteEntry(ctx, request)
		if err != nil {
			// Ignore "not found" errors - policy may already be deleted
			if strings.Contains(err.Error(), "not found") {
				return nil
			}
			return fmt.Errorf("failed to delete policy %s: %v", name, err)
		}

		// Check response error
		if resp.Error != "" {
			// Ignore "not found" errors - policy may already be deleted
			if strings.Contains(resp.Error, "not found") {
				return nil
			}
			return fmt.Errorf("failed to delete policy %s: %s", name, resp.Error)
		}

		return nil
	})
}

// ListPolicies lists all policies in filer with metadata
func (s *FilerPolicyStore) ListPolicies(ctx context.Context, filerAddress string) ([]PolicyMetadata, error) {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && s.filerAddressProvider != nil {
		filerAddress = s.filerAddressProvider()
	}
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerPolicyStore")
	}

	var policies []PolicyMetadata

	err := s.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		// List all entries in the policy directory
		request := &filer_pb.ListEntriesRequest{
			Directory:          s.basePath,
			Prefix:             "", // No prefix restriction
			StartFromFileName:  "",
			InclusiveStartFrom: false,
			Limit:              1000,
		}

		stream, err := client.ListEntries(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to list policies: %v", err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				break // End of stream or error
			}

			if resp.Entry == nil || resp.Entry.IsDirectory {
				continue
			}

			// Exclude .hidden or system files if any (simple check)
			if strings.HasPrefix(resp.Entry.Name, ".") {
				continue
			}

			entry := resp.Entry
			createdAt := time.Unix(entry.Attributes.Crtime, 0)
			updatedAt := time.Unix(entry.Attributes.Mtime, 0)

			policies = append(policies, PolicyMetadata{
				Name:      strings.TrimSuffix(entry.Name, ".json"),
				CreatedAt: createdAt,
				UpdatedAt: updatedAt,
			})
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return policies, nil
}

// Helper methods

// withFilerClient executes a function with a filer client
func (s *FilerPolicyStore) withFilerClient(filerAddress string, fn func(client filer_pb.SeaweedFilerClient) error) error {
	if filerAddress == "" {
		return fmt.Errorf("filer address is required for FilerPolicyStore")
	}

	// Use the pb.WithGrpcFilerClient helper similar to existing SeaweedFS code
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(filerAddress), s.grpcDialOption, fn)
}

// getPolicyPath returns the full path for a policy
func (s *FilerPolicyStore) getPolicyPath(policyName string) string {
	return s.basePath + "/" + s.getPolicyFileName(policyName)
}

// getPolicyFileName returns the filename for a policy
func (s *FilerPolicyStore) getPolicyFileName(policyName string) string {
	if strings.HasSuffix(policyName, ".json") {
		return policyName
	}
	return policyName + ".json"
}
