package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
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

// ListPolicies lists all policy names in memory (filerAddress ignored for memory store)
func (s *MemoryPolicyStore) ListPolicies(ctx context.Context, filerAddress string) ([]string, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	names := make([]string, 0, len(s.policies))
	for name := range s.policies {
		names = append(names, name)
	}

	return names, nil
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
}

// NewFilerPolicyStore creates a new filer-based policy store
func NewFilerPolicyStore(config map[string]interface{}, filerAddressProvider func() string) (*FilerPolicyStore, error) {
	store := &FilerPolicyStore{
		basePath:             "/etc/iam/policies", // Default path for policy storage - aligned with /etc/ convention
		filerAddressProvider: filerAddressProvider,
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

	policyPath := s.getPolicyPath(name)

	// Store in filer
	return s.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		glog.V(3).Infof("Storing policy %s at %s", name, policyPath)
		if err := s.savePolicyFile(ctx, client, s.getPolicyFileName(name), policyData); err != nil {
			return fmt.Errorf("failed to store policy %s: %v", name, err)
		}
		if err := s.deleteLegacyPolicyFileIfPresent(ctx, client, name); err != nil {
			return err
		}

		return nil
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
		for _, fileName := range s.getPolicyLookupFileNames(name) {
			request := &filer_pb.LookupDirectoryEntryRequest{
				Directory: s.basePath,
				Name:      fileName,
			}

			glog.V(3).Infof("Looking up policy %s as %s", name, fileName)
			response, err := client.LookupDirectoryEntry(ctx, request)
			if err != nil {
				if strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
					continue
				}
				return fmt.Errorf("policy lookup failed: %v", err)
			}

			if response.Entry == nil {
				continue
			}

			policyData = response.Entry.Content
			return nil
		}

		return fmt.Errorf("policy not found")
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
		for _, fileName := range s.getPolicyLookupFileNames(name) {
			request := &filer_pb.DeleteEntryRequest{
				Directory:            s.basePath,
				Name:                 fileName,
				IsDeleteData:         true,
				IsRecursive:          false,
				IgnoreRecursiveError: false,
			}

			glog.V(3).Infof("Deleting policy %s as %s", name, fileName)
			resp, err := client.DeleteEntry(ctx, request)
			if err != nil {
				if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
					continue
				}
				return fmt.Errorf("failed to delete policy %s: %v", name, err)
			}

			if resp.Error != "" && !strings.Contains(resp.Error, "not found") {
				return fmt.Errorf("failed to delete policy %s: %s", name, resp.Error)
			}
		}

		return nil
	})
}

// ListPolicies lists all policy names in filer
func (s *FilerPolicyStore) ListPolicies(ctx context.Context, filerAddress string) ([]string, error) {
	// Use provider function if filerAddress is not provided
	if filerAddress == "" && s.filerAddressProvider != nil {
		filerAddress = s.filerAddressProvider()
	}
	if filerAddress == "" {
		return nil, fmt.Errorf("filer address is required for FilerPolicyStore")
	}

	var policyNames []string

	err := s.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
		// List all entries in the policy directory
		request := &filer_pb.ListEntriesRequest{
			Directory:          s.basePath,
			Prefix:             "",
			StartFromFileName:  "",
			InclusiveStartFrom: false,
			Limit:              1000, // Process in batches of 1000
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

			if policyName, ok := s.policyNameFromFileName(resp.Entry.Name); ok {
				policyNames = append(policyNames, policyName)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	uniquePolicyNames := make([]string, 0, len(policyNames))
	seen := make(map[string]struct{}, len(policyNames))
	for _, policyName := range policyNames {
		if _, found := seen[policyName]; found {
			continue
		}
		seen[policyName] = struct{}{}
		uniquePolicyNames = append(uniquePolicyNames, policyName)
	}

	return uniquePolicyNames, nil
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
	return s.getCanonicalPolicyFileName(policyName)
}

func (s *FilerPolicyStore) getLegacyPolicyFileName(policyName string) string {
	return "policy_" + policyName + ".json"
}

func (s *FilerPolicyStore) getCanonicalPolicyFileName(policyName string) string {
	return policyName + ".json"
}

func (s *FilerPolicyStore) getPolicyLookupFileNames(policyName string) []string {
	return []string{
		s.getCanonicalPolicyFileName(policyName),
		s.getLegacyPolicyFileName(policyName),
	}
}

func (s *FilerPolicyStore) policyNameFromFileName(fileName string) (string, bool) {
	if !strings.HasSuffix(fileName, ".json") {
		return "", false
	}
	policyName := strings.TrimSuffix(fileName, ".json")
	if strings.HasPrefix(fileName, "policy_") {
		policyName = strings.TrimPrefix(policyName, "policy_")
	}
	if s.isSupportedPolicyName(policyName) {
		return policyName, true
	}
	return "", false
}

func (s *FilerPolicyStore) isSupportedPolicyName(policyName string) bool {
	if policyName == "" {
		return false
	}
	if strings.HasPrefix(policyName, "bucket-policy:") {
		return len(policyName) > len("bucket-policy:")
	}
	return credential.ValidatePolicyName(policyName) == nil
}

func (s *FilerPolicyStore) deleteLegacyPolicyFileIfPresent(ctx context.Context, client filer_pb.SeaweedFilerClient, policyName string) error {
	legacyFileName := s.getLegacyPolicyFileName(policyName)
	response, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
		Directory:            s.basePath,
		Name:                 legacyFileName,
		IsDeleteData:         true,
		IsRecursive:          false,
		IgnoreRecursiveError: false,
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
			return nil
		}
		return fmt.Errorf("failed to delete legacy policy %s: %v", policyName, err)
	}
	if response.Error != "" && !strings.Contains(response.Error, "not found") {
		return fmt.Errorf("failed to delete legacy policy %s: %s", policyName, response.Error)
	}
	return nil
}

func (s *FilerPolicyStore) savePolicyFile(ctx context.Context, client filer_pb.SeaweedFilerClient, fileName string, content []byte) error {
	response, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: s.basePath,
		Name:      fileName,
	})
	if err != nil {
		if err == filer_pb.ErrNotFound {
			now := time.Now().Unix()
			return filer_pb.CreateEntry(ctx, client, &filer_pb.CreateEntryRequest{
				Directory: s.basePath,
				Entry: &filer_pb.Entry{
					Name:        fileName,
					IsDirectory: false,
					Attributes: &filer_pb.FuseAttributes{
						Mtime:    now,
						Crtime:   now,
						FileMode: uint32(0600),
						Uid:      uint32(0),
						Gid:      uint32(0),
						FileSize: uint64(len(content)),
					},
					Content: content,
				},
			})
		}
		return err
	}

	entry := response.Entry
	if entry == nil {
		return fmt.Errorf("lookup returned empty entry for %s", fileName)
	}
	if entry.Attributes == nil {
		entry.Attributes = &filer_pb.FuseAttributes{}
	}
	now := time.Now().Unix()
	if entry.Attributes.Crtime == 0 {
		entry.Attributes.Crtime = now
	}
	if entry.Attributes.FileMode == 0 {
		entry.Attributes.FileMode = uint32(0600)
	}
	entry.Attributes.Mtime = now
	entry.Attributes.FileSize = uint64(len(content))
	entry.Content = content

	return filer_pb.UpdateEntry(ctx, client, &filer_pb.UpdateEntryRequest{
		Directory: s.basePath,
		Entry:     entry,
	})
}
