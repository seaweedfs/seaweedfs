package filer_etc

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

const (
	IamPoliciesDirectory     = "policies"
	IamLegacyPoliciesOldFile = "policies.json.old"
)

type PoliciesCollection struct {
	Policies       map[string]policy_engine.PolicyDocument            `json:"policies"`
	InlinePolicies map[string]map[string]policy_engine.PolicyDocument `json:"inlinePolicies"`
}

func validatePolicyName(name string) error {
	return credential.ValidatePolicyName(name)
}

func newPoliciesCollection() *PoliciesCollection {
	return &PoliciesCollection{
		Policies:       make(map[string]policy_engine.PolicyDocument),
		InlinePolicies: make(map[string]map[string]policy_engine.PolicyDocument),
	}
}

func (store *FilerEtcStore) loadLegacyPoliciesCollection(ctx context.Context) (*PoliciesCollection, bool, error) {
	policiesCollection := newPoliciesCollection()

	content, foundLegacy, err := store.readInsideFiler(ctx, filer.IamConfigDirectory, filer.IamPoliciesFile)
	if err != nil {
		return nil, false, err
	}
	if !foundLegacy || len(content) == 0 {
		return policiesCollection, foundLegacy, nil
	}

	if err := json.Unmarshal(content, policiesCollection); err != nil {
		return nil, false, err
	}
	if policiesCollection.Policies == nil {
		policiesCollection.Policies = make(map[string]policy_engine.PolicyDocument)
	}
	if policiesCollection.InlinePolicies == nil {
		policiesCollection.InlinePolicies = make(map[string]map[string]policy_engine.PolicyDocument)
	}

	return policiesCollection, true, nil
}

func (store *FilerEtcStore) saveLegacyPoliciesCollection(ctx context.Context, policiesCollection *PoliciesCollection) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		content, err := json.MarshalIndent(policiesCollection, "", "  ")
		if err != nil {
			return err
		}
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory, filer.IamPoliciesFile, content)
	})
}

func policyDocumentToPbPolicy(name string, policy policy_engine.PolicyDocument) (*iam_pb.Policy, error) {
	content, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	return &iam_pb.Policy{Name: name, Content: string(content)}, nil
}

// LoadManagedPolicies loads managed policies for the S3 runtime without
// triggering legacy-to-multifile migration. This lets the runtime hydrate
// policies while preserving any legacy inline policy data stored alongside
// managed policies.
func (store *FilerEtcStore) LoadManagedPolicies(ctx context.Context) ([]*iam_pb.Policy, error) {
	policiesCollection, _, err := store.loadLegacyPoliciesCollection(ctx)
	if err != nil {
		return nil, err
	}

	policies := make(map[string]policy_engine.PolicyDocument, len(policiesCollection.Policies))
	for name, policy := range policiesCollection.Policies {
		policies[name] = policy
	}

	if err := store.loadPoliciesFromMultiFile(ctx, policies); err != nil {
		return nil, err
	}

	managedPolicies := make([]*iam_pb.Policy, 0, len(policies))
	for name, policy := range policies {
		pbPolicy, err := policyDocumentToPbPolicy(name, policy)
		if err != nil {
			return nil, err
		}
		managedPolicies = append(managedPolicies, pbPolicy)
	}

	return managedPolicies, nil
}

// LoadInlinePolicies loads legacy inline policies keyed by user name. Inline
// policies are still stored in the legacy shared policies file.
func (store *FilerEtcStore) LoadInlinePolicies(ctx context.Context) (map[string]map[string]policy_engine.PolicyDocument, error) {
	policiesCollection, _, err := store.loadLegacyPoliciesCollection(ctx)
	if err != nil {
		return nil, err
	}

	inlinePolicies := make(map[string]map[string]policy_engine.PolicyDocument, len(policiesCollection.InlinePolicies))
	for userName, userPolicies := range policiesCollection.InlinePolicies {
		inlinePolicies[userName] = make(map[string]policy_engine.PolicyDocument, len(userPolicies))
		for policyName, policy := range userPolicies {
			inlinePolicies[userName][policyName] = policy
		}
	}

	return inlinePolicies, nil
}

// GetPolicies retrieves all IAM policies from the filer
func (store *FilerEtcStore) GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error) {
	policies := make(map[string]policy_engine.PolicyDocument)

	// Check if filer client is configured (with mutex protection)
	store.mu.RLock()
	configured := store.filerAddressFunc != nil
	store.mu.RUnlock()

	if !configured {
		glog.V(1).Infof("Filer client not configured for policy retrieval, returning empty policies")
		return policies, nil
	}

	glog.V(2).Infof("Loading IAM policies from %s/%s (using current active filer)",
		filer.IamConfigDirectory, filer.IamPoliciesFile)

	// 1. Load from legacy single file (low priority)
	policiesCollection, _, err := store.loadLegacyPoliciesCollection(ctx)
	if err != nil {
		return nil, err
	}
	for name, policy := range policiesCollection.Policies {
		policies[name] = policy
	}

	// 2. Load from multi-file structure (high priority, overrides legacy)
	if err := store.loadPoliciesFromMultiFile(ctx, policies); err != nil {
		return nil, err
	}

	return policies, nil
}

func (store *FilerEtcStore) loadPoliciesFromMultiFile(ctx context.Context, policies map[string]policy_engine.PolicyDocument) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamPoliciesDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}

		for _, entry := range entries {
			if entry.IsDirectory {
				continue
			}

			var content []byte
			if len(entry.Content) > 0 {
				content = entry.Content
			} else {
				c, err := filer.ReadInsideFiler(client, dir, entry.Name)
				if err != nil {
					glog.Warningf("Failed to read policy file %s: %v", entry.Name, err)
					continue
				}
				content = c
			}

			if len(content) > 0 {
				var policy policy_engine.PolicyDocument
				if err := json.Unmarshal(content, &policy); err != nil {
					glog.Warningf("Failed to unmarshal policy %s: %v", entry.Name, err)
					continue
				}

				// The file name is "policyName.json"
				policyName := entry.Name
				if len(policyName) > 5 && policyName[len(policyName)-5:] == ".json" {
					policyName = policyName[:len(policyName)-5]
					policies[policyName] = policy
				}
			}
		}
		return nil
	})
}

func (store *FilerEtcStore) migratePoliciesToMultiFile(ctx context.Context, policies map[string]policy_engine.PolicyDocument) error {
	glog.Infof("Migrating IAM policies to multi-file layout...")

	// 1. Save all policies to individual files
	for name, policy := range policies {
		if err := store.savePolicy(ctx, name, policy); err != nil {
			return err
		}
	}

	// 2. Rename legacy file
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.AtomicRenameEntry(ctx, &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: filer.IamConfigDirectory,
			OldName:      filer.IamPoliciesFile,
			NewDirectory: filer.IamConfigDirectory,
			NewName:      IamLegacyPoliciesOldFile,
		})
		if err != nil {
			glog.Errorf("Failed to rename legacy IAM policies file %s/%s to %s: %v",
				filer.IamConfigDirectory, filer.IamPoliciesFile, IamLegacyPoliciesOldFile, err)
		}
		return err
	})
}

func (store *FilerEtcStore) savePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	if err := validatePolicyName(name); err != nil {
		return err
	}
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		data, err := json.Marshal(document)
		if err != nil {
			return err
		}
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory+"/"+IamPoliciesDirectory, name+".json", data)
	})
}

// CreatePolicy creates a new IAM policy in the filer
func (store *FilerEtcStore) CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return store.savePolicy(ctx, name, document)
}

// UpdatePolicy updates an existing IAM policy in the filer
func (store *FilerEtcStore) UpdatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return store.savePolicy(ctx, name, document)
}

// PutPolicy creates or updates an IAM policy in the filer
func (store *FilerEtcStore) PutPolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return store.UpdatePolicy(ctx, name, document)
}

// DeletePolicy deletes an IAM policy from the filer
func (store *FilerEtcStore) DeletePolicy(ctx context.Context, name string) error {
	if err := validatePolicyName(name); err != nil {
		return err
	}
	policiesCollection, foundLegacy, err := store.loadLegacyPoliciesCollection(ctx)
	if err != nil {
		return err
	}

	deleteLegacyPolicy := false
	if foundLegacy {
		if _, exists := policiesCollection.Policies[name]; exists {
			delete(policiesCollection.Policies, name)
			deleteLegacyPolicy = true
		}
	}

	if err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory: filer.IamConfigDirectory + "/" + IamPoliciesDirectory,
			Name:      name + ".json",
		})
		if err != nil && !strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	if deleteLegacyPolicy {
		return store.saveLegacyPoliciesCollection(ctx, policiesCollection)
	}

	return nil
}

// GetPolicy retrieves a specific IAM policy by name from the filer
func (store *FilerEtcStore) GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error) {
	if err := validatePolicyName(name); err != nil {
		return nil, err
	}

	var policy *policy_engine.PolicyDocument
	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, filer.IamConfigDirectory+"/"+IamPoliciesDirectory, name+".json")
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}
		if len(data) == 0 {
			return nil
		}
		policy = &policy_engine.PolicyDocument{}
		return json.Unmarshal(data, policy)
	})

	if policy != nil {
		return policy, err
	}

	// fallback to full list if single file read fails (e.g. before migration completes or if partially migrated)
	// Although migration should happen on first GetPolicies call.
	policies, err := store.GetPolicies(ctx)
	if err != nil {
		return nil, err
	}

	if p, exists := policies[name]; exists {
		return &p, nil
	}

	return nil, nil // Policy not found
}

// ListPolicyNames returns all managed policy names stored in the filer.
func (store *FilerEtcStore) ListPolicyNames(ctx context.Context) ([]string, error) {
	names := make([]string, 0)
	seenNames := make(map[string]struct{})

	store.mu.RLock()
	configured := store.filerAddressFunc != nil
	store.mu.RUnlock()

	if !configured {
		return names, nil
	}

	policiesCollection, _, err := store.loadLegacyPoliciesCollection(ctx)
	if err != nil {
		return nil, err
	}
	for name := range policiesCollection.Policies {
		if _, found := seenNames[name]; found {
			continue
		}
		names = append(names, name)
		seenNames[name] = struct{}{}
	}

	err = store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir := filer.IamConfigDirectory + "/" + IamPoliciesDirectory
		entries, err := listEntries(ctx, client, dir)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return nil
			}
			return err
		}

		for _, entry := range entries {
			if entry.IsDirectory {
				continue
			}
			name := entry.Name
			if strings.HasSuffix(name, ".json") {
				name = name[:len(name)-5]
			}
			if _, found := seenNames[name]; found {
				continue
			}
			names = append(names, name)
			seenNames[name] = struct{}{}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return names, nil
}
