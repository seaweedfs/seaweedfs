package filer_etc

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

const (
	IamPoliciesDirectory     = "policies"
	IamLegacyPoliciesOldFile = "policies.json.old"
)

type PoliciesCollection struct {
	Policies map[string]policy_engine.PolicyDocument `json:"policies"`
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
	content, foundLegacy, err := store.readInsideFiler(filer.IamConfigDirectory, filer.IamPoliciesFile)
	if err != nil {
		return nil, err
	}

	if foundLegacy && len(content) > 0 {
		policiesCollection := &PoliciesCollection{
			Policies: make(map[string]policy_engine.PolicyDocument),
		}
		if err := json.Unmarshal(content, policiesCollection); err != nil {
			glog.Errorf("Failed to parse legacy IAM policies from %s/%s: %v",
				filer.IamConfigDirectory, filer.IamPoliciesFile, err)
		} else {
			for name, policy := range policiesCollection.Policies {
				policies[name] = policy
			}
		}
	}

	// 2. Load from multi-file structure (high priority, overrides legacy)
	if err := store.loadPoliciesFromMultiFile(ctx, policies); err != nil {
		return nil, err
	}

	// 3. Perform migration if we loaded legacy config
	if foundLegacy {
		if err := store.migratePoliciesToMultiFile(ctx, policies); err != nil {
			glog.Errorf("Failed to migrate IAM policies to multi-file layout: %v", err)
			return policies, err
		}
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
		return err
	})
}

func (store *FilerEtcStore) savePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
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
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory: filer.IamConfigDirectory + "/" + IamPoliciesDirectory,
			Name:      name + ".json",
		})
		if err != nil && !strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
			return err
		}
		return nil
	})
}

// GetPolicy retrieves a specific IAM policy by name from the filer
func (store *FilerEtcStore) GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error) {
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
