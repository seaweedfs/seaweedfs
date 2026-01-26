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
	IamPoliciesDirectory = "policies"
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

	// 1. Load from legacy file (low priority)
	content, foundLegacy, err := store.readInsideFiler(filer.IamConfigDirectory, filer.IamPoliciesFile)
	if err != nil {
		return nil, err
	}
	if foundLegacy && len(content) > 0 {
		legacyCollection := &PoliciesCollection{}
		if err := json.Unmarshal(content, legacyCollection); err != nil {
			glog.Errorf("Failed to parse legacy IAM policies: %v", err)
		} else {
			for k, v := range legacyCollection.Policies {
				policies[k] = v
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

			// We iterate, so we can lazily read or used passed content.
			// Safest to read if content missing or use helper.

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
				policy := policy_engine.PolicyDocument{}
				if err := json.Unmarshal(content, &policy); err != nil {
					glog.Warningf("Failed to unmarshal policy %s: %v", entry.Name, err)
					continue
				}

				// Policies are stored as <name>.json
				name := entry.Name
				if strings.HasSuffix(name, ".json") {
					name = name[:len(name)-5]
				}
				policies[name] = policy
			}
		}
		return nil
	})
}

func (store *FilerEtcStore) migratePoliciesToMultiFile(ctx context.Context, policies map[string]policy_engine.PolicyDocument) error {
	glog.Infof("Migrating IAM policies to multi-file layout...")

	// 1. Save all policies to individual files
	for name, policy := range policies {
		if err := store.savePolicyfile(ctx, name, policy); err != nil {
			return err
		}
	}

	// 2. Rename legacy file
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Using .old extension for legacy file
		oldName := filer.IamPoliciesFile + ".old"
		_, err := client.AtomicRenameEntry(ctx, &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: filer.IamConfigDirectory,
			OldName:      filer.IamPoliciesFile,
			NewDirectory: filer.IamConfigDirectory,
			NewName:      oldName,
		})
		return err
	})
}

func (store *FilerEtcStore) savePolicyfile(ctx context.Context, name string, policy policy_engine.PolicyDocument) error {
	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		data, err := json.Marshal(policy)
		if err != nil {
			return err
		}
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory+"/"+IamPoliciesDirectory, name+".json", data)
	})
}

// CreatePolicy creates a new IAM policy in the filer
func (store *FilerEtcStore) CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	// Check if already exists? GetPolicy logic.
	// Spec says PutPolicy is create/update. CreatePolicy might strictly be create?
	// The store interface usually implies overwrite for Put, but Create implies existence check.
	// But let's check current implementation: it was just map assignment.
	// Safe to overwrite or do we want to check?
	// Existing: "policies[name] = document". So it overwrites.

	return store.savePolicyfile(ctx, name, document)
}

// UpdatePolicy updates an existing IAM policy in the filer
func (store *FilerEtcStore) UpdatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return store.savePolicyfile(ctx, name, document)
}

// PutPolicy creates or updates an IAM policy in the filer
func (store *FilerEtcStore) PutPolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return store.savePolicyfile(ctx, name, document)
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
	// Optimization: Read directly from file
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

	if err != nil {
		return nil, err
	}

	// If not found in multi-file, check legacy?
	// If migration hasn't happened yet, we might miss it.
	// But `GetPolicies` triggers migration.
	// If we access `GetPolicy` directly before `GetPolicies` (server start), migration might not have run.
	// Ideally `LoadConfiguration` triggers migration on start.
	// For policies, we don't have a "LoadPoliciesConfiguration" that runs on start always.
	// But accessing via `GetPolicy` directly is rare without listing?
	// Actually `GetPolicy` is used by verifying access.
	// To be safe and transparent: if not found in multi-file, we COULD check legacy.
	// Or we force migration on first access?
	// Or simplistic approach: just use GetPolicies logic which handles migration.
	// BUT `GetPolicies` reads ALL files. `GetPolicy` should be fast.
	// Let's implement fallback: if file read fails (not found), read `policies.json`.

	if policy != nil {
		return policy, nil
	}

	// Fallback to legacy check (inefficient but safe for transition if migration missed)
	legacyPolicies, err := store.GetPolicies(ctx) // This triggers migration if needed!
	if err != nil {
		return nil, err
	}
	if p, ok := legacyPolicies[name]; ok {
		return &p, nil
	}

	return nil, nil // Not found
}
