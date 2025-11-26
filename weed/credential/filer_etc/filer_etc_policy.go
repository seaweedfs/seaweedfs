package filer_etc

import (
	"context"
	"encoding/json"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

type PoliciesCollection struct {
	Policies map[string]policy_engine.PolicyDocument `json:"policies"`
}

// GetPolicies retrieves all IAM policies from the filer
func (store *FilerEtcStore) GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error) {
	policiesCollection := &PoliciesCollection{
		Policies: make(map[string]policy_engine.PolicyDocument),
	}

	// Check if filer client is configured
	if store.filerAddressFunc == nil {
		glog.V(1).Infof("Filer client not configured for policy retrieval, returning empty policies")
		// Return empty policies if filer client is not configured
		return policiesCollection.Policies, nil
	}

	glog.V(2).Infof("Loading IAM policies from %s/%s (using current active filer)", 
		filer.IamConfigDirectory, filer.IamPoliciesFile)

	err := store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Use ReadInsideFiler instead of ReadEntry since policies.json is small
		// and stored inline. ReadEntry requires a master client for chunked files,
		// but ReadInsideFiler only reads inline content.
		content, err := filer.ReadInsideFiler(client, filer.IamConfigDirectory, filer.IamPoliciesFile)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				glog.V(1).Infof("Policies file not found at %s/%s, returning empty policies", 
					filer.IamConfigDirectory, filer.IamPoliciesFile)
				// If file doesn't exist, return empty collection
				return nil
			}
			glog.Errorf("Failed to read IAM policies file from %s/%s: %v", 
				filer.IamConfigDirectory, filer.IamPoliciesFile, err)
			return err
		}

		if len(content) == 0 {
			glog.V(2).Infof("IAM policies file at %s/%s is empty", 
				filer.IamConfigDirectory, filer.IamPoliciesFile)
			return nil
		}

		glog.V(2).Infof("Read %d bytes from %s/%s", 
			len(content), filer.IamConfigDirectory, filer.IamPoliciesFile)

		if err := json.Unmarshal(content, policiesCollection); err != nil {
			glog.Errorf("Failed to parse IAM policies from %s/%s: %v", 
				filer.IamConfigDirectory, filer.IamPoliciesFile, err)
			return err
		}

		glog.V(1).Infof("Successfully loaded %d IAM policies", len(policiesCollection.Policies))
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Log policy names for debugging
	if glog.V(2) && len(policiesCollection.Policies) > 0 {
		for policyName := range policiesCollection.Policies {
			glog.V(2).Infof("  Policy: %s", policyName)
		}
	}

	return policiesCollection.Policies, nil
}

// CreatePolicy creates a new IAM policy in the filer
func (store *FilerEtcStore) CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return store.updatePolicies(ctx, func(policies map[string]policy_engine.PolicyDocument) {
		policies[name] = document
	})
}

// UpdatePolicy updates an existing IAM policy in the filer
func (store *FilerEtcStore) UpdatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	return store.updatePolicies(ctx, func(policies map[string]policy_engine.PolicyDocument) {
		policies[name] = document
	})
}

// DeletePolicy deletes an IAM policy from the filer
func (store *FilerEtcStore) DeletePolicy(ctx context.Context, name string) error {
	return store.updatePolicies(ctx, func(policies map[string]policy_engine.PolicyDocument) {
		delete(policies, name)
	})
}

// updatePolicies is a helper method to update policies atomically
func (store *FilerEtcStore) updatePolicies(ctx context.Context, updateFunc func(map[string]policy_engine.PolicyDocument)) error {
	// Load existing policies
	policies, err := store.GetPolicies(ctx)
	if err != nil {
		return err
	}

	// Apply update
	updateFunc(policies)

	// Save back to filer
	policiesCollection := &PoliciesCollection{
		Policies: policies,
	}

	data, err := json.Marshal(policiesCollection)
	if err != nil {
		return err
	}

	return store.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory, filer.IamPoliciesFile, data)
	})
}

// GetPolicy retrieves a specific IAM policy by name from the filer
func (store *FilerEtcStore) GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error) {
	policies, err := store.GetPolicies(ctx)
	if err != nil {
		return nil, err
	}

	if policy, exists := policies[name]; exists {
		return &policy, nil
	}

	return nil, nil // Policy not found
}
