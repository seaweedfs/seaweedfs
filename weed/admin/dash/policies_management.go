package dash

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// Policy management data structures
type PolicyStatement struct {
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

type PolicyDocument struct {
	Version   string             `json:"Version"`
	Statement []*PolicyStatement `json:"Statement"`
}

type IAMPolicy struct {
	Name         string         `json:"name"`
	Document     PolicyDocument `json:"document"`
	DocumentJSON string         `json:"document_json"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

type PoliciesCollection struct {
	Policies map[string]PolicyDocument `json:"policies"`
}

type PoliciesData struct {
	Username      string      `json:"username"`
	Policies      []IAMPolicy `json:"policies"`
	TotalPolicies int         `json:"total_policies"`
	LastUpdated   time.Time   `json:"last_updated"`
}

// Policy management request structures
type CreatePolicyRequest struct {
	Name         string         `json:"name" binding:"required"`
	Document     PolicyDocument `json:"document" binding:"required"`
	DocumentJSON string         `json:"document_json"`
}

type UpdatePolicyRequest struct {
	Document     PolicyDocument `json:"document" binding:"required"`
	DocumentJSON string         `json:"document_json"`
}

// GetPolicies retrieves all IAM policies
func (s *AdminServer) GetPolicies() ([]IAMPolicy, error) {
	var policies []IAMPolicy

	policiesCollection := &PoliciesCollection{
		Policies: make(map[string]PolicyDocument),
	}

	// Load policies from filer
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamPoliciesFile, &buf); err != nil {
			if err == filer_pb.ErrNotFound {
				// If file doesn't exist, return empty collection
				return nil
			}
			return err
		}

		if buf.Len() > 0 {
			return json.Unmarshal(buf.Bytes(), policiesCollection)
		}
		return nil
	})

	if err != nil {
		glog.Errorf("Failed to load policies: %v", err)
		return policies, err
	}

	// Convert to IAMPolicy format
	now := time.Now()
	for name, document := range policiesCollection.Policies {
		documentJSON, _ := json.MarshalIndent(document, "", "  ")

		policy := IAMPolicy{
			Name:         name,
			Document:     document,
			DocumentJSON: string(documentJSON),
			CreatedAt:    now, // We don't have actual creation time stored
			UpdatedAt:    now,
		}
		policies = append(policies, policy)
	}

	return policies, nil
}

// CreatePolicy creates a new IAM policy
func (s *AdminServer) CreatePolicy(name string, document PolicyDocument) error {
	policiesCollection := &PoliciesCollection{
		Policies: make(map[string]PolicyDocument),
	}

	// Load existing policies
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamPoliciesFile, &buf); err != nil {
			if err == filer_pb.ErrNotFound {
				// If file doesn't exist, start with empty collection
				return nil
			}
			return err
		}

		if buf.Len() > 0 {
			return json.Unmarshal(buf.Bytes(), policiesCollection)
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Add new policy
	policiesCollection.Policies[name] = document

	// Save back to filer
	return s.savePolicies(policiesCollection)
}

// UpdatePolicy updates an existing IAM policy
func (s *AdminServer) UpdatePolicy(name string, document PolicyDocument) error {
	policiesCollection := &PoliciesCollection{
		Policies: make(map[string]PolicyDocument),
	}

	// Load existing policies
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamPoliciesFile, &buf); err != nil {
			return err
		}

		if buf.Len() > 0 {
			return json.Unmarshal(buf.Bytes(), policiesCollection)
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Update policy
	policiesCollection.Policies[name] = document

	// Save back to filer
	return s.savePolicies(policiesCollection)
}

// DeletePolicy deletes an IAM policy
func (s *AdminServer) DeletePolicy(name string) error {
	policiesCollection := &PoliciesCollection{
		Policies: make(map[string]PolicyDocument),
	}

	// Load existing policies
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamPoliciesFile, &buf); err != nil {
			return err
		}

		if buf.Len() > 0 {
			return json.Unmarshal(buf.Bytes(), policiesCollection)
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Delete policy
	delete(policiesCollection.Policies, name)

	// Save back to filer
	return s.savePolicies(policiesCollection)
}

// savePolicies saves policies collection to filer
func (s *AdminServer) savePolicies(policiesCollection *PoliciesCollection) error {
	data, err := json.Marshal(policiesCollection)
	if err != nil {
		return err
	}

	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, filer.IamConfigDirectory, filer.IamPoliciesFile, data)
	})
}

// GetPolicy retrieves a specific IAM policy
func (s *AdminServer) GetPolicy(name string) (*IAMPolicy, error) {
	policies, err := s.GetPolicies()
	if err != nil {
		return nil, err
	}

	for _, policy := range policies {
		if policy.Name == name {
			return &policy, nil
		}
	}

	return nil, nil // Policy not found
}
