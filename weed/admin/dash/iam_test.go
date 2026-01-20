package dash

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory" // Register memory store
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
)

// TestIAMManagement runs comprehensive tests for Users, Roles, and Policies
func TestIAMManagement(t *testing.T) {
	// Initialize AdminServer with memory stores
	server, err := setupTestAdminServer()
	if err != nil {
		t.Fatalf("Failed to setup test admin server: %v", err)
	}

	// Run sub-tests
	t.Run("UserManagement", func(t *testing.T) {
		testUserManagement(t, server)
	})

	t.Run("RoleManagement", func(t *testing.T) {
		testRoleManagement(t, server)
	})

	t.Run("PolicyManagement", func(t *testing.T) {
		testPolicyManagement(t, server)
	})
}

// setupTestAdminServer creates an AdminServer with memory-based stores
func setupTestAdminServer() (*AdminServer, error) {
	// Initialize with fake cached filer to avoid masterClient panic
	server := &AdminServer{
		cachedFilers:         []string{"localhost:8888"},
		lastFilerUpdate:      time.Now(),
		filerCacheExpiration: time.Hour,
	}

	// 1. Setup CredentialManager (Users) using Memory Store
	// Note: We need to use a distinct prefix/config to avoid sharing state if parallel
	// Memory store doesn't use configuration, so passing nil is safe if interface allows.
	// NewCredentialManager takes util.Configuration interface.
	cm, err := credential.NewCredentialManager("memory", nil, "test")
	if err != nil {
		return nil, err
	}
	server.credentialManager = cm

	// 2. Setup Role Store using Memory Store
	roleStore := integration.NewMemoryRoleStore()
	server.SetRoleStore(roleStore)

	// 3. Setup Policy Store using Memory Store
	policyStore := policy.NewMemoryPolicyStore()
	server.SetPolicyStore(policyStore)

	return server, nil
}

// testUserManagement tests Create, Read, Update, Delete for Users
func testUserManagement(t *testing.T, s *AdminServer) {
	// 1. Create User
	req := CreateUserRequest{
		Username:    "testuser",
		Email:       "test@example.com",
		Actions:     []string{"Read", "Write"},
		Roles:       []string{"Admin"},
		GenerateKey: true,
	}

	user, err := s.CreateObjectStoreUser(req)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	if user.Username != req.Username {
		t.Errorf("Expected username %s, got %s", req.Username, user.Username)
	}
	if user.AccessKey == "" || user.SecretKey == "" {
		t.Error("Expected access and secret keys to be generated")
	}

	// 2. Get User Details
	details, err := s.GetObjectStoreUserDetails("testuser")
	if err != nil {
		t.Fatalf("Failed to get user details: %v", err)
	}

	if len(details.Actions) != 2 {
		t.Errorf("Expected 2 actions, got %d", len(details.Actions))
	}
	if len(details.Roles) != 1 {
		t.Errorf("Expected 1 role, got %d", len(details.Roles))
	}

	// 3. Update User
	updateReq := UpdateUserRequest{
		Email:   "updated@example.com",
		Actions: []string{"Read"},
	}
	updatedUser, err := s.UpdateObjectStoreUser("testuser", updateReq)
	if err != nil {
		t.Fatalf("Failed to update user: %v", err)
	}

	if updatedUser.Email != "updated@example.com" {
		t.Errorf("Expected updated email, got %s", updatedUser.Email)
	}
	// Verify actions updated
	policies, err := s.GetUserPolicies("testuser")
	if err != nil {
		t.Errorf("Failed to get user policies: %v", err)
	}
	if len(policies) != 1 || policies[0] != "Read" {
		t.Errorf("Policies not updated correctly: %v", policies)
	}

	// 4. Access Key Management
	newKey, err := s.CreateAccessKey("testuser")
	if err != nil {
		t.Fatalf("Failed to create access key: %v", err)
	}
	if newKey.AccessKey == "" {
		t.Error("Created empty access key")
	}

	// Verify key existence (list details again)
	details, _ = s.GetObjectStoreUserDetails("testuser")
	if len(details.AccessKeys) != 2 { // Initial + New
		t.Errorf("Expected 2 access keys, got %d", len(details.AccessKeys))
	}

	// Delete key
	err = s.DeleteAccessKey("testuser", newKey.AccessKey)
	if err != nil {
		t.Errorf("Failed to delete access key: %v", err)
	}

	// 5. Delete User
	err = s.DeleteObjectStoreUser("testuser")
	if err != nil {
		t.Fatalf("Failed to delete user: %v", err)
	}

	// Verify deletion
	_, err = s.GetObjectStoreUserDetails("testuser")
	if err == nil {
		t.Error("Expected error getting deleted user, got nil")
	}
}

// testRoleManagement tests CRUD for Roles
func testRoleManagement(t *testing.T, s *AdminServer) {
	// 1. Create Role
	roleName := "TestRole"
	roleReq := CreateRoleRequest{
		RoleName:         roleName,
		Description:      "Test Role Description",
		AttachedPolicies: []string{"Policy1"},
		TrustPolicyJSON:  `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"art:aws:iam::123456789012:root"},"Action":"sts:AssumeRole"}]}`,
	}

	err := s.CreateRole(roleReq)
	if err != nil {
		t.Fatalf("Failed to create role: %v", err)
	}

	// 2. Get Roles
	roles, err := s.GetRoles()
	if err != nil {
		t.Fatalf("Failed to get roles: %v", err)
	}

	var foundRole bool
	for _, r := range roles {
		if r.RoleName == roleName {
			foundRole = true
			if r.Description != roleReq.Description {
				t.Errorf("Expected description %s, got %s", roleReq.Description, r.Description)
			}
			if len(r.AttachedPolicies) != 1 || r.AttachedPolicies[0] != "Policy1" {
				t.Errorf("Attached policies mismatch: %v", r.AttachedPolicies)
			}
			break
		}
	}
	if !foundRole {
		t.Errorf("Role %s not found in list", roleName)
	}

	// 3. Get Specific Role
	role, err := s.GetRole(roleName)
	if err != nil {
		t.Fatalf("Failed to get specific role: %v", err)
	}
	if role == nil {
		t.Fatal("Role is nil")
	}
	if role.RoleName != roleName {
		t.Errorf("Expected role name %s, got %s", roleName, role.RoleName)
	}

	// 4. Delete Role
	err = s.DeleteRole(roleName)
	if err != nil {
		t.Fatalf("Failed to delete role: %v", err)
	}

	// Verify deletion
	role, err = s.GetRole(roleName)
	if err == nil && role != nil {
		t.Error("Role should be deleted")
	}
}

// testPolicyManagement tests CRUD for Policies
func testPolicyManagement(t *testing.T, s *AdminServer) {
	// 1. Create Policy
	policyName := "TestPolicy"
	doc := policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Effect: "Allow",
				Action: []string{"s3:ListBucket"},
				Resource: []string{"arn:aws:s3:::*"},
			},
		},
	}

	err := s.CreatePolicy(policyName, doc)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	// 2. Get Policies
	policies, err := s.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}

	var foundPolicy bool
	for _, p := range policies {
		if p.Name == policyName {
			foundPolicy = true
			break
		}
	}
	if !foundPolicy {
		t.Errorf("Policy %s not found in list", policyName)
	}

	// 3. Get Specific Policy
	p, err := s.GetPolicy(policyName)
	if err != nil {
		t.Fatalf("Failed to get specific policy: %v", err)
	}
	if p == nil {
		t.Fatal("Policy is nil")
	}
	if p.Name != policyName {
		t.Errorf("Expected policy name %s, got %s", policyName, p.Name)
	}
	if len(p.Document.Statement) != 1 {
		t.Errorf("Expected 1 statement, got %d", len(p.Document.Statement))
	}

	// 4. Update Policy
	doc.Statement[0].Action = []string{"s3:ListBucket", "s3:GetObject"}
	err = s.UpdatePolicy(policyName, doc)
	if err != nil {
		t.Fatalf("Failed to update policy: %v", err)
	}

	// Verify update
	p, err = s.GetPolicy(policyName)
	if err != nil {
		t.Fatalf("Failed to get updated policy: %v", err)
	}
	if len(p.Document.Statement[0].Action) != 2 {
		t.Errorf("Policy not updated correctly")
	}

	// 5. Delete Policy
	err = s.DeletePolicy(policyName)
	if err != nil {
		t.Fatalf("Failed to delete policy: %v", err)
	}

	// Verify deletion
	p, err = s.GetPolicy(policyName)
	if err == nil && p != nil {
		t.Error("Policy should be deleted")
	}
}
