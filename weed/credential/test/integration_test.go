package test

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	// Import all store implementations to register them
	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/postgres"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/sqlite"
)

func TestStoreRegistration(t *testing.T) {
	// Test that stores are registered
	storeNames := credential.GetAvailableStores()
	if len(storeNames) == 0 {
		t.Fatal("No credential stores registered")
	}

	expectedStores := []string{string(credential.StoreTypeFilerEtc), string(credential.StoreTypeMemory), string(credential.StoreTypeSQLite), string(credential.StoreTypePostgres)}

	// Verify all expected stores are present
	for _, expected := range expectedStores {
		found := false
		for _, storeName := range storeNames {
			if string(storeName) == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected store not found: %s", expected)
		}
	}

	t.Logf("Available stores: %v", storeNames)
}

func TestMemoryStoreIntegration(t *testing.T) {
	// Test creating credential manager with memory store
	config := util.GetViper()
	cm, err := credential.NewCredentialManager(credential.StoreTypeMemory, config, "test.")
	if err != nil {
		t.Fatalf("Failed to create memory credential manager: %v", err)
	}
	defer cm.Shutdown()

	// Test that the store is of the correct type
	if cm.GetStore().GetName() != credential.StoreTypeMemory {
		t.Errorf("Expected memory store, got %s", cm.GetStore().GetName())
	}

	// Test basic operations
	ctx := context.Background()

	// Create test user
	testUser := &iam_pb.Identity{
		Name:    "testuser",
		Actions: []string{"Read", "Write"},
		Account: &iam_pb.Account{
			Id:           "123456789012",
			DisplayName:  "Test User",
			EmailAddress: "test@example.com",
		},
		Credentials: []*iam_pb.Credential{
			{
				AccessKey: "AKIAIOSFODNN7EXAMPLE",
				SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
		},
	}

	// Test CreateUser
	err = cm.CreateUser(ctx, testUser)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Test GetUser
	user, err := cm.GetUser(ctx, "testuser")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	if user.Name != "testuser" {
		t.Errorf("Expected user name 'testuser', got %s", user.Name)
	}

	// Test ListUsers
	users, err := cm.ListUsers(ctx)
	if err != nil {
		t.Fatalf("ListUsers failed: %v", err)
	}
	if len(users) != 1 || users[0] != "testuser" {
		t.Errorf("Expected ['testuser'], got %v", users)
	}

	// Test GetUserByAccessKey
	userByKey, err := cm.GetUserByAccessKey(ctx, "AKIAIOSFODNN7EXAMPLE")
	if err != nil {
		t.Fatalf("GetUserByAccessKey failed: %v", err)
	}
	if userByKey.Name != "testuser" {
		t.Errorf("Expected user name 'testuser', got %s", userByKey.Name)
	}

	// Test DeleteUser
	err = cm.DeleteUser(ctx, "testuser")
	if err != nil {
		t.Fatalf("DeleteUser failed: %v", err)
	}

	// Verify user was deleted
	_, err = cm.GetUser(ctx, "testuser")
	if err != credential.ErrUserNotFound {
		t.Errorf("Expected ErrUserNotFound, got %v", err)
	}
}
