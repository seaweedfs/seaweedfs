package credential

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCredentialStoreInterface(t *testing.T) {
	// Note: This test may fail if run without importing store packages
	// For full integration testing, see the test/ package
	if len(Stores) == 0 {
		t.Skip("No credential stores registered - this is expected when testing the base package without store imports")
	}

	// Check that expected stores are available
	storeNames := GetAvailableStores()
	expectedStores := []string{string(StoreTypeFilerEtc), string(StoreTypeMemory)}

	// Add SQLite and PostgreSQL if they're available (build tags dependent)
	for _, storeName := range storeNames {
		found := false
		for _, expected := range append(expectedStores, string(StoreTypeSQLite), string(StoreTypePostgres)) {
			if string(storeName) == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Unexpected store found: %s", storeName)
		}
	}

	// Test that filer_etc store is always available
	filerEtcStoreFound := false
	memoryStoreFound := false
	for _, storeName := range storeNames {
		if string(storeName) == string(StoreTypeFilerEtc) {
			filerEtcStoreFound = true
		}
		if string(storeName) == string(StoreTypeMemory) {
			memoryStoreFound = true
		}
	}
	if !filerEtcStoreFound {
		t.Error("FilerEtc store should always be available")
	}
	if !memoryStoreFound {
		t.Error("Memory store should always be available")
	}
}

func TestCredentialManagerCreation(t *testing.T) {
	config := util.GetViper()

	// Test creating credential manager with invalid store
	_, err := NewCredentialManager(CredentialStoreTypeName("nonexistent"), config, "test.")
	if err == nil {
		t.Error("Expected error for nonexistent store")
	}

	// Skip store-specific tests if no stores are registered
	if len(Stores) == 0 {
		t.Skip("No credential stores registered - skipping store-specific tests")
	}

	// Test creating credential manager with available stores
	availableStores := GetAvailableStores()
	if len(availableStores) == 0 {
		t.Skip("No stores available for testing")
	}

	// Test with the first available store
	storeName := availableStores[0]
	cm, err := NewCredentialManager(storeName, config, "test.")
	if err != nil {
		t.Fatalf("Failed to create credential manager with store %s: %v", storeName, err)
	}
	if cm == nil {
		t.Error("Credential manager should not be nil")
	}
	defer cm.Shutdown()

	// Test that the store is of the correct type
	if cm.GetStore().GetName() != storeName {
		t.Errorf("Expected %s store, got %s", storeName, cm.GetStore().GetName())
	}
}

func TestCredentialInterface(t *testing.T) {
	// Skip if no stores are registered
	if len(Stores) == 0 {
		t.Skip("No credential stores registered - for full testing see test/ package")
	}

	// Test the interface with the first available store
	availableStores := GetAvailableStores()
	if len(availableStores) == 0 {
		t.Skip("No stores available for testing")
	}

	testCredentialInterfaceWithStore(t, availableStores[0])
}

func testCredentialInterfaceWithStore(t *testing.T, storeName CredentialStoreTypeName) {
	// Create a test identity
	testIdentity := &iam_pb.Identity{
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

	// Test the interface methods exist (compile-time check)
	config := util.GetViper()
	cm, err := NewCredentialManager(storeName, config, "test.")
	if err != nil {
		t.Fatalf("Failed to create credential manager: %v", err)
	}
	defer cm.Shutdown()

	ctx := context.Background()

	// Test LoadConfiguration
	_, err = cm.LoadConfiguration(ctx)
	if err != nil {
		t.Fatalf("LoadConfiguration failed: %v", err)
	}

	// Test CreateUser
	err = cm.CreateUser(ctx, testIdentity)
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
}

func TestCredentialManagerIntegration(t *testing.T) {
	// Skip if no stores are registered
	if len(Stores) == 0 {
		t.Skip("No credential stores registered - for full testing see test/ package")
	}

	// Test with the first available store
	availableStores := GetAvailableStores()
	if len(availableStores) == 0 {
		t.Skip("No stores available for testing")
	}

	storeName := availableStores[0]
	config := util.GetViper()
	cm, err := NewCredentialManager(storeName, config, "test.")
	if err != nil {
		t.Fatalf("Failed to create credential manager: %v", err)
	}
	defer cm.Shutdown()

	ctx := context.Background()

	// Test complete workflow
	user1 := &iam_pb.Identity{
		Name:    "user1",
		Actions: []string{"Read"},
		Account: &iam_pb.Account{
			Id:           "111111111111",
			DisplayName:  "User One",
			EmailAddress: "user1@example.com",
		},
		Credentials: []*iam_pb.Credential{
			{
				AccessKey: "AKIAUSER1",
				SecretKey: "secret1",
			},
		},
	}

	user2 := &iam_pb.Identity{
		Name:    "user2",
		Actions: []string{"Write"},
		Account: &iam_pb.Account{
			Id:           "222222222222",
			DisplayName:  "User Two",
			EmailAddress: "user2@example.com",
		},
		Credentials: []*iam_pb.Credential{
			{
				AccessKey: "AKIAUSER2",
				SecretKey: "secret2",
			},
		},
	}

	// Create users
	err = cm.CreateUser(ctx, user1)
	if err != nil {
		t.Fatalf("Failed to create user1: %v", err)
	}

	err = cm.CreateUser(ctx, user2)
	if err != nil {
		t.Fatalf("Failed to create user2: %v", err)
	}

	// List users
	users, err := cm.ListUsers(ctx)
	if err != nil {
		t.Fatalf("Failed to list users: %v", err)
	}

	if len(users) != 2 {
		t.Errorf("Expected 2 users, got %d", len(users))
	}

	// Test access key lookup
	foundUser, err := cm.GetUserByAccessKey(ctx, "AKIAUSER1")
	if err != nil {
		t.Fatalf("Failed to get user by access key: %v", err)
	}
	if foundUser.Name != "user1" {
		t.Errorf("Expected user1, got %s", foundUser.Name)
	}

	// Delete user
	err = cm.DeleteUser(ctx, "user1")
	if err != nil {
		t.Fatalf("Failed to delete user: %v", err)
	}

	// Verify user is deleted
	_, err = cm.GetUser(ctx, "user1")
	if err != ErrUserNotFound {
		t.Errorf("Expected ErrUserNotFound, got %v", err)
	}

	// Clean up
	err = cm.DeleteUser(ctx, "user2")
	if err != nil {
		t.Fatalf("Failed to delete user2: %v", err)
	}
}

// TestErrorTypes tests that the custom error types are defined correctly
func TestErrorTypes(t *testing.T) {
	// Test that error types are defined
	if ErrUserNotFound == nil {
		t.Error("ErrUserNotFound should be defined")
	}
	if ErrUserAlreadyExists == nil {
		t.Error("ErrUserAlreadyExists should be defined")
	}
	if ErrAccessKeyNotFound == nil {
		t.Error("ErrAccessKeyNotFound should be defined")
	}

	// Test error messages
	if ErrUserNotFound.Error() != "user not found" {
		t.Errorf("Expected 'user not found', got '%s'", ErrUserNotFound.Error())
	}
	if ErrUserAlreadyExists.Error() != "user already exists" {
		t.Errorf("Expected 'user already exists', got '%s'", ErrUserAlreadyExists.Error())
	}
	if ErrAccessKeyNotFound.Error() != "access key not found" {
		t.Errorf("Expected 'access key not found', got '%s'", ErrAccessKeyNotFound.Error())
	}
}

// TestGetAvailableStores tests the store discovery function
func TestGetAvailableStores(t *testing.T) {
	stores := GetAvailableStores()
	if len(stores) == 0 {
		t.Skip("No stores available for testing")
	}

	// Convert to strings for comparison
	storeNames := make([]string, len(stores))
	for i, store := range stores {
		storeNames[i] = string(store)
	}

	t.Logf("Available stores: %v (count: %d)", storeNames, len(storeNames))

	// We expect at least memory and filer_etc stores to be available
	expectedStores := []string{string(StoreTypeFilerEtc), string(StoreTypeMemory)}

	// Add SQLite and PostgreSQL if they're available (build tags dependent)
	for _, storeName := range storeNames {
		found := false
		for _, expected := range append(expectedStores, string(StoreTypeSQLite), string(StoreTypePostgres)) {
			if storeName == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Unexpected store found: %s", storeName)
		}
	}

	// Test that filer_etc store is always available
	filerEtcStoreFound := false
	memoryStoreFound := false
	for _, storeName := range storeNames {
		if storeName == string(StoreTypeFilerEtc) {
			filerEtcStoreFound = true
		}
		if storeName == string(StoreTypeMemory) {
			memoryStoreFound = true
		}
	}
	if !filerEtcStoreFound {
		t.Error("FilerEtc store should always be available")
	}
	if !memoryStoreFound {
		t.Error("Memory store should always be available")
	}
}
