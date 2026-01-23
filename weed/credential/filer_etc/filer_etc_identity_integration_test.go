package filer_etc

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// TestScalableIAMIntegration tests the full scalable IAM implementation
// This requires a running filer instance - run with: go test -tags=integration
func TestScalableIAMIntegration(t *testing.T) {
	t.Skip("Integration test - requires running filer. Run manually with -tags=integration")
	
	// Setup: Initialize store with test filer
	store := setupTestStore(t)
	ctx := context.Background()

	// Test 1: Create multiple users (individual files)
	t.Run("CreateMultipleUsers", func(t *testing.T) {
		for i := 1; i <= 50; i++ {
			identity := &iam_pb.Identity{
				Name: fmt.Sprintf("testuser%03d", i),
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: fmt.Sprintf("AKIATEST%03d", i),
						SecretKey: fmt.Sprintf("secret%03d", i),
					},
				},
			}
			
			if err := store.CreateUser(ctx, identity); err != nil {
				t.Fatalf("Failed to create user %s: %v", identity.Name, err)
			}
		}
	})

	// Test 2: Verify pagination in GetUserByAccessKey
	t.Run("GetUserByAccessKeyWithPagination", func(t *testing.T) {
		// Should find user in middle of dataset
		identity, err := store.GetUserByAccessKey(ctx, "AKIATEST025")
		if err != nil {
			t.Fatalf("Failed to find user by access key: %v", err)
		}
		if identity.Name != "testuser025" {
			t.Errorf("Expected user testuser025, got %s", identity.Name)
		}

		// Should find first user
		identity, err = store.GetUserByAccessKey(ctx, "AKIATEST001")
		if err != nil {
			t.Fatalf("Failed to find first user: %v", err)
		}
		if identity.Name != "testuser001" {
			t.Errorf("Expected user testuser001, got %s", identity.Name)
		}

		// Should find last user
		identity, err = store.GetUserByAccessKey(ctx, "AKIATEST050")
		if err != nil {
			t.Fatalf("Failed to find last user: %v", err)
		}
		if identity.Name != "testuser050" {
			t.Errorf("Expected user testuser050, got %s", identity.Name)
		}
	})

	// Test 3: List all users (pagination implicit)
	t.Run("ListAllUsers", func(t *testing.T) {
		users, err := store.ListUsers(ctx)
		if err != nil {
			t.Fatalf("Failed to list users: %v", err)
		}
		
		if len(users) < 50 {
			t.Errorf("Expected at least 50 users, got %d", len(users))
		}
		
		// Verify all test users are present
		userMap := make(map[string]bool)
		for _, username := range users {
			userMap[username] = true
		}
		
		for i := 1; i <= 50; i++ {
			expectedName := fmt.Sprintf("testuser%03d", i)
			if !userMap[expectedName] {
				t.Errorf("Expected user %s not found in list", expectedName)
			}
		}
	})

	// Test 4: Update user (rename scenario)
	t.Run("UpdateUserWithRename", func(t *testing.T) {
		// Get existing user
		user, err := store.GetUser(ctx, "testuser010")
		if err != nil {
			t.Fatalf("Failed to get user: %v", err)
		}

		// Rename user
		user.Name = "testuser010_renamed"
		if err := store.UpdateUser(ctx, "testuser010", user); err != nil {
			t.Fatalf("Failed to rename user: %v", err)
		}

		// Verify old name doesn't exist
		_, err = store.GetUser(ctx, "testuser010")
		if err == nil {
			t.Error("Old username should not exist after rename")
		}

		// Verify new name exists
		renamedUser, err := store.GetUser(ctx, "testuser010_renamed")
		if err != nil {
			t.Fatalf("Failed to get renamed user: %v", err)
		}
		if renamedUser.Credentials[0].AccessKey != "AKIATEST010" {
			t.Error("Credentials should be preserved after rename")
		}
	})

	// Test 5: Delete user
	t.Run("DeleteUser", func(t *testing.T) {
		if err := store.DeleteUser(ctx, "testuser050"); err != nil {
			t.Fatalf("Failed to delete user: %v", err)
		}

		// Verify user is gone
		_, err := store.GetUser(ctx, "testuser050")
		if err == nil {
			t.Error("User should not exist after deletion")
		}
	})

	// Test 6: Concurrent access (stress test)
	t.Run("ConcurrentAccess", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, 10)

		// Concurrent reads
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				accessKey := fmt.Sprintf("AKIATEST%03d", (n%49)+1)
				_, err := store.GetUserByAccessKey(ctx, accessKey)
				if err != nil {
					errors <- fmt.Errorf("concurrent read failed: %w", err)
				}
			}(i)
		}

		// Concurrent writes (non-overlapping users)
		for i := 100; i < 110; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				identity := &iam_pb.Identity{
					Name: fmt.Sprintf("concurrent%03d", n),
					Credentials: []*iam_pb.Credential{
						{
							AccessKey: fmt.Sprintf("AKIACONC%03d", n),
							SecretKey: fmt.Sprintf("secret%03d", n),
						},
					},
				}
				if err := store.CreateUser(ctx, identity); err != nil {
					errors <- fmt.Errorf("concurrent write failed: %w", err)
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Errorf("Concurrent operation error: %v", err)
		}
	})

	// Cleanup
	t.Run("Cleanup", func(t *testing.T) {
		users, _ := store.ListUsers(ctx)
		for _, username := range users {
			if username[:8] == "testuser" || username[:10] == "concurrent" {
				store.DeleteUser(ctx, username)
			}
		}
	})
}

// TestMigrationFromMonolithicConfig tests the migration logic
func TestMigrationFromMonolithicConfig(t *testing.T) {
	t.Skip("Integration test - requires running filer. Run manually with -tags=integration")
	
	store := setupTestStore(t)
	ctx := context.Background()

	// Test 1: Create legacy monolithic config
	t.Run("SetupLegacyConfig", func(t *testing.T) {
		config := &iam_pb.S3ApiConfiguration{
			Identities: []*iam_pb.Identity{
				{
					Name: "legacy_user1",
					Credentials: []*iam_pb.Credential{
						{AccessKey: "AKIALEGACY1", SecretKey: "legacy1"},
					},
				},
				{
					Name: "legacy_user2",
					Credentials: []*iam_pb.Credential{
						{AccessKey: "AKIALEGACY2", SecretKey: "legacy2"},
					},
				},
			},
			ServiceAccounts: []*iam_pb.ServiceAccount{
				{
					Id:         "sa_legacy1",
					ParentUser: "legacy_user1",
					Credential: &iam_pb.Credential{AccessKey: "SAKEY1", SecretKey: "sasecret1"},
				},
			},
		}
		
		if err := store.SaveConfiguration(ctx, config); err != nil {
			t.Fatalf("Failed to save legacy config: %v", err)
		}
	})

	// Test 2: Run migration
	t.Run("ExecuteMigration", func(t *testing.T) {
		if err := store.MigrateToIndividualFiles(ctx); err != nil {
			t.Fatalf("Migration failed: %v", err)
		}
	})

	// Test 3: Verify migrated users exist as individual files
	t.Run("VerifyMigratedUsers", func(t *testing.T) {
		user1, err := store.GetUser(ctx, "legacy_user1")
		if err != nil {
			t.Fatalf("Failed to get migrated user1: %v", err)
		}
		if user1.Credentials[0].AccessKey != "AKIALEGACY1" {
			t.Error("Migrated user1 credentials don't match")
		}

		user2, err := store.GetUser(ctx, "legacy_user2")
		if err != nil {
			t.Fatalf("Failed to get migrated user2: %v", err)
		}
		if user2.Credentials[0].AccessKey != "AKIALEGACY2" {
			t.Error("Migrated user2 credentials don't match")
		}
	})

	// Test 4: Verify GetUserByAccessKey finds migrated users
	t.Run("FindMigratedUsersByAccessKey", func(t *testing.T) {
		identity, err := store.GetUserByAccessKey(ctx, "AKIALEGACY1")
		if err != nil {
			t.Fatalf("Failed to find migrated user by access key: %v", err)
		}
		if identity.Name != "legacy_user1" {
			t.Errorf("Expected legacy_user1, got %s", identity.Name)
		}
	})
	
	// Test 4.5: Verify Service Accounts are loaded from individual files
	t.Run("VerifyMigratedServiceAccounts", func(t *testing.T) {
		// LoadConfiguration now reads from individual files
		config, err := store.LoadConfiguration(ctx)
		if err != nil {
			t.Fatalf("Failed to load configuration: %v", err)
		}
		
		found := false
		for _, sa := range config.ServiceAccounts {
			if sa.Id == "sa_legacy1" {
				found = true
				if sa.ParentUser != "legacy_user1" {
					t.Errorf("Expected parent user legacy_user1, got %s", sa.ParentUser)
				}
				break
			}
		}
		if !found {
			t.Error("Migrated service account sa_legacy1 not found in configuration")
		}
	})

	// Test 5: Verify migration is idempotent
	t.Run("MigrationIdempotency", func(t *testing.T) {
		// Run migration again - should skip
		if err := store.MigrateToIndividualFiles(ctx); err != nil {
			t.Fatalf("Second migration run failed: %v", err)
		}

		// Verify users still exist and haven't been duplicated
		users, err := store.ListUsers(ctx)
		if err != nil {
			t.Fatalf("Failed to list users: %v", err)
		}

		legacyCount := 0
		for _, username := range users {
			if username == "legacy_user1" || username == "legacy_user2" {
				legacyCount++
			}
		}

		if legacyCount != 2 {
			t.Errorf("Expected exactly 2 legacy users, found %d", legacyCount)
		}
	})

	// Cleanup
	t.Run("Cleanup", func(t *testing.T) {
		store.DeleteUser(ctx, "legacy_user1")
		store.DeleteUser(ctx, "legacy_user2")
	})
}

// TestPaginationBoundaries tests edge cases in pagination
func TestPaginationBoundaries(t *testing.T) {
	t.Skip("Integration test - requires running filer. Run manually with -tags=integration")
	
	store := setupTestStore(t)
	ctx := context.Background()

	// Test 1: Create users with alphabetically diverse names
	t.Run("SetupDiverseUsers", func(t *testing.T) {
		names := []string{"aaa_user", "zzz_user", "mmm_user", "111_user", "999_user"}
		for i, name := range names {
			identity := &iam_pb.Identity{
				Name: name,
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: fmt.Sprintf("AKIADIV%03d", i),
						SecretKey: fmt.Sprintf("secret%03d", i),
					},
				},
			}
			if err := store.CreateUser(ctx, identity); err != nil {
				t.Fatalf("Failed to create diverse user %s: %v", name, err)
			}
		}
	})

	// Test 2: Verify all users are found regardless of name sorting
	t.Run("FindAllDiverseUsers", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			accessKey := fmt.Sprintf("AKIADIV%03d", i)
			identity, err := store.GetUserByAccessKey(ctx, accessKey)
			if err != nil {
				t.Errorf("Failed to find user with key %s: %v", accessKey, err)
			}
			if identity == nil {
				t.Errorf("User with key %s not found", accessKey)
			}
		}
	})

	// Test 3: List includes all diverse users
	t.Run("ListIncludesAllUsers", func(t *testing.T) {
		users, err := store.ListUsers(ctx)
		if err != nil {
			t.Fatalf("Failed to list users: %v", err)
		}

		expectedNames := []string{"aaa_user", "zzz_user", "mmm_user", "111_user", "999_user"}
		userMap := make(map[string]bool)
		for _, username := range users {
			userMap[username] = true
		}

		for _, expected := range expectedNames {
			if !userMap[expected] {
				t.Errorf("Expected user %s not found in list", expected)
			}
		}
	})

	// Cleanup
	t.Run("Cleanup", func(t *testing.T) {
		names := []string{"aaa_user", "zzz_user", "mmm_user", "111_user", "999_user"}
		for _, name := range names {
			store.DeleteUser(ctx, name)
		}
	})
}

// setupTestStore initializes a test store
// In a real integration test, this would connect to a running filer
func setupTestStore(t *testing.T) *FilerEtcStore {
	// This is a placeholder - in real integration tests, you would:
	// 1. Start a test filer instance or connect to a running one
	// 2. Create a FilerEtcStore with proper filer client
	// 3. Return the configured store
	
	// For now, return nil to remind that this needs proper setup
	t.Fatal("setupTestStore requires implementation with actual filer connection")
	return nil
}
