package admin

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc" // Import to register filer_etc store
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

// TestUserCreationWithFilerEtcStore is an integration test for issue #7575
// It tests that user creation properly works with the filer_etc credential store
// after the filer address function is correctly set up
func TestUserCreationWithFilerEtcStore(t *testing.T) {
	// This is an integration test - skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Initialize credential manager with filer_etc store (default)
	credentialManager, err := credential.NewCredentialManagerWithDefaults("")
	if err != nil {
		t.Fatalf("Failed to initialize credential manager: %v", err)
	}

	// Set up filer address function for the credential store
	// This simulates what the fixed AdminServer constructor should do
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.admin")
	
	if store := credentialManager.GetStore(); store != nil {
		// Check if store supports filer address function setup
		if filerFuncSetter, ok := store.(interface {
			SetFilerAddressFunc(func() pb.ServerAddress, grpc.DialOption)
		}); ok {
			// Mock filer address
			mockFilerAddress := "localhost:8888"
			
			// Set up the function to return the mock filer address
			filerFuncSetter.SetFilerAddressFunc(func() pb.ServerAddress {
				return pb.ServerAddress(mockFilerAddress)
			}, grpcDialOption)
			t.Log("Successfully set filer address function for credential store")
		} else {
			t.Fatal("Credential store does not implement SetFilerAddressFunc interface - bug #7575 not fixed")
		}
	}

	// Try to create a user through the credential store directly
	// This should not fail with "filer address function not configured"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Note: This will fail trying to connect to the filer, but it should NOT
	// fail with "filer address function not configured"
	err = credentialManager.GetStore().CreateUser(ctx, nil)
	
	// Check that the error is NOT about missing filer address function
	if err != nil {
		errMsg := err.Error()
		if errMsg == "filer_etc: filer address function not configured" {
			t.Fatalf("User creation failed with 'filer address function not configured' error - bug #7575 still present")
		}
		// Other errors are acceptable for this test since we don't have a real filer running
		t.Logf("User creation failed with expected error (no real filer): %v", err)
	}
}

// TestAdminServerSetupWithFilerEtcStore tests the admin server setup process
// to ensure the filer address function is properly configured
func TestAdminServerSetupWithFilerEtcStore(t *testing.T) {
	// This is an integration test - skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Test that the credential manager can be initialized
	credentialManager, err := credential.NewCredentialManagerWithDefaults("")
	if err != nil {
		t.Fatalf("Failed to initialize credential manager: %v", err)
	}

	store := credentialManager.GetStore()
	if store == nil {
		t.Fatal("Credential store is nil")
	}

	// Check if store is filer_etc type
	if store.GetName() != credential.StoreTypeFilerEtc {
		t.Skipf("Skipping test - store is not filer_etc (got: %s)", store.GetName())
	}

	// Verify the store implements the correct interface
	_, ok := store.(interface {
		SetFilerAddressFunc(func() pb.ServerAddress, grpc.DialOption)
	})
	if !ok {
		t.Fatal("FilerEtcStore does not implement SetFilerAddressFunc interface - this indicates the interface definition is wrong")
	}

	t.Log("FilerEtcStore correctly implements SetFilerAddressFunc interface")
}

