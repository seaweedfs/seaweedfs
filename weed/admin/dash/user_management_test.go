package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc" // Import to register filer_etc store
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
)

// TestFilerAddressFunctionInterface tests that the filer_etc store
// implements the correct SetFilerAddressFunc interface (issue #7575)
func TestFilerAddressFunctionInterface(t *testing.T) {
	// Create credential manager with filer_etc store
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

	// Check if store implements SetFilerAddressFunc interface
	// This is the critical check for bug #7575
	filerFuncSetter, ok := store.(interface {
		SetFilerAddressFunc(func() pb.ServerAddress, grpc.DialOption)
	})
	if !ok {
		t.Fatal("FilerEtcStore does not implement SetFilerAddressFunc interface - bug #7575")
	}

	// Verify we can call the method without panic
	mockFilerAddress := pb.ServerAddress("localhost:8888")
	filerFuncSetter.SetFilerAddressFunc(func() pb.ServerAddress {
		return mockFilerAddress
	}, grpc.WithInsecure())

	t.Log("FilerEtcStore correctly implements SetFilerAddressFunc interface")
}

// TestGenerateAccessKey tests the access key generation function
func TestGenerateAccessKey(t *testing.T) {
	key1 := generateAccessKey()
	key2 := generateAccessKey()

	// Check length
	if len(key1) != 20 {
		t.Errorf("Expected access key length 20, got %d", len(key1))
	}

	// Check uniqueness
	if key1 == key2 {
		t.Error("Generated access keys should be unique")
	}

	// Check character set
	for _, c := range key1 {
		if !((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			t.Errorf("Access key contains invalid character: %c", c)
		}
	}
}

// TestGenerateSecretKey tests the secret key generation function
func TestGenerateSecretKey(t *testing.T) {
	key1 := generateSecretKey()
	key2 := generateSecretKey()

	// Check length (base64 encoding of 30 bytes = 40 characters)
	if len(key1) != 40 {
		t.Errorf("Expected secret key length 40, got %d", len(key1))
	}

	// Check uniqueness
	if key1 == key2 {
		t.Error("Generated secret keys should be unique")
	}
}

// TestGenerateAccountId tests the account ID generation function
func TestGenerateAccountId(t *testing.T) {
	id1 := generateAccountId()
	id2 := generateAccountId()

	// Check length
	if len(id1) != 12 {
		t.Errorf("Expected account ID length 12, got %d", len(id1))
	}

	// Check that it's a number
	for _, c := range id1 {
		if c < '0' || c > '9' {
			t.Errorf("Account ID contains non-digit character: %c", c)
		}
	}

	// Check uniqueness (they should usually be different)
	if id1 == id2 {
		t.Log("Warning: Generated account IDs are the same (rare but possible)")
	}
}
