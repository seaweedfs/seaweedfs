package filer_etc

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

func init() {
	credential.Stores = append(credential.Stores, &FilerEtcStore{})
}

// FilerEtcStore implements CredentialStore using SeaweedFS filer for storage
type FilerEtcStore struct {
	filerAddressFunc func() pb.ServerAddress // Function to get current active filer
	grpcDialOption   grpc.DialOption
	mu               sync.RWMutex // Protects filerAddressFunc and grpcDialOption
}

func (store *FilerEtcStore) GetName() credential.CredentialStoreTypeName {
	return credential.StoreTypeFilerEtc
}

func (store *FilerEtcStore) Initialize(configuration util.Configuration, prefix string) error {
	// Handle nil configuration gracefully
	if configuration != nil {
		filerAddr := configuration.GetString(prefix + "filer")
		if filerAddr != "" {
			// Static configuration - use fixed address
			store.mu.Lock()
			store.filerAddressFunc = func() pb.ServerAddress {
				return pb.ServerAddress(filerAddr)
			}
			store.mu.Unlock()
		}
		// TODO: Initialize grpcDialOption based on configuration
	}
	// Note: filerAddressFunc can be set later via SetFilerAddressFunc method
	return nil
}

// SetFilerAddressFunc sets a function that returns the current active filer address
// This enables high availability by using the currently active filer
func (store *FilerEtcStore) SetFilerAddressFunc(getFiler func() pb.ServerAddress, grpcDialOption grpc.DialOption) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.filerAddressFunc = getFiler
	store.grpcDialOption = grpcDialOption
}

// withFilerClient executes a function with a filer client
func (store *FilerEtcStore) withFilerClient(fn func(client filer_pb.SeaweedFilerClient) error) error {
	store.mu.RLock()
	if store.filerAddressFunc == nil {
		store.mu.RUnlock()
		return fmt.Errorf("filer_etc: filer address function not configured")
	}

	filerAddress := store.filerAddressFunc()
	dialOption := store.grpcDialOption
	store.mu.RUnlock()
	
	if filerAddress == "" {
		return fmt.Errorf("filer_etc: filer address is empty")
	}

	// Use the pb.WithGrpcFilerClient helper similar to existing code
	return pb.WithGrpcFilerClient(false, 0, filerAddress, dialOption, fn)
}

func (store *FilerEtcStore) Shutdown() {
	// No cleanup needed for file store
}
