package filer_etc

import (
	"fmt"

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
			store.filerAddressFunc = func() pb.ServerAddress {
				return pb.ServerAddress(filerAddr)
			}
		}
		// TODO: Initialize grpcDialOption based on configuration
	}
	// Note: filerAddressFunc can be set later via SetFilerClient method
	return nil
}

// SetFilerClient sets the filer client details for the file store
// Deprecated: Use SetFilerAddressFunc for better HA support
func (store *FilerEtcStore) SetFilerClient(filerAddress string, grpcDialOption grpc.DialOption) {
	store.filerAddressFunc = func() pb.ServerAddress {
		return pb.ServerAddress(filerAddress)
	}
	store.grpcDialOption = grpcDialOption
}

// SetFilerAddressFunc sets a function that returns the current active filer address
// This enables high availability by using the currently active filer
func (store *FilerEtcStore) SetFilerAddressFunc(getFiler func() pb.ServerAddress, grpcDialOption grpc.DialOption) {
	store.filerAddressFunc = getFiler
	store.grpcDialOption = grpcDialOption
}

// withFilerClient executes a function with a filer client
func (store *FilerEtcStore) withFilerClient(fn func(client filer_pb.SeaweedFilerClient) error) error {
	if store.filerAddressFunc == nil {
		return fmt.Errorf("filer address not configured")
	}

	filerAddress := store.filerAddressFunc()
	if filerAddress == "" {
		return fmt.Errorf("filer address is empty")
	}

	// Use the pb.WithGrpcFilerClient helper similar to existing code
	return pb.WithGrpcFilerClient(false, 0, filerAddress, store.grpcDialOption, fn)
}

func (store *FilerEtcStore) Shutdown() {
	// No cleanup needed for file store
}
