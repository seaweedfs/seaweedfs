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
	filerGrpcAddress string
	grpcDialOption   grpc.DialOption
}

func (store *FilerEtcStore) GetName() credential.CredentialStoreTypeName {
	return credential.StoreTypeFilerEtc
}

func (store *FilerEtcStore) Initialize(configuration util.Configuration, prefix string) error {
	// Handle nil configuration gracefully
	if configuration != nil {
		store.filerGrpcAddress = configuration.GetString(prefix + "filer")
		// TODO: Initialize grpcDialOption based on configuration
	}
	// Note: filerGrpcAddress can be set later via SetFilerClient method
	return nil
}

// SetFilerClient sets the filer client details for the file store
func (store *FilerEtcStore) SetFilerClient(filerAddress string, grpcDialOption grpc.DialOption) {
	store.filerGrpcAddress = filerAddress
	store.grpcDialOption = grpcDialOption
}

// withFilerClient executes a function with a filer client
func (store *FilerEtcStore) withFilerClient(fn func(client filer_pb.SeaweedFilerClient) error) error {
	if store.filerGrpcAddress == "" {
		return fmt.Errorf("filer address not configured")
	}

	// Use the pb.WithGrpcFilerClient helper similar to existing code
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(store.filerGrpcAddress), store.grpcDialOption, fn)
}

func (store *FilerEtcStore) Shutdown() {
	// No cleanup needed for file store
}
