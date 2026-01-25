package grpc

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

func init() {
	credential.Stores = append(credential.Stores, &IamGrpcStore{})
}

// IamGrpcStore implements CredentialStore using SeaweedFS IAM gRPC service
type IamGrpcStore struct {
	filerAddressFunc func() pb.ServerAddress // Function to get current active filer
	grpcDialOption   grpc.DialOption
	mu               sync.RWMutex // Protects filerAddressFunc and grpcDialOption
}

func (store *IamGrpcStore) GetName() credential.CredentialStoreTypeName {
	return credential.StoreTypeGrpc
}

func (store *IamGrpcStore) Initialize(configuration util.Configuration, prefix string) error {
	if configuration != nil {
		filerAddr := configuration.GetString(prefix + "filer")
		if filerAddr != "" {
			store.mu.Lock()
			store.filerAddressFunc = func() pb.ServerAddress {
				return pb.ServerAddress(filerAddr)
			}
			store.mu.Unlock()
		}
	}
	return nil
}

func (store *IamGrpcStore) SetFilerAddressFunc(getFiler func() pb.ServerAddress, grpcDialOption grpc.DialOption) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.filerAddressFunc = getFiler
	store.grpcDialOption = grpcDialOption
}

func (store *IamGrpcStore) withIamClient(fn func(client iam_pb.SeaweedIdentityAccessManagementClient) error) error {
	store.mu.RLock()
	if store.filerAddressFunc == nil {
		store.mu.RUnlock()
		return fmt.Errorf("iam_grpc: filer not yet available")
	}

	filerAddress := store.filerAddressFunc()
	dialOption := store.grpcDialOption
	store.mu.RUnlock()

	if filerAddress == "" {
		return fmt.Errorf("iam_grpc: no filer discovered yet")
	}

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		return fn(client)
	}, filerAddress.ToGrpcAddress(), false, dialOption)
}

func (store *IamGrpcStore) Shutdown() {
}
