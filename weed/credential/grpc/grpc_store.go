package grpc

import (
	"context"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func init() {
	credential.Stores = append(credential.Stores, &IamGrpcStore{})
}

// IamGrpcStore implements CredentialStore by calling the filer's IAM gRPC
// service. The filer requires an admin-signed Bearer token on every RPC
// (see weed/server/filer_server_handlers_iam_grpc.go); SetAdminSigning must
// be called with the same jwt.filer_signing.key value that the filer reads
// from security.toml, or every call will fail with Unauthenticated.
type IamGrpcStore struct {
	filerAddressFunc func() pb.ServerAddress // Function to get current active filer
	grpcDialOption   grpc.DialOption
	// adminSigningKey is the HS256 secret used to mint Bearer tokens that the
	// filer's IAM gRPC service validates. Must match jwt.filer_signing.key on
	// the filer side. Empty means no token is sent (the filer will reject).
	adminSigningKey             security.SigningKey
	adminSigningExpiresAfterSec int
	mu                          sync.RWMutex // Protects filerAddressFunc, grpcDialOption, and adminSigningKey
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

// SetAdminSigning configures the HS256 secret used to mint Bearer tokens for
// the filer's IAM gRPC service. The key must match jwt.filer_signing.key in
// the filer's security.toml. If expiresAfterSec is 0, tokens are minted
// without an exp claim.
func (store *IamGrpcStore) SetAdminSigning(key security.SigningKey, expiresAfterSec int) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.adminSigningKey = key
	store.adminSigningExpiresAfterSec = expiresAfterSec
}

// withIamClient invokes fn against a (possibly cached) gRPC client to the
// filer's IAM service. If an admin signing key is configured the call attaches
// a freshly minted Bearer token via outgoing metadata; otherwise no auth
// header is sent and the filer will return Unauthenticated.
func (store *IamGrpcStore) withIamClient(ctx context.Context, fn func(ctx context.Context, client iam_pb.SeaweedIdentityAccessManagementClient) error) error {
	store.mu.RLock()
	if store.filerAddressFunc == nil {
		store.mu.RUnlock()
		return fmt.Errorf("iam_grpc: filer not yet available")
	}

	filerAddress := store.filerAddressFunc()
	dialOption := store.grpcDialOption
	signingKey := store.adminSigningKey
	expiresAfterSec := store.adminSigningExpiresAfterSec
	store.mu.RUnlock()

	if filerAddress == "" {
		return fmt.Errorf("iam_grpc: no filer discovered yet")
	}

	if len(signingKey) > 0 {
		token := security.GenJwtForFilerAdmin(signingKey, expiresAfterSec)
		if token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+string(token))
		}
	}

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		return fn(ctx, client)
	}, filerAddress.ToGrpcAddress(), false, dialOption)
}

func (store *IamGrpcStore) Shutdown() {
}
