package weed_server

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

// peerContext authenticates an RPC the filer makes on its own behalf. Resolve
// the key when sending each attempt, so pooled connections do not retain either
// a token or the authority of a previous caller. Sign with the key filesystem
// gRPC verifies, so a dedicated grpc_key works alone and peers without the
// shared-key admin fallback still accept the token.
func (fs *FilerServer) peerContext(ctx context.Context) context.Context {
	var key security.SigningKey
	if fs.filerGuard != nil {
		key, _ = fs.grpcScopeVerificationKeys()
	}
	return security.WithFilerPeerCredentials(ctx, key)
}

// peerSubscriptionContext keeps the shared key for metadata subscriptions,
// whose verifier accepts admin tokens signed only with that key.
func (fs *FilerServer) peerSubscriptionContext(ctx context.Context) context.Context {
	var key security.SigningKey
	if fs.filerGuard != nil {
		key = fs.filerGuard.SigningKey()
	}
	if len(key) == 0 {
		return fs.peerContext(ctx)
	}
	return security.WithFilerPeerCredentials(ctx, key)
}

func (fs *FilerServer) newLockClient() *cluster.LockClient {
	client := cluster.NewLockClient(fs.grpcDialOption, fs.option.Host)
	client.RequestContext = fs.peerContext
	return client
}
