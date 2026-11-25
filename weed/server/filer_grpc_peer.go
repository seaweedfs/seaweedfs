package weed_server

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

// peerContext authenticates an RPC the filer makes on its own behalf. Resolve
// the key when sending each attempt, so pooled connections do not retain either
// a token or the authority of a previous caller.
func (fs *FilerServer) peerContext(ctx context.Context) context.Context {
	var key security.SigningKey
	if fs.filerGuard != nil {
		key = fs.filerGuard.SigningKey()
	}
	return security.WithFilerPeerCredentials(ctx, key)
}

func (fs *FilerServer) newLockClient() *cluster.LockClient {
	client := cluster.NewLockClient(fs.grpcDialOption, fs.option.Host)
	client.RequestContext = fs.peerContext
	return client
}
