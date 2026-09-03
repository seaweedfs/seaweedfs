package weed_server

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// writeOwner returns the filer that serializes writes to key, or "" when this
// filer is the serialization point — because it owns the key, or because there
// is no ring and every filer applies locally.
//
// A ring change hands a key to its new owner before that owner has rebuilt the
// locks the prior owner still holds, so the prior owner keeps the key until the
// cooling-off window closes.
func (fs *FilerServer) writeOwner(key string) pb.ServerAddress {
	if fs.filer.Dlm == nil {
		return ""
	}
	owner := fs.filer.Dlm.LockRing.WriteOwner(key)
	if owner == fs.option.Host {
		return ""
	}
	return owner
}

// forwardToWriteOwner sends the request to key's write owner so a single filer's
// per-path lock arbitrates every writer of that key. handled=false means this
// filer is the owner and the caller should apply the request locally.
//
// An unreachable owner fails the request; it is never re-sent to another filer.
// gRPC reports a response lost in transit as Unavailable, indistinguishable from
// one the owner never saw, so a retry elsewhere could re-apply what the owner
// already committed — and an owner unreachable from here may be partitioned
// rather than down, still serving the key to everyone else. The ring hands the
// key on when the cooling-off window closes, so the outage is bounded.
func (fs *FilerServer) forwardToWriteOwner(ctx context.Context, key string, send func(owner pb.ServerAddress) error) (handled bool, err error) {
	owner := fs.writeOwner(key)
	if owner == "" {
		return false, nil
	}
	if err := send(owner); err != nil {
		glog.V(1).InfofCtx(ctx, "route %s to owner %s: %v", key, owner, err)
		return true, err
	}
	return true, nil
}

// entryRouteKey is the ring key for an entry's writes. It shares the S3
// gateway's namespace so an object's ObjectTransaction and its CreateEntry
// resolve to the same owner, and land on that filer's one per-path lock.
func entryRouteKey(fullpath util.FullPath) string {
	return s3_constants.ObjectWriteRouteKeyPrefix + string(fullpath)
}
