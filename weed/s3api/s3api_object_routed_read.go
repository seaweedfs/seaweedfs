package s3api

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// getObjectEntryRoutedByKey resolves an object's entry, preferring the filer that
// owns the object's key on the write ring — where routed writes land — so a read
// observes a just-written object without waiting for cross-filer metadata
// replication. It reuses the same route key as the write path, so reader and writer
// resolve the same owner.
//
// Failover to the gateway's filer set happens only on transport errors; a NotFound
// from the owner is authoritative (no fan-out, no resurrecting a peer's
// not-yet-replicated tombstone). The owner is the home of the routed-write path, so
// an object created via the non-routed lock path on another filer can briefly read
// as absent until it replicates to the owner.
//
// When no owner is resolvable (single filer, lock client unavailable) it behaves
// exactly like getEntry.
func (s3a *S3ApiServer) getObjectEntryRoutedByKey(bucket, object string) (*filer_pb.Entry, error) {
	fullPath := util.NewFullPath(s3a.bucketDir(bucket), object)

	owner := s3a.routableWriteOwner(bucket, object)
	if owner == "" || s3a.filerClient == nil {
		return filer_pb.GetEntry(context.Background(), s3a, fullPath)
	}

	dir, name := fullPath.DirAndName()
	var entry *filer_pb.Entry
	err := s3a.withFilerClientFailover(owner, false, func(client filer_pb.SeaweedFilerClient) error {
		resp, lookupErr := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if lookupErr != nil {
			return lookupErr
		}
		entry = resp.Entry
		return nil
	})
	return entry, err
}
