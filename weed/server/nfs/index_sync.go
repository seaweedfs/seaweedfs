package nfs

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ensureInodeIndex writes (or refreshes) the inode->path reverse-lookup index
// row for `path`/`inode` through the filer's gRPC KV interface.
//
// Why this exists: `weed nfs` resolves filehandles via FromHandle, which does a
// KvGet(InodeIndexKey(inode)) and returns ErrStaleHandle when the row is absent
// — the inode (hash(path)+37*crtime) is not reversible to a path, so there is
// no fallback. The filer process only writes the index when started with
// -nfs.inodeIndexPrefixes covering the export (an opt-in flag); with the
// DEFAULT filer config no index rows are ever written, so reads/writes on a
// mounted export fail with "Stale file handle" even though mount succeeds.
//
// Because `weed nfs` runs as a separate process and cannot toggle the filer's
// in-process inodeIndexPrefixes variable remotely, it self-manages its own
// index rows: every time it creates/updates an entry under the export root, it
// KvPuts the index row itself. This is strictly additive — if the filer is also
// configured to write the index, the two writers produce byte-identical records
// (same InodeIndexRecord JSON, same InodeIndexInitialGeneration), so they
// converge rather than conflict.
//
// Record format: this MUST stay byte-for-byte decodable by
// filer.DecodeInodeIndexRecord (the function FromHandle uses). To guarantee
// that, we build the exact same InodeIndexRecord the filer-side storeInodeIndex
// builds — read-modify-write merging the new path into any existing record,
// then Encode() it via the exported method shared with the filer. Do not hand-
// roll the JSON here; if filer.InodeIndexRecord changes, this picks up the new
// shape automatically.
//
// Errors are best-effort: a failure to update the index is logged but never
// returned to the caller. The primary entry mutation has already succeeded, so
// surfacing an index error would make a successful create/update look failed
// to the NFS client (which then cannot recover — the entry exists). A missing
// row degrades to ESTALE on the next FromHandle for that inode, and a later
// mutation of the same entry will rebuild the row.
func ensureInodeIndex(ctx context.Context, client nfsFilerClient, path util.FullPath, inode uint64) {
	if inode == 0 || path == "" {
		return
	}

	record := &filer.InodeIndexRecord{Generation: filer.InodeIndexInitialGeneration}

	// Read-modify-write, mirroring FilerStoreWrapper.storeInodeIndex so hard
	// links and renames keep all known paths on the same inode row.
	resp, err := client.KvGet(ctx, &filer_pb.KvGetRequest{Key: filer.InodeIndexKey(inode)})
	if err != nil {
		glog.V(0).Infof("nfs inode index: read for %s (inode %d) failed: %v", path, inode, err)
		return
	}
	if resp != nil && len(resp.GetValue()) > 0 {
		if existing, decErr := filer.DecodeInodeIndexRecord(resp.GetValue()); decErr == nil && existing != nil {
			record = existing
		}
	}
	// InodeIndexRecord.addPath is unexported, so normalize+dedup by hand: we
	// only ever add our own single path, and Encode() re-normalizes/sorts
	// before marshal, so a duplicate append is harmless and gets deduped.
	record.Paths = append(record.Paths, string(path))
	if record.Generation == 0 {
		record.Generation = filer.InodeIndexInitialGeneration
	}

	value, err := record.Encode()
	if err != nil {
		glog.V(0).Infof("nfs inode index: encode for %s (inode %d) failed: %v", path, inode, err)
		return
	}
	putResp, err := client.KvPut(ctx, &filer_pb.KvPutRequest{
		Key:   filer.InodeIndexKey(inode),
		Value: value,
	})
	if err != nil {
		glog.V(0).Infof("nfs inode index: write for %s (inode %d) failed: %v", path, inode, err)
		return
	}
	if putResp != nil && putResp.GetError() != "" {
		glog.V(0).Infof("nfs inode index: write for %s (inode %d) rejected by filer: %s", path, inode, putResp.GetError())
	}
}

// ensureInodeIndexForEntry is a convenience wrapper that no-ops when the entry
// carries no usable inode, so call sites can pass an entry straight through.
func ensureInodeIndexForEntry(ctx context.Context, client nfsFilerClient, actualPath util.FullPath, entry *filer_pb.Entry) {
	if entry == nil || entry.Attributes == nil {
		return
	}
	ensureInodeIndex(ctx, client, actualPath, entry.Attributes.GetInode())
}
