package mount

import (
	"context"
	"fmt"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (wfs *WFS) saveEntry(path util.FullPath, entry *filer_pb.Entry) (code fuse.Status) {

	parentDir, _ := path.DirAndName()

	wfs.mapPbIdFromLocalToFiler(entry)
	defer wfs.mapPbIdFromFilerToLocal(entry)

	request := &filer_pb.UpdateEntryRequest{
		Directory:  parentDir,
		Entry:      entry,
		Signatures: []int32{wfs.signature},
	}

	glog.V(1).Infof("save entry: %v", request)

	var resp *filer_pb.UpdateEntryResponse
	err := retryMetadataFlushIf(func() error {
		var callErr error
		resp, callErr = wfs.streamUpdateEntry(context.Background(), request)
		return callErr
	}, isRetryableFilerError, func(nextAttempt, totalAttempts int, backoff time.Duration, err error) {
		glog.Warningf("saveEntry %s: retrying UpdateEntry (attempt %d/%d) after %v: %v",
			path, nextAttempt, totalAttempts, backoff, err)
	})

	if err != nil {
		// Wrap with %w so grpcErrorToFuseStatus can still unwrap the gRPC status
		// (e.g. codes.Canceled → ETIMEDOUT). Using %v would stringify the error and
		// status.FromError would fall through to the default EIO.
		err = fmt.Errorf("UpdateEntry dir %s: %w", path, err)
		fuseStatus := grpcErrorToFuseStatus(err)
		if fuseStatus == fuse.EIO {
			glog.Errorf("saveEntry failed for %s: %v (returning EIO)", path, err)
		} else {
			glog.V(1).Infof("saveEntry failed for %s: %v (returning %v)", path, err, fuseStatus)
		}
		return fuseStatus
	}

	event := resp.GetMetadataEvent()
	if event == nil {
		event = metadataUpdateEvent(parentDir, entry)
	}
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("saveEntry %s: best-effort metadata apply failed: %v", path, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(parentDir))
	}

	return fuse.OK
}

func (wfs *WFS) mapPbIdFromFilerToLocal(entry *filer_pb.Entry) {
	if entry.Attributes == nil {
		return
	}
	entry.Attributes.Uid, entry.Attributes.Gid = wfs.option.UidGidMapper.FilerToLocal(entry.Attributes.Uid, entry.Attributes.Gid)
}
func (wfs *WFS) mapPbIdFromLocalToFiler(entry *filer_pb.Entry) {
	if entry.Attributes == nil {
		return
	}
	entry.Attributes.Uid, entry.Attributes.Gid = wfs.option.UidGidMapper.LocalToFiler(entry.Attributes.Uid, entry.Attributes.Gid)
}

// sanitizeFuseName replaces any invalid-UTF-8 byte in a name arriving from the
// kernel with '_'. Linux (and macOS) pass raw bytes for filenames; apps like
// GNOME Trash produce partial files whose names contain binary payloads. Proto3
// `string` fields require valid UTF-8, so an unsanitized name causes gRPC to
// fail the whole AssignVolume / CreateEntry / DeleteEntry RPC with
// "grpc: error while marshaling: string field contains invalid UTF-8", which
// surfaces to userspace as EIO. Sanitizing at every FUSE boundary keeps the
// filer RPCs marshalable and prevents a single ill-named file from poisoning
// the shared gRPC channel for every other in-flight request.
//
// '_' is chosen because the sanitized name is also used downstream in HTTP
// URLs (volume-server uploads, filer HTTP API, S3/WebDAV gateways); '?' would
// be interpreted as a query-string delimiter and split the path. The
// replacement is single-byte so length checks downstream remain valid.
func sanitizeFuseName(name string) string {
	if utf8.ValidString(name) {
		return name
	}
	return strings.ToValidUTF8(name, "_")
}

func checkName(name string) (string, fuse.Status) {
	name = sanitizeFuseName(name)
	// The Linux FUSE kernel module enforces NAME_MAX=255 at the VFS layer.
	// Return ENAMETOOLONG early to avoid creating entries that cannot be
	// looked up via normal syscalls (stat, chmod, etc.).
	if len(name) > 255 {
		return name, fuse.Status(syscall.ENAMETOOLONG)
	}
	return name, fuse.OK
}
