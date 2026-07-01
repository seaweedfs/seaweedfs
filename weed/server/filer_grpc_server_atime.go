package weed_server

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (fs *FilerServer) TouchAccessTime(ctx context.Context, req *filer_pb.TouchAccessTimeRequest) (*filer_pb.TouchAccessTimeResponse, error) {
	now := time.Now()
	candidate := now
	if req.ClientAtimeNs > 0 {
		if t := time.Unix(0, req.ClientAtimeNs); !t.After(now) {
			candidate = t
		}
	}
	persistedNs, updated, err := fs.touchAccessTime(ctx, util.NewFullPath(req.Directory, req.Name), candidate)
	if err != nil {
		return &filer_pb.TouchAccessTimeResponse{}, err
	}
	return &filer_pb.TouchAccessTimeResponse{PersistedAtimeNs: persistedNs, Updated: updated}, nil
}

// touchAccessTime applies the configured atime policy to an entry and persists
// it through the filer's canonical update path. Returns (persistedNs, updated,
// err) where persistedNs is 0 when no write was performed.
//
// Atime updates are intentionally best-effort: this is a read-modify-write
// against the underlying filer store and a concurrent write to the same entry
// can overwrite the bump. That trade-off matches POSIX atime semantics and is
// made safe in practice by the relatime debouncing and by the fact that bumps
// only happen on read paths, where concurrent metadata mutations are rare.
func (fs *FilerServer) touchAccessTime(ctx context.Context, fullpath util.FullPath, candidate time.Time) (int64, bool, error) {
	policy := fs.atimePolicy()
	if !policy.AppliesToPath(string(fullpath)) {
		return 0, false, nil
	}

	entry, err := fs.filer.FindEntry(ctx, fullpath)
	if err == filer_pb.ErrNotFound {
		return 0, false, nil
	}
	if err != nil {
		glog.V(3).InfofCtx(ctx, "touchAccessTime %s: lookup: %v", fullpath, err)
		return 0, false, err
	}

	if !policy.ShouldUpdate(entry.Attr, candidate) {
		return 0, false, nil
	}

	updated := entry.ShallowClone()
	updated.Attr.Atime = candidate
	if err := fs.filer.UpdateEntry(ctx, entry, updated); err != nil {
		glog.V(3).InfofCtx(ctx, "touchAccessTime %s: update: %v", fullpath, err)
		return 0, false, err
	}
	return candidate.UnixNano(), true, nil
}

// disabledAtimePolicy is returned when the server has no policy configured so
// that callers do not allocate on the hot read path.
var disabledAtimePolicy = &filer.AtimePolicy{
	Mode:              filer.AtimeModeOff,
	RelatimeThreshold: filer.DefaultRelatimeThreshold,
}

func (fs *FilerServer) atimePolicy() *filer.AtimePolicy {
	if fs.option == nil || fs.option.AtimePolicy == nil {
		return disabledAtimePolicy
	}
	return fs.option.AtimePolicy
}
