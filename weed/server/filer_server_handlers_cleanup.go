package weed_server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// cleanupCollectionHandler handles POST /admin/cleanup-collection
// Body: {"collection": "ok_raw_20260602", "recentDirSeconds": 0}
// Deletes all filer entries recorded under the given collection via the
// collection reverse index, then removes directories that became empty.
// recentDirSeconds is optional: empty directories created within that many
// seconds are kept (default 24h = 86400; 0 disables the guard, for testing).
// Intended to run after the collection's volumes were deleted on the master.
func (fs *FilerServer) cleanupCollectionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req struct {
		Collection       string `json:"collection"`
		RecentDirSeconds *int64 `json:"recentDirSeconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if req.Collection == "" {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("collection is required"))
		return
	}
	recentDirSeconds := int64(-1) // -1 = use default (24h)
	if req.RecentDirSeconds != nil && *req.RecentDirSeconds >= 0 {
		recentDirSeconds = *req.RecentDirSeconds
	}

	glog.V(0).InfofCtx(ctx, "cleanupCollectionHandler: cleaning collection %s (recentDirSeconds=%d)", req.Collection, recentDirSeconds)

	deletedFiles, deletedDirs, err := fs.filer.CleanupCollection(ctx, req.Collection, recentDirSeconds)
	if err != nil {
		glog.ErrorfCtx(ctx, "cleanupCollectionHandler %s: %v", req.Collection, err)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	stats.FilerHandlerCounter.WithLabelValues("cleanupCollection").Inc()

	writeJsonQuiet(w, r, http.StatusOK, map[string]string{
		"collection":   req.Collection,
		"deletedFiles": strconv.Itoa(deletedFiles),
		"deletedDirs":  strconv.Itoa(deletedDirs),
	})
}
