package mount

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The filer sends one event per moved entry, so children follow their parent.
func TestRemoteRenameMovesInodePaths(t *testing.T) {
	dir := util.FullPath("/images")
	wfs := newPagingWFS(t, dir, []string{"f.jpg"}, 0)

	now := time.Now().Unix()
	subInode := wfs.inodeToPath.Lookup(dir.Child("sub"), now, true, false, 0, true)
	childInode := wfs.inodeToPath.Lookup(dir.Child("sub").Child("f.jpg"), now, false, false, 0, true)

	wfs.onEntryInvalidation(meta_cache.EntryInvalidation{
		Path:         dir.Child("sub"),
		RenamedTo:    dir.Child("moved"),
		WasDirectory: true,
	})
	wfs.onEntryInvalidation(meta_cache.EntryInvalidation{
		Path:      dir.Child("sub").Child("f.jpg"),
		RenamedTo: dir.Child("moved").Child("f.jpg"),
	})

	if got, _ := wfs.inodeToPath.GetPath(subInode); got != dir.Child("moved") {
		t.Errorf("renamed directory resolves to %s, want %s", got, dir.Child("moved"))
	}
	if got, _ := wfs.inodeToPath.GetPath(childInode); got != dir.Child("moved").Child("f.jpg") {
		t.Errorf("child resolves to %s, want %s", got, dir.Child("moved").Child("f.jpg"))
	}
	if wfs.inodeToPath.HasPath(dir.Child("sub")) {
		t.Error("pre-rename directory path still in the table")
	}
}
