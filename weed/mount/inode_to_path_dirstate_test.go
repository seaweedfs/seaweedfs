package mount

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The two indexes hold the same states, at the path the inode entry carries.
func checkDirIndexes(t *testing.T, itp *InodeToPath) {
	t.Helper()
	itp.RLock()
	defer itp.RUnlock()
	for path, d := range itp.dirPaths {
		if d.path != path {
			t.Errorf("dirPaths[%s] holds a state for %s", path, d.path)
		}
	}
	for inode, d := range itp.dirStates {
		e := itp.inode2path[inode]
		if e == nil || e.path == "" {
			continue
		}
		if e.path != d.path {
			t.Errorf("inode %d: entry at %s, directory state at %s", inode, e.path, d.path)
		}
		if itp.dirPaths[d.path] != d {
			t.Errorf("inode %d: directory state at %s is missing from dirPaths", inode, d.path)
		}
	}
}

func TestDirStateIndexesStayInStep(t *testing.T) {
	itp := NewInodeToPath(util.FullPath("/"), 0)
	now := time.Now().Unix()

	dirInode := itp.Lookup("/a", now, true, false, 0, true)
	subInode := itp.Lookup("/a/sub", now, true, false, 0, true)
	itp.Lookup("/a/sub/f.txt", now, false, false, 0, true)
	checkDirIndexes(t, itp)

	if itp.dirStateOf("/a/sub") == nil {
		t.Fatal("/a/sub not indexed as a directory")
	}
	if itp.dirStateOf("/a/sub/f.txt") != nil {
		t.Error("a file landed in the directory index")
	}

	itp.MovePath("/a/sub", "/a/moved")
	checkDirIndexes(t, itp)
	if itp.dirStateOf("/a/sub") != nil {
		t.Error("pre-rename directory still indexed")
	}
	if itp.dirStateOf("/a/moved") == nil {
		t.Error("renamed directory not indexed")
	}

	itp.RemovePath("/a/moved")
	if itp.dirStateOf("/a/moved") != nil {
		t.Error("removed directory still indexed")
	}
	checkDirIndexes(t, itp)

	itp.Forget(subInode, 1, nil, nil)
	itp.Forget(dirInode, 1, nil, nil)
	checkDirIndexes(t, itp)
	if itp.dirStateOf("/a") != nil {
		t.Error("forgotten directory still indexed")
	}
}

// A released directory's state keeps its old path. Forgetting it must not take
// the index entry of whatever holds that name now.
func TestForgetDoesNotDropAReusedDirPath(t *testing.T) {
	itp := NewInodeToPath(util.FullPath("/"), 0)
	now := time.Now().Unix()

	oldInode := itp.Lookup("/a", now, true, false, 0, true)
	itp.RemovePath("/a")
	newInode := itp.Lookup("/a", now+1, true, false, 0, true)
	if newInode == oldInode {
		t.Fatalf("recreated directory reused inode %d", newInode)
	}

	itp.Forget(oldInode, 1, nil, nil)

	if itp.dirStateOf("/a") == nil {
		t.Error("live directory lost its index when the released one was forgotten")
	}
	checkDirIndexes(t, itp)
}
