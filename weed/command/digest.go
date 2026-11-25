package command

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// adoptFilerDigest makes this process write the kind of digest its filer
// uses, and exits when it cannot: a gateway writing the other kind would mix
// digest kinds in one bucket.
func adoptFilerDigest(filer string, digest string) {
	if err := util.AdoptFilerDigest(filer, digest); err != nil {
		glog.Fatalf("%v", err)
	}
}

// applyFilerDigest sets this process's digest mode from the filer's -digest
// flag, and exits when that setting is refused. weed server and weed mini call
// it before they start their master and volume server, so every part of the
// process hashes with one digest from the start.
func applyFilerDigest(digest string) {
	if err := util.SetDigest(digest); err != nil {
		glog.Fatalf("filer -digest: %v", err)
	}
}
