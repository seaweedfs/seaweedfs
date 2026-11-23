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
