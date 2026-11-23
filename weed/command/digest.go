package command

import (
	"fmt"

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

// checkSyncDigests refuses to sync filers that use different digests:
// filer.sync copies entry digests and ETags as they are, so one side would
// hold digests of the other kind. A filer that reports none predates the
// setting and cannot be checked.
func checkSyncDigests(filerA, digestA, filerB, digestB string) error {
	switch {
	case digestA == "" || digestB == "":
		glog.Warningf("cannot check that filers %s and %s use the same digest: one of them does not report it", filerA, filerB)
	case digestA != digestB:
		return fmt.Errorf("filer %s uses digest %s and filer %s uses digest %s; filer.sync would copy digests of one kind into the other", filerA, digestA, filerB, digestB)
	}
	return nil
}
